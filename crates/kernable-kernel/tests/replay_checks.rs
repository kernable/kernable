//! Commands replayed on a developer's own masked copy: `kernable replay` sends a source's commands
//! after a masked copy's position to a kernel on that copy (`--replay on`) where a new deployment's
//! workers run, and reports which outcomes differ: payload fields, new event types, new rejections.
//! Equal code writes equal events, as each command runs at its original time and with its own ID.

mod common;

use std::{path::Path, process::Command as Process, sync::Arc};

use common::*;
use kernable_core::{
    proto::{
        Command, CommandResult, Event, NewEvent, Rejected, SubmitRequest, Wait,
        command_service_client::CommandServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{
    Kernel,
    config::VaultConfig,
    replay_check::{self, Options, Since, Threshold},
};
use serde_json::{Value, json};
use tonic::Code;

fn kernable(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Process::new(env!("CARGO_BIN_EXE_kernable"))
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

async fn run(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let dir = dir.to_owned();
    let args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        kernable(&dir, &args)
    })
    .await
    .unwrap()
}

/// The domain as deployment `v1` decides: an order is placed once, items go to placed orders.
/// Every event holds the command's time and a derived ID (`ctx.now()`, `ctx.id()`).
fn v1(cmd: &Command, received_at: i64, history: &[Event]) -> Result<Vec<NewEvent>, Rejected> {
    let payload: Value = serde_json::from_slice(&cmd.payload).unwrap_or(Value::Null);
    let reject = |code: &str| Rejected {
        code: code.into(),
        message: format!("{} refused", cmd.command_type),
        ..Default::default()
    };
    let event = |event_type: &str, data: Value| NewEvent {
        event_type: event_type.into(),
        payload: serde_json::to_vec(&data).unwrap(),
        schema: 1,
        ..Default::default()
    };
    let at = json!({ "at": received_at, "ref": signing::derived_id(&cmd.command_id, 0) });
    match cmd.command_type.as_str() {
        "PlaceOrder" if history.is_empty() => Ok(vec![event("OrderPlaced", at)]),
        "AddItem" if !history.is_empty() => {
            let mut data = at;
            data["qty"] = payload["qty"].clone();
            Ok(vec![event("ItemAdded", data)])
        }
        _ => Err(reject("invalid_state")),
    }
}

/// A domain's decision as a plain function.
type Decision = fn(&Command, i64, &[Event]) -> Result<Vec<NewEvent>, Rejected>;

fn decide(f: Decision) -> Decide {
    Arc::new(f)
}

fn worker(deployment: &str, decide: Decide) -> TestWorker {
    TestWorker {
        service: "shop".into(),
        deployment: deployment.into(),
        schemas: vec![
            ("orders.OrderPlaced".into(), 1),
            ("orders.ItemAdded".into(), 1),
        ],
        decide: Some(decide),
        ..TestWorker::honest()
    }
}

fn shop(deployment: &str) -> kernable_core::proto::Membership {
    kernable_core::proto::Membership {
        deployment: deployment.into(),
        ..membership("shop", &["orders.Order"], &[], &[])
    }
}

fn order(id: &str, command_type: &str, order: &str, payload: Value) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.payload = payload.to_string().into_bytes();
    signed(cmd, &key(1))
}

async fn submitted(addr: &str, cmd: Command) -> CommandResult {
    submit_with(addr, cmd, Wait::Committed).await.unwrap()
}

/// A command that keeps the customer's email in the vault.
async fn submit_personal(addr: &str, id: &str, order_id: &str) -> CommandResult {
    let email = vault_value("k-1", "ann@example.com");
    let mut cmd = command(id, "PlaceOrder", order_id);
    cmd.payload = json!({ "email": { "$vault": email.r#ref } })
        .to_string()
        .into_bytes();
    cmd.vault_refs = vec![email.r#ref.clone()];
    let cmd = signed(cmd, &key(1));
    let request = SubmitRequest {
        command: Some(cmd),
        wait: Wait::Committed.into(),
        protocol: kernable_core::PROTOCOL,
        vault: vec![email],
        ..Default::default()
    };
    let channel = transport::connect(addr).await.unwrap();
    CommandServiceClient::new(channel)
        .submit(with_grant(request, &key(1)))
        .await
        .unwrap()
        .into_inner()
}

/// A masked copy's stores, in `dir/name`.
struct Copy {
    store: String,
    vault: VaultConfig,
}

fn backend() -> &'static str {
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => "file",
        _ => "sqlite",
    }
}

/// The source of the check: a kernel with a vault where `v1` serves, two orders placed before
/// a masked copy is made, then seven commands after it: an item added, a rejected item, an
/// order placed, an order with a personal value and an item for it, an item scheduled for that
/// order, an item of 5 for the second order.
struct Scenario {
    source: Env,
    copy: Copy,
    /// The copy's position.
    position: u64,
}

async fn scenario(name: &str) -> Scenario {
    let source = start_custom(name, with_vault).await;
    apply(&source.addr, shop("v1")).await.unwrap();
    start_worker(source.addr.clone(), worker("v1", decide(v1))).await;
    for (id, o) in [("c1", "o1"), ("c2", "o2")] {
        let placed = submitted(&source.addr, order(id, "PlaceOrder", o, json!({}))).await;
        assert_eq!(committed_versions(&placed), [1]);
    }

    // The masked copy, made while the source runs.
    std::fs::write(source.dir.join("kernel.pub"), id(&key(3))).unwrap();
    let dir = source.dir.join("copy");
    std::fs::create_dir_all(&dir).unwrap();
    let file = if backend() == "file" { "k.log" } else { "k.db" };
    let copy = Copy {
        store: format!("{}:{}", backend(), dir.join(file).display()),
        vault: VaultConfig {
            store: format!("sqlite:{}", dir.join("vault.db").display()),
            key: dir.join("vault.key"),
        },
    };
    let (code, out, err) = run(
        &source.dir,
        &[
            "copy",
            &source.store,
            "--mask-vault",
            "--store",
            &copy.store,
            "--vault-store",
            &copy.vault.store,
            "--vault-key",
            &copy.vault.key.display().to_string(),
            "--data-store",
            &format!("file:{}", dir.join("data").display()),
            "--kernel-pub",
            "kernel.pub",
        ],
    )
    .await;
    assert_eq!(code, 0, "{out}{err}");
    let position = kernable_store::open_read_only(&copy.store)
        .unwrap()
        .copy_mark()
        .unwrap()
        .unwrap()
        .position;
    assert_eq!(position, 2);

    // What happens after the copy.
    let a = &source.addr;
    submitted(a, order("c3", "AddItem", "o1", json!({ "qty": 2 }))).await;
    let rejected = submitted(a, order("c4", "AddItem", "o9", json!({ "qty": 1 }))).await;
    assert!(matches!(
        rejected.outcome,
        Some(kernable_core::proto::command_result::Outcome::Rejected(_))
    ));
    submitted(a, order("c5", "PlaceOrder", "o3", json!({}))).await;
    submit_personal(a, "c6", "o4").await;
    submitted(a, order("c7", "AddItem", "o4", json!({ "qty": 1 }))).await;
    let mut later = command("c8", "AddItem", "o3");
    later.payload = json!({ "qty": 1 }).to_string().into_bytes();
    later.deliver_at = now_ms() + 200;
    submitted(a, signed(later, &key(1))).await;
    // Opened again each time: a `file:` log is read as far as it was written then.
    until("the scheduled command ran", || {
        let store = kernable_store::open_read_only(&source.store).unwrap();
        store.result("c8").unwrap().is_some()
    })
    .await;
    submitted(a, order("c9", "AddItem", "o2", json!({ "qty": 5 }))).await;
    Scenario {
        source,
        copy,
        position,
    }
}

impl Scenario {
    /// A kernel of the check on the copy, where `deployment`'s workers decide with `decide`.
    async fn check_kernel(&self, name: &str, deployment: &str, decide: Decide) -> Env {
        let (store, vault) = (self.copy.store.clone(), self.copy.vault.clone());
        let env = start_custom(name, move |c, _| {
            c.store = store;
            c.vault = Some(vault);
            c.replay = true;
        })
        .await;
        let mut w = worker(deployment, decide);
        w.schemas.push(("orders.OrderFlagged".into(), 1));
        start_worker(env.addr.clone(), w).await;
        // The copy's membership names the source's deployment: the check switches at once.
        apply_switching(&env.addr, shop(deployment), true, false, false)
            .await
            .unwrap();
        env
    }

    async fn check(&self, check: &Env, deployment: &str) -> Value {
        let store = kernable_store::open_read_only(&self.source.store).unwrap();
        let options = Options {
            source: "source".into(),
            target: "check".into(),
            since: Since::Position(self.position),
            limit: None,
            deployment: deployment.into(),
            max_differences: Threshold::Count(0),
        };
        let channel = transport::connect(&check.addr).await.unwrap();
        replay_check::run(store.as_ref(), channel, &options)
            .await
            .unwrap()
    }

    /// `kernable replay` from the source's store to `check`, with `extra` flags.
    async fn cli(&self, check: &Env, deployment: &str, extra: &[&str]) -> (i32, String, String) {
        let position = self.position.to_string();
        let mut args = vec![
            "replay",
            "--from",
            &self.source.store,
            "--since",
            &position,
            "--to",
            &check.addr,
            "--deployment",
            deployment,
        ];
        args.extend(extra);
        run(&self.source.dir, &args).await
    }
}

fn ids(list: &Value) -> Vec<String> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|e| e["commandId"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_code_writes_the_same_events() {
    let s = scenario("replay-same").await;
    let check = s.check_kernel("replay-same-check", "v2", decide(v1)).await;
    let report = s.check(&check, "v2").await;
    assert_eq!(report["commands"], 7, "{report:#}");
    assert_eq!(report["replayed"], 4, "{report:#}");
    assert_eq!(report["identical"], 4, "{report:#}");
    assert_eq!(report["different"], 0, "{report:#}");
    assert_eq!(report["errors"], 0, "{report:#}");
    assert_eq!(report["passed"], true);
    assert_eq!(report["newTypes"], json!([]));
    assert_eq!(report["newRejections"], json!([]));
    assert_eq!(ids(&report["scheduledCommands"]), ["c8"]);
    assert_eq!(
        report["scheduledCommands"][0]["original"]["committed"][0]["type"],
        "orders.ItemAdded"
    );

    // Equal events: the original time and the derived IDs, written by v2.
    let source = kernable_store::open_read_only(&s.source.store).unwrap();
    let copied = kernable_store::open_read_only(&s.copy.store).unwrap();
    for id in ["c3", "c5", "c9"] {
        let events = |store: &dyn kernable_store::Store| match store.result(id).unwrap() {
            Some(CommandResult {
                outcome: Some(kernable_core::proto::command_result::Outcome::Committed(c)),
                ..
            }) => c.events,
            other => panic!("{id}: {other:?}"),
        };
        let (original, replayed) = (events(source.as_ref()), events(copied.as_ref()));
        assert_eq!(original[0].payload, replayed[0].payload, "{id}");
        assert_eq!(replayed[0].deployment, "v2");
        assert_eq!(
            copied.command(id).unwrap().unwrap().received_at,
            source.command(id).unwrap().unwrap().received_at
        );
    }

    // The command line: the text, the JSON, exit 0.
    let (code, out, err) = s.cli(&check, "v2", &[]).await;
    assert_eq!(code, 0, "{out}{err}");
    assert!(
        out.contains("7 commands: 4 replayed (4 identical, 0 different, 0 errors), 2 skipped, not sent: 1 scheduled command and 0 process steps"),
        "{out}"
    );
    assert!(
        out.lines().any(|l| l == "OK: 0 differences (at most 0)"),
        "{out}"
    );
    let (code, out, _) = s.cli(&check, "v2", &["--json"]).await;
    assert_eq!(code, 0);
    let json: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(json["identical"], 4);

    // The same from a running kernel (a key granted export) and from an export file.
    use prost::Message;
    let ops = key(7);
    let ops_grant = grant(&ops, &[("export", &["*"])]);
    s.source.kernel.grant(ops_grant.clone()).unwrap();
    std::fs::write(s.source.dir.join("ops.key"), hex(ops.as_bytes())).unwrap();
    std::fs::write(
        s.source.dir.join("ops.grant"),
        hex(&ops_grant.encode_to_vec()),
    )
    .unwrap();
    let position = s.position.to_string();
    for from in [
        vec![s.source.addr.as_str(), "--key", "ops.key"],
        vec!["source.kx"],
    ] {
        if from == ["source.kx"] {
            let (code, out, err) = run(
                &s.source.dir,
                &["export", &s.source.addr, "ops.key", "source.kx"],
            )
            .await;
            assert_eq!(code, 0, "{out}{err}");
        }
        let mut args = vec!["replay", "--from"];
        args.extend(&from);
        args.extend([
            "--since",
            &position,
            "--to",
            &check.addr,
            "--deployment",
            "v2",
            "--json",
        ]);
        let (code, out, err) = run(&s.source.dir, &args).await;
        assert_eq!(code, 0, "{from:?}: {out}{err}");
        let json: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(
            (json["identical"].as_u64(), json["skipped"].as_u64()),
            (Some(4), Some(2))
        );
    }

    // The kernel of the check takes only what the check sends: no plain submit, so a reactor
    // of the new deployment acts on nothing; no scheduled command fires.
    let plain = submit_with(
        &check.addr,
        order("x1", "AddItem", "o1", json!({})),
        Wait::Committed,
    )
    .await
    .unwrap_err();
    assert_eq!(plain.code(), Code::FailedPrecondition);
    assert!(
        plain.message().starts_with("replay_only"),
        "{}",
        plain.message()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn changed_payloads_are_reported_by_field_and_fail_the_check() {
    fn doubled(cmd: &Command, at: i64, history: &[Event]) -> Result<Vec<NewEvent>, Rejected> {
        let mut events = v1(cmd, at, history)?;
        for e in events.iter_mut().filter(|e| e.event_type == "ItemAdded") {
            let mut data: Value = serde_json::from_slice(&e.payload).unwrap();
            data["qty"] = json!(data["qty"].as_i64().unwrap() * 2);
            data["unit"] = json!("piece");
            e.payload = serde_json::to_vec(&data).unwrap();
        }
        Ok(events)
    }
    let s = scenario("replay-changed").await;
    let check = s
        .check_kernel("replay-changed-check", "v2", decide(doubled))
        .await;
    let report = s.check(&check, "v2").await;
    assert_eq!(report["different"], 2, "{report:#}");
    assert_eq!(report["identical"], 2, "{report:#}");
    assert_eq!(report["passed"], false);
    assert_eq!(ids(&report["differences"]), ["c3", "c9"]);
    assert_eq!(
        report["differences"][0]["changes"],
        json!(["payload of orders.ItemAdded: changed qty; added unit"])
    );
    assert_eq!(report["differences"][0]["masked"], false);

    // Exit 3 above the threshold, 0 within it; the text names fields, never values.
    let (code, out, err) = s.cli(&check, "v2", &[]).await;
    assert_eq!(code, 3, "{out}{err}");
    assert!(
        out.contains("c3 orders.AddItem on orders.Order: payload of orders.ItemAdded: changed qty; added unit"),
        "{out}"
    );
    assert!(
        out.contains("FAILED: 2 differences, more than --max-differences 0"),
        "{out}"
    );
    assert!(!out.contains("piece"), "{out}");
    let (code, _, _) = s.cli(&check, "v2", &["--max-differences", "2"]).await;
    assert_eq!(code, 0);
    let (code, _, _) = s.cli(&check, "v2", &["--max-differences", "50%"]).await;
    assert_eq!(code, 0);
    let (code, _, _) = s.cli(&check, "v2", &["--max-differences", "49%"]).await;
    assert_eq!(code, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_event_type_is_listed() {
    fn flagged(cmd: &Command, at: i64, history: &[Event]) -> Result<Vec<NewEvent>, Rejected> {
        let mut events = v1(cmd, at, history)?;
        if cmd.command_type == "PlaceOrder" {
            events.push(NewEvent {
                event_type: "OrderFlagged".into(),
                payload: b"{}".to_vec(),
                schema: 1,
                ..Default::default()
            });
        }
        Ok(events)
    }
    let s = scenario("replay-type").await;
    let check = s
        .check_kernel("replay-type-check", "v2", decide(flagged))
        .await;
    let report = s.check(&check, "v2").await;
    assert_eq!(
        report["newTypes"],
        json!([{ "type": "orders.OrderFlagged", "schema": 1, "newType": true, "newSchema": false, "commands": 1 }]),
        "{report:#}"
    );
    assert_eq!(ids(&report["differences"]), ["c5"]);
    assert_eq!(
        report["differences"][0]["changes"],
        json!(["events: orders.OrderPlaced/1 -> orders.OrderPlaced/1, orders.OrderFlagged/1"])
    );
    let (code, out, _) = s.cli(&check, "v2", &[]).await;
    assert_eq!(code, 3);
    assert!(
        out.contains("  orders.OrderFlagged schema 1  NEW TYPE  (1 command)"),
        "{out}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_rejection_is_listed() {
    fn limited(cmd: &Command, at: i64, history: &[Event]) -> Result<Vec<NewEvent>, Rejected> {
        let payload: Value = serde_json::from_slice(&cmd.payload).unwrap_or(Value::Null);
        if payload["qty"].as_i64().is_some_and(|q| q > 3) {
            return Err(Rejected {
                code: "too_many".into(),
                message: "at most 3".into(),
                ..Default::default()
            });
        }
        v1(cmd, at, history)
    }
    let s = scenario("replay-rejection").await;
    let check = s
        .check_kernel("replay-rejection-check", "v2", decide(limited))
        .await;
    let report = s.check(&check, "v2").await;
    assert_eq!(
        report["newRejections"],
        json!([{ "code": "too_many", "commands": 1 }]),
        "{report:#}"
    );
    assert_eq!(ids(&report["differences"]), ["c9"]);
    assert_eq!(
        report["differences"][0]["changes"],
        json!(["outcome: committed orders.ItemAdded/1 -> rejected too_many"])
    );
    assert_eq!(
        report["differences"][0]["replayed"],
        json!({ "rejected": "too_many" })
    );
    let (code, out, _) = s.cli(&check, "v2", &[]).await;
    assert_eq!(code, 3);
    assert!(
        out.contains("new rejections:\n  too_many  (1 command)"),
        "{out}"
    );
    assert!(
        !out.contains("at most 3"),
        "rejection messages stay out: {out}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_with_personal_values_are_not_sent_nor_what_follows_on_their_aggregate() {
    let s = scenario("replay-personal").await;
    let check = s
        .check_kernel("replay-personal-check", "v2", decide(v1))
        .await;
    let report = s.check(&check, "v2").await;
    assert_eq!(report["skipped"], 2, "{report:#}");
    assert_eq!(
        report["skippedCommands"],
        json!([
            { "commandId": "c6", "command": "orders.PlaceOrder", "aggregate": "orders.Order", "reason": "personal" },
            { "commandId": "c7", "command": "orders.AddItem", "aggregate": "orders.Order", "reason": "afterSkipped" },
        ])
    );
    // Never sent: the copy does not know them.
    let copied = kernable_store::open_read_only(&s.copy.store).unwrap();
    assert!(copied.command("c6").unwrap().is_none());
    assert!(copied.command("c7").unwrap().is_none());
    let (_, out, _) = s.cli(&check, "v2", &[]).await;
    assert!(
        out.contains("c6 orders.PlaceOrder on orders.Order: carries personal values, which the copy does not hold"),
        "{out}"
    );
    assert!(!out.contains("ann@example.com"));

    // --limit takes the oldest commands after the point.
    let (code, out, _) = s.cli(&check, "v2", &["--limit", "2", "--json"]).await;
    assert_eq!(code, 0);
    let json: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(
        (json["commands"].as_u64(), json["replayed"].as_u64()),
        (Some(2), Some(2))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_check_runs_only_on_a_kernel_of_a_check_with_the_deployment_named() {
    let s = scenario("replay-refusals").await;

    // Production refuses what the check sends: nothing runs there.
    let (code, out, err) = s.cli(&s.source, "v1", &[]).await;
    assert_eq!(code, 1, "{out}{err}");
    assert!(err.contains("--replay on"), "{err}");

    // --replay on starts only on a masked copy.
    let dir = &s.source.dir;
    let fresh = format!("{}:{}", backend(), dir.join("fresh.db").display());
    let kernel_key = dir.join("kernel.key").display().to_string();
    let root = id(&root());
    let config = kernable_kernel::config::Config::from_args(
        &[
            "--root",
            &root,
            "--store",
            &fresh,
            "--kernel-key",
            &kernel_key,
            "--replay",
            "on",
        ],
        |_| None,
    )
    .unwrap();
    assert!(config.replay);
    let refused = Kernel::open(config).err().unwrap().to_string();
    assert!(refused.contains("runs only on a masked copy"), "{refused}");

    // Events written by another deployment than the one named stop the check.
    let check = s
        .check_kernel("replay-refusals-check", "v2", decide(v1))
        .await;
    let (code, _, err) = s.cli(&check, "v3", &[]).await;
    assert_eq!(code, 1);
    assert!(err.contains("not v3"), "{err}");
}
