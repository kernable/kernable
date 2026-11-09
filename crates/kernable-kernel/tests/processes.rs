//! Business processes, stage 1 (design/processes.md): models registered signed, checked and
//! versioned; removed by a membership that no longer lists them or by `Remove`; active again
//! when registered again.

mod common;

use std::path::Path;

use common::*;
use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Grant, ProcessRecord, ProcessRegistered, ProcessRegistration, ProcessRemoval,
        process_service_client::ProcessServiceClient,
    },
    signing, transport,
};
use serde_json::{Value, json};
use tonic::Code;

/// The example of `proto/test-vectors/process-model.json`.
fn example() -> Value {
    let file: Value = serde_json::from_str(include_str!(
        "../../../proto/test-vectors/process-model.json"
    ))
    .unwrap();
    file["model"].clone()
}

/// The deploy key of the shop: what the example needs, and no more.
fn deployer() -> SigningKey {
    key(21)
}

fn deployer_grant() -> Grant {
    grant(
        &deployer(),
        &[
            ("process", &["shop.*"]),
            ("submit", &["shop.*"]),
            ("query", &["shop.*"]),
            ("listen", &["shop.*", "payments.PaymentReceived"]),
        ],
    )
}

/// A kernel whose membership of `shop` lists the aggregate `shop.order` (so its commands are
/// known), and that holds the deployer's grant.
async fn shop(name: &str) -> Env {
    let env = start(name).await;
    prepare(&env).await;
    env
}

async fn prepare(env: &Env) {
    env.kernel.grant(deployer_grant()).unwrap();
    let mut m = membership("shop", &["shop.order"], &[], &[]);
    m.processes = vec!["shop.fulfilment".into()];
    apply(&env.addr, m).await.unwrap();
}

async fn register_as(
    addr: &str,
    model: &[u8],
    signer: &SigningKey,
    grant: &Grant,
    issued_at: i64,
) -> Result<ProcessRegistered, tonic::Status> {
    let request = ProcessRegistration {
        model: model.to_vec(),
        issued_at,
        sig: Some(signing::sign(
            signer,
            &signing::process_message(model, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(addr).await.unwrap())
        .register(presenting(request, grant))
        .await
        .map(tonic::Response::into_inner)
}

async fn register(env: &Env, model: &Value) -> Result<ProcessRegistered, tonic::Status> {
    let bytes = serde_json::to_vec(model).unwrap();
    register_as(
        &env.addr,
        &bytes,
        &deployer(),
        &deployer_grant(),
        issued_now(),
    )
    .await
}

async fn remove_as(
    addr: &str,
    name: &str,
    signer: &SigningKey,
    grant: &Grant,
) -> Result<bool, tonic::Status> {
    let issued_at = issued_now();
    let request = ProcessRemoval {
        name: name.into(),
        issued_at,
        sig: Some(signing::sign(
            signer,
            &signing::process_remove_message(name, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    ProcessServiceClient::new(transport::connect(addr).await.unwrap())
        .remove(presenting(request, grant))
        .await
        .map(|r| r.into_inner().removed)
}

/// The versions and the state (removed) of `name` as the store holds them.
fn stored(store: &str, name: &str) -> (Vec<ProcessRecord>, bool) {
    let mut versions = Vec::new();
    let mut removed = false;
    for r in kernable_store::open_read_only(store)
        .unwrap()
        .processes()
        .unwrap()
        .into_iter()
        .filter(|r| r.name == name)
    {
        if r.version == 0 {
            removed = r.removed;
        } else {
            versions.push(r);
        }
    }
    (versions, removed)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_is_registered_once_per_version() {
    let env = shop("proc-versions").await;
    let first = register(&env, &example()).await.unwrap();
    assert_eq!(
        (first.name.as_str(), first.version, first.new),
        ("shop.fulfilment", 1, true)
    );

    // The same model, written differently: nothing new.
    let pretty = serde_json::to_vec_pretty(&example()).unwrap();
    let again = register_as(
        &env.addr,
        &pretty,
        &deployer(),
        &deployer_grant(),
        issued_now(),
    )
    .await
    .unwrap();
    assert_eq!((again.version, again.new), (1, false));

    // A changed one is the next version; the first stays.
    let mut changed = example();
    changed["steps"][4]["timeoutMs"] = json!(86_400_000);
    let second = register(&env, &changed).await.unwrap();
    assert_eq!((second.version, second.new), (2, true));
    let (versions, removed) = stored(&env.store, "shop.fulfilment");
    assert!(!removed);
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].model, serde_json::to_vec(&example()).unwrap());
    assert_eq!(
        versions[1].canonical,
        kernable_canonical(&changed),
        "kept with its canonical form"
    );
    let sig = versions[1].sig.clone().unwrap();
    assert_eq!(sig.key_id, id(&deployer()));
    assert!(
        signing::verify(
            &sig,
            &signing::process_message(&versions[1].model, versions[1].issued_at)
        )
        .is_ok(),
        "stored with the signature it came with"
    );
    // Back to the first: a third version, equal to the first.
    let third = register(&env, &example()).await.unwrap();
    assert_eq!((third.version, third.new), (3, true));
}

/// The canonical form, as the kernel computes it: keys sorted, no whitespace.
fn kernable_canonical(v: &Value) -> Vec<u8> {
    fn sorted(v: &Value) -> Value {
        match v {
            Value::Object(o) => {
                let mut keys: Vec<&String> = o.keys().collect();
                keys.sort();
                Value::Object(
                    keys.into_iter()
                        .map(|k| (k.clone(), sorted(&o[k])))
                        .collect(),
                )
            }
            Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
            v => v.clone(),
        }
    }
    serde_json::to_vec(&sorted(v)).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_model_is_refused_with_the_step_and_why() {
    let env = shop("proc-invalid").await;
    let refused = |model: Value| {
        let env = &env;
        async move {
            let status = register(env, &model).await.unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
            status.message().to_owned()
        }
    };
    let with = |f: &dyn Fn(&mut Value)| {
        let mut m = example();
        f(&mut m);
        m
    };
    for (model, want) in [
        (
            with(&|m| m["steps"][0]["next"] = json!("nowhere")),
            "process shop.fulfilment, step confirm: goes on to nowhere, which is no step",
        ),
        (
            with(&|m| m["steps"][1]["when"][0]["if"] = json!("case.total >> 1")),
            "process shop.fulfilment, step large: the condition \"case.total >> 1\"",
        ),
        (
            with(&|m| m["steps"][4]["timeoutMs"] = json!(-1)),
            "process shop.fulfilment, step paid: timeoutMs is not a positive number",
        ),
        (
            with(&|m| m["steps"][0]["aggregate"] = json!("orderId")),
            "process shop.fulfilment, step confirm: orderId is no reference",
        ),
        (
            with(&|m| {
                m["steps"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"name": "orphan", "kind": "end"}));
            }),
            "process shop.fulfilment, step orphan: no path from the start reaches it",
        ),
        (
            with(&|m| m["start"]["on"] = json!("OrderSubmitted")),
            "process shop.fulfilment, start: on OrderSubmitted is not a qualified type",
        ),
        (
            with(&|m| m["steps"][0]["command"] = json!("shop.invoice.Send")),
            "process shop.fulfilment, step confirm: no worker registered shop.Send for \
             shop.invoice, and no membership lists shop.invoice",
        ),
    ] {
        let message = refused(model).await;
        assert!(message.contains(want), "{message:?} lacks {want:?}");
    }
    let not_json = register_as(
        &env.addr,
        b"{",
        &deployer(),
        &deployer_grant(),
        issued_now(),
    )
    .await
    .unwrap_err();
    assert_eq!(not_json.code(), Code::InvalidArgument);
    assert!(stored(&env.store, "shop.fulfilment").0.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_registration_needs_its_signature_and_every_grant_the_model_uses() {
    let env = shop("proc-grants").await;
    let model = serde_json::to_vec(&example()).unwrap();
    let narrow = |permissions: &[(&str, &[&str])]| {
        let g = grant(&key(22), permissions);
        env.kernel.grant(g.clone()).unwrap();
        g
    };
    let all: [(&str, &[&str]); 4] = [
        ("process", &["shop.fulfilment"]),
        ("submit", &["shop.*"]),
        ("query", &["shop.NeedsApproval"]),
        ("listen", &["shop.*", "payments.*"]),
    ];
    for (missing, want) in [
        ("process", "it needs a grant process for shop.fulfilment"),
        ("submit", "it needs a grant submit for shop.Confirm"),
        ("query", "it needs a grant query for shop.NeedsApproval"),
        ("listen", "it needs a grant listen for shop.OrderSubmitted"),
    ] {
        let without: Vec<(&str, &[&str])> =
            all.iter().copied().filter(|(a, _)| *a != missing).collect();
        let g = narrow(&without);
        let denied = register_as(&env.addr, &model, &key(22), &g, issued_now())
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied, "{denied:?}");
        assert!(denied.message().contains(want), "{denied:?}");
    }
    // Every event it waits for, too.
    let g = narrow(&[
        ("process", &["shop.*"]),
        ("submit", &["shop.*"]),
        ("query", &["shop.*"]),
        ("listen", &["shop.*"]),
    ]);
    let denied = register_as(&env.addr, &model, &key(22), &g, issued_now())
        .await
        .unwrap_err();
    assert!(
        denied
            .message()
            .contains("listen for payments.PaymentReceived"),
        "{denied:?}"
    );

    // Signed over other bytes, or too long ago.
    let mut request_model = model.clone();
    request_model.push(b' ');
    let request = ProcessRegistration {
        model: request_model,
        issued_at: issued_now(),
        sig: Some(signing::sign(
            &deployer(),
            &signing::process_message(&model, issued_now()),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let forged = ProcessServiceClient::new(transport::connect(&env.addr).await.unwrap())
        .register(presenting(request, &deployer_grant()))
        .await
        .unwrap_err();
    assert_eq!(forged.code(), Code::Unauthenticated, "{forged:?}");
    let old = register_as(
        &env.addr,
        &model,
        &deployer(),
        &deployer_grant(),
        issued_now() - 120_000,
    )
    .await
    .unwrap_err();
    assert_eq!(old.code(), Code::Unauthenticated, "{old:?}");
    assert!(old.message().contains("expired"), "{old:?}");
    assert!(stored(&env.store, "shop.fulfilment").0.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_process_is_removed_and_registered_again() {
    let env = shop("proc-remove").await;
    register(&env, &example()).await.unwrap();

    // The membership no longer lists it: removed.
    apply(&env.addr, membership("shop", &["shop.order"], &[], &[]))
        .await
        .unwrap();
    assert!(stored(&env.store, "shop.fulfilment").1);
    // Registered again (unchanged): active again, no new version.
    let again = register(&env, &example()).await.unwrap();
    assert_eq!((again.version, again.new), (1, false));
    assert!(!stored(&env.store, "shop.fulfilment").1);

    // Removed by an operator.
    let ops = grant(&key(23), &[("process", &["shop.fulfilment"])]);
    env.kernel.grant(ops.clone()).unwrap();
    assert!(
        remove_as(&env.addr, "shop.fulfilment", &key(23), &ops)
            .await
            .unwrap()
    );
    assert!(stored(&env.store, "shop.fulfilment").1);
    assert!(
        !remove_as(&env.addr, "shop.fulfilment", &key(23), &ops)
            .await
            .unwrap(),
        "removed already"
    );
    let unknown = remove_as(&env.addr, "shop.returns", &key(23), &ops)
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::PermissionDenied, "{unknown:?}");
    let unknown = remove_as(&env.addr, "shop.returns", &deployer(), &deployer_grant())
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::NotFound, "{unknown:?}");
    let denied = remove_as(
        &env.addr,
        "shop.fulfilment",
        &key(1),
        &default_grant(&key(1)).unwrap(),
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied, "{denied:?}");

    // A changed model registers a new version and makes it active.
    let mut changed = example();
    changed["steps"][4]["timeoutMs"] = json!(1_000);
    let v2 = register(&env, &changed).await.unwrap();
    assert_eq!((v2.version, v2.new), (2, true));
    assert!(!stored(&env.store, "shop.fulfilment").1);

    // A membership that never listed it leaves it as it is.
    let mut other = membership("billing", &[], &[], &[]);
    other.processes = vec!["billing.dunning".into()];
    apply(&env.addr, other).await.unwrap();
    apply(&env.addr, membership("billing", &[], &[], &[]))
        .await
        .unwrap();
    assert!(!stored(&env.store, "shop.fulfilment").1);
    let bad = {
        let mut m = membership("shop", &[], &[], &[]);
        m.processes = vec!["fulfilment".into()];
        apply(&env.addr, m).await.unwrap_err()
    };
    assert_eq!(bad.code(), Code::InvalidArgument, "{bad:?}");
}

/// A second kernel on a copy of `env`'s store, as after a restart.
async fn restarted(env: &Env, name: &str) -> Env {
    let from = env.dir.clone();
    start_custom(name, move |_, dir| copy_store(&from, dir)).await
}

fn copy_store(from: &Path, to: &Path) {
    for file in ["k.db", "k.db-wal", "k.log"] {
        if from.join(file).exists() {
            std::fs::copy(from.join(file), to.join(file)).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn processes_survive_a_restart() {
    let env = shop("proc-restart").await;
    register(&env, &example()).await.unwrap();
    let ops = grant(&key(23), &[("process", &["shop.*"])]);
    env.kernel.grant(ops.clone()).unwrap();
    assert!(
        remove_as(&env.addr, "shop.fulfilment", &key(23), &ops)
            .await
            .unwrap()
    );

    let back = restarted(&env, "proc-restart-back").await;
    back.kernel.grant(ops.clone()).unwrap();
    back.kernel.grant(deployer_grant()).unwrap();
    assert!(
        !remove_as(&back.addr, "shop.fulfilment", &key(23), &ops)
            .await
            .unwrap(),
        "still removed"
    );
    let again = register(&back, &example()).await.unwrap();
    assert_eq!((again.version, again.new), (1, false), "version 1 is held");
    assert!(!stored(&back.store, "shop.fulfilment").1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_command_line_lists_shows_and_removes_processes() {
    let env = shop("proc-cli").await;
    register(&env, &example()).await.unwrap();
    let mut changed = example();
    changed["steps"][4]["timeoutMs"] = json!(1_000);
    register(&env, &changed).await.unwrap();
    std::fs::write(env.dir.join("ops.key"), hex(deployer().as_bytes())).unwrap();
    std::fs::write(
        env.dir.join("ops.grant"),
        hex(&prost::Message::encode_to_vec(&deployer_grant())),
    )
    .unwrap();
    let kernable = |args: &[&str]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&env.dir)
            .args(args)
            .output()
            .unwrap();
        (
            out.status.code().unwrap(),
            format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ),
        )
    };
    let (code, printed) = kernable(&[
        "processes",
        "remove",
        &env.addr,
        "ops.key",
        "shop.fulfilment",
    ]);
    assert_eq!(
        (code, printed.trim()),
        (0, "removed shop.fulfilment: it starts no new cases")
    );
    let (code, printed) = kernable(&[
        "processes",
        "remove",
        &env.addr,
        "ops.key",
        "shop.fulfilment",
    ]);
    assert_eq!(
        (code, printed.trim()),
        (1, "shop.fulfilment was removed already")
    );

    let (code, printed) = kernable(&["processes", "list", &env.store]);
    assert_eq!(code, 0, "{printed}");
    let line = printed.trim();
    assert!(
        line.starts_with("shop.fulfilment version 2 removed (registered "),
        "{line}"
    );
    assert!(
        line.ends_with(&format!(" by {})", id(&deployer()))),
        "{line}"
    );

    let (code, printed) = kernable(&["processes", "show", &env.store, "shop.fulfilment", "1"]);
    assert_eq!(code, 0, "{printed}");
    let shown: Value = serde_json::from_str(&printed).unwrap();
    assert_eq!(shown, example());
    let (_, printed) = kernable(&["processes", "show", &env.store, "shop.fulfilment"]);
    assert_eq!(serde_json::from_str::<Value>(&printed).unwrap(), changed);
    let (code, printed) = kernable(&["processes", "show", &env.store, "shop.fulfilment", "3"]);
    assert_eq!(
        (code, printed.trim()),
        (1, "no version 3 of process shop.fulfilment")
    );
    let (code, printed) = kernable(&["processes", "show", &env.store, "shop.returns"]);
    assert_eq!((code, printed.trim()), (1, "no process shop.returns"));
}
