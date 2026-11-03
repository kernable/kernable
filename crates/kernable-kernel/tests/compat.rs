//! Compatibility: a frozen store of the current data format keeps working, and requests of a
//! protocol version the kernel does not speak are refused with a clear message.
//!
//! `tests/fixtures/format-<n>` is a frozen store of the current format. Until the first release
//! the format stays 1 and changes in place: after a change, delete the directory and write it
//! again (with `KERNABLE_WRITE_FIXTURES=1` and `--test-threads=1`, which only fills directories
//! that do not exist yet).

mod common;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use common::*;
use kernable_core::{
    PROTOCOL,
    proto::{
        AggregateIndex, Attribute, Command, Hello, ListenerMessage, ListenerRecord, Membership,
        MembershipRecord, Middleware, NewEvent, ParkedEvent, QueryRequest, Register, Rejected,
        RequestContext, RevocationRecord, StoredKey, SubmitRequest, SubscribeRequest, UniqueKey,
        VaultValue, Wait, When, WorkerMessage, WorkerRecord, command_result::Outcome,
        command_service_client::CommandServiceClient, event_service_client::EventServiceClient,
        listener_message, listener_service_client::ListenerServiceClient,
        query_service_client::QueryServiceClient, worker_gateway_client::WorkerGatewayClient,
        worker_message,
    },
    signing, transport,
};
use kernable_kernel::config::VaultConfig;
use kernable_store::{
    Store, StreamId,
    vault::{Owner, Vault},
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

const AT: i64 = 1_700_000_000_000;
const VAULT_KEY: [u8; 32] = [6; 32];

fn fixtures(format: u32) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("tests/fixtures/format-{format}"))
}

fn context() -> RequestContext {
    RequestContext {
        sender_identity: "i-1".into(),
        correlation_id: "r-1".into(),
        attributes: vec![Attribute {
            key: "channel".into(),
            value: "web".into(),
        }],
        ..Default::default()
    }
}

/// A personal (with a subject) or secret value, with a fixed salt.
fn value(salt: u8, subject: &str, value: &str) -> VaultValue {
    let salt = vec![salt; 16];
    let value = format!("\"{value}\"").into_bytes();
    let secret = subject.is_empty();
    VaultValue {
        r#ref: signing::vault_ref(&salt, subject, secret, &value),
        subject: subject.into(),
        secret,
        salt,
        value,
    }
}

fn stream(order: &str) -> StreamId {
    StreamId {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: order.into(),
    }
}

/// The keys of `o1` in the fixture: its number, and its customer's email (personal,
/// subject `k-1`).
fn keys() -> [UniqueKey; 2] {
    let key = |name: &str, value: &str, subject: &str| UniqueKey {
        name: name.into(),
        global: false,
        value_hash: <sha2::Sha256 as sha2::Digest>::digest(value).to_vec(),
        subject: subject.into(),
    };
    [
        key("number", "A-1001", ""),
        key("email", "max@firma.de", "k-1"),
    ]
}

/// What the fixture holds: the customer's email in `c1` and its event (subject `k-1`), an item
/// added by `c2`, a rejected `c3`, a secret coupon code with the pending `c4`, an order `o3`
/// whose customer `k-2` was erased (by `c7`), two listeners, and `c11` signed by a key revoked
/// since; key 8's grant, which replaced a broader one; and the [`keys`] and the fact `open` that
/// `c2` gave `o1`.
fn fill(store: &dyn Store, vault: &Vault) {
    let cmd = |id: &str, command_type: &str, aggregate: &str, values: &[&VaultValue]| {
        let mut cmd = command(id, command_type, aggregate);
        cmd.context = Some(context());
        cmd.vault_refs = values.iter().map(|v| v.r#ref.clone()).collect();
        let values: Vec<VaultValue> = values.iter().map(|v| (*v).clone()).collect();
        let owner = Owner {
            command_id: id,
            tenant: &cmd.tenant_id,
            stream: None,
        };
        vault.put(&values, &cmd.vault_refs, &owner, AT).unwrap();
        signed(cmd, &key(1))
    };
    let commit_indexed = |cmd: &Command,
                          version: u64,
                          event: NewEvent,
                          values: &[&VaultValue],
                          index: Option<&AggregateIndex>| {
        store.enqueue(cmd, AT).unwrap();
        let events = [event];
        let msg = signing::result_message(cmd, version, &events);
        let values: Vec<VaultValue> = values.iter().map(|v| (*v).clone()).collect();
        let s = stream(&cmd.aggregate_id);
        let owner = Owner {
            command_id: &cmd.command_id,
            tenant: &cmd.tenant_id,
            stream: Some(&s),
        };
        vault
            .put(&values, &events[0].vault_refs, &owner, AT)
            .unwrap();
        if !events[0].erases.is_empty() {
            vault
                .erase(&cmd.tenant_id, &events[0].erases, &cmd.command_id, AT)
                .unwrap();
        }
        store
            .commit(
                &cmd.command_id,
                version,
                &events,
                &signing::sign(&key(2), &msg),
                index,
                AT,
            )
            .unwrap();
    };
    let commit = |cmd: &Command, version: u64, event: NewEvent, values: &[&VaultValue]| {
        commit_indexed(cmd, version, event, values, None)
    };
    let event = |event_type: &str, schema: u32, payload: String, values: &[&VaultValue]| NewEvent {
        event_type: event_type.into(),
        schema,
        payload: payload.into_bytes(),
        vault_refs: values.iter().map(|v| v.r#ref.clone()).collect(),
        erases: String::new(),
        ..Default::default()
    };
    let email = value(1, "k-1", "max@firma.de");
    let placed_email = value(2, "k-1", "max@firma.de");
    let c1 = cmd("c1", "PlaceOrder", "o1", &[&email]);
    let payload = format!(
        r#"{{"customer":"k-1","email":{{"$vault":"{}"}}}}"#,
        placed_email.r#ref
    );
    commit(
        &c1,
        0,
        event("OrderPlaced", 2, payload, &[&placed_email]),
        &[&placed_email],
    );
    let c2 = cmd("c2", "AddItem", "o1", &[]);
    let [number, email_key] = keys();
    let email_id = value(5, "k-1", &hex(&signing::unique_key_id(&c2, &email_key)));
    let s = stream("o1");
    let owner = Owner {
        command_id: "c2",
        tenant: "t1",
        stream: Some(&s),
    };
    let email_key_id = signing::unique_key_id(&c2, &email_key);
    vault.put_key(&email_id, &email_key_id, &owner, AT).unwrap();
    let index = AggregateIndex {
        keys: vec![
            StoredKey {
                name: number.name.clone(),
                global: false,
                id: signing::unique_key_id(&c2, &number).to_vec(),
                vault_ref: String::new(),
            },
            StoredKey {
                name: email_key.name.clone(),
                global: false,
                id: Vec::new(),
                vault_ref: email_id.r#ref.clone(),
            },
        ],
        facts: vec!["open".into()],
    };
    commit_indexed(
        &c2,
        1,
        event("ItemAdded", 1, r#"{"sku":"apple"}"#.into(), &[]),
        &[],
        Some(&index),
    );
    let c3 = cmd("c3", "AddItem", "o9", &[]);
    store.enqueue(&c3, AT).unwrap();
    let reason = Rejected {
        code: "invalid_state".into(),
        message: "AddItem not allowed now".into(),
        ..Default::default()
    };
    store.reject("c3", reason).unwrap();
    let coupon = value(3, "", "SAVE10");
    let c4 = cmd("c4", "PlaceOrder", "o2", &[&coupon]);
    store.enqueue(&c4, AT).unwrap();
    let other = value(4, "k-2", "erika@firma.de");
    let c6 = cmd("c6", "PlaceOrder", "o3", &[]);
    let payload = format!(
        r#"{{"customer":"k-2","email":{{"$vault":"{}"}}}}"#,
        other.r#ref
    );
    commit(
        &c6,
        0,
        event("OrderPlaced", 2, payload, &[&other]),
        &[&other],
    );
    let c7 = cmd("c7", "AddItem", "o3", &[]);
    let mut erased = event("CustomerErased", 1, r#"{"customer":"k-2"}"#.into(), &[]);
    erased.erases = "k-2".into();
    commit(&c7, 1, erased, &[]);
    let listener = |name: &str, types: &str, middlewares: Vec<Middleware>| ListenerRecord {
        name: name.into(),
        key_id: id(&key(5)),
        order: 0,
        event_types: vec![types.into()],
        position: 2,
        removed: false,
        middlewares,
        service: String::new(),
        last_seen: AT,
        parked: Vec::new(),
        parked_changed: false,
    };
    let guard = Middleware {
        order: 0,
        types: vec!["identity.*".into()],
        when: When::Before.into(),
    };
    store
        .save_listeners(&[
            ListenerRecord {
                parked: vec![ParkedEvent {
                    position: 1,
                    event_type: "orders.OrderPlaced".into(),
                    code: "not_found".into(),
                    message: "no such customer".into(),
                    attempts: 5,
                    parked_at: AT,
                    retry: true,
                }],
                parked_changed: true,
                ..listener("orders", "orders.*", Vec::new())
            },
            listener("guard", "identity.*", vec![guard]),
        ])
        .unwrap();
    store
        .save_workers(&[WorkerRecord {
            aggregate_type: "orders.Order".into(),
            key_id: id(&key(2)),
            command_types: vec!["orders.PlaceOrder".into(), "orders.AddItem".into()],
            service: "orders".into(),
            last_seen: AT,
            removed: false,
        }])
        .unwrap();
    let shop = Membership {
        service: "orders".into(),
        workers: vec!["orders.Order".into()],
        listeners: vec!["orders".into()],
        middlewares: vec!["guard".into()],
        metadata: vec![Attribute {
            key: "version".into(),
            value: "1.0.0".into(),
        }],
        issued_at: AT,
    };
    store
        .save_membership(&MembershipRecord {
            sig: Some(signing::sign(&key(1), &signing::membership_message(&shop))),
            membership: Some(shop),
            applied_at: AT,
        })
        .unwrap();
    // A client key stolen at AT. What it signed from then on is suspect.
    let mut stolen = command("c11", "PlaceOrder", "o4");
    stolen.context = Some(context());
    let stolen = signed(stolen, &key(7));
    let placed = event("OrderPlaced", 2, r#"{"customer":"k-3"}"#.into(), &[]);
    commit(&stolen, 0, placed, &[]);
    let msg = signing::revocation_message(&id(&key(7)), AT, AT);
    let revocation = RevocationRecord {
        key_id: id(&key(7)),
        since: AT,
        created_at: AT,
        kernel_sig: Some(signing::sign(&key(3), &msg)),
    };
    store.revoke(&revocation).unwrap();
    store.checkpoint(&key(3), AT).unwrap();
    store.save_grant(&narrow_grant()).unwrap();
}

/// Key 8's grant in the fixture: query only, issued after one that let it submit.
fn narrow_grant() -> kernable_core::proto::Grant {
    signing::grant(
        &root(),
        &id(&key(8)),
        vec![kernable_core::proto::Permission {
            action: "query".into(),
            types: vec!["*".into()],
        }],
        "",
        AT + 1,
    )
}

/// Copies the store file of `format` and `backend` into `dir` with its vault,
/// writing the fixture first if asked to; returns the copy's store URL.
fn copy(format: u32, backend: &str, file: &str, dir: &Path) -> String {
    let fixtures = fixtures(format);
    let fixture = fixtures.join(file);
    let write =
        std::env::var_os("KERNABLE_WRITE_FIXTURES").is_some() && format == kernable_store::FORMAT;
    if write && !fixture.exists() {
        std::fs::create_dir_all(&fixtures).unwrap();
        let url = format!("{backend}:{}", fixture.display());
        let vault_path = fixtures.join(format!("vault-{backend}.db"));
        let vault = Vault::open(&format!("sqlite:{}", vault_path.display()), &VAULT_KEY).unwrap();
        fill(kernable_store::open(&url).unwrap().as_ref(), &vault);
        std::fs::write(fixtures.join("vault.key"), hex(&VAULT_KEY)).unwrap();
        if backend == "sqlite" {
            // One self-contained file: fold the write-ahead log into it.
            let conn = rusqlite::Connection::open(&fixture).unwrap();
            conn.pragma_update(None, "journal_mode", "DELETE").unwrap();
            let _ = std::fs::remove_file(format!("{}.lock", fixture.display()));
        }
    }
    let target = dir.join(file);
    std::fs::copy(&fixture, &target).unwrap();
    let vault = format!("vault-{backend}.db");
    std::fs::copy(fixtures.join(&vault), dir.join(&vault)).unwrap();
    std::fs::copy(fixtures.join("vault.key"), dir.join("vault.key")).unwrap();
    format!("{backend}:{}", target.display())
}

fn outcome(result: &kernable_core::proto::CommandResult) -> &Outcome {
    result.outcome.as_ref().unwrap()
}

fn verified(url: &str) -> kernable_store::Report {
    let report = kernable_store::open_read_only(url)
        .unwrap()
        .verify(&[id(&key(3)).to_string()])
        .unwrap();
    assert!(report.is_ok(), "{url}: {:?}", report.problems);
    report
}

/// Checks the context, schema, rejection and listeners a store holds.
fn check_common(store: &dyn Store, backend: &str) {
    let events = store.read_all(0, 10).unwrap();
    assert_eq!(
        events[0].context.as_ref().unwrap().attributes[0].value,
        "web"
    );
    assert_eq!(events[0].schema, 2);
    let rejected = store.result("c3").unwrap().unwrap();
    assert!(matches!(outcome(&rejected), Outcome::Rejected(r) if r.code == "invalid_state"));
    let listeners = store.listeners().unwrap();
    assert_eq!(listeners.len(), 2);
    assert!(
        !listeners[0].middlewares.is_empty(),
        "{backend}: guard is a middleware"
    );
    assert_eq!(listeners[0].last_seen, AT);
    assert_eq!(
        listeners[1].parked.len(),
        1,
        "{backend}: orders parked an event"
    );
    assert_eq!(listeners[1].parked[0].message, "no such customer");
    assert!(listeners[1].parked[0].retry, "{backend}");
    let workers = store.workers().unwrap();
    assert_eq!(workers[0].command_types.len(), 2, "{backend}");
    let memberships = store.memberships().unwrap();
    let shop = memberships[0].membership.as_ref().unwrap();
    assert_eq!(shop.middlewares, ["guard"], "{backend}");
    assert_eq!(shop.metadata[0].value, "1.0.0", "{backend}");
}

/// The current format, on each backend.
const STORES: [(u32, &str, &str); 2] = [
    (kernable_store::FORMAT, "sqlite", "k.db"),
    (kernable_store::FORMAT, "file", "k.log"),
];

#[tokio::test(flavor = "multi_thread")]
async fn stores_of_every_format_verify_and_run_with_their_vault() {
    for (format, backend, file) in STORES {
        let mut url = String::new();
        let mut vault_url = String::new();
        let name = format!("compat-vault-{format}-{backend}");
        let env = start_custom(&name, |config, dir| {
            url = copy(format, backend, file, dir);
            vault_url = format!(
                "sqlite:{}",
                dir.join(format!("vault-{backend}.db")).display()
            );
            config.store = url.clone();
            config.vault = Some(VaultConfig {
                store: vault_url.clone(),
                key: dir.join("vault.key"),
            });
        })
        .await;

        let report = verified(&url);
        assert_eq!(
            (report.events, report.commands, report.checkpoints),
            (5, 7, 1),
            "{backend}"
        );
        let store = kernable_store::open_read_only(&url).unwrap();
        check_common(store.as_ref(), backend);
        let kept: Vec<_> = store
            .grants()
            .unwrap()
            .into_iter()
            .filter(|g| g.key_id == id(&key(8)))
            .collect();
        assert_eq!(kept, [narrow_grant()], "{backend}");
        let index = store.index().unwrap();
        assert_eq!(index.len(), 1, "{backend}");
        assert_eq!(index[0].0, stream("o1"));
        assert_eq!(index[0].1.facts, ["open"]);
        assert_eq!(index[0].1.keys.len(), 2);
        let events = store.read_all(0, 10).unwrap();
        let (placed, erased_order, erasure) = (&events[0], &events[2], &events[3]);
        assert_eq!(placed.vault_refs.len(), 1);
        assert_eq!(erasure.erases, "k-2");
        drop(store);

        // A worker of today gets the pending command's secret; the secret goes once it ran.
        let (tx, mut vaults) = mpsc::unbounded_channel();
        start_worker(
            env.addr.clone(),
            TestWorker {
                schemas: vec![("orders.OrderPlaced".into(), 2)],
                vaults: Some(tx),
                // `c9` and `c10` take the keys o1 holds.
                index: Some(Arc::new(|cmd: &Command| {
                    let [number, email] = keys();
                    match cmd.command_id.as_str() {
                        "c9" => (vec![number], Vec::new(), Vec::new()),
                        "c10" => (vec![email], Vec::new(), Vec::new()),
                        _ => Default::default(),
                    }
                })),
                ..TestWorker::honest()
            },
        )
        .await;
        let coupon = value(3, "", "SAVE10");
        let mut pending = command("c4", "PlaceOrder", "o2");
        pending.context = Some(context());
        pending.vault_refs = vec![coupon.r#ref.clone()];
        let c4 = submit_with(&env.addr, signed(pending, &key(1)), Wait::Committed).await;
        assert_eq!(committed_versions(&c4.unwrap()), [1], "{backend}");
        assert_eq!(vaults.recv().await.unwrap()[0].value, coupon.value);
        for (id, order) in [("c9", "o5"), ("c10", "o6")] {
            let taking = signed(command(id, "PlaceOrder", order), &key(1));
            let taken = submit_with(&env.addr, taking, Wait::Committed)
                .await
                .unwrap();
            let rejected =
                matches!(outcome(&taken), Outcome::Rejected(r) if r.code == "unique_taken");
            assert!(rejected, "{backend}: {id}");
            vaults.recv().await.unwrap();
        }

        // The history of o1 comes with the customer's email; the erased one stays away.
        let c5 = signed(command("c5", "AddItem", "o1"), &key(1));
        submit_with(&env.addr, c5, Wait::Committed).await.unwrap();
        let sent = vaults.recv().await.unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].r#ref, placed.vault_refs[0]);
        let c8 = signed(command("c8", "AddItem", "o3"), &key(1));
        submit_with(&env.addr, c8, Wait::Committed).await.unwrap();
        assert!(vaults.recv().await.unwrap().is_empty(), "k-2 was erased");

        let vault = Vault::open(&vault_url, &VAULT_KEY).unwrap();
        assert!(
            vault.get(&[coupon.r#ref]).unwrap().is_empty(),
            "the secret is gone"
        );
        assert!(vault.get(&erased_order.vault_refs).unwrap().is_empty());
        verified(&url);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stores_of_every_format_report_what_revoked_keys_signed() {
    for (format, backend, file) in STORES {
        let mut url = String::new();
        let name = format!("compat-revoked-{format}-{backend}");
        let env = start_custom(&name, |config, dir| {
            url = copy(format, backend, file, dir);
            config.store = url.clone();
            config.vault = Some(VaultConfig {
                store: format!(
                    "sqlite:{}",
                    dir.join(format!("vault-{backend}.db")).display()
                ),
                key: dir.join("vault.key"),
            });
        })
        .await;
        // Revoked in the store: the kernel refuses a grant for it.
        let refused = env.kernel.grant(grant(&key(7), &[("submit", &["*"])]));
        assert_eq!(refused.unwrap_err().code(), tonic::Code::PermissionDenied);
        let report = verified(&url);
        assert_eq!(
            report.suspects,
            [format!(
                "command c11: signed by revoked key {}",
                id(&key(7))
            )],
            "{backend}"
        );
        let refused = submit(&env.addr, signed(command("c12", "AddItem", "o4"), &key(7))).await;
        assert_eq!(refused.unwrap_err().code(), tonic::Code::PermissionDenied);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn other_protocol_versions_are_refused_everywhere() {
    let env = start("compat-protocol").await;
    for (version, advice) in [(0, "update the SDK"), (PROTOCOL + 1, "update the kernel")] {
        let channel = transport::connect(&env.addr).await.unwrap();
        let mut refusals = Vec::new();

        let submit = SubmitRequest {
            protocol: version,
            ..Default::default()
        };
        refusals.push(
            CommandServiceClient::new(channel.clone())
                .submit(submit)
                .await
                .err(),
        );
        let query = QueryRequest {
            protocol: version,
            ..Default::default()
        };
        refusals.push(
            QueryServiceClient::new(channel.clone())
                .query(query)
                .await
                .err(),
        );
        let subscribe = SubscribeRequest {
            protocol: version,
            ..Default::default()
        };
        let subscribed = EventServiceClient::new(channel.clone())
            .subscribe(subscribe)
            .await;
        refusals.push(subscribed.err());

        let register = WorkerMessage {
            msg: Some(worker_message::Msg::Register(Register {
                protocol: version,
                ..Default::default()
            })),
        };
        let session = WorkerGatewayClient::new(channel.clone())
            .session(tokio_stream::iter([register]))
            .await;
        refusals.push(match session {
            Ok(r) => r.into_inner().next().await.and_then(Result::err),
            Err(status) => Some(status),
        });
        let hello = ListenerMessage {
            msg: Some(listener_message::Msg::Hello(Hello {
                protocol: version,
                ..Default::default()
            })),
        };
        let listen = ListenerServiceClient::new(channel)
            .listen(tokio_stream::iter([hello]))
            .await;
        refusals.push(match listen {
            Ok(r) => r.into_inner().next().await.and_then(Result::err),
            Err(status) => Some(status),
        });

        for (i, refusal) in refusals.into_iter().enumerate() {
            let status = refusal.unwrap_or_else(|| panic!("request {i} was not refused"));
            assert_eq!(
                status.code(),
                tonic::Code::FailedPrecondition,
                "{i}: {status:?}"
            );
            assert!(status.message().contains(advice), "{i}: {status:?}");
        }
    }
}
