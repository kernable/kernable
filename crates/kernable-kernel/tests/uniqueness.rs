//! Unique keys, facts and requirements: the kernel checks them atomically at commit, from
//! the keys and facts each aggregate holds. A key is held by one aggregate at a time, within its
//! tenant or across tenants; a command commits only while the facts it requires are held; a
//! personal key lives in the vault and its subject's erasure frees it. The index survives a
//! restart, and workers of protocol 1 leave it alone.

mod common;

use std::{path::Path, sync::Arc};

use common::*;
use kernable_core::{
    proto::{
        Command, CommandResult, Rejected, Requirement, UniqueKey, VaultValue,
        command_result::Outcome,
    },
    signing,
};
use kernable_store::{StreamId, vault::Vault};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The keys (name, value, global, subject), facts and requirements (aggregate ID, fact; of
/// `orders.Order` in the command's tenant) a test command asks the worker to send.
fn payload(keys: &[(&str, &str, bool, &str)], facts: &[&str], requires: &[(&str, &str)]) -> String {
    json!({ "keys": keys, "facts": facts, "requires": requires }).to_string()
}

fn order(id: &str, command_type: &str, order: &str, tenant: &str, payload: String) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.tenant_id = tenant.into();
    cmd.payload = payload.into_bytes();
    signed(cmd, &key(1))
}

/// The keys, facts and requirements of a [`payload`], as a worker computes them.
fn from_payload(cmd: &Command) -> (Vec<UniqueKey>, Vec<String>, Vec<Requirement>) {
    let p: Value = serde_json::from_slice(&cmd.payload).unwrap_or_default();
    let list = |name: &str| p[name].as_array().cloned().unwrap_or_default();
    let keys = list("keys")
        .iter()
        .map(|k| UniqueKey {
            name: k[0].as_str().unwrap().into(),
            value_hash: Sha256::digest(k[1].as_str().unwrap()).to_vec(),
            global: k[2].as_bool().unwrap(),
            subject: k[3].as_str().unwrap().into(),
        })
        .collect();
    let facts = list("facts")
        .iter()
        .map(|f| f.as_str().unwrap().into())
        .collect();
    let requires = list("requires")
        .iter()
        .map(|r| Requirement {
            domain: "orders".into(),
            aggregate_type: "Order".into(),
            aggregate_id: r[0].as_str().unwrap().into(),
            fact: r[1].as_str().unwrap().into(),
        })
        .collect();
    (keys, facts, requires)
}

fn indexing_worker() -> TestWorker {
    TestWorker {
        command_types: vec![
            "orders.PlaceOrder".into(),
            "orders.AddItem".into(),
            "orders.Reindex".into(),
        ],
        index: Some(Arc::new(from_payload)),
        ..TestWorker::honest()
    }
}

fn rejection(result: &CommandResult) -> &Rejected {
    match &result.outcome {
        Some(Outcome::Rejected(r)) => r,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// Submits `cmd` and returns its rejection code, or `committed`.
async fn outcome(env: &Env, cmd: Command) -> String {
    let result = submit(&env.addr, cmd).await.unwrap();
    match &result.outcome {
        Some(Outcome::Committed(_)) => "committed".into(),
        Some(Outcome::Rejected(r)) => r.code.clone(),
        other => panic!("unexpected outcome {other:?}"),
    }
}

fn stream(tenant: &str, order: &str) -> StreamId {
    StreamId {
        tenant_id: tenant.into(),
        domain: "orders".into(),
        aggregate_type: "Order".into(),
        aggregate_id: order.into(),
    }
}

/// A second kernel on a copy of `env`'s store and vault, as after a restart.
async fn restarted(env: &Env, name: &str) -> Env {
    let from = env.dir.clone();
    start_custom(name, move |config, dir| {
        copy_store(&from, dir);
        with_vault(config, dir);
    })
    .await
}

fn copy_store(from: &Path, to: &Path) {
    for file in ["k.db", "k.db-wal", "k.log", "vault.db"] {
        if from.join(file).exists() {
            std::fs::copy(from.join(file), to.join(file)).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_taken_by_one_aggregate_rejects_another_of_its_scope() {
    let env = start("unique-taken").await;
    start_worker(env.addr.clone(), indexing_worker()).await;
    let number = |n: &str, global: bool| payload(&[("number", n, global, "")], &[], &[]);

    assert_eq!(
        outcome(
            &env,
            order("c1", "PlaceOrder", "o1", "t1", number("A-1", false))
        )
        .await,
        "committed"
    );
    let taken = submit(
        &env.addr,
        order("c2", "PlaceOrder", "o2", "t1", number("A-1", false)),
    )
    .await
    .unwrap();
    let reason = rejection(&taken);
    assert_eq!(reason.code, "unique_taken");
    assert_eq!(reason.message, "orders.Order key number is taken");
    // Nothing of the rejected command was written.
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert!(
        store
            .read_stream("t1", "orders", "Order", "o2")
            .unwrap()
            .is_empty()
    );
    // The same value in another tenant, and the same key again by its holder, are fine.
    assert_eq!(
        outcome(
            &env,
            order("c3", "PlaceOrder", "o3", "t2", number("A-1", false))
        )
        .await,
        "committed"
    );
    assert_eq!(
        outcome(
            &env,
            order("c4", "AddItem", "o1", "t1", number("A-1", false))
        )
        .await,
        "committed"
    );

    // A global key is unique across tenants.
    assert_eq!(
        outcome(
            &env,
            order("c5", "PlaceOrder", "g1", "t1", number("G-1", true))
        )
        .await,
        "committed"
    );
    assert_eq!(
        outcome(
            &env,
            order("c6", "PlaceOrder", "g2", "t2", number("G-1", true))
        )
        .await,
        "unique_taken"
    );
    // A global and a tenant key of one value are different keys.
    assert_eq!(
        outcome(
            &env,
            order("c7", "PlaceOrder", "g3", "t1", number("G-1", false))
        )
        .await,
        "committed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_is_freed_when_its_value_changes_or_goes() {
    let env = start("unique-freed").await;
    start_worker(env.addr.clone(), indexing_worker()).await;
    let number = |n: &str| payload(&[("number", n, false, "")], &[], &[]);
    let take = |id: &str, o: &str, n: &str| order(id, "PlaceOrder", o, "t1", number(n));

    outcome(&env, take("c1", "o1", "A-1")).await;
    assert_eq!(outcome(&env, take("c2", "o2", "A-1")).await, "unique_taken");
    // A new value frees the old one in the same commit.
    let changed = order("c3", "AddItem", "o1", "t1", number("A-2"));
    assert_eq!(outcome(&env, changed).await, "committed");
    assert_eq!(outcome(&env, take("c4", "o2", "A-1")).await, "committed");
    assert_eq!(outcome(&env, take("c5", "o3", "A-2")).await, "unique_taken");
    // No value (null, or a deleted aggregate) holds nothing.
    let gone = order("c6", "AddItem", "o1", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&env, gone).await, "committed");
    assert_eq!(outcome(&env, take("c7", "o3", "A-2")).await, "committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregates_racing_for_one_key_commit_one_of_them() {
    let env = start("unique-race").await;
    for _ in 0..2 {
        start_worker(env.addr.clone(), indexing_worker()).await;
    }
    let submits: Vec<_> = (0..8)
        .map(|i| {
            let addr = env.addr.clone();
            let cmd = order(
                &format!("c{i}"),
                "PlaceOrder",
                &format!("o{i}"),
                "t1",
                payload(&[("number", "A-1", false, "")], &[], &[]),
            );
            tokio::spawn(async move { submit(&addr, cmd).await.unwrap() })
        })
        .collect();
    let mut codes = Vec::new();
    for s in submits {
        codes.push(match s.await.unwrap().outcome {
            Some(Outcome::Committed(_)) => "committed".to_owned(),
            Some(Outcome::Rejected(r)) => r.code,
            other => panic!("{other:?}"),
        });
    }
    codes.sort();
    assert_eq!(codes[0], "committed", "{codes:?}");
    assert!(codes[1..].iter().all(|c| c == "unique_taken"), "{codes:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_commits_only_while_the_facts_it_requires_are_held() {
    let env = start("unique-requires").await;
    start_worker(env.addr.clone(), indexing_worker()).await;
    let open = |id: &str, command_type: &str, tenant: &str| {
        order(id, command_type, "v1", tenant, payload(&[], &["open"], &[]))
    };
    let requiring = |id: &str, command_type: &str| {
        order(
            id,
            command_type,
            "a1",
            "t1",
            payload(&[], &[], &[("v1", "open")]),
        )
    };

    let missing = submit(&env.addr, requiring("c1", "PlaceOrder"))
        .await
        .unwrap();
    let reason = rejection(&missing);
    assert_eq!(reason.code, "requirement_missing");
    assert_eq!(reason.message, "orders.Order v1 does not hold open");
    // Held in another tenant only: still missing.
    assert_eq!(
        outcome(&env, open("c2", "PlaceOrder", "t2")).await,
        "committed"
    );
    assert_eq!(
        outcome(&env, requiring("c3", "PlaceOrder")).await,
        "requirement_missing"
    );

    assert_eq!(
        outcome(&env, open("c4", "PlaceOrder", "t1")).await,
        "committed"
    );
    assert_eq!(
        outcome(&env, requiring("c5", "PlaceOrder")).await,
        "committed"
    );
    // A commit before removes the fact: the next command requiring it is rejected.
    let closed = order("c6", "AddItem", "v1", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&env, closed).await, "committed");
    assert_eq!(
        outcome(&env, requiring("c7", "AddItem")).await,
        "requirement_missing"
    );

    // A command requiring its own aggregate's fact sees the state before it.
    let own = |id: &str, facts: &[&str]| {
        order(
            id,
            "AddItem",
            "v1",
            "t1",
            payload(&[], facts, &[("v1", "open")]),
        )
    };
    assert_eq!(
        outcome(&env, own("c8", &["open"])).await,
        "requirement_missing"
    );
    assert_eq!(
        outcome(&env, open("c9", "AddItem", "t1")).await,
        "committed"
    );
    assert_eq!(outcome(&env, own("c10", &[])).await, "committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reindex_stores_the_set_and_a_restart_restores_the_index() {
    let env = start("unique-reindex").await;
    start_worker(env.addr.clone(), indexing_worker()).await;
    let number = |n: &str| payload(&[("number", n, false, "")], &["open"], &[]);

    // The aggregate existed before its key was declared: a reindex, without events, stores it.
    let placed = order("c1", "PlaceOrder", "o1", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&env, placed).await, "committed");
    let reindexed = submit(&env.addr, order("c2", "Reindex", "o1", "t1", number("A-1")))
        .await
        .unwrap();
    assert_eq!(committed_versions(&reindexed), Vec::<u64>::new());
    // A reindex of an ID never written holds nothing: it does not exist.
    assert_eq!(
        outcome(&env, order("c3", "Reindex", "o9", "t1", number("A-9"))).await,
        "committed"
    );
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let index = store.index().unwrap();
    assert_eq!(index.len(), 1, "{index:?}");
    assert_eq!(index[0].0, stream("t1", "o1"));
    assert_eq!(index[0].1.facts, ["open"]);
    drop(store);

    let after = restarted(&env, "unique-reindex-restarted").await;
    start_worker(after.addr.clone(), indexing_worker()).await;
    let taking = order("c4", "PlaceOrder", "o2", "t1", number("A-1"));
    assert_eq!(outcome(&after, taking).await, "unique_taken");
    let requiring = order(
        "c5",
        "PlaceOrder",
        "o3",
        "t1",
        payload(&[], &[], &[("o1", "open")]),
    );
    assert_eq!(outcome(&after, requiring).await, "committed");
    let free = order("c6", "PlaceOrder", "o4", "t1", number("A-9"));
    assert_eq!(outcome(&after, free).await, "committed");
}

/// Whether any file in `dir` holds `needle`.
fn on_disk(dir: &Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir).unwrap().any(|f| {
        let bytes = std::fs::read(f.unwrap().path()).unwrap_or_default();
        bytes.windows(needle.len()).any(|w| w == needle)
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_personal_key_lives_in_the_vault_and_erasing_its_subject_frees_it() {
    let env = start_custom("unique-personal", with_vault).await;
    let eraser = grant(
        &key(2),
        &[("run", &["orders.*"]), ("vault_erase", &["orders.*"])],
    );
    env.kernel.grant(eraser).unwrap();
    let writes: VaultWriter = Arc::new(|cmd: &Command| {
        let p: Value = serde_json::from_slice(&cmd.payload).unwrap_or_default();
        (
            Vec::<VaultValue>::new(),
            p["erase"].as_str().unwrap_or_default().to_owned(),
        )
    });
    start_worker(
        env.addr.clone(),
        TestWorker {
            writes_vault: Some(writes),
            ..indexing_worker()
        },
    )
    .await;
    let personal = payload(&[("email", "max@firma.de", false, "k-1")], &[], &[]);
    let take = |id: &str, o: &str| order(id, "PlaceOrder", o, "t1", personal.clone());

    let placed = take("c1", "o1");
    assert_eq!(outcome(&env, placed.clone()).await, "committed");
    let (keys, _, _) = from_payload(&placed);
    let id = signing::unique_key_id(&placed, &keys[0]);
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let index = store.index().unwrap();
    drop(store);
    let stored = &index[0].1.keys[0];
    assert!(stored.id.is_empty(), "{stored:?}");
    let vault = Vault::open(
        &format!("sqlite:{}", env.dir.join("vault.db").display()),
        &[6; 32],
    )
    .unwrap();
    let held = vault.get(std::slice::from_ref(&stored.vault_ref)).unwrap();
    assert_eq!(held[0].subject, "k-1");
    assert_eq!(held[0].value, format!("\"{}\"", hex(&id)).into_bytes());
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(!on_disk(&env.dir, &id), "the key's ID is in no file");
    assert!(!on_disk(&env.dir, hex(&id).as_bytes()));
    assert!(!on_disk(&env.dir, &keys[0].value_hash));

    assert_eq!(outcome(&env, take("c2", "o2")).await, "unique_taken");
    // Another aggregate's event erases the subject: the key is free.
    let opened = order("c3", "PlaceOrder", "e1", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&env, opened).await, "committed");
    let erasing = order(
        "c4",
        "AddItem",
        "e1",
        "t1",
        json!({"erase": "k-1"}).to_string(),
    );
    assert_eq!(outcome(&env, erasing).await, "committed");
    assert!(
        vault
            .get(std::slice::from_ref(&stored.vault_ref))
            .unwrap()
            .is_empty()
    );
    assert_eq!(outcome(&env, take("c5", "o2")).await, "committed");

    // After a restart, o2 holds it and o1's erased key stays free.
    let after = restarted(&env, "unique-personal-restarted").await;
    start_worker(after.addr.clone(), indexing_worker()).await;
    assert_eq!(outcome(&after, take("c6", "o3")).await, "unique_taken");
    let dropped = order("c7", "Reindex", "o2", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&after, dropped).await, "committed");
    assert_eq!(outcome(&after, take("c8", "o3")).await, "committed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_needs_a_vault_for_personal_keys() {
    let env = start_custom("unique-personal-vault", with_vault).await;
    start_worker(env.addr.clone(), indexing_worker()).await;
    let personal = payload(&[("email", "max@firma.de", false, "k-1")], &[], &[]);
    assert_eq!(
        outcome(
            &env,
            order("c1", "PlaceOrder", "o1", "t1", personal.clone())
        )
        .await,
        "committed"
    );

    // Its store holds a personal key: without the vault, a kernel refuses to start on it.
    let from = env.dir.clone();
    let plain = start_custom("unique-personal-no-vault", move |config, dir| {
        copy_store(&from, dir);
        let store = kernable_store::open_with(&config.store, config.durability).unwrap();
        let refused = kernable_kernel::Kernel::open_on(config.clone(), store)
            .err()
            .unwrap();
        assert!(refused.to_string().contains("--vault-key"), "{refused}");
        std::fs::remove_file(dir.join("k.db")).ok();
        std::fs::remove_file(dir.join("k.db-wal")).ok();
        std::fs::remove_file(dir.join("k.log")).ok();
    })
    .await;
    // A kernel without a vault rejects a personal key.
    start_worker(plain.addr.clone(), indexing_worker()).await;
    assert_eq!(
        outcome(&plain, order("c2", "PlaceOrder", "o2", "t1", personal)).await,
        "no_vault"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_worker_of_protocol_1_leaves_the_index_alone() {
    let env = start("unique-protocol-1").await;
    let current = start_worker(env.addr.clone(), indexing_worker()).await;
    let number = payload(&[("number", "A-1", false, "")], &["open"], &[]);
    let placed = order("c1", "PlaceOrder", "o1", "t1", number.clone());
    assert_eq!(outcome(&env, placed).await, "committed");
    current.abort();
    let _ = current.await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    start_worker(
        env.addr.clone(),
        TestWorker {
            protocol: 1,
            ..indexing_worker()
        },
    )
    .await;
    let store = kernable_store::open_read_only(&env.store).unwrap();
    let before = store.index().unwrap();
    // It sends no index: what o1 holds stays, and so does the key.
    let other = order("c2", "AddItem", "o1", "t1", payload(&[], &[], &[]));
    assert_eq!(outcome(&env, other).await, "committed");
    assert_eq!(store.index().unwrap(), before);
    assert_eq!(
        outcome(&env, order("c3", "PlaceOrder", "o2", "t1", number)).await,
        "committed",
        "a protocol-1 result is not checked against the index either"
    );
    assert_eq!(store.index().unwrap(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_facts_and_requirements_over_a_limit_are_an_invalid_index() {
    let env = start("unique-limits").await;
    let key_ = |name: &str, hash: Vec<u8>| UniqueKey {
        name: name.into(),
        global: false,
        value_hash: hash,
        subject: String::new(),
    };
    let requirement = |domain: &str, aggregate_type: &str, id: &str, fact: &str| Requirement {
        domain: domain.into(),
        aggregate_type: aggregate_type.into(),
        aggregate_id: id.into(),
        fact: fact.into(),
    };
    let hash = |n: u32| Sha256::digest(n.to_be_bytes()).to_vec();
    let fine = requirement("orders", "Order", "o1", "open");
    type Index = (Vec<UniqueKey>, Vec<String>, Vec<Requirement>);
    let cases: Vec<(&str, Index)> = vec![
        (
            "too many keys",
            (
                (0..65).map(|n| key_("k", hash(n))).collect(),
                vec![],
                vec![],
            ),
        ),
        (
            "too many facts",
            (vec![], (0..65).map(|n| format!("f{n}")).collect(), vec![]),
        ),
        (
            "too many requirements",
            (vec![], vec![], vec![fine.clone(); 65]),
        ),
        (
            "an empty key name",
            (vec![key_("", hash(0))], vec![], vec![]),
        ),
        (
            "a key name with a dot",
            (vec![key_("a.b", hash(0))], vec![], vec![]),
        ),
        (
            "a long key name",
            (vec![key_(&"k".repeat(129), hash(0))], vec![], vec![]),
        ),
        (
            "a short value hash",
            (vec![key_("k", vec![0; 31])], vec![], vec![]),
        ),
        (
            "a key twice",
            (vec![key_("k", hash(0)), key_("k", hash(0))], vec![], vec![]),
        ),
        ("a fact with a dot", (vec![], vec!["a.b".into()], vec![])),
        (
            "a fact twice",
            (vec![], vec!["f".into(), "f".into()], vec![]),
        ),
        (
            "a requirement without ID",
            (
                vec![],
                vec![],
                vec![requirement("orders", "Order", "", "open")],
            ),
        ),
        (
            "a qualified aggregate type",
            (
                vec![],
                vec![],
                vec![requirement("orders", "orders.Order", "o1", "open")],
            ),
        ),
        (
            "an invalid domain",
            (
                vec![],
                vec![],
                vec![requirement("1orders", "Order", "o1", "open")],
            ),
        ),
        (
            "an empty fact",
            (
                vec![],
                vec![],
                vec![requirement("orders", "Order", "o1", "")],
            ),
        ),
    ];
    let by_id: std::collections::HashMap<String, Index> = cases
        .iter()
        .enumerate()
        .map(|(i, (_, index))| (format!("c{i}"), index.clone()))
        .collect();
    start_worker(
        env.addr.clone(),
        TestWorker {
            index: Some(Arc::new(move |cmd: &Command| {
                by_id.get(&cmd.command_id).cloned().unwrap_or_default()
            })),
            ..TestWorker::honest()
        },
    )
    .await;
    for (i, (case, _)) in cases.iter().enumerate() {
        let cmd = signed(
            command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
            &key(1),
        );
        let result = submit(&env.addr, cmd).await.unwrap();
        assert_eq!(rejection(&result).code, "invalid_index", "{case}");
    }
    // At the limits, a result commits.
    let at_limits: Index = (
        (0..64).map(|n| key_(&"k".repeat(128), hash(n))).collect(),
        (0..64).map(|n| format!("f{n}")).collect(),
        vec![],
    );
    let id = format!("c{}", cases.len());
    let worker_index = at_limits.clone();
    let env2 = start("unique-limits-at").await;
    start_worker(
        env2.addr.clone(),
        TestWorker {
            index: Some(Arc::new(move |_: &Command| worker_index.clone())),
            ..TestWorker::honest()
        },
    )
    .await;
    let cmd = signed(command(&id, "PlaceOrder", "o1"), &key(1));
    assert_eq!(
        committed_versions(&submit(&env2.addr, cmd).await.unwrap()),
        [1]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_index_signed_by_another_key_ends_the_session() {
    let env = start("unique-forged").await;
    let worker = start_worker(
        env.addr.clone(),
        TestWorker {
            behavior: Behavior::ForgeIndex,
            ..indexing_worker()
        },
    )
    .await;
    let cmd = order("c1", "PlaceOrder", "o1", "t1", payload(&[], &["open"], &[]));
    let addr = env.addr.clone();
    tokio::spawn(async move { submit(&addr, cmd).await });
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), worker).await;
    let status = ended.expect("the session ends").unwrap();
    assert_eq!(status.code(), tonic::Code::Unauthenticated, "{status:?}");
    let store = kernable_store::open_read_only(&env.store).unwrap();
    assert!(store.index().unwrap().is_empty());
    assert!(store.read_all(0, 10).unwrap().is_empty());
}
