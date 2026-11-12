//! Masked copies for previews: `kernable copy`, `kernable import --mask-vault` and (in
//! `backup.rs`) `kernable restore --mask-vault` make a store with the source's history and none
//! of its vault values. A kernel on it reads the source's personal values as null, writes its
//! own with the copy's new vault key, runs only alone, and the copy never becomes anything but a
//! copy.

mod common;

use std::{
    path::{Path, PathBuf},
    process::Command as Process,
    sync::Arc,
    time::Duration,
};

use common::*;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, Hello, ListenerMessage, VaultValue,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_kernel::{Kernel, config::VaultConfig};
use kernable_store::data::{Data, DataKey};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn kernable(dir: &Path, args: &[&str]) -> (i32, String) {
    let out = Process::new(env!("CARGO_BIN_EXE_kernable"))
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap(), printed)
}

async fn run(dir: &Path, args: &[&str]) -> (i32, String) {
    let dir = dir.to_owned();
    let args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        kernable(&dir, &args)
    })
    .await
    .unwrap()
}

fn order(id: &str, command_type: &str, order: &str, payload: &str) -> Command {
    let mut cmd = command(id, command_type, order);
    cmd.payload = payload.as_bytes().to_vec();
    signed(cmd, &key(1))
}

/// A worker whose `PlaceOrder` events keep an email in the vault (`<who>@example.com` of the
/// payload, which holds no email: the test worker's events take the command's payload), of the
/// payload's `customer`, and whose `AddItem` events erase the subject in the payload's `erase`.
fn customer_worker(
    vaults: Option<mpsc::UnboundedSender<Vec<VaultValue>>>,
    behavior: Behavior,
) -> TestWorker {
    let writes: VaultWriter = Arc::new(|cmd: &Command| {
        let payload: serde_json::Value = serde_json::from_slice(&cmd.payload).unwrap();
        let field = |f: &str| payload[f].as_str().unwrap_or_default().to_owned();
        match cmd.command_type.as_str() {
            "PlaceOrder" => (
                vec![vault_value(
                    &field("customer"),
                    &format!("{}@example.com", field("who")),
                )],
                String::new(),
            ),
            _ => (Vec::new(), field("erase")),
        }
    });
    TestWorker {
        behavior,
        writes_vault: Some(writes),
        vaults,
        ..TestWorker::honest()
    }
}

/// Listens with key 5 (granted `vault_read`), sending the number of events and of vault values
/// of every batch to `counts`.
fn listen(addr: &str, name: &str, counts: mpsc::UnboundedSender<(usize, usize)>) {
    let addr = addr.to_owned();
    let name = name.to_owned();
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name,
            key_id: id(&key(5)),
            event_types: vec!["orders.OrderPlaced".into()],
            from_position: Some(0),
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = ListenerServiceClient::new(channel)
            .listen(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(msg)) = inbound.message().await {
            let reply = match msg.msg.unwrap() {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Batch(b) => {
                    let _ = counts.send((b.events.len(), b.vault.len()));
                    In::Ack(Ack {
                        position: b.up_to,
                        ..Default::default()
                    })
                }
                _ => continue,
            };
            let _ = tx.send(send(reply)).await;
        }
    });
}

/// The next message of `rx`, within 10 s.
async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>, what: &str) -> T {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap()
}

/// Whether any file under `dir` holds `needle`.
fn on_disk(dir: &Path, needle: &str) -> bool {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for f in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = f.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.iter().any(|f| {
        let bytes = std::fs::read(f).unwrap_or_default();
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    })
}

fn backend() -> &'static str {
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => "file",
        _ => "sqlite",
    }
}

/// A source kernel with a vault: two customers' orders with their emails in the vault, one of
/// them erased since, snapshots of every order, and a file.
async fn source(name: &str) -> Env {
    let env = start_custom(name, with_vault).await;
    let eraser = grant(
        &key(2),
        &[("run", &["orders.*"]), ("vault_erase", &["orders.*"])],
    );
    env.kernel.grant(eraser).unwrap();
    start_worker(env.addr.clone(), customer_worker(None, Behavior::Snapshots)).await;
    for (c, o, customer, who) in [("c1", "o1", "k-1", "ann"), ("c2", "o2", "k-2", "bob")] {
        let payload = format!(r#"{{"customer":"{customer}","who":"{who}"}}"#);
        let placed = submit(&env.addr, order(c, "PlaceOrder", o, &payload)).await;
        assert_eq!(committed_versions(&placed.unwrap()), [1]);
    }
    let erase = order("c3", "AddItem", "o2", r#"{"erase":"k-2"}"#);
    assert_eq!(
        committed_versions(&submit(&env.addr, erase).await.unwrap()),
        [2]
    );
    let data =
        kernable_store::data::open(&format!("file:{}", env.dir.join("data").display())).unwrap();
    data.put(
        &DataKey {
            tenant_id: "t1".into(),
            domain: "orders".into(),
            key: "invoice".into(),
        },
        &Data {
            content_type: "text/plain".into(),
            data: b"invoice for a customer".to_vec(),
        },
    )
    .unwrap();
    std::fs::write(env.dir.join("kernel.pub"), id(&key(3))).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await; // a checkpoint
    env
}

/// The stores of a copy in `dir/name`.
struct Copy {
    dir: PathBuf,
    store: String,
    vault_store: String,
    vault_key: PathBuf,
    data: String,
}

impl Copy {
    fn new(dir: &Path, name: &str) -> Self {
        let dir = dir.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let file = if backend() == "file" { "k.log" } else { "k.db" };
        Copy {
            store: format!("{}:{}", backend(), dir.join(file).display()),
            vault_store: format!("sqlite:{}", dir.join("vault.db").display()),
            vault_key: dir.join("vault.key"),
            data: format!("file:{}", dir.join("data").display()),
            dir,
        }
    }

    fn flags(&self) -> Vec<String> {
        vec![
            "--store".into(),
            self.store.clone(),
            "--vault-store".into(),
            self.vault_store.clone(),
            "--vault-key".into(),
            self.vault_key.display().to_string(),
            "--data-store".into(),
            self.data.clone(),
            "--kernel-pub".into(),
            "kernel.pub".into(),
        ]
    }

    fn vault(&self) -> VaultConfig {
        VaultConfig {
            store: self.vault_store.clone(),
            key: self.vault_key.clone(),
        }
    }
}

/// A preview kernel on `copy`, with its own snapshot store.
async fn preview(name: &str, copy: &Copy) -> Env {
    let (store, vault) = (copy.store.clone(), copy.vault());
    start_custom(name, move |c, _| {
        c.store = store;
        c.vault = Some(vault);
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_masked_copy_of_a_store_reads_personal_values_as_null_and_writes_its_own() {
    let env = source("copy-local").await;
    let copy = Copy::new(&env.dir, "preview");
    let mut args = vec!["copy".to_owned(), env.store.clone(), "--mask-vault".into()];
    args.extend(copy.flags());
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    // Only masked copies, and only with a vault key of its own that does not exist yet.
    let (code, printed) = run(&env.dir, &args[..2]).await;
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("add --mask-vault"), "{printed}");
    let mut existing = args.clone();
    let at = existing.iter().position(|a| *a == "--vault-key").unwrap();
    existing[at + 1] = "vault.key";
    let (code, printed) = run(&env.dir, &existing).await;
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("vault.key exists"), "{printed}");

    // While the source kernel runs.
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");
    assert!(printed.contains("masked copy of store "), "{printed}");
    assert!(printed.contains("3 events, 3 commands, "), "{printed}");
    assert!(printed.lines().any(|l| l == "OK"), "{printed}");
    let (code, printed) = run(&env.dir, &["verify", &copy.store, "kernel.pub"]).await;
    assert_eq!(code, 0, "{printed}");
    assert!(
        printed.contains("a masked copy (a preview) of store "),
        "{printed}"
    );

    // The same events; no personal value, no file, nothing the source's vault key opens.
    let source = kernable_store::open_read_only(&env.store).unwrap();
    let copied = kernable_store::open_read_only(&copy.store).unwrap();
    assert_eq!(
        copied.read_all(0, 100).unwrap(),
        source.read_all(0, 100).unwrap()
    );
    let mark = copied.copy_mark().unwrap().unwrap();
    assert_eq!(mark.position, 3);
    drop(copied);
    assert!(!on_disk(&copy.dir, "ann@example.com"));
    assert!(!on_disk(&copy.dir, "invoice"), "no files");
    let own = signing::read_secret(&copy.vault_key).unwrap();
    assert_ne!(*own, [6; 32]);
    assert_eq!(
        std::fs::metadata(&copy.vault_key)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    // A preview kernel on it: the source's values read as null, its own work end to end.
    let p = preview("copy-local-preview", &copy).await;
    let (tx, mut vaults) = mpsc::unbounded_channel();
    start_worker(p.addr.clone(), customer_worker(Some(tx), Behavior::Honest)).await;
    let (counts_tx, mut counts) = mpsc::unbounded_channel();
    listen(&p.addr, "reader", counts_tx);
    let (mut events, mut values) = (0, 0);
    while events < 2 {
        let (e, v) = next(&mut counts, "counts 1").await;
        (events, values) = (events + e, values + v);
    }
    assert_eq!(values, 0, "ann's email reads as null, bob's was erased");
    let add = order("p1", "AddItem", "o1", "{}");
    assert_eq!(
        committed_versions(&submit(&p.addr, add).await.unwrap()),
        [2]
    );
    assert!(
        next(&mut vaults, "vaults 2").await.is_empty(),
        "as after an erasure"
    );
    let payload = r#"{"customer":"k-9","who":"pat"}"#;
    let placed = submit(&p.addr, order("p2", "PlaceOrder", "o9", payload)).await;
    assert_eq!(committed_versions(&placed.unwrap()), [1]);
    next(&mut vaults, "vaults 3").await;
    let mut got = next(&mut counts, "counts 4").await;
    while got.0 == 0 {
        got = next(&mut counts, "counts 5").await;
    }
    assert_eq!(got, (1, 1), "the preview's own value");
    let add = order("p3", "AddItem", "o9", "{}");
    submit(&p.addr, add).await.unwrap();
    let sent = next(&mut vaults, "vaults 6").await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].value, b"\"pat@example.com\"");
    // Erasing in the preview works as anywhere.
    let erase = order("p4", "AddItem", "o9", r#"{"erase":"k-9"}"#);
    submit(&p.addr, erase).await.unwrap();
    next(&mut vaults, "vaults 7").await;
    let add = order("p5", "AddItem", "o9", "{}");
    submit(&p.addr, add).await.unwrap();
    assert!(next(&mut vaults, "vaults 8").await.is_empty());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!on_disk(&copy.dir, "ann@example.com"));
    assert!(!on_disk(&p.dir, "ann@example.com"));
    let store = kernable_store::open_read_only(&copy.store).unwrap();
    let report = store.verify(&[id(&key(3))]).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    drop(store);

    // It runs alone, with its own vault key and its own snapshots.
    let config = (*p.kernel.config()).clone();
    let refused = |config: kernable_kernel::config::Config| {
        let store = kernable_store::open_read_only(&config.store).unwrap();
        match Kernel::open_on(config, store) {
            Ok(_) => panic!("a masked copy opened"),
            Err(e) => e.to_string(),
        }
    };
    let mut backup = config.clone();
    backup.replication.backup = true;
    assert!(refused(backup).contains("runs alone, without --backup on"));
    let mut sync = config.clone();
    sync.replication.sync = true;
    assert!(refused(sync).contains("without --replication sync"));
    let mut production_key = config.clone();
    production_key.vault = Some(VaultConfig {
        store: format!("sqlite:{}", env.dir.join("vault.db").display()),
        key: env.dir.join("vault.key"),
    });
    let e = refused(production_key);
    assert!(e.contains("only with the vault key its copy made"), "{e}");
    let mut old_snapshots = config.clone();
    old_snapshots.snapshots = env.snapshots.clone();
    let e = refused(old_snapshots);
    assert!(e.contains("snapshots from before the copy"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_preview_kernel_signs_with_its_own_key_and_the_copy_verifies_with_both() {
    let env = source("copy-keys").await;
    let copy = Copy::new(&env.dir, "preview");
    // Without --kernel-pub: the copy takes the kernel keys the source names.
    let mut args = vec!["copy".to_owned(), env.store.clone(), "--mask-vault".into()];
    args.extend(copy.flags().into_iter().take(8));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");
    let production = id(&key(3));
    let mark = kernable_store::open_read_only(&copy.store)
        .unwrap()
        .copy_mark()
        .unwrap()
        .unwrap();
    assert_eq!(mark.source_kernel_keys, std::slice::from_ref(&production));

    // The preview's kernel has a kernel key of its own, new: it starts on production's
    // checkpoints and signs its own.
    let (store, vault) = (copy.store.clone(), copy.vault());
    let p = start_custom("copy-keys-preview", move |c, dir| {
        std::fs::write(dir.join("preview-kernel.key"), hex(key(9).as_bytes())).unwrap();
        c.kernel_key = dir.join("preview-kernel.key");
        c.store = store;
        c.vault = Some(vault);
    })
    .await;
    start_worker(p.addr.clone(), customer_worker(None, Behavior::Honest)).await;
    let add = order("p1", "AddItem", "o1", "{}");
    assert_eq!(
        committed_versions(&submit(&p.addr, add).await.unwrap()),
        [2]
    );
    tokio::time::sleep(Duration::from_millis(300)).await; // a checkpoint of its own
    let preview = id(&key(9));
    let store = kernable_store::open_read_only(&copy.store).unwrap();
    assert!(
        !store
            .verify(std::slice::from_ref(&production))
            .unwrap()
            .is_ok(),
        "the preview's own checkpoints are not production's"
    );
    assert!(
        !store
            .verify(std::slice::from_ref(&preview))
            .unwrap()
            .is_ok()
    );
    let both = [production.clone(), preview.clone()];
    let report = store.verify(&both).unwrap();
    assert!(report.is_ok(), "{:?}", report.problems);
    drop(store);

    // `kernable verify` with the preview's key alone: the mark adds production's.
    std::fs::write(env.dir.join("preview.pub"), &preview).unwrap();
    let (code, printed) = run(&env.dir, &["verify", &copy.store, "preview.pub"]).await;
    assert_eq!(code, 0, "{printed}");
    assert!(
        printed.contains(&format!(
            "with the source's kernel keys the copy's mark names: {production}"
        )),
        "{printed}"
    );
    // The source itself: the mark is the copy's, never a way into production's checks.
    let (code, printed) = run(&env.dir, &["verify", &env.store, "preview.pub"]).await;
    assert_eq!(code, 1, "{printed}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_masked_copy_from_an_export_stays_a_copy() {
    let env = source("copy-import").await;
    let source_flags = [
        "--store",
        &env.store,
        "--vault-store",
        &format!("sqlite:{}", env.dir.join("vault.db").display()),
        "--vault-key",
        "vault.key",
        "--data-store",
        &format!("file:{}", env.dir.join("data").display()),
    ]
    .map(str::to_owned);
    let mut args = vec!["export".to_owned(), "source.kx".into()];
    args.extend(source_flags);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");

    // With its files, when asked.
    let copy = Copy::new(&env.dir, "preview");
    let mut args = vec![
        "import".to_owned(),
        "source.kx".into(),
        "--mask-vault".into(),
        "--with-files".into(),
    ];
    args.extend(copy.flags());
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");
    assert!(printed.contains("left out 1 vault values"), "{printed}");
    assert!(
        printed.contains("masked copy of export source.kx"),
        "{printed}"
    );
    assert!(printed.lines().any(|l| l == "OK"), "{printed}");
    assert!(on_disk(&copy.dir, "invoice"), "the file came along");

    // An export of the copy imports only as another masked copy; a standby never takes it.
    let mut args = vec!["export".to_owned(), "copy.kx".into()];
    args.extend(copy.flags().into_iter().take(8));
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");
    let again = Copy::new(&env.dir, "production");
    let mut args = vec!["import".to_owned(), "copy.kx".into()];
    args.extend(again.flags().into_iter().take(4));
    args.extend(["--vault-key".into(), copy.vault_key.display().to_string()]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 1, "{printed}");
    assert!(
        printed.contains("the export is a masked copy (a preview)"),
        "{printed}"
    );
    let second = Copy::new(&env.dir, "second");
    let mut args = vec!["import".to_owned(), "copy.kx".into(), "--mask-vault".into()];
    args.extend(second.flags());
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let (code, printed) = run(&env.dir, &args).await;
    assert_eq!(code, 0, "{printed}");
    let mark = kernable_store::open_read_only(&second.store)
        .unwrap()
        .copy_mark()
        .unwrap()
        .unwrap();
    assert_eq!(mark.source, "export copy.kx");
    assert_eq!(
        mark.vault_key,
        signing::vault_key_id(&signing::read_secret(&second.vault_key).unwrap())
    );

    // A standby on a copy's store does not start.
    let p = preview("copy-import-preview", &second).await;
    let mut standby = (*p.kernel.config()).clone();
    standby.replication.follow = Some(kernable_kernel::config::Follow {
        primary: "127.0.0.1:1".into(),
        key: env.dir.join("vault.key"),
        primary_kernel_keys: vec![id(&key(3))],
    });
    let store = kernable_store::open_read_only(&standby.store).unwrap();
    let e = Kernel::open_on(standby, store).err().unwrap().to_string();
    assert!(e.contains("runs alone, without --follow"), "{e}");
}

#[test]
fn records_with_a_copy_mark_are_refused_where_they_would_count_as_the_source() {
    use kernable_core::proto::{CopyRecord, log_record::Record};
    let plain = [Record::ProcessPosition(Default::default())];
    assert!(kernable_kernel::copy::refuse_marked(&plain, "the primary").is_ok());
    let marked = [
        Record::ProcessPosition(Default::default()),
        Record::Copy(CopyRecord::default()),
    ];
    let e = kernable_kernel::copy::refuse_marked(&marked, "the primary")
        .unwrap_err()
        .to_string();
    assert!(e.starts_with("the primary is a masked copy"), "{e}");
}

use std::os::unix::fs::PermissionsExt;
