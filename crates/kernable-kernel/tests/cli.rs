//! The command line: each command run as the binary, against copies of the frozen stores in
//! `tests/fixtures` (see compat.rs for what they hold) and against a running kernel.

mod common;

use std::{
    path::{Path, PathBuf},
    process::Command as Process,
};

use common::*;
use kernable_core::{
    proto::{ListenerRecord, Membership, MembershipRecord, Middleware, When},
    signing,
};
use kernable_store::vault::Vault;

/// Runs `kernable <args>` in `dir`; returns the exit code and what it printed (stdout, then
/// stderr).
fn kernable(dir: &Path, args: &[&str]) -> (i32, String) {
    kernable_with(dir, args, &[])
}

fn kernable_with(dir: &Path, args: &[&str], vars: &[(&str, PathBuf)]) -> (i32, String) {
    let mut process = Process::new(env!("CARGO_BIN_EXE_kernable"));
    process.current_dir(dir).args(args);
    for var in ["KERNABLE_TLS_CA", "KERNABLE_TLS_CERT", "KERNABLE_TLS_KEY"] {
        process.env_remove(var);
    }
    for (var, value) in vars {
        process.env(var, value);
    }
    let out = process.output().unwrap();
    let printed = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap(), printed)
}

/// Whether `printed` has `line` as a whole line.
fn has_line(printed: &str, line: &str) -> bool {
    printed.lines().any(|l| l == line)
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kn-cli-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The backend under test, as compat.rs names its fixture files.
fn backend() -> (&'static str, &'static str) {
    ("sqlite", "k.db")
}

/// The fixtures of the current store format.
fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("tests/fixtures/format-{}", kernable_store::FORMAT))
}

/// Copies the fixture store into `dir` as `name`; returns its store URL.
fn fixture(dir: &Path, name: &str) -> String {
    let (backend, file) = backend();
    std::fs::copy(fixtures().join(file), dir.join(name)).unwrap();
    format!("{backend}:{}", dir.join(name).display())
}

fn write_key(dir: &Path, name: &str, seed: u8) {
    let key = key(seed);
    std::fs::write(dir.join(format!("{name}.key")), hex(key.as_bytes())).unwrap();
    std::fs::write(dir.join(format!("{name}.pub")), id(&key)).unwrap();
}

#[test]
fn keys_are_written_once_and_readable_by_the_owner_only() {
    let dir = temp_dir("keys");
    let (code, printed) = kernable(&dir, &["keys", "new", "ops"]);
    assert_eq!(code, 0, "{printed}");
    let public = std::fs::read_to_string(dir.join("ops.pub")).unwrap();
    assert_eq!(printed.trim(), public.trim());
    let secret = std::fs::read(dir.join("ops.key")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.join("ops.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let (code, _) = kernable(&dir, &["keys", "new", "ops"]);
    assert_eq!(code, 1, "an existing key is never overwritten");
    assert_eq!(std::fs::read(dir.join("ops.key")).unwrap(), secret);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn arguments_a_command_does_not_take_are_refused_with_the_usage() {
    let dir = temp_dir("usage");
    write_key(&dir, "root", 9);
    for args in [
        &["grant", "root.key", "root.pub"][..],
        &["verify", "k.db"],
        &["certs", "new", "certs"],
        &["keys", "old", "x"],
        &[],
    ] {
        let (code, printed) = kernable(&dir, args);
        assert_eq!(code, 2, "{args:?}");
        assert!(printed.starts_with("usage:"), "{args:?}: {printed}");
    }
    assert!(!dir.join("root.grant").exists());

    let (code, printed) = kernable(&dir, &["certs", "new", "certs", "app"]);
    assert_eq!(code, 0, "{printed}");
    for file in [
        "ca.tls.crt",
        "kernel.tls.crt",
        "kernel.tls.key",
        "app.tls.crt",
        "app.tls.key",
    ] {
        assert!(dir.join("certs").join(file).exists(), "{file}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_inspection_commands_show_what_the_store_holds() {
    let dir = temp_dir("inspect");
    let store = fixture(&dir, "store");
    {
        // A listener of the orders service that its membership does not list, and a
        // membership whose worker, listener and middleware never registered.
        let store = kernable_store::open(&store).unwrap();
        store
            .save_listeners(&[ListenerRecord {
                name: "stray".into(),
                key_id: id(&key(5)),
                event_types: vec!["orders.OrderPlaced".into()],
                position: 1,
                service: "orders".into(),
                middlewares: vec![Middleware {
                    order: 4,
                    types: vec!["orders.*".into()],
                    when: When::After.into(),
                }],
                ..Default::default()
            }])
            .unwrap();
        let billing = Membership {
            service: "billing".into(),
            workers: vec!["billing.Invoice".into()],
            listeners: vec!["ghost".into()],
            middlewares: vec!["orders".into()],
            ..Default::default()
        };
        store
            .save_membership(&MembershipRecord {
                sig: Some(signing::sign(
                    &key(4),
                    &signing::membership_message(&billing),
                )),
                membership: Some(billing),
                applied_at: 0,
            })
            .unwrap();
    }

    let (code, printed) = kernable(&dir, &["schemas", &store]);
    assert_eq!(code, 0, "{printed}");
    assert!(
        has_line(&printed, "orders.OrderPlaced schema 2"),
        "{printed}"
    );
    assert!(has_line(&printed, "orders.ItemAdded schema 1"), "{printed}");

    let (code, printed) = kernable(&dir, &["workers", &store]);
    assert_eq!(code, 0, "{printed}");
    assert_eq!(
        printed.trim(),
        format!(
            "orders.Order commands orders.PlaceOrder,orders.AddItem key {} service orders \
             last seen 2023-11-14T22:13:20Z",
            id(&key(2))
        )
    );

    let (code, printed) = kernable(&dir, &["listeners", &store]);
    assert_eq!(code, 0, "{printed}");
    let k5 = id(&key(5));
    assert!(
        has_line(
            &printed,
            &format!(
                "guard order 0 position 2 types identity.* key {k5} middleware before order 0 \
                 checks identity.* last seen 2023-11-14T22:13:20Z"
            )
        ),
        "{printed}"
    );
    assert!(
        has_line(
            &printed,
            &format!(
                "stray order 0 position 1 types orders.OrderPlaced key {k5} service orders \
                 (not listed: counts for nobody) middleware after order 4 checks orders.* \
                 last seen 1970-01-01T00:00:00Z"
            )
        ),
        "{printed}"
    );

    let (code, printed) = kernable(&dir, &["services", &store]);
    assert_eq!(code, 0, "{printed}");
    let expected = format!(
        "billing (applied 1970-01-01T00:00:00Z by {})
  worker billing.Invoice (never registered)
  listener ghost (never registered)
  middleware orders
orders version=1.0.0 (applied 2023-11-14T22:13:20Z by {})
  worker orders.Order
  listener orders
  middleware guard
  dynamic listener stray",
        id(&key(4)),
        id(&key(1))
    );
    assert_eq!(printed.trim(), expected);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn listeners_and_workers_are_removed_by_name() {
    let dir = temp_dir("remove");
    let store = fixture(&dir, "store");

    let (code, printed) = kernable(&dir, &["listeners", "remove", &store, "guard"]);
    assert_eq!((code, printed.trim()), (0, "removed guard"));
    let (code, printed) = kernable(&dir, &["listeners", "remove", &store, "guard"]);
    assert_eq!((code, printed.trim()), (1, "no listener guard"));
    let names: Vec<String> = kernable_store::open_read_only(&store)
        .unwrap()
        .listeners()
        .unwrap()
        .into_iter()
        .map(|l| l.name)
        .collect();
    assert_eq!(names, ["orders"]);

    let (code, printed) = kernable(&dir, &["workers", "remove", &store, "orders.Order"]);
    assert_eq!((code, printed.trim()), (0, "removed orders.Order"));
    let (code, printed) = kernable(&dir, &["workers", "remove", &store, "orders.Order"]);
    assert_eq!((code, printed.trim()), (1, "no aggregate orders.Order"));
    let store = kernable_store::open_read_only(&store).unwrap();
    assert_eq!(store.workers().unwrap(), []);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn verify_tells_intact_suspect_and_tampered_stores_apart() {
    let dir = temp_dir("verify");
    write_key(&dir, "kernel", 3);
    let store = fixture(&dir, "store");

    // The fixture holds c11, signed by key 7 after its revocation: intact, one suspect.
    let (code, printed) = kernable(&dir, &["verify", &store, "kernel.pub"]);
    assert_eq!(code, 2, "{printed}");
    assert!(
        printed.starts_with("5 events, 7 commands, 1 checkpoints, 0 events after the last"),
        "{printed}"
    );
    assert!(printed.contains("SUSPECT "), "{printed}");
    assert!(has_line(&printed, "OK, 1 suspect"), "{printed}");

    // A key that never signed a checkpoint: the checkpoint does not verify.
    write_key(&dir, "other", 9);
    let (code, printed) = kernable(&dir, &["verify", &store, "other.pub"]);
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("PROBLEM "), "{printed}");
    assert!(has_line(&printed, "TAMPERED"), "{printed}");

    if backend().0 == "sqlite" {
        let path = dir.join("changed");
        let changed = fixture(&dir, "changed");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE events SET payload = '{\"sku\":\"pear\"}' WHERE global_position = 2",
            [],
        )
        .unwrap();
        drop(conn);
        let (code, printed) = kernable(&dir, &["verify", &changed, "kernel.pub"]);
        assert_eq!(code, 1, "{printed}");
        assert!(has_line(&printed, "TAMPERED"), "{printed}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_checkpoint_signs_the_newest_event() {
    let dir = temp_dir("checkpoint");
    write_key(&dir, "kernel", 3);
    let (backend, _) = backend();
    let empty = format!("{backend}:{}", dir.join("empty").display());
    let (code, printed) = kernable(&dir, &["checkpoint", &empty, "kernel.key"]);
    assert_eq!((code, printed.trim()), (0, "no events yet"));

    let store = fixture(&dir, "store");
    let (code, printed) = kernable(&dir, &["checkpoint", &store, "kernel.key"]);
    assert_eq!((code, printed.trim()), (0, "checkpoint at position 5"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vault_rekey_moves_every_value_to_the_new_key() {
    let dir = temp_dir("rekey");
    let fixtures = fixtures();
    std::fs::copy(fixtures.join("vault-sqlite.db"), dir.join("vault.db")).unwrap();
    std::fs::copy(fixtures.join("vault.key"), dir.join("old.key")).unwrap();
    std::fs::write(dir.join("new.key"), hex(&[7; 32])).unwrap();
    std::fs::write(dir.join("short.key"), "abcd\n").unwrap();
    let vault = format!("sqlite:{}", dir.join("vault.db").display());

    let (code, printed) = kernable(&dir, &["vault", "rekey", &vault, "old.key", "short.key"]);
    assert_eq!(code, 1, "{printed}");
    assert_eq!(printed.trim(), "error: short.key: not a 32-byte key in hex");

    let (code, printed) = kernable(&dir, &["vault", "rekey", &vault, "old.key", "new.key"]);
    assert_eq!(code, 0, "{printed}");
    assert!(printed.starts_with("re-encrypted "), "{printed}");
    // The customer's email in c1, as compat.rs seals it.
    let email = signing::vault_ref(&[1; 16], "k-1", false, b"\"max@example.com\"");
    let values = Vault::open(&vault, &[7; 32])
        .unwrap()
        .get(&[email])
        .unwrap();
    assert_eq!(values[0].value, b"\"max@example.com\"");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The vault fixture that belongs to the backend under test.
fn vault_fixture(dir: &Path, name: &str) -> String {
    let file = "vault-sqlite.db";
    std::fs::copy(fixtures().join(file), dir.join(name)).unwrap();
    format!("sqlite:{}", dir.join(name).display())
}

#[test]
fn an_export_of_stopped_stores_imports_into_new_ones() {
    let dir = temp_dir("export-stopped");
    write_key(&dir, "kernel", 3);
    std::fs::copy(fixtures().join("vault.key"), dir.join("vault.key")).unwrap();
    std::fs::write(dir.join("other-vault.key"), hex(&[7; 32])).unwrap();
    let store = fixture(&dir, "store");
    let vault = vault_fixture(&dir, "vault.db");
    let data = kernable_store::data::open(&format!("file:{}", dir.join("data").display())).unwrap();
    let invoice = kernable_store::data::DataKey {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        key: "invoice-1.pdf".into(),
    };
    let pdf = kernable_store::data::Data {
        content_type: "application/pdf".into(),
        data: b"%PDF-1.7 invoice".to_vec(),
    };
    data.put(&invoice, &pdf).unwrap();
    let source = [
        "--store",
        &store,
        "--vault-store",
        &vault,
        "--vault-key",
        "vault.key",
        "--data-store",
        "file:data",
    ];

    // Without the vault, the values the store refers to cannot be exported.
    let (code, printed) = kernable(&dir, &["export", "plain.kx", "--store", &store]);
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("refers to vault values"), "{printed}");
    assert!(
        !dir.join("plain.kx").exists(),
        "a failed export leaves no file"
    );

    let (code, printed) = kernable(&dir, &[&["export", "out.kx"][..], &source].concat());
    assert_eq!(code, 0, "{printed}");
    assert!(printed.starts_with("exported "), "{printed}");
    assert!(
        printed.contains(" vault values and 1 files to out.kx"),
        "{printed}"
    );
    assert!(
        printed.contains(&format!("signed by the kernel keys {}", id(&key(3)))),
        "{printed}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(dir.join("out.kx"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    // An export never overwrites a file.
    let (code, printed) = kernable(&dir, &[&["export", "out.kx"][..], &source].concat());
    assert_eq!(code, 1, "{printed}");

    let (backend, _) = backend();
    let target = |name: &str| {
        (
            format!("{backend}:{}", dir.join(name).display()),
            format!("sqlite:{}", dir.join(format!("{name}-vault.db")).display()),
            format!("file:{}", dir.join(format!("{name}-data")).display()),
        )
    };
    let import = |file: &str, name: &str, vault_key: &str| {
        let (store, vault, data) = target(name);
        kernable(
            &dir,
            &[
                "import",
                file,
                "--store",
                &store,
                "--vault-store",
                &vault,
                "--vault-key",
                vault_key,
                "--data-store",
                &data,
            ],
        )
    };

    // The fixture holds c11, signed by key 7 after its revocation: one suspect, as at the source.
    let (code, printed) = import("out.kx", "new", "vault.key");
    assert_eq!(code, 2, "{printed}");
    assert!(printed.starts_with("imported "), "{printed}");
    assert!(
        printed.contains("5 events, 7 commands, 1 checkpoints"),
        "{printed}"
    );
    assert!(has_line(&printed, "OK, 1 suspect"), "{printed}");
    let (new_store, new_vault, new_data) = target("new");
    for inspect in ["listeners", "workers", "services"] {
        assert_eq!(
            kernable(&dir, &[inspect, &new_store]),
            kernable(&dir, &[inspect, &store]),
            "{inspect}"
        );
    }
    let (_, verified) = kernable(&dir, &["verify", &new_store, "kernel.pub"]);
    assert!(has_line(&verified, "OK, 1 suspect"), "{verified}");
    // The values are readable with the target's vault key, which is the source's.
    let email = signing::vault_ref(&[1; 16], "k-1", false, b"\"max@example.com\"");
    let key: [u8; 32] = hex::decode(
        std::fs::read_to_string(dir.join("vault.key"))
            .unwrap()
            .trim(),
    )
    .unwrap()
    .try_into()
    .unwrap();
    let values = Vault::open(&new_vault, &key)
        .unwrap()
        .get(&[email])
        .unwrap();
    assert_eq!(values[0].value, b"\"max@example.com\"");
    let copied = kernable_store::data::open(&new_data).unwrap();
    assert_eq!(copied.get(&invoice).unwrap(), Some(pdf));

    // Into another store: the same.
    let other = format!("sqlite:{}", dir.join("other.db").display());
    let (code, printed) = kernable(
        &dir,
        &[
            "import",
            "out.kx",
            "--store",
            &other,
            "--vault-store",
            &format!("sqlite:{}", dir.join("other-vault.db").display()),
            "--vault-key",
            "vault.key",
            "--data-store",
            &format!("file:{}", dir.join("other-data").display()),
            "--kernel-pub",
            "kernel.pub",
        ],
    );
    assert_eq!(code, 2, "{printed}");
    assert!(
        printed.contains("5 events, 7 commands, 1 checkpoints"),
        "{printed}"
    );
    for inspect in ["listeners", "workers", "services"] {
        assert_eq!(
            kernable(&dir, &[inspect, &other]),
            kernable(&dir, &[inspect, &store]),
            "{inspect}"
        );
    }

    // Into a store that holds data, or with another vault key: refused.
    let (code, printed) = import("out.kx", "new", "vault.key");
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("holds data already"), "{printed}");
    let (code, printed) = import("out.kx", "wrong-key", "other-vault.key");
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("sealed with another key"), "{printed}");

    // A copy cut short, or with one byte changed, is refused.
    let bytes = std::fs::read(dir.join("out.kx")).unwrap();
    std::fs::write(dir.join("short.kx"), &bytes[..bytes.len() - 40]).unwrap();
    let (code, printed) = import("short.kx", "short", "vault.key");
    assert_eq!(code, 1, "{printed}");
    assert!(
        printed.contains("cut short") || printed.contains("damaged"),
        "{printed}"
    );
    let mut changed = bytes.clone();
    changed[bytes.len() / 2] ^= 1;
    std::fs::write(dir.join("changed.kx"), &changed).unwrap();
    let (code, printed) = import("changed.kx", "changed", "vault.key");
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("damaged"), "{printed}");
    let (code, printed) = import("vault.key", "garbage", "vault.key");
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("not a kernable export"), "{printed}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn grant_apply_submit_and_revoke_work_against_a_running_kernel() {
    let env = start("cli-online").await;
    spawn_worker(&env).await;
    let dir = &env.dir;
    std::fs::write(dir.join("root.key"), hex(root().as_bytes())).unwrap();
    write_key(dir, "ops", 9);
    write_key(dir, "next", 10);
    let ops = id(&key(9));

    let (code, printed) = kernable(dir, &["grant", "root.key", "ops.pub", "bogus=x"]);
    assert_eq!(code, 1, "{printed}");
    assert!(!dir.join("ops.grant").exists());
    let (code, printed) = kernable(
        dir,
        &[
            "grant",
            "root.key",
            "ops.pub",
            "submit=orders.*",
            "manage=orders",
            "orders.*",
        ],
    );
    assert_eq!(code, 0, "{printed}");
    assert_eq!(
        printed.trim(),
        "submit orders.*\nmanage orders\n* orders.*\nwrote ops.grant"
    );
    let (code, printed) = kernable(
        dir,
        &[
            "grant",
            "root.key",
            "next.pub",
            "--succeeds",
            "ops.pub",
            "query=*",
        ],
    );
    assert_eq!(code, 0, "{printed}");
    assert_eq!(
        printed.trim(),
        format!("query *\nsucceeds {ops}\nwrote next.grant")
    );

    // Key 9 has no grant but the one in ops.grant, which the commands send along.
    let submit = |aggregate: &'static str| {
        let addr = env.addr.clone();
        let dir = dir.clone();
        move || {
            kernable(
                &dir,
                &[
                    "submit",
                    &addr,
                    "ops.key",
                    "t1",
                    "orders",
                    "Order",
                    aggregate,
                    "PlaceOrder",
                    "{}",
                ],
            )
        }
    };
    let (code, first) = tokio::task::spawn_blocking(submit("o1")).await.unwrap();
    assert_eq!(code, 0, "{first}");
    let lines: Vec<&str> = first.lines().collect();
    assert_eq!(lines.len(), 2, "{first}");
    let command_id = lines[0].strip_prefix("command ").unwrap();
    assert!(
        command_id.len() == 32 && command_id.chars().all(|c| c.is_ascii_hexdigit()),
        "{first}"
    );
    assert!(
        lines[1].starts_with("committed #1 v1 OrderPlaced "),
        "{first}"
    );
    let (code, again) = tokio::task::spawn_blocking(submit("o1")).await.unwrap();
    assert_eq!(code, 1, "{again}");
    assert_ne!(
        again.lines().next(),
        first.lines().next(),
        "a new command ID each time"
    );
    assert!(again.contains("\nrejected invalid_state: "), "{again}");

    std::fs::write(
        dir.join("orders.json"),
        r#"{"service": "orders", "workers": ["orders.Order"], "listeners": ["audit"],
            "metadata": {"team": "shop", "replicas": 2}}"#,
    )
    .unwrap();
    std::fs::write(dir.join("broken.json"), r#"{"workers": []}"#).unwrap();
    let apply = |manifest: &'static str| {
        let addr = env.addr.clone();
        let dir = dir.clone();
        move || kernable(&dir, &["apply", &addr, "ops.key", manifest])
    };
    let (code, printed) = tokio::task::spawn_blocking(apply("broken.json"))
        .await
        .unwrap();
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("broken.json: no service"), "{printed}");
    let (code, printed) = tokio::task::spawn_blocking(apply("orders.json"))
        .await
        .unwrap();
    assert_eq!(code, 0, "{printed}");
    assert_eq!(
        printed.trim(),
        "applied orders: 1 workers, 1 listeners, 0 middlewares"
    );
    let applied = kernable_store::open_read_only(&env.store)
        .unwrap()
        .memberships()
        .unwrap();
    let membership = applied[0].membership.as_ref().unwrap();
    let metadata: Vec<(&str, &str)> = membership
        .metadata
        .iter()
        .map(|a| (a.key.as_str(), a.value.as_str()))
        .collect();
    assert_eq!(metadata, [("team", "shop"), ("replicas", "2")]);
    assert_eq!(applied[0].sig.as_ref().unwrap().key_id, ops);

    // Revoked from a minute ago: its commands are refused, and the one it submitted is suspect.
    let since = kernable_kernel::config::utc(issued_now() - 60_000);
    let revoke = {
        let addr = env.addr.clone();
        let dir = dir.clone();
        move || kernable(&dir, &["revoke", &addr, "root.key", "ops.pub", &since])
    };
    let (code, printed) = tokio::task::spawn_blocking(revoke).await.unwrap();
    assert_eq!(
        (code, printed.trim()),
        (0, format!("revoked {ops}").as_str())
    );
    let (code, printed) = tokio::task::spawn_blocking(submit("o2")).await.unwrap();
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("error: "), "{printed}");
    let bad = {
        let addr = env.addr.clone();
        let dir = dir.clone();
        move || kernable(&dir, &["revoke", &addr, "root.key", "ops.pub", "yesterday"])
    };
    let (code, printed) = tokio::task::spawn_blocking(bad).await.unwrap();
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("yesterday is not a UTC time"), "{printed}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_commands_reach_a_kernel_over_mutual_tls() {
    let env = start_custom("cli-tls", |config, dir| {
        kernable_core::certs::create(dir, &["app", "worker"]).unwrap();
        config.listen = free_addr();
        config.tls = Some(kernable_kernel::config::Tls {
            cert: dir.join("kernel.tls.crt"),
            key: dir.join("kernel.tls.key"),
            client_ca: dir.join("ca.tls.crt"),
        });
    })
    .await;
    write_key(&env.dir, "app", 1);
    let vars = [
        ("KERNABLE_TLS_CA", env.dir.join("ca.tls.crt")),
        ("KERNABLE_TLS_CERT", env.dir.join("app.tls.crt")),
        ("KERNABLE_TLS_KEY", env.dir.join("app.tls.key")),
    ];
    std::fs::write(env.dir.join("app.json"), r#"{"service": "app"}"#).unwrap();
    let apply = |vars: Vec<(&'static str, PathBuf)>| {
        let addr = env.addr.clone();
        let dir = env.dir.clone();
        move || {
            let vars: Vec<(&str, PathBuf)> = vars;
            kernable_with(&dir, &["apply", &addr, "app.key", "app.json"], &vars)
        }
    };
    let (code, printed) = tokio::task::spawn_blocking(apply(vars.to_vec()))
        .await
        .unwrap();
    assert_eq!(code, 0, "{printed}");
    // Without a client certificate, or with an empty variable: plain HTTP/2, refused.
    let (code, printed) = tokio::task::spawn_blocking(apply(Vec::new()))
        .await
        .unwrap();
    assert_eq!(code, 1, "{printed}");
    let mut empty = vars.to_vec();
    empty[2].1 = PathBuf::new();
    let (code, printed) = tokio::task::spawn_blocking(apply(empty)).await.unwrap();
    assert_eq!(code, 1, "{printed}");
}

#[test]
fn start_explains_its_flags_and_logs_json_if_asked() {
    let dir = temp_dir("start");
    let (code, printed) = kernable(&dir, &["start", "--help"]);
    assert_eq!(code, 0, "{printed}");
    assert!(printed.starts_with("kernable start flags"), "{printed}");
    assert!(printed.contains("--durability"), "{printed}");
    let (code, printed) = kernable(&dir, &["start", "--no-such-flag", "x"]);
    assert_eq!(code, 2, "{printed}");
    assert!(printed.contains("\n\nkernable start flags"), "{printed}");

    let log = std::fs::File::create(dir.join("stderr")).unwrap();
    let addr = free_addr();
    let _kernel = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .args([
                "start",
                "--dev",
                "--listen",
                &addr,
                "--ui-listen",
                "127.0.0.1:0",
            ])
            .env("KERNABLE_LOG_FORMAT", "json")
            .stderr(log),
    );
    let started = std::time::Instant::now();
    let line = loop {
        let stderr = std::fs::read_to_string(dir.join("stderr")).unwrap();
        // The development notes are for a person; the log lines are JSON.
        if let Some(line) = stderr.lines().find(|l| l.contains("\"level\"")) {
            break line.to_owned();
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "no log line: {stderr}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert!(
        serde_json::from_str::<serde_json::Value>(&line).is_ok(),
        "{line}"
    );
    drop(_kernel);
    let _ = std::fs::remove_dir_all(&dir);
}

/// How long one request to the demo waits for its answer. Its handlers answer at once (a step
/// runs in the background), so this only bounds a hung server.
#[cfg(unix)]
const DEMO_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// One request to the demo's UI: the status code and the body; `None` only if the demo did
/// not take the connection (not listening yet). Any other failure, an empty or malformed
/// answer among them, panics: it is a bug, not a demo still starting.
#[cfg(unix)]
fn try_demo_http(addr: &str, method: &str, path: &str, from_stage: bool) -> Option<(u16, String)> {
    use std::io::{Read, Write};
    let mut conn = match std::net::TcpStream::connect(addr) {
        Ok(conn) => conn,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => return None,
        Err(e) => panic!("{method} {path}: connecting failed: {e}"),
    };
    conn.set_read_timeout(Some(DEMO_READ_TIMEOUT)).unwrap();
    let header = if from_stage {
        "x-kernable-demo: 1\r\n"
    } else {
        ""
    };
    // In one write: the server reads the request with one read.
    let request =
        format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{header}content-length: 0\r\n\r\n");
    conn.write_all(request.as_bytes())
        .unwrap_or_else(|e| panic!("{method} {path}: sending failed: {e}"));
    let mut response = String::new();
    conn.read_to_string(&mut response)
        .unwrap_or_else(|e| panic!("{method} {path}: reading the answer failed: {e}"));
    let status = response
        .get(9..12)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("{method} {path}: no status in {response:?}"));
    let body = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("{method} {path}: no end of headers in {response:?}"))
        .1
        .to_owned();
    Some((status, body))
}

/// One request to the demo's UI: the status code and the body. A GET is asked again while the
/// demo refuses the connection; anything else is not, it may have been done.
#[cfg(unix)]
fn demo_http(addr: &str, method: &str, path: &str, from_stage: bool) -> (u16, String) {
    let tries = if method == "GET" { 5 } else { 1 };
    for _ in 0..tries {
        if let Some(answer) = try_demo_http(addr, method, path, from_stage) {
            return answer;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("the demo did not take {method} {path}");
}

#[cfg(unix)]
fn demo_state(addr: &str) -> serde_json::Value {
    let (status, body) = demo_http(addr, "GET", "/api/demo", false);
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("{status} {body:?}: {e}"))
}

/// A demo in `dir`, its UI on a free address: on another one if the demo could not bind the
/// first (another process took it meanwhile). What it logs goes to `dir/demo.log`.
#[cfg(unix)]
fn spawn_demo(dir: &std::path::Path) -> (KillOnDrop, String) {
    for _ in 0..5 {
        let addr = free_addr();
        let log = std::fs::File::create(dir.join("demo.log")).unwrap();
        let mut demo = KillOnDrop::spawn(
            Process::new(env!("CARGO_BIN_EXE_kernable"))
                .current_dir(dir)
                .args(["demo", "--ui-listen", &addr])
                .stdout(std::process::Stdio::null())
                .stderr(log),
        );
        // The demo binds its address before anything else.
        let started = std::time::Instant::now();
        while std::net::TcpStream::connect(&addr).is_err()
            && demo.child.try_wait().unwrap().is_none()
            && started.elapsed() < std::time::Duration::from_secs(10)
        {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        match demo.child.try_wait().unwrap() {
            None => return (demo, addr),
            Some(_) => {
                let log = std::fs::read_to_string(dir.join("demo.log")).unwrap_or_default();
                assert!(log.contains(&addr), "the demo exited: {log}");
            }
        }
    }
    panic!("the demo found no free address");
}

/// Waits until the demo is ready and idle; returns its state.
#[cfg(unix)]
fn demo_idle(addr: &str, demo: &mut KillOnDrop, dir: &std::path::Path) -> serde_json::Value {
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = demo.child.try_wait().unwrap() {
            let log = std::fs::read_to_string(dir.join("demo.log")).unwrap_or_default();
            panic!("the demo exited ({status}): {log}");
        }
        // Refused: not listening yet. Anything else answers, and must be a state.
        if let Some((status, body)) = try_demo_http(addr, "GET", "/api/demo", false) {
            assert!((200..300).contains(&status), "{status} {body:?}");
            let state: serde_json::Value =
                serde_json::from_str(&body).unwrap_or_else(|e| panic!("{status} {body:?}: {e}"));
            assert!(state["error"].is_null(), "{state}");
            if state["ready"] == true && state["busy"].is_null() {
                return state;
            }
        }
        // Generous: a debug build on a slow machine takes over 10 s for a chapter of
        // hundreds of commands (a release build there: under 2 s).
        assert!(
            started.elapsed() < std::time::Duration::from_secs(120),
            "the demo did not become ready: {}",
            std::fs::read_to_string(dir.join("demo.log")).unwrap_or_default()
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn the_demo_tells_its_story_resets_on_a_new_store_and_leaves_nothing_behind() {
    let dir = temp_dir("demo");
    let (mut demo, addr) = spawn_demo(&dir);
    let base = std::env::temp_dir().join(format!("kernable-demo-{}", demo.child.id()));
    let state = demo_idle(&addr, &mut demo, &dir);
    assert_eq!(state["generation"], 1);
    assert!(base.join("run-1").join("kernable.db").exists());
    assert!(state["scene"]["chapters"].as_array().unwrap().len() > 5);

    // A page elsewhere cannot drive it: no header, no step.
    let (status, _) = demo_http(&addr, "POST", "/api/demo/next", false);
    assert_eq!(status, 403);

    for _ in 0..3 {
        let (status, body) = demo_http(&addr, "POST", "/api/demo/next", true);
        assert_eq!(status, 200, "{body}");
        demo_idle(&addr, &mut demo, &dir);
    }
    let state = demo_state(&addr);
    let outcomes: Vec<Vec<String>> = state["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(demo_outcomes)
        .collect();
    assert_eq!(
        outcomes,
        [
            vec!["committed".to_owned()],
            vec!["committed".to_owned()],
            vec!["rejected:insufficient_funds".to_owned()]
        ]
    );
    assert_eq!(state["rows"]["accounts"]["a1"]["balance"], 100);
    let first = &state["traces"][1]["sends"][0];
    assert_eq!(
        first["answer"]["row"]["balance"], 100,
        "the client reads its write"
    );
    assert_eq!(
        first["state"],
        serde_json::json!({ "owner": "Ada", "balance": 0, "open": true }),
        "the state the worker rebuilt before the deposit"
    );
    // What each part wants, in words, for the Stage to show next to it.
    assert!(state["scene"]["intents"]["kernel"].is_string());
    assert!(state["scene"]["steps"][2]["intents"]["worker"].is_string());

    // The kernel's own UI answers through the demo, from the store the steps wrote.
    let (status, body) = demo_http(
        &addr,
        "GET",
        "/api/history?tenant=default&domain=bank&type=account&id=a1",
        false,
    );
    assert_eq!(status, 200, "{body}");
    let history: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(history["events"].as_array().unwrap().len(), 2);
    assert_eq!(history["index"]["facts"], serde_json::json!(["open"]));

    // Reset: a new kernel on a new, empty store; the old one is gone.
    let (status, _) = demo_http(&addr, "POST", "/api/demo/reset", true);
    assert_eq!(status, 200);
    let state = demo_idle(&addr, &mut demo, &dir);
    assert_eq!(state["generation"], 2);
    assert_eq!(state["traces"].as_array().unwrap().len(), 0);
    assert!(!base.join("run-1").exists());
    assert!(base.join("run-2").join("kernable.db").exists());

    // Ctrl-C ends it and removes everything.
    Process::new("kill")
        .args(["-INT", &demo.child.id().to_string()])
        .status()
        .unwrap();
    assert!(demo.child.wait().unwrap().success());
    assert!(!base.exists());
    let _ = std::fs::remove_dir_all(&dir);
}

/// What a step's commands came to: `committed` or `rejected:<code>` per command sent, the
/// outcome of each command an action lists.
#[cfg(unix)]
fn demo_outcomes(trace: &serde_json::Value) -> Vec<String> {
    let list = |key: &str| trace[key].as_array().cloned().unwrap_or_default();
    if trace["kind"] == "act" {
        return list("commands")
            .iter()
            .map(|c| c["outcome"].as_str().unwrap().to_owned())
            .collect();
    }
    list("sends")
        .iter()
        .map(|s| match s["rejection"]["code"].as_str() {
            Some(code) => format!("{}:{code}", s["outcome"].as_str().unwrap()),
            None => s["outcome"].as_str().unwrap().to_owned(),
        })
        .collect()
}

/// Every badge an action's beats put on a node.
#[cfg(unix)]
fn demo_badges(trace: &serde_json::Value) -> Vec<String> {
    trace["beats"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|b| b["badges"].as_array().unwrap().clone())
        .filter_map(|b| b["text"].as_str().map(str::to_owned))
        .collect()
}

#[cfg(unix)]
#[test]
fn every_chapter_of_the_demo_shows_what_it_says() {
    let dir = temp_dir("demo-story");
    let (mut demo, addr) = spawn_demo(&dir);
    let state = demo_idle(&addr, &mut demo, &dir);
    let steps = state["scene"]["steps"].as_array().unwrap().clone();
    let mut traces = Vec::new();
    // The step of the story whose title starts with `title`, run.
    let mut until = |title: &str| -> serde_json::Value {
        loop {
            let i = traces.len();
            let (status, body) = demo_http(&addr, "POST", "/api/demo/next", true);
            assert_eq!(status, 200, "{body}");
            let state = demo_idle(&addr, &mut demo, &dir);
            let trace = state["traces"][i].clone();
            traces.push(trace.clone());
            if steps[i]["title"].as_str().unwrap().starts_with(title) {
                return trace;
            }
        }
    };
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();

    // Usernames: refused at the commit; of two racing, exactly one.
    assert_eq!(
        demo_outcomes(&until("Bob wants ADA")),
        s(&["rejected:unique_taken"])
    );
    let mut race = demo_outcomes(&until("The race"));
    race.sort();
    assert_eq!(race, s(&["committed", "rejected:unique_taken"]));
    assert_eq!(
        demo_outcomes(&until("Ada renames")),
        s(&["committed", "committed"])
    );
    // Payments: to a closed account refused by its requirement.
    assert_eq!(
        demo_outcomes(&until("Ada pays Bob again")),
        s(&["rejected:requirement_missing"])
    );
    // Rush hour: 200 and 200, all committed.
    assert_eq!(
        demo_outcomes(&until("200 deposits into one")),
        s(&["committed"])
    );
    assert_eq!(
        demo_outcomes(&until("200 deposits into 20")),
        s(&["committed"])
    );
    // Tellers: refused before the queue, a teller passes, queries checked, fails closed.
    assert_eq!(
        demo_outcomes(&until("Ada withdraws by herself")),
        s(&["rejected:forbidden"])
    );
    assert_eq!(
        demo_outcomes(&until("A teller withdraws 20")),
        s(&["committed"])
    );
    assert_eq!(
        demo_outcomes(&until("Bob looks")),
        s(&["rejected", "committed"])
    );
    assert_eq!(
        demo_outcomes(&until("The middleware goes away")),
        s(&["rejected", "committed"])
    );
    // Outage: parked, retried, credited once.
    assert_eq!(demo_outcomes(&until("Fix it")), s(&["committed"]));
    // Leaked key: refused once revoked, the app's key goes on.
    assert_eq!(
        demo_outcomes(&until("The key leaks")),
        s(&["rejected", "committed"])
    );
    // Disaster: nothing lost, nothing twice, the store intact.
    let crash = until("kill -9");
    let detail = crash["commands"][0]["detail"].as_str().unwrap();
    assert!(detail.ends_with("0 lost, 0 twice"), "{detail}");
    assert!(demo_badges(&until("Verify")).contains(&"intact".to_owned()));
    // Forget me: the events stay, the vault and the read model hold nothing of Ada.
    until("Forget Ada");
    let state = demo_state(&addr);
    assert!(state["rows"]["people"]["p1"].is_null() && !state["rows"]["people"]["p2"].is_null());
    let (_, body) = demo_http(
        &addr,
        "GET",
        "/api/history?tenant=default&domain=bank&type=person&id=p1",
        false,
    );
    let history: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(history["events"].as_array().unwrap().len(), 2);
    assert_eq!(history["vault"], serde_json::json!({}));
    assert!(!body.contains("Lovelace"), "{body}");
    // New report: rebuilt from the events, the same rows.
    assert!(demo_badges(&until("Rebuild")).contains(&"equal".to_owned()));
    // Tampering: found.
    until("Change a deposit");
    assert!(demo_badges(&until("Verify")).contains(&"tampered".to_owned()));
    assert_eq!(traces.len(), steps.len(), "the story ran to its end");
    let state = demo_state(&addr);
    assert_eq!(state["rows"]["accounts"]["a2"]["open"], false);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_kernel_exports_to_a_key_granted_export() {
    let env = start_custom("cli-export-running", |config, dir| {
        std::fs::write(dir.join("vault.key"), hex(&[8; 32])).unwrap();
        config.vault = Some(kernable_kernel::config::VaultConfig {
            store: format!("sqlite:{}", dir.join("vault.db").display()),
            key: dir.join("vault.key"),
        });
    })
    .await;
    spawn_worker(&env).await;
    let dir = env.dir.clone();
    std::fs::write(dir.join("root.key"), hex(root().as_bytes())).unwrap();
    write_key(&dir, "ops", 9);
    write_key(&dir, "kernel", 3);
    write_key(&dir, "nobody", 10);
    let run = |args: Vec<String>| {
        let dir = dir.clone();
        tokio::task::spawn_blocking(move || {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            kernable(&dir, &args)
        })
    };
    let args = |a: &[&str]| a.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let (code, printed) = run(args(&[
        "grant",
        "root.key",
        "ops.pub",
        "export=*",
        "submit=orders.*",
    ]))
    .await
    .unwrap();
    assert_eq!(code, 0, "{printed}");
    let (code, printed) = run(args(&[
        "grant",
        "root.key",
        "nobody.pub",
        "submit=orders.*",
    ]))
    .await
    .unwrap();
    assert_eq!(code, 0, "{printed}");
    for order in ["o1", "o2", "o3"] {
        let (code, printed) = run(args(&[
            "submit",
            &env.addr,
            "ops.key",
            "t1",
            "orders",
            "Order",
            order,
            "PlaceOrder",
            "{}",
        ]))
        .await
        .unwrap();
        assert_eq!(code, 0, "{printed}");
    }

    let (code, printed) = run(args(&["export", &env.addr, "nobody.key", "denied.kx"]))
        .await
        .unwrap();
    assert_eq!(code, 1, "{printed}");
    assert!(printed.contains("needs export for *"), "{printed}");
    assert!(!dir.join("denied.kx").exists());

    let (code, printed) = run(args(&["export", &env.addr, "ops.key", "out.kx"]))
        .await
        .unwrap();
    assert_eq!(code, 0, "{printed}");
    assert!(printed.starts_with("exported "), "{printed}");

    // The kernel signed a checkpoint first: every event is anchored.
    let (backend, _) = backend();
    let store = format!("{backend}:{}", dir.join("imported").display());
    let vault = format!("sqlite:{}", dir.join("imported-vault.db").display());
    let data = format!("file:{}", dir.join("imported-data").display());
    let (code, printed) = run(args(&[
        "import",
        "out.kx",
        "--store",
        &store,
        "--vault-store",
        &vault,
        "--vault-key",
        "vault.key",
        "--data-store",
        &data,
        "--kernel-pub",
        "kernel.pub",
    ]))
    .await
    .unwrap();
    assert_eq!(code, 0, "{printed}");
    assert!(
        printed.contains("3 events, 3 commands, ") && printed.contains(" 0 events after the last"),
        "{printed}"
    );
    assert!(has_line(&printed, "OK"), "{printed}");
    let (_, imported) = run(args(&["workers", &store])).await.unwrap();
    let (_, source) = run(args(&["workers", &env.store])).await.unwrap();
    assert_eq!(imported, source);
}
