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
    match std::env::var("KERNABLE_TEST_STORE").as_deref() {
        Ok("file") => ("file", "k.log"),
        _ => ("sqlite", "k.db"),
    }
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
    let email = signing::vault_ref(&[1; 16], "k-1", false, b"\"max@firma.de\"");
    let values = Vault::open(&vault, &[7; 32])
        .unwrap()
        .get(&[email])
        .unwrap();
    assert_eq!(values[0].value, b"\"max@firma.de\"");
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

/// One request to the demo's UI: the status code and the body.
#[cfg(unix)]
fn demo_http(addr: &str, method: &str, path: &str, from_stage: bool) -> (u16, String) {
    use std::io::{Read, Write};
    let mut conn = std::net::TcpStream::connect(addr).unwrap();
    let header = if from_stage {
        "x-kernable-demo: 1\r\n"
    } else {
        ""
    };
    // In one write: the server reads the request with one read.
    let request =
        format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{header}content-length: 0\r\n\r\n");
    conn.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    conn.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split_once("\r\n\r\n").unwrap().1.to_owned();
    (status, body)
}

#[cfg(unix)]
fn demo_state(addr: &str) -> serde_json::Value {
    let (status, body) = demo_http(addr, "GET", "/api/demo", false);
    serde_json::from_str(&body).unwrap_or_else(|e| panic!("{status} {body:?}: {e}"))
}

/// Waits until the demo is ready and idle; returns its state.
#[cfg(unix)]
fn demo_idle(addr: &str) -> serde_json::Value {
    let started = std::time::Instant::now();
    loop {
        if let Ok(conn) = std::net::TcpStream::connect(addr) {
            drop(conn);
            let state = demo_state(addr);
            assert!(state["error"].is_null(), "{state}");
            if state["ready"] == true && state["busy"].is_null() {
                return state;
            }
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "the demo did not become ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn the_demo_runs_its_scene_resets_on_a_new_store_and_leaves_nothing_behind() {
    let dir = temp_dir("demo");
    let addr = free_addr();
    let demo = KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(&dir)
            .args(["demo", "--ui-listen", &addr])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null()),
    );
    let base = std::env::temp_dir().join(format!("kernable-demo-{}", demo.child.id()));
    let state = demo_idle(&addr);
    assert_eq!(state["generation"], 1);
    assert!(base.join("run-1").join("kernable.db").exists());

    // A page elsewhere cannot drive it: no header, no step.
    let (status, _) = demo_http(&addr, "POST", "/api/demo/next", false);
    assert_eq!(status, 403);

    let steps = state["scene"]["steps"].as_array().unwrap().len();
    for _ in 0..steps {
        let (status, body) = demo_http(&addr, "POST", "/api/demo/next", true);
        assert_eq!(status, 200, "{body}");
        demo_idle(&addr);
    }
    let (status, _) = demo_http(&addr, "POST", "/api/demo/next", true);
    assert_eq!(status, 409, "the scene is over");
    let state = demo_state(&addr);
    let outcomes: Vec<&str> = state["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["sends"][0]["outcome"].as_str().unwrap())
        .collect();
    assert_eq!(
        outcomes,
        ["committed", "committed", "rejected", "committed"]
    );
    assert_eq!(state["rows"]["a1"]["balance"], 70);
    let last = &state["traces"][steps - 1]["sends"][0];
    assert_eq!(
        last["answer"]["row"]["balance"], 70,
        "the client reads its write"
    );
    assert_eq!(
        last["events"][0]["position"], 3,
        "the rejection stored nothing"
    );
    assert_eq!(
        last["state"],
        serde_json::json!({ "owner": "Ada", "balance": 100, "open": true }),
        "the state the worker rebuilt before the last command"
    );

    // The kernel's own UI answers through the demo, from the store the steps wrote.
    let (status, body) = demo_http(
        &addr,
        "GET",
        "/api/history?tenant=default&domain=bank&type=account&id=a1",
        false,
    );
    assert_eq!(status, 200, "{body}");
    let history: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(history["events"].as_array().unwrap().len(), 3);
    let code: serde_json::Value =
        serde_json::from_str(&demo_http(&addr, "GET", "/api/demo/code", false).1).unwrap();
    assert!(code["regions"]["withdraw"].is_array());

    // Scene 2: a unique username, also when two commands race for it.
    let (status, _) = demo_http(&addr, "POST", "/api/demo/scene?n=2", true);
    assert_eq!(status, 200);
    let state = demo_idle(&addr);
    assert_eq!(state["scene"]["index"], 1);
    assert_eq!(state["traces"].as_array().unwrap().len(), 0, "a new store");
    let steps = state["scene"]["steps"].as_array().unwrap().len();
    for _ in 0..steps {
        let (status, body) = demo_http(&addr, "POST", "/api/demo/next", true);
        assert_eq!(status, 200, "{body}");
        demo_idle(&addr);
    }
    let state = demo_state(&addr);
    let outcomes: Vec<Vec<(&str, &str)>> = state["traces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| {
            t["sends"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| {
                    let code = s["rejection"]["code"].as_str().unwrap_or("");
                    (s["outcome"].as_str().unwrap(), code)
                })
                .collect()
        })
        .collect();
    let committed = ("committed", "");
    let taken = ("rejected", "unique_taken");
    assert_eq!(outcomes[0], [committed]);
    assert_eq!(outcomes[1], [taken], "ADA is ada");
    let mut race = outcomes[2].clone();
    race.sort();
    assert_eq!(race, [committed, taken], "exactly one wins the race");
    assert_eq!(outcomes[3], [committed]);
    assert_eq!(outcomes[4], [committed], "the rename freed ada");
    assert_eq!(state["rows"]["c2"]["username"], "ada");
    // The kernel's UI shows the key c2 holds by name and identity, never the value.
    let (_, body) = demo_http(
        &addr,
        "GET",
        "/api/history?tenant=default&domain=bank&type=customer&id=c2",
        false,
    );
    let history: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(history["index"]["keys"][0]["name"], "username");
    assert_eq!(
        history["index"]["keys"][0]["id"].as_str().unwrap().len(),
        64
    );
    assert!(!history["index"].to_string().contains("ada"));

    // Reset: a new kernel on a new, empty store; the old one is gone.
    let (status, _) = demo_http(&addr, "POST", "/api/demo/reset", true);
    assert_eq!(status, 200);
    let state = demo_idle(&addr);
    assert_eq!(state["generation"], 3, "the scene switch was a reset too");
    assert_eq!(state["scene"]["index"], 1, "a reset keeps the scene");
    assert_eq!(state["traces"].as_array().unwrap().len(), 0);
    assert_eq!(state["rows"], serde_json::json!({}));
    assert!(!base.join("run-1").exists());
    assert!(!base.join("run-2").exists());
    assert!(base.join("run-3").join("kernable.db").exists());

    // Ctrl-C ends it and removes everything.
    let mut demo = demo;
    Process::new("kill")
        .args(["-INT", &demo.child.id().to_string()])
        .status()
        .unwrap();
    assert!(demo.child.wait().unwrap().success());
    assert!(!base.exists());
    let _ = std::fs::remove_dir_all(&dir);
}
