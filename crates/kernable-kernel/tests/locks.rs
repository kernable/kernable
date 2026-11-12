//! One kernel per store, with the real binary: a second `kernable start` on the stores of a
//! running kernel is refused before it opens anything (the event store's lock, taken first,
//! keeps the vault and the snapshots out too), the first one serves on; a second kernel with
//! an event store of its own but the vault or the snapshots of a running one is refused by
//! their own locks; and a kernel killed with `kill -9` leaves no lock behind, so a restart on
//! its stores serves at once.

mod common;

use std::{
    path::{Path, PathBuf},
    process::{Command as Process, Stdio},
    time::{Duration, SystemTime},
};

use common::*;

/// The flags of a kernel on the stores in `dir`, with a vault, the snapshots and the metrics
/// at `metrics`.
fn flags(dir: &Path, metrics: &str) -> Vec<String> {
    let path = |name: &str| dir.join(name).display().to_string();
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    if !dir.join("vault.key").exists() {
        std::fs::write(dir.join("vault.key"), hex(&[9; 32])).unwrap();
    }
    [
        "start",
        "--root",
        &id(&root()),
        "--store",
        &store_url(dir),
        "--listen",
        &format!("unix:{}", path("k.sock")),
        "--kernel-key",
        &path("kernel.key"),
        "--vault-key",
        &path("vault.key"),
        "--vault-store",
        &format!("sqlite:{}", path("vault.db")),
        "--snapshots",
        &format!("sqlite:{}", path("snapshots.db")),
        "--metrics-listen",
        metrics,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn spawn(dir: &Path, flags: &[String]) -> KillOnDrop {
    KillOnDrop::spawn(
        Process::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(dir)
            .args(flags)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
}

/// What a file is now: its bytes and when it last changed.
fn state(file: &PathBuf) -> (Vec<u8>, SystemTime) {
    (
        std::fs::read(file).unwrap(),
        std::fs::metadata(file).unwrap().modified().unwrap(),
    )
}

async fn healthy(metrics: &str) {
    let health = get(metrics, "/health").await;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// Commits one order, so the event store, the snapshots and the vault are in use.
async fn place(addr: &str, i: u32) {
    let cmd = signed(
        command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
        &key(1),
    );
    committed_versions(&submit(addr, cmd).await.unwrap());
}

/// Starts a second kernel with `flags` (its own metrics address) and returns what it said
/// on stderr; it must stop by itself, refused.
async fn second_kernel(dir: &Path, flags: &[String]) -> String {
    let mut flags = flags.to_vec();
    let at = flags.len() - 1;
    flags[at] = free_addr();
    let second = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_kernable"))
            .current_dir(dir)
            .args(&flags)
            .output(),
    )
    .await
    .expect("the second kernel stops by itself")
    .unwrap();
    assert!(!second.status.success());
    String::from_utf8_lossy(&second.stderr).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_kernel_on_the_same_stores_is_refused_and_the_first_serves_on() {
    let dir = Dir::new("locks-second");
    let metrics = free_addr();
    let flags = flags(&dir.0, &metrics);
    let addr = format!("unix:{}", dir.0.join("k.sock").display());
    let _first = spawn(&dir.0, &flags);
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    place(&addr, 0).await;

    let said = second_kernel(&dir.0, &flags).await;
    assert!(
        said.contains("in use by another process"),
        "a clear error: {said}"
    );
    // The first serves on, on its socket (the second never replaced it).
    healthy(&metrics).await;
    place(&addr, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_kernel_never_opens_the_vault_or_the_snapshots_of_a_running_one() {
    let dir = Dir::new("locks-vault");
    let metrics = free_addr();
    let flags = flags(&dir.0, &metrics);
    let addr = format!("unix:{}", dir.0.join("k.sock").display());
    let _first = spawn(&dir.0, &flags);
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    place(&addr, 0).await;
    let (vault, snapshots) = (dir.0.join("vault.db"), dir.0.join("snapshots.db"));
    let before = (state(&vault), state(&snapshots));
    // A little later, so a write would show in the time as well as the bytes.
    tokio::time::sleep(Duration::from_millis(20)).await;

    second_kernel(&dir.0, &flags).await;
    // The event store's lock is taken first: the refused kernel wrote neither file.
    assert_eq!((state(&vault), state(&snapshots)), before);
    healthy(&metrics).await;
}

/// `flags` with `flag`'s value replaced.
fn with(flags: &[String], flag: &str, value: String) -> Vec<String> {
    let mut flags = flags.to_vec();
    let at = flags.iter().position(|f| f == flag).unwrap() + 1;
    flags[at] = value;
    flags
}

#[tokio::test(flavor = "multi_thread")]
async fn the_vault_and_the_snapshots_have_locks_of_their_own() {
    let dir = Dir::new("locks-own");
    let metrics = free_addr();
    let flags = flags(&dir.0, &metrics);
    let addr = format!("unix:{}", dir.0.join("k.sock").display());
    let _first = spawn(&dir.0, &flags);
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    place(&addr, 0).await;
    let (vault, snapshots) = (dir.0.join("vault.db"), dir.0.join("snapshots.db"));
    let before = (state(&vault), state(&snapshots));
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Another event store and socket, the same snapshots (and vault): refused at the snapshots.
    let path = |name: &str| dir.0.join(name).display().to_string();
    let other = with(&flags, "--store", format!("sqlite:{}", path("other.db")));
    let other = with(&other, "--listen", format!("unix:{}", path("other.sock")));
    let said = second_kernel(&dir.0, &other).await;
    assert!(
        said.contains("the snapshot store is in use by another process"),
        "a clear error: {said}"
    );
    // Its own snapshots too, the same vault: refused at the vault.
    let other = with(
        &other,
        "--snapshots",
        format!("sqlite:{}", path("other-snapshots.db")),
    );
    let said = second_kernel(&dir.0, &other).await;
    assert!(
        said.contains("the vault is in use by another process"),
        "a clear error: {said}"
    );
    // Neither refused kernel wrote the first one's files, and the first serves on.
    assert_eq!((state(&vault), state(&snapshots)), before);
    healthy(&metrics).await;
    place(&addr, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kernel_killed_with_sigkill_leaves_its_stores_free_for_a_restart() {
    let dir = Dir::new("locks-killed");
    let metrics = free_addr();
    let flags = flags(&dir.0, &metrics);
    let addr = format!("unix:{}", dir.0.join("k.sock").display());
    let mut first = spawn(&dir.0, &flags);
    wait_for(&addr).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    place(&addr, 0).await;

    // kill -9: no shutdown, no unlock.
    first.child.kill().unwrap();
    first.child.wait().unwrap();
    drop(first);

    let _again = spawn(&dir.0, &flags);
    wait_for(&addr).await;
    healthy(&metrics).await;
    start_worker(addr.clone(), TestWorker::honest()).await;
    // What the killed kernel committed is there: a resubmit gets its outcome.
    let cmd = signed(command("c0", "PlaceOrder", "o0"), &key(1));
    assert_eq!(committed_versions(&submit(&addr, cmd).await.unwrap()), [1]);
    place(&addr, 1).await;
}
