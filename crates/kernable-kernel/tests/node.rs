//! `kernable node`: it runs the kernel from `project.toml`, starts it again when it dies
//! or the file changes, and stops it with itself.

mod common;

use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use common::*;

/// The node, and the pids of the children it says it started, as they come.
struct Node {
    process: Child,
    started: mpsc::Receiver<(String, u32)>,
}

impl Node {
    fn start(dir: &Path) -> Node {
        let mut process = Command::new(env!("CARGO_BIN_EXE_kernable"))
            .args(["node", "project.toml"])
            .current_dir(dir)
            .env("NO_COLOR", "1")
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = process.stderr.take().unwrap();
        let (tx, started) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let line = plain(&line);
                // A service's own output, passed on by the node with its name.
                let own = line.split_once(" | ").map_or("", |(_, rest)| rest);
                if let Some(rest) = own.strip_prefix("reached-kernel ") {
                    let _ = tx.send((format!("reached {rest}"), 0));
                    continue;
                }
                if !line.contains("started") {
                    continue;
                }
                let field = |name: &str| {
                    line.split_whitespace()
                        .find_map(|w| w.strip_prefix(&format!("{name}=")))
                        .map(str::to_owned)
                };
                if let (Some(child), Some(pid)) = (field("child"), field("pid")) {
                    let _ = tx.send((child, pid.parse().unwrap_or(0)));
                }
            }
        });
        Node { process, started }
    }

    /// The pid of the next start of `child`, within 180 s.
    fn next_start(&self, child: &str) -> u32 {
        loop {
            let (name, pid) = self
                .started
                .recv_timeout(Duration::from_secs(180))
                .expect("no start within 180 s (a build pulls its base image)");
            if name == child {
                return pid;
            }
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

/// `line` without ANSI colours.
fn plain(line: &str) -> String {
    let mut out = String::new();
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Where the kernel of a project in `dir` listens: on Linux the socket the node chooses in the
/// pod's own network, elsewhere a free TCP port.
fn kernel_addr(dir: &Path) -> String {
    if cfg!(target_os = "linux") {
        format!("unix:{}/run/kernel.sock", dir.display())
    } else {
        free_addr()
    }
}

/// The `listen` flag for `addr`, which the node sets itself for a socket.
fn listen(addr: &str) -> String {
    if addr.starts_with("unix:") {
        String::new()
    } else {
        format!(", listen = \"{addr}\"")
    }
}

fn project(dir: &Path, addr: &str, extra: &str, metrics: &str) {
    let listen = listen(addr);
    let text = format!(
        "[kernel]\nflags = {{ dev = true, ui-listen = \"\"{listen}{extra} }}\nstop-seconds = 5\n\n[node]\nmetrics-listen = \"{metrics}\"\n"
    );
    std::fs::write(dir.join("project.toml"), text).unwrap();
}

fn alive(pid: u32) -> bool {
    // SAFETY: signal 0 only asks whether the process exists.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_restarts_its_kernel_and_stops_it_with_itself() {
    let dir = std::env::temp_dir().join(format!("kn-node-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (addr, metrics) = (kernel_addr(&dir), free_addr());
    project(&dir, &addr, "", &metrics);
    let mut node = Node::start(&dir);
    let first = node.next_start("kernel");
    wait_for(&addr).await;

    // The kernel dies: the node starts it again.
    // SAFETY: kill(2) on the kernel the node started.
    unsafe { libc::kill(first as libc::pid_t, libc::SIGKILL) };
    let second = node.next_start("kernel");
    assert_ne!(first, second);
    wait_for(&addr).await;
    let shown = get(&metrics, "/metrics").await;
    assert!(
        shown.contains("kernable_node_child_restarts_total{child=\"kernel\"} 1"),
        "{shown}"
    );
    assert!(
        shown.contains("kernable_node_child_up{child=\"kernel\"} 1"),
        "{shown}"
    );

    // A changed flag restarts it, stopped first.
    project(&dir, &addr, ", drain-timeout-ms = 500", &metrics);
    let third = node.next_start("kernel");
    assert!(!alive(second), "the old kernel was stopped");
    wait_for(&addr).await;

    // Stopping the node stops the kernel.
    // SAFETY: SIGTERM to the node we started.
    unsafe { libc::kill(node.process.id() as libc::pid_t, libc::SIGTERM) };
    let status = node.process.wait().unwrap();
    assert!(status.success(), "{status}");
    assert!(!alive(third), "the kernel stopped with the node");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Whether `podman` runs here; the container test is skipped without it.
fn podman() -> bool {
    Command::new("podman")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_builds_and_runs_a_service_after_its_kernel() {
    if !podman() {
        eprintln!("skipped: podman is not installed");
        return;
    }
    let dir = std::env::temp_dir().join(format!("kn-node-svc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("svc")).unwrap();
    std::fs::write(
        dir.join("svc/Containerfile"),
        // It waits until the kernel answers at KERNABLE_URL: its socket in the pod's own
        // network, or a port on the host's.
        "FROM docker.io/library/alpine:3.20\nCMD [\"sh\", \"-c\", \"case $KERNABLE_URL in unix:*) until [ -S ${KERNABLE_URL#unix:} ]; do sleep 0.2; done;; *) until nc -z ${KERNABLE_URL%:*} ${KERNABLE_URL##*:}; do sleep 0.2; done;; esac; echo reached-kernel instance=$KERNABLE_INSTANCE >&2; exec sleep 600\"]\n",
    )
    .unwrap();
    let addr = kernel_addr(&dir);
    let listen = listen(&addr);
    let text = format!(
        "[kernel]\nflags = {{ dev = true, ui-listen = \"\"{listen} }}\nstop-seconds = 5\n\n[node]\ninstance = \"node-a\"\nproject = \"t{}\"\n\n[[service]]\nname = \"echo\"\nbuild = \"svc\"\nstop-seconds = 5\n",
        std::process::id()
    );
    std::fs::write(dir.join("project.toml"), text).unwrap();
    let mut node = Node::start(&dir);
    node.next_start("kernel");
    let service = node.next_start("echo");
    assert!(alive(service));
    wait_for(&addr).await;
    // On Linux the service reaches its kernel's socket; on a Mac it runs in Podman's VM, apart.
    if cfg!(target_os = "linux") {
        node.next_start("reached instance=node-a");
    }

    // SAFETY: SIGTERM to the node we started.
    unsafe { libc::kill(node.process.id() as libc::pid_t, libc::SIGTERM) };
    let status = node.process.wait().unwrap();
    assert!(status.success(), "{status}");
    let ps = Command::new("podman")
        .args(["ps", "--all", "--format", "{{.Names}}"])
        .output()
        .unwrap();
    let names = String::from_utf8_lossy(&ps.stdout);
    assert!(
        !names.contains(&format!("kernable-t{}-echo", std::process::id())),
        "the container is gone: {names}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Linux only: a killed node takes its children along. The kernel runs in the instance's pod
/// there, which the node's watchdog removes once the node is gone.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_killed_node_takes_its_kernel_along() {
    let dir = std::env::temp_dir().join(format!("kn-node-killed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (addr, metrics) = (kernel_addr(&dir), free_addr());
    project(&dir, &addr, "", &metrics);
    let mut node = Node::start(&dir);
    let kernel = node.next_start("kernel");
    wait_for(&addr).await;
    // SAFETY: SIGKILL to the node we started: it cannot stop its children itself.
    unsafe { libc::kill(node.process.id() as libc::pid_t, libc::SIGKILL) };
    let _ = node.process.wait();
    for _ in 0..600 {
        if !alive(kernel) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!alive(kernel), "the kernel ended with its node");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A kernel update: the project names another kernel binary; the node starts the new
/// kernel, which waits for the stores, and the old one hands them over. A client that commits
/// one command after the other sees a short gap and loses nothing.
#[cfg(not(target_os = "linux"))]
#[tokio::test(flavor = "multi_thread")]
async fn a_new_kernel_binary_takes_the_stores_over_while_writes_go_on() {
    let dir = std::env::temp_dir().join(format!("kn-node-update-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for version in ["a", "b"] {
        std::fs::create_dir_all(dir.join(version)).unwrap();
        std::fs::copy(
            env!("CARGO_BIN_EXE_kernable"),
            dir.join(version).join("kernable"),
        )
        .unwrap();
    }
    std::fs::write(dir.join("kernel.key"), hex(key(3).as_bytes())).unwrap();
    let addr = free_addr();
    let write = |version: &str| {
        std::fs::write(
            dir.join("project.toml"),
            format!(
                "[kernel]\nbinary = \"{version}/kernable\"\nflags = {{ root = \"{}\", kernel-key = \"kernel.key\", store = \"sqlite:k.db\", ui-listen = \"\", listen = \"{addr}\", drain-timeout-ms = 2000 }}\nstop-seconds = 5\n",
                id(&root())
            ),
        )
        .unwrap();
    };
    write("a");
    let node = Node::start(&dir);
    let first = node.next_start("kernel");
    wait_for(&addr).await;
    // A worker that connects again after it lost its kernel, as the SDKs' workers do.
    tokio::spawn({
        let addr = addr.clone();
        async move {
            loop {
                run_worker(addr.clone(), TestWorker::honest()).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    });

    // One command after the other; a command that meets no kernel is sent again (by its ID).
    let writer = tokio::spawn({
        let addr = addr.clone();
        async move {
            let (mut last, mut longest) = (std::time::Instant::now(), Duration::ZERO);
            for i in 0..400 {
                let cmd = signed(
                    command(&format!("c{i}"), "PlaceOrder", &format!("o{i}")),
                    &key(1),
                );
                loop {
                    if let Ok(channel) = kernable_core::transport::connect(&addr).await
                        && submit_on(channel, cmd.clone(), kernable_core::proto::Wait::Committed)
                            .await
                            .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                longest = longest.max(last.elapsed());
                last = std::time::Instant::now();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            longest
        }
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    write("b");
    let second = node.next_start("kernel");
    assert_ne!(first, second);
    let longest = writer.await.unwrap();
    eprintln!("the longest time between two commits across the update: {longest:?}");
    assert!(!alive(first), "the old kernel handed over and ended");
    assert!(alive(second), "the new kernel serves");
    assert!(longest < Duration::from_secs(3), "{longest:?}");
    // Every command is in the store, once.
    drop(node);
    let store =
        kernable_store::open_read_only(&format!("sqlite:{}", dir.join("k.db").display())).unwrap();
    for i in 0..400 {
        assert!(store.result(&format!("c{i}")).unwrap().is_some(), "c{i}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
