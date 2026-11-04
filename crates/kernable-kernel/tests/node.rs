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

    /// The pid of the next start of `child`, within 10 s.
    fn next_start(&self, child: &str) -> u32 {
        loop {
            let (name, pid) = self
                .started
                .recv_timeout(Duration::from_secs(10))
                .expect("no start within 10 s");
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

fn project(dir: &Path, addr: &str, extra: &str) {
    let text = format!(
        "[kernel]\nflags = {{ dev = true, listen = \"{addr}\", ui-listen = \"\"{extra} }}\nstop-seconds = 5\n"
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
    let addr = free_addr();
    project(&dir, &addr, "");
    let mut node = Node::start(&dir);
    let first = node.next_start("kernel");
    wait_for(&addr).await;

    // The kernel dies: the node starts it again.
    // SAFETY: kill(2) on the kernel the node started.
    unsafe { libc::kill(first as libc::pid_t, libc::SIGKILL) };
    let second = node.next_start("kernel");
    assert_ne!(first, second);
    wait_for(&addr).await;

    // A changed flag restarts it, stopped first.
    project(&dir, &addr, ", drain-timeout-ms = 500");
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
