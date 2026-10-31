//! Test helper: runs the real kernel binary in a temp dir, as the TypeScript SDK's tests do.
#![allow(dead_code)] // each test file uses a different subset

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Mutex, Once},
    time::Duration,
};

use kernable::{Kernable, Options};

/// The workspace's `kernable` binary, built once per test process so it matches the sources.
fn binary() -> PathBuf {
    static BUILD: Once = Once::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    BUILD.call_once(|| {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "-q", "--workspace", "--bin", "kernable"])
            .current_dir(&root)
            .status()
            .expect("cargo runs");
        assert!(status.success(), "the kernel builds");
    });
    let target =
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| root.join("target"), PathBuf::from);
    target.join("debug").join("kernable")
}

pub struct TestKernel {
    pub dir: PathBuf,
    pub url: String,
    /// One key for submitting commands, running workers and listening; its grant lies next to it.
    pub app_key: PathBuf,
    store: String,
    flags: Vec<String>,
    /// The kernel and its watchdog.
    child: Mutex<Option<(Child, Child)>>,
}

impl TestKernel {
    /// The app key may run `command_types` (`Orders.*`, `Orders.PlaceOrder`), use files and the
    /// cache of `shop`, and do everything else (also apply memberships). `extra` goes to `kernable start`, e.g.
    /// `["--lease-ttl-ms", "500"]`.
    pub fn new(command_types: &[&str], extra: &[&str]) -> TestKernel {
        // Short: Unix socket paths are limited to ~104 bytes.
        let dir = PathBuf::from(format!("/tmp/kn-rs-{}-{}", std::process::id(), unique()));
        std::fs::create_dir_all(&dir).unwrap();
        let kernable = |args: &[&str]| {
            let out = Command::new(binary())
                .args(args)
                .current_dir(&dir)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "kernable {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        for name in ["root", "kernel", "app"] {
            kernable(&["keys", "new", name]);
        }
        let run = format!("run={}", command_types.join(","));
        let mut grant = vec!["grant", "root.key", "app.pub", run.as_str()];
        let everything: Vec<String> = [
            "submit",
            "subscribe",
            "listen",
            "query",
            "answer",
            "check",
            "vault_erase",
            "vault_read",
            "manage",
        ]
        .iter()
        .map(|a| format!("{a}=*"))
        .collect();
        grant.extend(everything.iter().map(String::as_str));
        // Files and the cache in the `shop` domain only, so other domains are refused.
        grant.extend(["data_read=shop.*", "data_write=shop.*", "cache=shop.*"]);
        kernable(&grant);
        let mut vault_key = [0u8; 32];
        getrandom_fill(&mut vault_key);
        std::fs::write(dir.join("vault.key"), hex(&vault_key)).unwrap();
        let store = match std::env::var("KERNABLE_TEST_STORE").as_deref() {
            Ok("file") => format!("file:{}", dir.join("k.log").display()),
            _ => format!("sqlite:{}", dir.join("k.db").display()),
        };
        let url = format!("unix:{}", dir.join("kernable.sock").display());
        let mut flags: Vec<String> = [
            "--root",
            "root.pub",
            "--store",
            &store,
            "--checkpoint-interval-ms",
            "50",
            "--vault-key",
            "vault.key",
            "--listen",
            &url,
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        flags.extend(extra.iter().map(|s| (*s).to_owned()));
        TestKernel {
            app_key: dir.join("app.key"),
            dir,
            url,
            store,
            flags,
            child: Mutex::new(None),
        }
    }

    pub async fn start(&self) {
        let child = Command::new(binary())
            .arg("start")
            .args(&self.flags)
            .current_dir(&self.dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let watchdog = watchdog(child.id());
        *self.child.lock().unwrap() = Some((child, watchdog));
        let socket = self.dir.join("kernable.sock");
        let tcp = self.url.strip_prefix("https://").map(str::to_owned);
        for _ in 0..250 {
            let up = match &tcp {
                Some(addr) => std::net::TcpStream::connect(addr).is_ok(),
                None => std::os::unix::net::UnixStream::connect(&socket).is_ok(),
            };
            if up {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("kernel did not start");
    }

    /// Like [`TestKernel::new`], listening on 127.0.0.1 over mutual TLS; `tls()` has the app's
    /// certificate files.
    pub fn with_tls(command_types: &[&str]) -> TestKernel {
        let mut kernel = TestKernel::new(command_types, &[]);
        let status = Command::new(binary())
            .args(["certs", "new", ".", "app"])
            .current_dir(&kernel.dir)
            .output()
            .unwrap()
            .status;
        assert!(status.success());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let at = kernel.flags.iter().position(|f| f == "--listen").unwrap();
        kernel.flags[at + 1] = addr.clone();
        kernel.flags.extend(
            [
                "--tls-cert",
                "kernel.tls.crt",
                "--tls-key",
                "kernel.tls.key",
                "--tls-client-ca",
                "ca.tls.crt",
            ]
            .map(String::from),
        );
        kernel.url = format!("https://{addr}");
        kernel
    }

    /// The app's certificate files, for a kernel made with [`TestKernel::with_tls`].
    pub fn tls(&self) -> kernable::Tls {
        kernable::Tls {
            ca: self.dir.join("ca.tls.crt"),
            cert: self.dir.join("app.tls.crt"),
            key: self.dir.join("app.tls.key"),
        }
    }

    /// Kills the kernel (SIGKILL).
    pub fn kill(&self) {
        if let Some((mut child, mut watchdog)) = self.child.lock().unwrap().take() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = watchdog.kill();
            let _ = watchdog.wait();
        }
    }

    /// A client with the app key.
    pub fn client(&self) -> Kernable {
        Kernable::new(self.options()).unwrap()
    }

    pub fn options(&self) -> Options {
        Options::new(&self.app_key).url(&self.url)
    }

    /// Output of `kernable verify` on the event log.
    pub fn verify(&self) -> String {
        let out = Command::new(binary())
            .args([
                "verify",
                &self.store,
                &self.dir.join("kernel.pub").display().to_string(),
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }
}

impl Drop for TestKernel {
    fn drop(&mut self) {
        self.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn unique() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

fn getrandom_fill(b: &mut [u8]) {
    // Tests need no strong randomness; the time is enough to tell runs apart.
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    for (i, x) in b.iter_mut().enumerate() {
        *x = (t >> (i % 16 * 8)) as u8 ^ i as u8;
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Waits up to two seconds for `done`.
pub async fn eventually(mut done: impl FnMut() -> bool) {
    for _ in 0..100 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A shell reading a pipe from this process. However this process ends (a timeout, Ctrl-C),
/// the pipe closes and the shell kills the kernel `pid`; kill the watchdog once it is gone.
fn watchdog(pid: u32) -> Child {
    Command::new("sh")
        .args(["-c", &format!("read _; kill -9 {pid} 2>/dev/null")])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}
