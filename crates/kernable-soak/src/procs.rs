//! The processes the soak runs (the kernel and one app per SDK): started with their output
//! appended to a log file, killed, stopped, resumed, and restarted when they exit. A watchdog
//! per process (a shell reading a pipe from the soak) kills it when the soak ends, however it
//! ends.

use std::{
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

pub struct Proc {
    pub name: String,
    program: PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
    cwd: PathBuf,
    log: PathBuf,
    child: Option<(Child, Child)>,
    /// Stopped with SIGSTOP: not restarted while stopped.
    pub paused: bool,
}

impl Proc {
    pub fn new(
        name: &str,
        program: impl Into<PathBuf>,
        cwd: impl Into<PathBuf>,
        log: PathBuf,
    ) -> Proc {
        Proc {
            name: name.into(),
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            cwd: cwd.into(),
            log,
            child: None,
            paused: false,
        }
    }

    pub fn args(mut self, args: &[&str]) -> Proc {
        self.args.extend(args.iter().map(|a| (*a).to_owned()));
        self
    }

    pub fn env(mut self, name: &str, value: &str) -> Proc {
        self.env.push((name.into(), value.into()));
        self
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        writeln!(log, "---- {} start", crate::now())?;
        let child = Command::new(&self.program)
            .args(&self.args)
            .envs(self.env.iter().cloned())
            .current_dir(&self.cwd)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        let watchdog = Command::new("sh")
            .arg("-c")
            .arg(format!("read _; kill -9 {} 2>/dev/null", child.id()))
            .stdin(Stdio::piped())
            .spawn()?;
        self.child = Some((child, watchdog));
        self.paused = false;
        Ok(())
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|(c, _)| c.id())
    }

    fn signal(&self, signal: &str) {
        if let Some(pid) = self.pid() {
            let _ = Command::new("kill")
                .args([signal, &pid.to_string()])
                .status();
        }
    }

    /// `kill -9`.
    pub fn kill(&mut self) {
        if self.paused {
            self.signal("-CONT");
        }
        if let Some((mut child, mut watchdog)) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
            let _ = watchdog.kill();
            let _ = watchdog.wait();
        }
        self.paused = false;
    }

    /// SIGTERM, then `kill -9` after `grace`.
    pub fn terminate(&mut self, grace: Duration) {
        self.signal("-TERM");
        let started = Instant::now();
        while started.elapsed() < grace {
            if let Some((child, _)) = self.child.as_mut()
                && !matches!(child.try_wait(), Ok(None))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.kill();
    }

    pub fn pause(&mut self) {
        self.signal("-STOP");
        self.paused = true;
    }

    pub fn resume(&mut self) {
        self.signal("-CONT");
        self.paused = false;
    }

    /// How it ended, if it was running and has ended since.
    pub fn exited(&mut self) -> Option<ExitStatus> {
        let (child, _) = self.child.as_mut()?;
        let status = child.try_wait().ok()??;
        self.kill();
        Some(status)
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
    }
}
