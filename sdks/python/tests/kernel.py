"""Test helper: runs the real kernel binary (`cargo build` first) in a temp dir."""

import os
import shutil
import socket
import subprocess
import tempfile
import time


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "debug")


class TestKernel:
    def __init__(self, command_types: list[str], extra_flags: list[str] | None = None,
                 grants: list[str] | None = None):
        """The app key may run `command_types` and do everything else; `extra_flags` go to
        `kernable start`, e.g. `["--lease-ttl-ms", "500"]`; `grants` are more permissions for it,
        e.g. `["shop.*"]` for every action in the domain shop (files and cache too)."""
        self.dir = tempfile.mkdtemp(prefix="kn-", dir="/tmp")  # short: Unix socket path limit
        self.socket = f"{self.dir}/k.sock"
        self.url = f"unix:{self.socket}"
        #: One key for submitting, the worker and listeners; its grant lies next to it.
        self.app_key = f"{self.dir}/app.key"
        self.store = (f"file:{self.dir}/k.log" if os.environ.get("KERNABLE_TEST_STORE") == "file"
                      else f"sqlite:{self.dir}/k.db")
        for name in ("root", "kernel", "app"):
            self._kernable("keys", "new", name)
        actions = ("submit", "subscribe", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage")
        self._kernable("grant", "root.key", "app.pub", f"run={','.join(command_types)}",
                       *(f"{a}=*" for a in actions), *(grants or []))
        with open(f"{self.dir}/vault.key", "w") as f:
            f.write(os.urandom(32).hex())
        self.flags = ["--root", "root.pub", "--store", self.store, "--listen", self.url,
                      "--checkpoint-interval-ms", "50", "--vault-key", "vault.key", *(extra_flags or [])]
        self.process: subprocess.Popen | None = None
        self._watchdog: subprocess.Popen | None = None

    def _kernable(self, *args: str) -> str:
        return subprocess.run([f"{BIN}/kernable", *args], cwd=self.dir, check=True,
                              capture_output=True, text=True).stdout

    def start(self) -> None:
        self.process = subprocess.Popen([f"{BIN}/kernable", "start", *self.flags], cwd=self.dir,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self._watchdog = _watchdog(self.process.pid)
        for _ in range(200):
            try:
                with socket.socket(socket.AF_UNIX) as s:
                    s.connect(self.socket)
                return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("kernel did not start")

    def kill(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait()
        if self._watchdog:
            self._watchdog.kill()
            self._watchdog.wait()
            if self._watchdog.stdin:
                self._watchdog.stdin.close()
            self._watchdog = None

    def verify(self) -> subprocess.CompletedProcess:
        return subprocess.run([f"{BIN}/kernable", "verify", self.store, f"{self.dir}/kernel.pub"],
                              capture_output=True, text=True)

    def dispose(self) -> None:
        self.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


def _watchdog(pid: int) -> subprocess.Popen:
    """A shell reading a pipe from this process. However this process ends (a timeout, Ctrl-C,
    a crash), the pipe closes and the shell kills the kernel `pid`; kill the watchdog once the
    kernel is gone."""
    return subprocess.Popen(["sh", "-c", f"read _; kill -9 {pid} 2>/dev/null"],
                            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
