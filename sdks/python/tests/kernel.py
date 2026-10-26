"""Test helper: runs the real kernel binary (`cargo build` first) in a temp dir."""

import os
import shutil
import socket
import subprocess
import tempfile
import time

from kernable import Key

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(ROOT, "target", "debug")


class TestKernel:
    def __init__(self, command_types: list[str], extra_config: str = ""):
        self.dir = tempfile.mkdtemp(prefix="kn-", dir="/tmp")  # short: Unix socket path limit
        self.socket = f"{self.dir}/k.sock"
        self.url = f"unix:{self.socket}"
        #: One key for submitting, the worker and listeners.
        self.app_key = f"{self.dir}/app.key"
        self.store = (f"file:{self.dir}/k.log" if os.environ.get("KERNABLE_TEST_STORE") == "file"
                      else f"sqlite:{self.dir}/k.db")
        ids = {name: self._write_key(name) for name in ("kernel", "app")}
        types = ", ".join(f'"{t}"' for t in command_types)
        with open(f"{self.dir}/kernable.toml", "w") as f:
            f.write(f'store = "{self.store}"\nlisten = "{self.url}"\nkernel_key = "kernel.key"\n'
                    f"checkpoint_interval_ms = 50\n{extra_config}\n"
                    f'[clients]\n"{ids["app"]}" = ["*"]\n'
                    f'[workers]\n"{ids["app"]}" = [{types}]\n'
                    f'[listeners]\n"{ids["app"]}" = ["*"]\n'
                    f'[queriers]\n"{ids["app"]}" = ["*"]\n'
                    f'[query_handlers]\n"{ids["app"]}" = ["*"]\n')
        self.process: subprocess.Popen | None = None

    def _write_key(self, name: str) -> str:
        seed = os.urandom(32).hex()
        with open(f"{self.dir}/{name}.key", "w") as f:
            f.write(seed)
        with open(f"{self.dir}/{name}.pub", "w") as f:
            f.write(Key(seed).id)
        return Key(seed).id

    def start(self) -> None:
        self.process = subprocess.Popen([f"{BIN}/kernable-kernel", f"{self.dir}/kernable.toml"],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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

    def verify(self) -> subprocess.CompletedProcess:
        return subprocess.run([f"{BIN}/kernable", "verify", self.store, f"{self.dir}/kernel.pub"],
                              capture_output=True, text=True)

    def dispose(self) -> None:
        self.kill()
        shutil.rmtree(self.dir, ignore_errors=True)
