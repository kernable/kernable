"""End to end: Python signs commands and runs the worker; the Rust kernel verifies everything.

Run: .venv/bin/python -m unittest test_order.py   (after `cargo build`)
"""

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

import grpc

from worker import Key, channel, command_message, pb, rpc, run

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN = os.path.join(ROOT, "target", "debug")
VECTORS = os.path.join(ROOT, "proto", "test-vectors", "signing.json")


class SigningVectors(unittest.TestCase):
    def test_command_message_and_signature_match(self):
        with open(VECTORS) as f:
            v = json.load(f)
        c = v["command"]["input"]
        cmd = pb.Command(**{k: c[k] for k in ("command_id", "tenant_id", "aggregate_type", "aggregate_id", "command_type")},
                         payload=bytes.fromhex(c["payload_hex"]))
        key = Key(v["key"]["seed"])
        self.assertEqual(key.id, v["key"]["key_id"])
        self.assertEqual(command_message(cmd).hex(), v["command"]["message"])
        self.assertEqual(key.sign(command_message(cmd)).sig.hex(), v["command"]["signature"])


class OrderEndToEnd(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="kn-", dir="/tmp")  # short: Unix socket path limit
        self.url = f"unix:{self.dir}/k.sock"
        seeds = {name: os.urandom(32).hex() for name in ("kernel", "app")}
        for name, seed in seeds.items():
            with open(f"{self.dir}/{name}.key", "w") as f:
                f.write(seed)
            with open(f"{self.dir}/{name}.pub", "w") as f:
                f.write(Key(seed).id)
        self.key = Key(seeds["app"])
        with open(f"{self.dir}/kernable.toml", "w") as f:
            f.write(f'db = "k.db"\nlisten = "{self.url}"\nkernel_key = "kernel.key"\ncheckpoint_interval_ms = 50\n'
                    f'[clients]\n"{self.key.id}" = ["*"]\n'
                    f'[workers]\n"{self.key.id}" = ["PlaceOrder", "AddItem", "CancelOrder", "ShipOrder"]\n')
        self.kernel = subprocess.Popen([f"{BIN}/kernable-kernel", f"{self.dir}/kernable.toml"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.channel = channel(self.url)
        grpc.channel_ready_future(self.channel).result(timeout=5)
        ready = threading.Event()
        threading.Thread(target=run, args=(self.url, self.key, ready), daemon=True).start()
        self.assertTrue(ready.wait(5), "worker did not register")

    def tearDown(self):
        self.kernel.kill()
        self.kernel.wait()
        shutil.rmtree(self.dir, ignore_errors=True)

    def submit(self, aggregate_id: str, command_type: str, data: dict | None = None):
        cmd = pb.Command(command_id=str(uuid.uuid4()), tenant_id="t1", aggregate_type="Order",
                         aggregate_id=aggregate_id, command_type=command_type, payload=json.dumps(data or {}).encode())
        cmd.client_sig.CopyFrom(self.key.sign(command_message(cmd)))
        return rpc.CommandServiceStub(self.channel).Submit(cmd, timeout=10)

    def test_order_lifecycle_and_rust_verification(self):
        placed = self.submit("o1", "PlaceOrder", {"customerId": "k-1"})
        self.assertEqual([e.version for e in placed.committed.events], [1])
        self.submit("o1", "AddItem", {"sku": "a", "qty": 2})
        shipped = self.submit("o1", "ShipOrder")
        self.assertEqual(shipped.committed.events[0].event_type, "OrderShipped")

        late = self.submit("o1", "CancelOrder")
        self.assertEqual(late.WhichOneof("outcome"), "rejected")
        self.assertEqual(late.rejected.message, "Cannot CancelOrder on a shipped order")

        time.sleep(0.15)  # let the kernel write a checkpoint
        out = subprocess.run([f"{BIN}/kernable", "verify", f"{self.dir}/k.db", f"{self.dir}/kernel.pub"],
                             capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stdout)
        self.assertTrue(out.stdout.strip().endswith("OK"))


if __name__ == "__main__":
    unittest.main()
