"""Partitions: two kernels share the tenants by a map. One client, given only the first,
sends each tenant's commands to its partition, runs its worker on both, and a read model with a
store sees the events of both, each partition's position kept apart.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import json
import os
import shutil
import threading
import time
import unittest
from unittest import mock

import grpc

from kernable import Kernable, domain
from tests.kernel import TestKernel
from tests.test_sdk import counter, counters_domain


class Partitions(unittest.TestCase):
    def test_one_client_serves_the_tenants_of_two_partitions(self):
        p1 = TestKernel(["counters.*"], ["--partition", "p1"])
        self.addCleanup(p1.dispose)
        p1.cli("keys", "new", "node")
        p1.cli("grant", "root.key", "node.pub", "replicate=*")
        p2 = TestKernel(["counters.*"], ["--partition", "p2", "--control", p1.url, "--node-key", "node.key"])
        self.addCleanup(p2.dispose)
        for f in ("root.pub", "app.key", "app.pub", "app.grant", "node.key", "node.grant"):
            shutil.copy(f"{p1.dir}/{f}", f"{p2.dir}/{f}")
        p1.start()
        p2.start()
        with open(f"{p1.dir}/map.json", "w") as f:
            json.dump({"epoch": 1, "partitions": {"p1": [p1.url], "p2": [p2.url]}, "default": "p1",
                       "control": "p1", "tenants": {"acme": "p2"}}, f)
        applied = p1.cli("partitions", "apply", p1.url, "app.key", "map.json")
        self.assertEqual(applied.returncode, 0, applied.stderr)

        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"}):
            k = Kernable(p1.url, p1.app_key, tenant="t1")
        self.addCleanup(k.close)
        totals = counters_domain.read_model("totals", store=f"sqlite:{p1.dir}/totals.db", reconnect_s=0.05)

        @totals.on("Incremented")
        def _(data, evt, ctx):
            ctx.store.set("totals", evt.tenant, (ctx.store.get("totals", evt.tenant) or 0) + data["by"])

        worker = k.worker(counter)
        listener = k.listener(totals)
        for stream in (worker, listener):
            threading.Thread(target=stream.run, daemon=True).start()
            self.addCleanup(stream.stop)
        self.assertTrue(worker.ready.wait(10), "the worker reached both partitions")
        self.assertTrue(listener.ready.wait(10), "the read model reached both partitions")

        # p2 takes the map from p1 within a second or two; until then it refuses acme.
        for _ in range(100):
            try:
                k.submit("counters", "Counter", "c1", "Increment", {"by": 5}, tenant="acme", id="acme-1")
                break
            except grpc.RpcError:
                time.sleep(0.05)
        else:
            self.fail("acme's command never reached partition p2")
        k.submit("counters", "Counter", "c1", "Increment", {"by": 2}, id="t1-1")
        for _ in range(250):
            store = listener.store
            if store and store.get("totals", "acme") == 5 and store.get("totals", "t1") == 2:
                break
            time.sleep(0.02)
        else:
            self.fail("the read model did not see both partitions")
        self.assertGreater(listener.store.committed_for("p1"), 0)
        self.assertGreater(listener.store.committed_for("p2"), 0)


if __name__ == "__main__":
    unittest.main()
