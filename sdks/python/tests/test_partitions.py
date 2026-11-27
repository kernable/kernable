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
        p2 = TestKernel(["counters.*"], ["--partition", "p2", "--control", p1.url, "--instance-key", "node.key"])
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
        seen: list[str] = []
        subscription = k.subscribe(lambda e: seen.append(e.tenant), types=["counters.Incremented"], reconnect_s=0.05)
        self.addCleanup(subscription.stop)
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
        for _ in range(250):
            if "acme" in seen and "t1" in seen:
                break
            time.sleep(0.02)
        else:
            self.fail("the subscription did not see both partitions")
        self.assertGreater(listener.store.committed_for("p1"), 0)
        self.assertGreater(listener.store.committed_for("p2"), 0)

        # t1 moves to p2 with its events; the client finds it there.
        k.submit("counters", "Counter", "c1", "Increment", {"by": 3}, id="t1-2")
        moved = p1.cli("partitions", "move", p1.url, "app.key", "t1", "p2")
        self.assertEqual(moved.returncode, 0, moved.stdout + moved.stderr)
        k.submit("counters", "Counter", "c1", "Increment", {"by": 4}, id="t1-3")
        for _ in range(250):
            if listener.store.get("totals", "t1") == 9:
                break
            time.sleep(0.02)
        else:
            self.fail("the running read model did not see t1's event on p2")

        # A new read model replays both partitions: t1's events once each, in their order,
        # though p2's stream is the shorter one.
        order: list[int] = []
        replay = counters_domain.read_model("replay", reconnect_s=0.05)

        @replay.on("Incremented")
        def _(data, evt, ctx):
            if evt.tenant == "t1":
                order.append(evt.version)

        replaying = k.listener(replay)
        threading.Thread(target=replaying.run, daemon=True).start()
        self.addCleanup(replaying.stop)
        for _ in range(250):
            if len(order) >= 3:
                break
            time.sleep(0.02)
        time.sleep(0.2)
        self.assertEqual(order, [1, 2, 3])

        # A read model across tenants, slow on acme's events: asked in t9's partition (p1), a
        # query with all_partitions waits until it has processed p2 as well.
        total = {"sum": 0}
        slow = counters_domain.read_model("sum", reconnect_s=0.05)

        @slow.on("Incremented")
        def _(data, evt, ctx):
            if evt.tenant == "acme":
                time.sleep(1)
            total["sum"] += data["by"]

        @slow.query("Sum")
        def _(data, ctx):
            return total["sum"]

        summing = k.listener(slow)
        threading.Thread(target=summing.run, daemon=True).start()
        self.addCleanup(summing.stop)
        self.assertTrue(summing.ready.wait(10))

        def ask(all_partitions):
            return k.query("counters", "Sum", tenant="t9", all_partitions=all_partitions)

        self.assertEqual(ask(True), 14, "caught up on both partitions")
        placed = k.submit("counters", "Counter", "c2", "Increment", {"by": 10}, tenant="acme", id="acme-2",
                          wait="committed")
        self.assertTrue(placed.committed, placed)
        self.assertEqual(ask(False), 14, "a plain query is current only on p1")
        self.assertEqual(ask(True), 24, "all_partitions waits for acme's event on p2")


if __name__ == "__main__":
    unittest.main()
