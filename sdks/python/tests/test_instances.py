"""Instances: one listener run by several processes under one name. A single reactor runs
on one instance and the next takes over; a replicated read model runs on every instance."""

import os
import threading
import time
import unittest
from unittest import mock

from kernable import Kernable, domain
from tests.kernel import TestKernel


def background(target) -> threading.Thread:
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


def eventually(done) -> bool:
    for _ in range(150):
        if done():
            return True
        time.sleep(0.02)
    return done()


def shop_on(node: str, seen: list[str]):
    """The shop as a process on machine `node` defines it: its own domain, the same names."""
    shop = domain("shop")
    cart = shop.aggregate("Cart", 0)
    cart.command("Add", lambda state, data: [{"type": "Added"}])
    mailer = shop.reactor("mailer", instance=node, reconnect_s=0.05)

    @mailer.on("Added")
    def _(data, evt, ctx):
        seen.append(f"{node}:{evt.aggregate_id}")

    carts = shop.read_model("carts", instance=node, reconnect_s=0.05)

    @carts.on("Added")
    def _(data, evt, ctx):
        seen.append(f"{node}:carts:{evt.aggregate_id}")

    return cart, mailer, carts


class Instances(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["shop.*"], ["--settle-timeout-ms", "1000"])
        self.addCleanup(self.kernel.dispose)
        self.kernel.start()
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            self.k = Kernable(self.kernel.url, self.kernel.app_key)
        self.addCleanup(self.k.close)

    def start(self, runnable):
        self.addCleanup(runnable.stop)
        thread = background(runnable.run)
        self.assertTrue(runnable.ready.wait(5), "not connected")
        return thread

    def test_a_single_reactor_runs_on_one_instance_and_the_next_takes_over(self):
        seen: list[str] = []
        cart, mailer_a, _ = shop_on("node-a", seen)
        _, mailer_b, _ = shop_on("node-b", seen)
        self.start(self.k.worker(cart))
        a = self.k.listener(mailer_a)
        a_thread = self.start(a)
        # node-b waits, and counts as started: it does not hold up its process.
        self.start(self.k.listener(mailer_b))
        self.k.submit("shop", "Cart", "c1", "Add")
        self.assertEqual(seen, ["node-a:c1"])

        a.stop()
        a_thread.join(5)
        self.k.submit("shop", "Cart", "c2", "Add")
        self.assertTrue(eventually(lambda: "node-b:c2" in seen), seen)
        self.assertEqual(seen, ["node-a:c1", "node-b:c2"], "c1 is not mailed again")

    def test_every_instance_of_a_read_model_gets_every_event(self):
        seen: list[str] = []
        cart, _, carts_a = shop_on("node-a", seen)
        _, _, carts_b = shop_on("node-b", seen)
        self.start(self.k.worker(cart))
        a = self.k.listener(carts_a)
        a_thread = self.start(a)
        self.start(self.k.listener(carts_b))
        self.k.submit("shop", "Cart", "c1", "Add")
        self.assertTrue(eventually(lambda: len(seen) == 2), seen)
        self.assertEqual(sorted(seen), ["node-a:carts:c1", "node-b:carts:c1"])

        # node-a is gone: node-b settles alone.
        a.stop()
        a_thread.join(5)
        self.k.submit("shop", "Cart", "c2", "Add")
        self.assertIn("node-b:carts:c2", seen)


if __name__ == "__main__":
    unittest.main()
