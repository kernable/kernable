"""Deployments: two code versions of a service side by side, a rollback, and events newer than a
listener's code."""

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


def shop(schema: int):
    """The shop as one deployment knows it: `ItemAdded` at `schema`; the cart counts its handled
    commands."""
    shop = domain("shop")
    if schema > 1:
        shop.event("ItemAdded", schema).upcast(1, lambda d: {**d, "qty": 1})
    cart = shop.aggregate("cart", {"items": 0})
    handled: list[str] = []

    @cart.apply("ItemAdded")
    def _(state, data):
        state["items"] += 1

    @cart.command("AddItem")
    def _(state, data, cmd):
        handled.append(cmd.aggregate_id)
        return [{"type": "ItemAdded", "data": {"sku": data["sku"], "qty": 2}}]

    return shop, cart, handled


class Deployments(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["shop.*"])
        self.addCleanup(self.kernel.dispose)
        self.kernel.start()

    def client(self, deployment: str, service: str | None = None) -> Kernable:
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(self.kernel.url, self.kernel.app_key, deployment=deployment, service=service)
        self.addCleanup(k.close)
        return k

    def run_all(self, *runnables):
        for r in runnables:
            self.addCleanup(r.stop)
            background(r.run)
        for r in runnables:
            self.assertTrue(r.ready.wait(5), "not connected")

    def carts(self, shop_domain, deployment: str):
        """A read model `carts` that answers with its deployment and the events it saw."""
        carts = shop_domain.read_model("carts")
        seen: list[tuple[str, str]] = []

        @carts.on("ItemAdded")
        def _(data, evt):
            seen.append((evt.aggregate_id, evt.deployment))

        @carts.query("GetCarts")
        def _(data):
            return {"deployment": deployment, "seen": len(seen)}

        return carts, seen

    def test_two_deployments_run_side_by_side_and_queries_go_to_their_own(self):
        k1, k2 = self.client("v1"), self.client("v2")
        shop1, cart1, _ = shop(1)
        shop2, _, _ = shop(1)
        carts1, seen1 = self.carts(shop1, "v1")
        carts2, seen2 = self.carts(shop2, "v2")
        self.run_all(k1.worker(cart1))
        first = k1.submit("shop", "cart", "a", "AddItem", {"sku": "apple"}, wait="committed")
        # Stored events carry the deployment of the worker that wrote them.
        self.assertEqual([e.deployment for e in first.events], ["v1"])
        self.run_all(k1.listener(carts1), k2.listener(carts2))
        last = k1.submit("shop", "cart", "b", "AddItem", {"sku": "pear"}, wait="committed").events[-1].position
        # v2's read model is new under its name: it rebuilds from the start beside v1's.
        self.assertEqual(k1.query("shop", "GetCarts", {}, min_position=last), {"deployment": "v1", "seen": 2})
        self.assertEqual(k2.query("shop", "GetCarts", {}, min_position=last), {"deployment": "v2", "seen": 2})
        self.assertEqual(seen1, [("a", "v1"), ("b", "v1")])
        self.assertEqual(seen2, [("a", "v1"), ("b", "v1")])
        subscribed = []
        sub = k1.subscribe(lambda e: subscribed.append(e.deployment), types=["shop.*"])
        self.addCleanup(sub.stop)
        self.assertTrue(eventually(lambda: len(subscribed) == 2))
        self.assertEqual(subscribed, ["v1", "v1"])

    def test_a_rollback_keeps_the_older_code_off_aggregates_it_cannot_read(self):
        k1, k2 = self.client("v1", "shop"), self.client("v2", "shop")
        shop1, cart1, handled1 = shop(1)
        shop2, cart2, handled2 = shop(2)
        carts1, seen1 = self.carts(shop1, "v1")
        k2.apply(workers=[cart2])
        model = k1.listener(carts1)
        self.run_all(k2.worker(cart2), k1.worker(cart1), model)

        a = k1.submit("shop", "cart", "A", "AddItem", {"sku": "apple"}, wait="committed")
        self.assertTrue(a.committed)
        self.assertEqual([(e.deployment, e.schema) for e in a.events], [("v2", 2)])
        self.assertEqual((handled1, handled2), ([], ["A"]))

        time.sleep(0.002)  # the rollback's membership is issued later
        k1.apply(workers=[cart1])
        again = k1.submit("shop", "cart", "A", "AddItem", {"sku": "apple"}, wait="committed")
        self.assertFalse(again.committed)
        self.assertEqual(again.code, "unknown_history")
        b = k1.submit("shop", "cart", "B", "AddItem", {"sku": "pear"}, wait="committed")
        self.assertTrue(b.committed, b.message)
        self.assertEqual([(e.deployment, e.schema) for e in b.events], [("v1", 1)])
        self.assertEqual((handled1, handled2), (["B"], ["A"]))

        # v1's read model skipped A's schema-2 event and still handles B's.
        self.assertEqual(k1.query("shop", "GetCarts", {}, min_position=b.events[-1].position),
                         {"deployment": "v1", "seen": 1})
        self.assertEqual(seen1, [("B", "v1")])
        self.assertEqual(model.skipped, 1)


if __name__ == "__main__":
    unittest.main()
