"""Reactors park events their handlers give up on, and get them again when an operator asks."""

import os
import threading
import time
import unittest
from unittest import mock

from kernable import Kernable, Rejected, domain
from tests.kernel import TestKernel


def background(target) -> threading.Thread:
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


def eventually(done) -> None:
    for _ in range(100):
        if done():
            return
        time.sleep(0.02)


def shop():
    """A shop whose `Ship` is rejected for an order that was not placed."""
    shop = domain("shop")
    order = shop.aggregate("Order", False)
    order.apply("Placed", lambda placed, data: True)
    order.command("Place", lambda placed, data: [{"type": "Placed"}])
    order.command("Request", lambda placed, data: [{"type": "Requested"}])

    def ship(placed, data):
        if not placed:
            raise Rejected("not placed", "not_placed")
        return [{"type": "Shipped"}]

    order.command("Ship", ship)
    return shop, order


class Parking(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["shop.*"])
        self.addCleanup(self.kernel.dispose)
        self.kernel.start()
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            self.k = Kernable(self.kernel.url, self.kernel.app_key)
        self.addCleanup(self.k.close)

    def run_all(self, *runnables):
        for r in runnables:
            self.addCleanup(r.stop)
            background(r.run)
        for r in runnables:
            self.assertTrue(r.ready.wait(5), "not connected")

    def parked(self, n: int) -> list[str]:
        """The lines of `kernable listeners parked`, once there are `n`."""
        lines: list[str] = []
        for _ in range(100):
            done = self.kernel.cli("listeners", "parked", self.kernel.store)
            self.assertEqual(done.returncode, 0, done.stderr)
            lines = done.stdout.splitlines()
            if len(lines) == n:
                break
            time.sleep(0.05)
        return lines

    def test_a_rejected_follow_up_parks_the_event_and_the_reactor_moves_on(self):
        shop_domain, order = shop()
        shipped: list[str] = []
        shipper = shop_domain.reactor("shipper")

        @shipper.on("Requested")
        def _(data, evt, ctx):
            ctx.submit("Order", evt.aggregate_id, "Ship", id=f"ship-{evt.aggregate_id}")
            shipped.append(evt.aggregate_id)

        refused: list[Exception] = []
        no_id = shop_domain.reactor("no-id")

        @no_id.on("Placed")
        def _(data, evt, ctx):
            try:
                ctx.submit("Order", evt.aggregate_id, "Ship", id="")
            except ValueError as err:
                refused.append(err)

        self.run_all(self.k.worker(order), self.k.listener(shipper), self.k.listener(no_id))
        self.k.submit("shop", "Order", "o1", "Request")
        self.k.submit("shop", "Order", "o2", "Place")
        self.k.submit("shop", "Order", "o2", "Request")
        # o1 was never placed: its shipping is rejected, parked at once; o2 ships.
        self.assertEqual(shipped, ["o2"])
        lines = self.parked(1)
        self.assertRegex(lines[0], r"^shipper position 1 shop\.Requested not_placed attempts 1 parked .*: not placed$")
        # A follow-up without a fixed id is refused before it is sent.
        self.assertRegex(str(refused[0]), "needs a fixed id")

    def test_a_failing_handler_parks_after_its_attempts_and_a_retry_handles_it(self):
        shop_domain, order = shop()
        down = [True]
        mailed: list[str] = []
        # With a store: a parked event's writes go, the rest of the batch stays.
        mailer = shop_domain.reactor("mailer", park_after=2, reconnect_s=0.02, store="memory:")

        @mailer.on("Placed")
        def _(data, evt, ctx):
            ctx.store.set("tried", evt.aggregate_id, True)
            if evt.aggregate_id == "o1" and down[0]:
                raise ConnectionError("the mail server is down")
            mailed.append(evt.aggregate_id)

        handle = self.k.listener(mailer)
        self.run_all(self.k.worker(order), handle)
        self.k.submit("shop", "Order", "o1", "Place")
        self.k.submit("shop", "Order", "o2", "Place")
        eventually(lambda: len(mailed) == 1)
        self.assertEqual(mailed, ["o2"])
        lines = self.parked(1)
        self.assertRegex(lines[0], r"^mailer position 1 shop\.Placed handler_error attempts 2 parked .*: ConnectionError$")
        self.assertIsNone(handle.store.get("tried", "o1"))
        self.assertTrue(handle.store.get("tried", "o2"))

        # Fixed: the operator retries it, and it is handled.
        down[0] = False
        retried = self.kernel.cli("listeners", "retry", self.kernel.url, "app.key", "mailer", "--all")
        self.assertEqual(retried.returncode, 0, retried.stdout + retried.stderr)
        self.assertEqual(retried.stdout.strip(), "retrying 1 parked events of mailer: 1")
        eventually(lambda: len(mailed) == 2)
        self.assertEqual(mailed, ["o2", "o1"])
        self.assertEqual(self.parked(0), [])
        self.assertTrue(handle.store.get("tried", "o1"))

    def test_a_read_model_never_parks_by_default(self):
        shop_domain, order = shop()
        seen: list[str] = []
        orders = shop_domain.read_model("orders", reconnect_s=0.02)

        @orders.on("Placed")
        def _(data, evt):
            seen.append(evt.aggregate_id)
            if evt.aggregate_id == "o1":
                raise Rejected("cannot show it")

        self.run_all(self.k.worker(order), self.k.listener(orders))
        self.k.submit("shop", "Order", "o1", "Place", wait="committed")
        self.k.submit("shop", "Order", "o2", "Place", wait="committed")
        # It keeps getting o1; o2 waits behind it.
        eventually(lambda: len(seen) >= 5)
        self.assertGreaterEqual(len(seen), 5)
        self.assertTrue(all(i == "o1" for i in seen), seen)
        self.assertEqual(self.parked(0), [])


if __name__ == "__main__":
    unittest.main()
