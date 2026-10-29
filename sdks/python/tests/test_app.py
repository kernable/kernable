"""An app made of whole domains: its membership comes from what the domains define, and `run()`
applies it and runs everything.

Run from sdks/python: .venv/bin/python -m unittest tests.test_app   (after `cargo build`)
"""

import contextlib
import io
import os
import threading
import time
import unittest
from unittest import mock

from kernable import Kernable, domain
from tests.kernel import TestKernel


def shop_and_billing():
    shop = domain("shop")
    cart = shop.aggregate("Cart", 0).command("Add", lambda state, data: [{"type": "Added"}])
    counts: dict[str, int] = {}

    def added(data, evt):
        counts[evt.aggregate_id] = counts.get(evt.aggregate_id, 0) + 1

    carts = shop.read_model("carts")
    carts.on("Added", added)
    carts.query("Count", lambda data: counts.get(data["id"], 0))
    shop.reactor("push", live=True).on("Added", lambda data: None)
    billing = domain("billing")
    billing.reactor("invoices").on("shop.Added", lambda data: None)
    guard = billing.read_model("guard")
    guard.on("shop.Added", lambda data: None)
    guard.middleware(["shop.*"], fn=lambda check: None)
    return shop, billing, cart


class AppTest(unittest.TestCase):
    def setUp(self):
        # Quiet: no connection lines in the test output.
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"})
        env.start()
        self.addCleanup(env.stop)

    def test_an_app_lists_its_domains_aggregates_listeners_and_middlewares_but_no_live_ones(self):
        kernel = TestKernel(["shop.*"])  # for its key only: not started
        self.addCleanup(kernel.dispose)
        shop, billing, _ = shop_and_billing()
        k = Kernable("127.0.0.1:1", kernel.app_key)
        self.addCleanup(k.close)
        app = k.app(service="shop", version="1.0.0", description="Carts", domains=[shop, billing],
                    metadata={"team": "checkout"})
        m = app.manifest
        self.assertEqual(m.service, "shop")
        self.assertEqual([f"{a.domain}.{a.name}" for a in m.workers], ["shop.Cart"])
        self.assertEqual([l.name for l in m.listeners], ["carts", "invoices"])
        self.assertEqual([l.name for l in m.middlewares], ["guard"])
        self.assertEqual(list(m.metadata.items()),
                         [("version", "1.0.0"), ("description", "Carts"), ("team", "checkout")])
        self.assertIsNotNone(app.worker)
        self.assertEqual(len(app.listeners), 4, "the live one runs too")
        self.assertFalse(app.ready.is_set())
        with self.assertRaisesRegex(ValueError, "service"):
            k.app(domains=[shop])
        # The client's service is the default.
        on_client = Kernable("127.0.0.1:1", kernel.app_key, service="carts")
        self.addCleanup(on_client.close)
        self.assertEqual(on_client.app(domains=[shop]).manifest.service, "carts")

    def test_an_app_applies_its_membership_and_runs_everything(self):
        kernel = TestKernel(["shop.*"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        shop, billing, _ = shop_and_billing()
        k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        app = k.app(service="shop", version="1.0.0", domains=[shop, billing])
        running = threading.Thread(target=app.run, daemon=True)
        running.start()
        try:
            self.assertTrue(app.ready.wait(5), "app did not connect")
            self.assertTrue(k.submit("shop", "Cart", "c1", "Add").committed)
            self.assertEqual(k.query("shop", "Count", {"id": "c1"}), 1)
        finally:
            app.stop()
            running.join(5)
        self.assertFalse(running.is_alive(), "run() returns after stop()")

    def test_an_app_prints_one_overview_once_everything_is_connected(self):
        kernel = TestKernel(["shop.*"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "info"}):
            k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        shop = domain("shop")
        shop.aggregate("Order", {}).command("Place", lambda state, data: [])
        shop.read_model("orders").on("Placed", lambda data: None).query("GetOrder", lambda data: None)
        app = k.app(service="shop", version="1.0.0", description="Orders", domains=[shop])
        out = io.StringIO()
        with contextlib.redirect_stderr(out):
            running = threading.Thread(target=app.run, daemon=True)
            running.start()
            try:
                self.assertTrue(app.ready.wait(5), "app did not connect")
                deadline = time.monotonic() + 2
                while not out.getvalue() and time.monotonic() < deadline:
                    time.sleep(0.02)
                time.sleep(0.05)  # no line follows the overview
            finally:
                app.stop()
                running.join(5)
        overview = out.getvalue().split("\n")
        self.assertEqual(overview[0], "kernable · shop 1.0.0 · Orders", "one overview, no connection lines")
        self.assertRegex(overview[1], r"^ {2}kernel {6}unix:/tmp/kn-[^ ]+ · key app \([0-9a-f]{12}…\)$")
        self.assertEqual(overview[2], "  membership  applied: 1 worker, 1 listener")
        self.assertEqual(overview[3:8], [
            "",
            "  worker      Order   runs Place",
            "  listener    orders  from 0 · reads Placed",
            "                      answers GetOrder",
            "",
        ])
        self.assertRegex(overview[8], r"^ {2}ready in \d+ ms$")
        self.assertEqual(overview[9:], ["", ""], "nothing after the overview")


if __name__ == "__main__":
    unittest.main()
