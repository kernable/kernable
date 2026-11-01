"""Python SDK: listeners and event schemas, checked when they are defined.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_definitions
"""

import unittest

from kernable import _upcast, domain


class ListenerDefinitionTest(unittest.TestCase):
    """Listeners are defined on a domain: unqualified types are the domain's own."""

    def test_unqualified_types_are_the_own_domains_and_qualified_ones_stay(self):
        shop, identity = domain("Shop"), domain("Identity")
        carts = shop.read_model("carts").use(identity)
        self.assertEqual((carts.name, carts.order, carts.start), ("carts", 0, 0))
        mailer = shop.reactor("mailer")
        self.assertEqual((mailer.order, mailer.start), (10, None))
        carts.on("ItemAdded", lambda data: None)
        carts.on("Identity.AccountRegistered", lambda data: None)
        carts.on("Identity.*", lambda data: None)
        carts.query("GetCart", lambda data: None)
        carts.query("Identity.GetAccount", lambda data: None)
        carts.middleware(["*", "Checkout", "Identity.*"], fn=lambda check: None)
        self.assertEqual(list(carts._handlers), ["Shop.ItemAdded", "Identity.AccountRegistered", "Identity.*"])
        self.assertEqual(list(carts._queries), ["Shop.GetCart", "Identity.GetAccount"])
        self.assertEqual(list(carts._middlewares["before"][0].types), ["*", "Shop.Checkout", "Identity.*"])
        with self.assertRaises(ValueError):
            carts.query("Identity.*")
        with self.assertRaises(ValueError):
            carts.on("Identity.Account.Registered")

    def test_listener_types_are_checked_when_defined(self):
        # A bare `*` is not the own domain (that is `counters.*`), as in TypeScript and Rust.
        model = domain("counters").read_model("model")
        for event_type in ("Added", "crm.Created", "crm.*", "counters.*"):
            model.on(event_type, lambda data: None)
        self.assertEqual(list(model._handlers), ["counters.Added", "crm.Created", "crm.*", "counters.*"])
        for event_type in ("*", "", "crm.a.b", "Add*"):
            with self.assertRaises(ValueError, msg=event_type):
                model.on(event_type, lambda data: None)

    def test_a_name_is_defined_once_per_domain(self):
        shop = domain("shop")
        shop.aggregate("Cart", 0)
        shop.read_model("carts")
        with self.assertRaisesRegex(ValueError, "shop has an aggregate Cart already"):
            shop.aggregate("Cart", 1)
        for define in (shop.read_model, shop.reactor):
            with self.assertRaisesRegex(ValueError, "shop has a listener carts already"):
                define("carts")
        self.assertEqual(([a.name for a in shop.aggregates], [l.name for l in shop.listeners]), (["Cart"], ["carts"]))

    def test_the_own_domains_event_schemas_apply_without_use(self):
        shop = domain("Shop")
        carts = shop.read_model("carts")
        shop.event("ItemAdded", 2).upcast(1, lambda d: {"sku": d["id"]})  # declared after the listener
        self.assertEqual(_upcast(carts._domains.get("Shop"), "Shop", "ItemAdded", 1, {"id": "x"}),
                         (2, {"sku": "x"}))
        self.assertIsNone(carts._domains.get("Identity"))


class EventSchemaDefinitionTest(unittest.TestCase):
    def test_event_schemas_are_checked_when_defined(self):
        counters = domain("counters")
        with self.assertRaises(ValueError):
            counters.event("Incremented", 0)
        with self.assertRaises(ValueError):
            counters.event("counters.Incremented", 2)
        with self.assertRaises(ValueError):
            counters.event("Incremented", 2).upcast(2, lambda d: d)


if __name__ == "__main__":
    unittest.main()
