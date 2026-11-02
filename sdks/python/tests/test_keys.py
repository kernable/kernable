"""Python SDK: unique keys, facts and requirements against the real kernel, which checks them
atomically with the commit.

Run from sdks/python: .venv/bin/python -m unittest tests.test_keys   (after `cargo build`)
"""

import hashlib
import os
import threading
import time
import unittest
from unittest import mock

from kernable import Kernable, Requirement, domain
from tests.kernel import TestKernel


def shop_domain():
    """Customers with an email unique per tenant and a handle unique across tenants; stores that
    are open or closed; orders placed only in an open store."""
    shop = domain("shop")
    customer = shop.aggregate("Customer", {})
    customer.apply("Registered", lambda state, data: {**state, **data})
    customer.unique("email", lambda s: s.get("email")).unique("handle", lambda s: s.get("handle"), scope="global")
    customer.command("Register", lambda state, data: [{"type": "Registered", "data": data}])

    store = shop.aggregate("Store", {"open": False})
    store.apply("Opened", lambda state, data: {"open": True})
    store.apply("Closed", lambda state, data: {"open": False})
    store.fact("open", lambda s: s["open"])
    store.command("Open", lambda state, data: [{"type": "Opened", "data": {}}])
    store.command("Close", lambda state, data: [{"type": "Closed", "data": {}}])

    order = shop.aggregate("Order", {})
    order.apply("Placed", lambda state, data: data)

    @order.command("Place", requires=lambda data, state: [Requirement("shop", "Store", data["store"], "open")])
    def _(state, data):
        return [{"type": "Placed", "data": data}]

    # By the customer's email (its key), in the own domain.
    @order.command("PlaceFor", requires=lambda data, state: [Requirement.by_key("Customer", "email", data["email"],
                                                                                 "registered")])
    def _(state, data):
        return [{"type": "Placed", "data": data}]

    # Only while the store is not open (or does not exist).
    @order.command("Plan", requires=lambda data, state: [Requirement.of("Store", data["store"], "open", absent=True)])
    def _(state, data):
        return [{"type": "Planned", "data": data}]

    customer.fact("registered", lambda s: "email" in s)
    return shop


class Keys(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["shop.*", "identity.*"])
        self.kernel.start()
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"})  # quiet: no connection lines
        env.start()
        self.addCleanup(env.stop)
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")
        self.streams = []

    def tearDown(self):
        for s in self.streams:
            s.stop()
        self.k.close()
        self.kernel.dispose()

    def run_worker(self, *aggregates):
        worker = self.k.worker(*aggregates)
        self.streams.append(worker)
        threading.Thread(target=worker.run, daemon=True).start()
        self.assertTrue(worker.ready.wait(5), "worker did not connect")
        return worker

    def register(self, id: str, email: str, handle: str, tenant: str = "t1"):
        return self.k.submit("shop", "Customer", id, "Register", {"email": email, "handle": handle},
                             tenant=tenant, wait="committed")

    def test_a_key_is_unique_in_its_tenant_or_across_tenants(self):
        self.run_worker(*shop_domain().aggregates)
        self.assertTrue(self.register("c1", "max@firma.de", "max").committed)
        taken = self.register("c2", "max@firma.de", "moritz")
        self.assertEqual((taken.committed, taken.code, taken.message, taken.key, taken.requirement),
                         (False, "unique_taken", "shop.Customer key email is taken", "email", None))
        self.assertTrue(self.register("c2", "max@firma.de", "moritz", tenant="t2").committed, "another tenant")
        global_ = self.register("c3", "erika@firma.de", "max", tenant="t2")
        self.assertEqual((global_.code, global_.message, global_.key),
                         ("unique_taken", "shop.Customer key handle is taken", "handle"))

    def test_a_new_value_frees_the_old_one(self):
        self.run_worker(*shop_domain().aggregates)
        self.assertTrue(self.register("c1", "max@firma.de", "max").committed)
        self.assertEqual(self.register("c2", "max@firma.de", "moritz").code, "unique_taken")
        self.assertTrue(self.register("c1", "max@example.com", "max").committed, "its own key is no conflict")
        self.assertTrue(self.register("c2", "max@firma.de", "moritz").committed)
        self.assertEqual(self.register("c3", "max@example.com", "erika").code, "unique_taken")

    def test_a_command_commits_only_while_the_fact_it_requires_is_held(self):
        self.run_worker(*shop_domain().aggregates)

        def place(order: str, store: str):
            return self.k.submit("shop", "Order", order, "Place", {"store": store}, wait="committed")

        missing = place("o1", "s1")
        self.assertEqual((missing.committed, missing.code, missing.message, missing.key, missing.requirement),
                         (False, "requirement_missing", "shop.Store s1 does not hold open", "",
                          Requirement("shop", "Store", "s1", "open")))
        self.assertTrue(self.k.submit("shop", "Store", "s1", "Open").committed)
        self.assertTrue(place("o1", "s1").committed)
        self.assertTrue(place("o2", "s1").committed)
        self.assertTrue(self.k.submit("shop", "Store", "s1", "Close").committed)
        self.assertEqual(place("o3", "s1").code, "requirement_missing")
        other_tenant = self.k.submit("shop", "Order", "o4", "Place", {"store": "s1"}, tenant="t2")
        self.assertEqual(other_tenant.code, "requirement_missing", "facts are required in the command's tenant")

    def test_a_requirement_names_its_aggregate_by_a_key_or_wants_the_fact_absent(self):
        self.run_worker(*shop_domain().aggregates)

        def order(order: str, type: str, data: dict):
            return self.k.submit("shop", "Order", order, type, data, wait="committed")

        missing = order("o1", "PlaceFor", {"email": "max@firma.de"})
        self.assertEqual((missing.code, missing.message), ("requirement_missing",
                                                          "shop.Customer with key email does not hold registered"))
        self.assertEqual(missing.requirement, Requirement("shop", "Customer", "", "registered", key="email"),
                         "the key, never its value")
        self.assertTrue(self.register("c1", "max@firma.de", "max").committed)
        self.assertTrue(order("o1", "PlaceFor", {"email": "max@firma.de"}).committed)

        self.assertTrue(order("o2", "Plan", {"store": "s1"}).committed, "s1 holds nothing")
        self.assertTrue(self.k.submit("shop", "Store", "s1", "Open").committed)
        planned = order("o3", "Plan", {"store": "s1"})
        self.assertEqual((planned.message, planned.requirement.absent), ("shop.Store s1 holds open", True))

    def test_erasing_its_subject_frees_a_personal_key(self):
        identity = domain("identity")
        identity.command("Open").personal(["email"], lambda d: d["person"])
        identity.event("AccountOpened", 1).personal(["email"], lambda d: d["person"])
        identity.event("PersonErased", 1).erases(lambda d: d["person"])
        account = identity.aggregate("Account", {})
        account.apply("AccountOpened", lambda state, data: data)
        account.unique("email", lambda s: s.get("email"), personal=lambda s: s.get("person"))
        account.command("Open", lambda state, data: [{"type": "AccountOpened", "data": data}])
        person = identity.aggregate("Person", {})
        person.command("Erase", lambda state, data: [{"type": "PersonErased", "data": {"person": data["id"]}}])
        self.run_worker(account, person)
        self.k.use(identity)

        def open_(id: str, who: str):
            return self.k.submit("identity", "Account", id, "Open", {"email": "max@firma.de", "person": who},
                                 wait="committed")

        self.assertTrue(open_("a1", "p-1").committed)
        self.assertEqual(open_("a2", "p-2").code, "unique_taken")
        for name in os.listdir(self.kernel.dir):  # the vault keeps the value and the key
            path = os.path.join(self.kernel.dir, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    content = f.read()
                self.assertFalse(b"max@firma.de" in content or hashlib.sha256(b"max@firma.de").digest() in content, name)
        self.assertTrue(self.k.submit("identity", "Person", "p-1", "Erase", {"id": "p-1"}).committed)
        self.assertTrue(open_("a2", "p-2").committed, "the erased subject's key is free")

    def test_reindex_gives_existing_aggregates_a_key_declared_later(self):
        v1 = domain("shop").aggregate("Customer", {})
        v1.apply("Registered", lambda state, data: {**state, **data})
        v1.command("Register", lambda state, data: [{"type": "Registered", "data": data}])
        old = self.run_worker(v1)
        for id, email in (("c1", "max@firma.de"), ("c2", "erika@firma.de"), ("c3", "max@firma.de")):
            self.assertTrue(self.register(id, email, id).committed)
        old.stop()
        time.sleep(0.1)

        v2 = shop_domain().aggregates[0]
        self.run_worker(v2)
        self.assertTrue(self.register("c4", "max@firma.de", "c4").committed, "not reindexed yet")
        self.assertTrue(self.register("c4", "moritz@firma.de", "c4").committed)
        outcomes = self.k.reindex("shop", "Customer", ["c1", "c2", "c3", "never"])
        self.assertEqual({id: (o.committed, o.code, o.events) for id, o in outcomes.items()},
                         {"c1": (True, "", []), "c2": (True, "", []), "c3": (False, "unique_taken", []),
                          "never": (True, "", [])})
        self.assertEqual(self.register("c5", "max@firma.de", "c5").code, "unique_taken")
        self.assertEqual(self.register("c5", "erika@firma.de", "c5").code, "unique_taken")
        self.assertEqual(self.register("c5", "c1@firma.de", "c1").code, "unique_taken", "the handle, global")


    def test_aggregates_of_one_domain_in_separate_workers_reindex_each_their_own(self):
        shop = domain("shop")
        aggregates = {}
        for name in ("Customer", "Supplier"):
            agg = shop.aggregate(name, {})
            agg.apply("Registered", lambda state, data: data)
            agg.unique("email", lambda s: s.get("email"))
            agg.command(f"Register{name}", lambda state, data: [{"type": "Registered", "data": data}])
            aggregates[name] = agg
            self.run_worker(agg)  # a worker of its own: each leases only its aggregate's commands
        ids = [f"x{i}" for i in range(6)]
        for name in aggregates:
            for id in ids:
                self.assertTrue(self.k.submit("shop", name, id, f"Register{name}", {"email": f"{id}@firma.de"},
                                              wait="committed").committed)
        for name in aggregates:
            outcomes = self.k.reindex("shop", name, ids)
            self.assertEqual({id: (o.committed, o.code) for id, o in outcomes.items()},
                             {id: (True, "") for id in ids}, name)
        taken = self.k.submit("shop", "Supplier", "y", "RegisterSupplier", {"email": "x0@firma.de"})
        self.assertEqual(taken.code, "unique_taken")


if __name__ == "__main__":
    unittest.main()
