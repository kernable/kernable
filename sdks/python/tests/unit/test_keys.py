"""Python SDK: unique keys, facts and requirements, computed and signed without a kernel.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_keys
"""

import hashlib
import unittest

import grpc

from kernable import CommandContext, HeldKey, Index, Requirement, Worker, _Log, domain, reindex_type
from kernable.signing import Key, index_message
from kernable.v1 import kernable_pb2 as pb
from tests.unit.test_signing import VECTORS, vector_command


def order_aggregate():
    """The aggregate behind the `result_index` test vector: an order with a number, the
    customer's email (global and personal) and the facts `open` and `paid`, placed only while
    its customer is active."""
    orders = domain("orders")
    order = orders.aggregate("Order", {})
    order.apply("OrderPlaced", lambda state, data: {**data, "open": True, "paid": True})
    order.unique("number", lambda s: s.get("number"))
    order.unique("email", lambda s: s.get("email"), scope="global", personal=lambda s: s.get("customerId"))
    order.fact("open", lambda s: s.get("open", False)).fact("paid", lambda s: s.get("paid", False))
    order.fact("closed", lambda s: s.get("closed", False))

    @order.command("PlaceOrder",
                   requires=lambda data, state: [
                       Requirement("identity", "Customer", data["customerId"], "active"),
                       Requirement.by_key("Vehicle", "isin", "VEH-lyra", "woundUp", domain="funds", absent=True),
                   ])
    def _(state, data):
        return [{"type": "OrderPlaced", "data": {"number": "A-1001", "email": "max@example.com",
                                                 "customerId": data["customerId"]}}]

    return order


def worker(*aggregates) -> Worker:
    key = Key(VECTORS["key"]["seed"])
    return Worker._new(grpc.insecure_channel("unix:/nonexistent"), key, aggregates, _Log("test", "test.key", key.id))


class IndexSigning(unittest.TestCase):
    def test_the_index_message_matches_the_vector(self):
        v = VECTORS["result_index"]["input"]
        keys = [pb.UniqueKey(name=k["name"], value_hash=bytes.fromhex(k["value_hash_hex"]), subject=k["subject"],
                             **{"global": k["global"]}) for k in v["keys"]]
        requires = [pb.Requirement(**{k: x for k, x in r.items() if k != "value_hash_hex"},
                                   value_hash=bytes.fromhex(r["value_hash_hex"])) for r in v["requires"]]
        message = index_message(vector_command(v["command"]), v["expected_version"], keys, v["facts"], requires)
        self.assertEqual(message.hex(), VECTORS["result_index"]["message"])
        self.assertEqual(Key(VECTORS["key"]["seed"]).sign(message).sig.hex(), VECTORS["result_index"]["signature"])

    def test_a_worker_sends_and_signs_the_keys_facts_and_requirements_of_the_vector(self):
        v = VECTORS["result_index"]["input"]
        execute = pb.Execute(lease_id="l-1", command=vector_command(v["command"]), expected_version=v["expected_version"])
        result = worker(order_aggregate())._execute(execute).result
        self.assertEqual([(k.name, getattr(k, "global"), k.value_hash.hex(), k.subject) for k in result.keys],
                         [(k["name"], k["global"], k["value_hash_hex"], k["subject"]) for k in v["keys"]])
        self.assertEqual(list(result.facts), v["facts"])
        self.assertEqual([(r.domain, r.aggregate_type, r.aggregate_id, r.fact) for r in result.requires],
                         [(r["domain"], r["aggregate_type"], r["aggregate_id"], r["fact"]) for r in v["requires"]])
        self.assertEqual(result.index_sig.sig.hex(), VECTORS["result_index"]["signature"])
        self.assertEqual(result.index_sig.key_id, VECTORS["key"]["key_id"])

    def test_reindex_sends_the_set_of_the_current_state_without_events(self):
        order = order_aggregate()
        placed = pb.Event(event_type="OrderPlaced", schema=1, version=1,
                          payload=b'{"number": "A-1001", "email": "max@example.com", "customerId": "k-1"}')
        cmd = pb.Command(command_id="c-2", tenant_id="t-1", domain="orders", aggregate_type="Order",
                         aggregate_id="o-1", command_type="_reindex_Order", payload=b"{}")
        result = worker(order)._execute(pb.Execute(lease_id="l-2", command=cmd, history=[placed],
                                                   expected_version=1)).result
        self.assertEqual(list(result.events), [])
        self.assertEqual([k.name for k in result.keys], ["number", "email"])
        self.assertEqual(result.keys[0].value_hash, hashlib.sha256(b"A-1001").digest())
        self.assertEqual(list(result.facts), ["open", "paid"])
        self.assertTrue(result.HasField("index_sig"))

    def test_without_keys_facts_or_requirements_nothing_is_signed(self):
        counter = domain("counters").aggregate("Counter", 0)
        counter.command("Increment", lambda state, data: [{"type": "Incremented", "data": {}}])
        cmd = pb.Command(command_id="c-1", tenant_id="t-1", domain="counters", aggregate_type="Counter",
                         aggregate_id="c1", command_type="Increment", payload=b"{}")
        result = worker(counter)._execute(pb.Execute(lease_id="l-1", command=cmd)).result
        self.assertEqual((len(result.keys), len(result.facts), len(result.requires)), (0, 0, 0))
        self.assertFalse(result.HasField("index_sig"))

    def test_a_requirement_that_is_not_one_or_a_failing_key_rejects_like_a_handler(self):
        order = domain("orders").aggregate("Order", {})
        order.command("PlaceOrder", lambda state, data: [], requires=lambda data, state: [("identity", "k-1")])
        order.unique("number", lambda s: s["number"])  # no number in the state: KeyError
        cmd = pb.Command(command_id="c-1", tenant_id="t-1", domain="orders", aggregate_type="Order",
                         aggregate_id="o-1", command_type="PlaceOrder", payload=b"{}")
        quiet = worker(order)
        quiet._log.print = lambda *args: None
        rejection = quiet._execute(pb.Execute(lease_id="l-1", command=cmd)).rejection
        self.assertEqual((rejection.reason.code, rejection.reason.message), ("handler_error", "TypeError"))
        order.requirements.clear()
        rejection = quiet._execute(pb.Execute(lease_id="l-1", command=cmd)).rejection
        self.assertEqual((rejection.reason.code, rejection.reason.message), ("handler_error", "KeyError"))
        order.keys.clear()
        order.unique("number", lambda s: 1001)  # not a string
        rejection = quiet._execute(pb.Execute(lease_id="l-1", command=cmd)).rejection
        self.assertEqual((rejection.reason.code, rejection.reason.message), ("handler_error", "TypeError"))


class IndexDefinition(unittest.TestCase):
    def test_index_returns_the_keys_and_facts_of_a_state(self):
        account = domain("identity").aggregate("Account", {})
        account.unique("email", lambda s: s.get("email"), personal=lambda s: s.get("id"))
        account.unique("alias", lambda s: s.get("aliases"), scope="global")

        @account.fact("active")
        def _(state):
            return state.get("status") == "active"

        self.assertEqual(account.index({}), Index([], []))
        state = {"id": "a1", "email": "max@example.com", "aliases": ["max", "mm", "max"], "status": "active"}
        self.assertEqual(account.index(state), Index([HeldKey("email", "max@example.com", "tenant", "a1"),
                                                      HeldKey("alias", "max", "global"), HeldKey("alias", "mm", "global")],
                                                     ["active"]))
        # A personal key without a subject is not held.
        self.assertEqual(account.index({"email": "max@example.com"}).keys, [])
        with self.assertRaises(TypeError):
            account.index({"aliases": [7]})

    def test_reindex_is_built_in_per_aggregate_once_it_has_keys_or_facts(self):
        shop = domain("shop")
        cart = shop.aggregate("Cart", {})
        self.assertNotIn("_reindex_Cart", cart.handlers)
        cart.fact("open", lambda s: True)
        self.assertEqual(cart.handle({}, "_reindex_Cart", None, CommandContext.new("r-1")), [])
        wishlist = shop.aggregate("Wishlist", {}).unique("sku", lambda s: None)
        self.assertEqual([t for t in [*cart.handlers, *wishlist.handlers] if t.startswith("_reindex")],
                         ["_reindex_Cart", "_reindex_Wishlist"])
        self.assertEqual(reindex_type("Cart"), "_reindex_Cart")
        # Any `_reindex_` type could be another aggregate's of the domain; plain `_reindex` is free.
        for command_type in ("_reindex_Cart", "_reindex_Gift", "_reindex_"):
            with self.assertRaisesRegex(ValueError, "_reindex_", msg=command_type):
                cart.command(command_type, lambda state, data: [])
        gift = shop.aggregate("Gift", {})
        gift.command("_reindex", lambda state, data: [])
        self.assertEqual(list(gift.handlers), ["_reindex"])

    def test_keys_and_facts_are_checked_when_defined(self):
        account = domain("identity").aggregate("Account", {})
        account.unique("email", lambda s: None).fact("active", lambda s: True)
        with self.assertRaisesRegex(ValueError, "identity.Account has a key email already"):
            account.unique("email", lambda s: None, scope="global")
        with self.assertRaisesRegex(ValueError, "identity.Account has a fact active already"):
            account.fact("active", lambda s: False)
        with self.assertRaisesRegex(ValueError, "scope"):
            account.unique("alias", lambda s: None, scope="tenants")
        for name in ("", "a.b", "x" * 129):
            with self.assertRaises(ValueError, msg=name):
                account.unique(name, lambda s: None)
            with self.assertRaises(ValueError, msg=name):
                account.fact(name, lambda s: True)
        account.unique("x" * 128, lambda s: None)
        self.assertEqual(list(account.keys), ["email", "x" * 128])


if __name__ == "__main__":
    unittest.main()
