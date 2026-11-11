"""Python SDK: deployments (code versions) without a kernel: the option, what a worker declares,
and what a listener does with events newer than its code.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_deployments
"""

import json
import os
import unittest
from unittest import mock

import grpc

from kernable import Kernable, ListenerHandle, Worker, _Log, domain
from kernable.signing import Key
from kernable.v1 import kernable_pb2 as pb

KEY = Key("00" * 32)
LOG = _Log("test", "test.key", KEY.id)


def channel():
    return grpc.insecure_channel("unix:/nonexistent")


def event(position: int, type: str, schema: int, data: dict, erases: str = "") -> pb.Event:
    return pb.Event(global_position=position, tenant_id="default", domain="shop", aggregate_type="cart",
                    aggregate_id="c1", version=position, event_type=type, schema=schema, erases=erases,
                    payload=json.dumps(data).encode(), deployment="v2" if schema > 1 else "v1")


class OptionTest(unittest.TestCase):
    def test_an_invalid_deployment_raises_at_construction(self):
        for bad in ("has space", "x" * 129, "a/b", "ü"):
            with self.assertRaises(ValueError):
                Kernable(deployment=bad)
        with mock.patch.dict(os.environ, {"KERNABLE_DEPLOYMENT": "bad one"}):
            with self.assertRaises(ValueError):
                Kernable()


class WorkerTest(unittest.TestCase):
    def test_register_declares_the_deployment_and_every_event_type_it_knows(self):
        shop = domain("shop")
        shop.event("ItemAdded", 2).upcast(1, lambda d: d)
        shop.event("CartAudited", 3)  # declared by the domain only: neither applied nor written
        cart = shop.aggregate("cart", {})
        cart.apply("ItemAdded", lambda s, d: None)
        cart.apply("CartCleared", lambda s, d: None)  # applied only, never produced here
        cart.command("AddItem", lambda s, d: [])
        worker = Worker._new(channel(), KEY, [cart], LOG, "shop", "2026-11-11.1")
        register = worker._hello().register
        self.assertEqual(register.deployment, "2026-11-11.1")
        self.assertEqual({s.event_type: s.schema for s in register.event_schemas},
                         {"shop.ItemAdded": 2, "shop.CartCleared": 1, "shop.CartAudited": 3})


class ListenerTest(unittest.TestCase):
    """An event newer than the listener's code never reaches a handler: skipped or parked."""

    def run_batch(self, listener, events, retries=()):
        handle = ListenerHandle._new(channel(), KEY, listener, LOG, "", None)
        parked, skipped = handle._handle_batch(pb.Batch(events=events, retries=list(retries),
                                                        up_to=events[-1].global_position if events else 0))
        return handle, parked, skipped

    def model(self, factory, **options):
        shop = domain("shop")
        listener = getattr(shop, factory)("carts", **options)
        seen = []
        listener.on("ItemAdded", lambda data, evt: seen.append((evt.position, evt.deployment)))
        return listener, seen

    def test_a_read_model_skips_newer_events_counts_them_and_goes_on(self):
        listener, seen = self.model("read_model")
        events = [event(1, "ItemAdded", 1, {}), event(2, "ItemAdded", 2, {}), event(3, "ItemAdded", 1, {}),
                  event(4, "Unhandled", 9, {})]
        handle, parked, skipped = self.run_batch(listener, events)
        self.assertEqual(seen, [(1, "v1"), (3, "v1")])
        self.assertEqual(parked, [])
        self.assertEqual(list(skipped), [pb.Skipped(position=2, event_type="shop.ItemAdded", schema=2)])
        self.assertEqual((handle.skipped, handle.position), (1, 4))

    def test_a_reactor_that_parks_parks_newer_events_at_once(self):
        listener, seen = self.model("reactor", park_after=5)
        handle, parked, skipped = self.run_batch(listener, [event(1, "ItemAdded", 2, {}), event(2, "ItemAdded", 1, {})])
        self.assertEqual(seen, [(2, "v1")])
        self.assertEqual(skipped, [])
        self.assertEqual([(p.position, p.code) for p in parked], [(1, "unknown_event")])
        self.assertIn("shop.ItemAdded schema 2", parked[0].message)
        self.assertEqual(handle.skipped, 0)

    def test_listeners_that_cannot_park_skip(self):
        for options in ({"park_after": 0}, {"live": True}, {"instance": "a", "single": False}):
            listener, seen = self.model("reactor", **options)
            handle, parked, skipped = self.run_batch(listener, [event(1, "ItemAdded", 2, {})])
            self.assertEqual((seen, parked, len(skipped), handle.skipped), ([], [], 1, 1), options)

    def test_an_unknown_event_is_parked_erasing_or_not(self):
        """The same fixture in the three SDKs: a reactor that parks parks every event newer than
        its code, an erasing one too; listeners that cannot park skip them."""
        events = [event(1, "ItemAdded", 1, {}), event(2, "ItemAdded", 2, {}),
                  event(3, "CustomerForgotten", 2, {}, erases="customer-7"), event(4, "ItemAdded", 1, {})]
        shop = domain("shop")
        seen = []
        mails = shop.reactor("mails", park_after=3)
        for t in ("ItemAdded", "CustomerForgotten"):
            mails.on(t, lambda data, evt: seen.append(evt.position))
        _, parked, skipped = self.run_batch(mails, events)
        self.assertEqual(list(skipped), [])
        self.assertEqual([(p.position, p.code) for p in parked], [(2, "unknown_event"), (3, "unknown_event")])
        self.assertIn("shop.CustomerForgotten schema 2", parked[1].message)
        self.assertEqual(seen, [1, 4])
        for listener in (shop.reactor("live", live=True),
                         shop.reactor("replicated", park_after=3, instance="node-a", single=False),
                         shop.read_model("carts")):
            for t in ("ItemAdded", "CustomerForgotten"):
                listener.on(t, lambda data, evt: None)
            _, parked, skipped = self.run_batch(listener, events)
            self.assertEqual((list(parked), [s.position for s in skipped]), ([], [2, 3]), listener.name)

    def test_a_raw_handler_gets_newer_events(self):
        shop = domain("shop")
        listener = shop.read_model("archive")
        seen = []
        listener.on("ItemAdded", lambda data, evt: seen.append(evt.schema), raw=True)
        _, parked, skipped = self.run_batch(listener, [event(1, "ItemAdded", 2, {})])
        self.assertEqual((seen, parked, skipped), ([2], [], []))


if __name__ == "__main__":
    unittest.main()
