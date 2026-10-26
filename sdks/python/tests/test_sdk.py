"""Python SDK: signing vectors, and a counter domain end to end against the real kernel.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import json
import os
import threading
import time
import unittest

from kernable import Kernable, NotSettled, Rejected, aggregate
from kernable.signing import Key, challenge_message, command_message, rejection_message, result_message
from kernable.v1 import kernable_pb2 as pb
from tests.kernel import ROOT, TestKernel

with open(os.path.join(ROOT, "proto", "test-vectors", "signing.json")) as f:
    VECTORS = json.load(f)


def vector_command(c) -> pb.Command:
    fields = ("command_id", "tenant_id", "aggregate_type", "aggregate_id", "command_type")
    return pb.Command(**{k: c[k] for k in fields}, payload=bytes.fromhex(c["payload_hex"]))


class SigningVectors(unittest.TestCase):
    def check(self, name: str, message: bytes):
        key = Key(VECTORS["key"]["seed"])
        self.assertEqual(key.id, VECTORS["key"]["key_id"])
        self.assertEqual(message.hex(), VECTORS[name]["message"])
        self.assertEqual(key.sign(message).sig.hex(), VECTORS[name]["signature"])

    def test_messages_and_signatures_match(self):
        self.check("command", command_message(vector_command(VECTORS["command"]["input"])))
        r = VECTORS["result"]["input"]
        events = [pb.NewEvent(event_type=e["event_type"], payload=bytes.fromhex(e["payload_hex"])) for e in r["events"]]
        self.check("result", result_message(vector_command(r["command"]), r["expected_version"], events))
        j = VECTORS["rejection"]["input"]
        self.check("rejection", rejection_message(j["command_id"], pb.Rejected(code=j["code"], message=j["message"])))
        self.check("challenge", challenge_message(bytes.fromhex(VECTORS["challenge"]["input"]["nonce_hex"])))


counter = aggregate("Counter", 0)


@counter.apply("Incremented")
def _(state, data):
    return state + data["by"]


@counter.command("Increment")
def _(state, data):
    if data["by"] <= 0:
        raise Rejected("must increase", "invalid")
    if state + data["by"] > 10:
        raise Rejected("limit reached", "limit")
    return [{"type": "Incremented", "data": {"by": data["by"]}}]


def background(target) -> threading.Thread:
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["Increment"], "settle_timeout_ms = 500")
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")
        self.streams = []

    def tearDown(self):
        for s in self.streams:
            s.stop()
        self.k.close()
        self.kernel.dispose()

    def run_stream(self, stream):
        self.streams.append(stream)
        background(stream.run)
        self.assertTrue(stream.ready.wait(5), "stream did not connect")
        return stream

    def increment(self, by: int, **kw):
        return self.k.submit("Counter", "c1", "Increment", {"by": by}, **kw)

    def test_worker_read_model_and_reactor(self):
        self.run_stream(self.k.worker(counter))
        totals: dict[str, int] = {}
        seen_by_reactor: list[tuple[int, int]] = []

        model = self.k.read_model("totals")
        model.on("Incremented", lambda e: totals.__setitem__(e.aggregate_id, totals.get(e.aggregate_id, 0) + e.data["by"]))
        reactor = self.k.reactor("audit")

        @reactor.on("Incremented")
        def _(e):
            seen_by_reactor.append((e.position, totals["c1"]))  # the read model ran first

        self.run_stream(model)
        self.run_stream(reactor)

        first = self.increment(3)
        self.assertTrue(first.committed)
        self.assertEqual([e.version for e in first.events], [1])
        self.assertEqual(totals, {"c1": 3})  # read your writes, no polling
        self.increment(4)
        self.assertEqual(totals, {"c1": 7})
        self.assertEqual(seen_by_reactor, [(1, 3), (2, 7)])

        limit = self.increment(5)
        self.assertEqual((limit.committed, limit.code), (False, "limit"))
        self.assertEqual(self.increment(0).code, "invalid")

        # Idempotent: the same ID returns the original outcome.
        once = self.increment(1, id="fixed")
        again = self.increment(1, id="fixed")
        self.assertEqual([e.position for e in once.events], [e.position for e in again.events])

        # A stopped listener still counts.
        reactor.stop()
        with self.assertRaises(NotSettled) as ctx:
            self.increment(1)
        self.assertTrue(ctx.exception.outcome.committed)
        self.assertTrue(self.increment(1, wait="committed").committed)
        command_id = self.k.enqueue("Counter", "c2", "Increment", {"by": 1})
        self.assertTrue(command_id)

        time.sleep(0.15)  # let the kernel write a checkpoint
        out = self.kernel.verify()
        self.assertEqual(out.returncode, 0, out.stdout)


if __name__ == "__main__":
    unittest.main()
