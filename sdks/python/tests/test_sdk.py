"""Python SDK: signing vectors, and a counter domain end to end against the real kernel.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import json
import os
import threading
import time
import unittest

from kernable import Kernable, NotSettled, Rejected, RequestContext, aggregate
from kernable.signing import (Key, challenge_message, command_message, rejection_message, result_message,
                              snapshot_message)
from kernable.v1 import kernable_pb2 as pb
from tests.kernel import ROOT, TestKernel

with open(os.path.join(ROOT, "proto", "test-vectors", "signing.json")) as f:
    VECTORS = json.load(f)


def vector_command(c) -> pb.Command:
    fields = ("command_id", "tenant_id", "aggregate_type", "aggregate_id", "command_type")
    cmd = pb.Command(**{k: c[k] for k in fields}, payload=bytes.fromhex(c["payload_hex"]))
    if "context" in c:
        context = dict(c["context"])
        attributes = [pb.Attribute(**a) for a in context.pop("attributes")]
        cmd.context.CopyFrom(pb.RequestContext(**context, attributes=attributes))
    return cmd


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
        n = VECTORS["snapshot"]["input"]
        stream = pb.Command(tenant_id=n["tenant_id"], aggregate_type=n["aggregate_type"], aggregate_id=n["aggregate_id"])
        self.check("snapshot", snapshot_message(stream, n["version"], n["schema"], bytes.fromhex(n["state_hex"])))


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

    def test_snapshots_shorten_replays_and_a_new_schema_rebuilds(self):
        applied = []

        def increment(total: int, data) -> list:
            if total + data["by"] > 20:
                raise Rejected("limit")
            return [{"type": "Incremented", "data": {"by": data["by"]}}]

        # v1 keeps the state as a dict.
        v1 = aggregate("Counter", {"n": 0}, snapshot_every=3)

        @v1.apply("Incremented")
        def _(state, e):
            applied.append(e)
            return {"n": state["n"] + e["by"]}

        v1.command("Increment", lambda state, data: increment(state["n"], data))
        worker = self.run_stream(self.k.worker(v1))
        for _ in range(10):
            self.assertTrue(self.increment(1).committed)
        self.assertLessEqual(len(applied), 25)  # without snapshots: 45 for the history alone
        worker.stop()
        time.sleep(0.1)

        # v2 keeps it as a number: the old snapshot does not fit.
        v2 = aggregate("Counter", 0, snapshot_every=3, schema="2")
        v2.apply("Incremented", lambda n, e: n + e["by"])
        v2.command("Increment", increment)
        self.run_stream(self.k.worker(v2))
        self.assertFalse(self.increment(11).committed)  # 10 + 11 > 20: rebuilt from the events
        self.assertTrue(self.increment(10).committed)

    def test_request_context_reaches_handlers_events_and_follow_up_commands(self):
        seen = []
        ticket = aggregate("Ticket", {"open": False})
        ticket.apply("Opened", lambda s, e: {"open": True})

        @ticket.command("Open")
        def _(state, data, ctx):
            seen.append(ctx.request)
            return [{"type": "Opened"}]

        @ticket.command("Note")
        def _(state, data, ctx):
            seen.append(ctx.request)
            return [{"type": "Noted"}]

        self.k.close()  # this test needs other command types: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["Open", "Note"])
        self.kernel.start()
        service = RequestContext(sender_identity="svc-1", attributes={"channel": "api"})
        self.k = Kernable(self.kernel.url, self.kernel.app_key, context=service)
        self.run_stream(self.k.worker(ticket))
        reactor = self.k.reactor("notes")

        @reactor.on("Opened")
        def _(e, ctx):
            self.k.submit("Ticket", e.aggregate_id, "Note", id=f"note-{e.aggregate_id}", context=ctx.follow(),
                          wait="committed")

        self.run_stream(reactor)
        opened = self.k.submit("Ticket", "t1", "Open", id="open-t1",
                               context=RequestContext(sender_identity="svc-1", sender_session="s-9"))
        request = RequestContext(sender_identity="svc-1", sender_session="s-9", correlation_id="open-t1")
        self.assertEqual(opened.events[0].request, request)
        follow_up = RequestContext(sender_identity="svc-1", sender_session="s-9", correlation_id="open-t1",
                                   causation_id="open-t1")
        self.assertEqual(seen, [request, follow_up])

        stale = self.k.submit("Ticket", "t1", "Note", context=RequestContext(expected_version=1))
        self.assertEqual((stale.committed, stale.code), (False, "version_mismatch"))
        time.sleep(0.15)
        self.assertEqual(self.kernel.verify().returncode, 0)


if __name__ == "__main__":
    unittest.main()
