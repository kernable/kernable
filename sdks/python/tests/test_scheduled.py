"""Scheduled commands: enqueued with `at`, run at their time while the aggregate's other commands
pass; cancelled before they are due; a middleware sees the cancel as the command's type.

Run from sdks/python: .venv/bin/python -m unittest tests.test_scheduled   (after `cargo build`)
"""

import os
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from kernable import CancelOutcome, Kernable, Rejected, RequestContext, domain
from tests.kernel import TestKernel


class Scheduled(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["shop.*"])
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

    def run_stream(self, stream):
        self.streams.append(stream)
        threading.Thread(target=stream.run, daemon=True).start()
        self.assertTrue(stream.ready.wait(5), "stream did not connect")
        return stream

    def test_a_scheduled_command_runs_at_its_time_others_pass_meanwhile_and_a_cancel_stops_one(self):
        shop = domain("shop")
        order = shop.aggregate("order", 0)
        ran: list[tuple[str, float]] = []

        @order.command("Remind")
        def _(state, data, cmd):
            ran.append((cmd.command_id, time.time() * 1000))
            return [{"type": "Reminded"}]

        order.command("Note")(lambda state, data: [{"type": "Noted"}])
        cancels: list[str] = []
        guard = shop.read_model("guard")

        @guard.middleware(["shop.*"])
        def _(check):
            if check.kind != "cancel":
                return
            cancels.append(f"{check.type} {check.aggregate_type} {check.aggregate_id}")
            if check.request.sender_identity == "mallory":
                raise Rejected("Not yours", "forbidden")

        self.run_stream(self.k.worker(order))
        self.run_stream(self.k.listener(guard))

        at = int(time.time() * 1000) + 2_000
        id = self.k.enqueue("shop", "order", "o1", "Remind", at=at)
        # The aggregate is not blocked by it.
        self.assertTrue(self.k.submit("shop", "order", "o1", "Note").committed)
        self.assertEqual(ran, [])
        with self.assertRaisesRegex(ValueError, "enqueue"):
            self.k.submit("shop", "order", "o1", "Remind", at=at)

        second = self.k.enqueue("shop", "order", "o2", "Remind", at=datetime.now(timezone.utc) + timedelta(seconds=60))
        self.assertEqual(self.k.cancel(second, context=RequestContext(sender_identity="mallory")),
                         CancelOutcome(cancelled=False, code="forbidden", message="Not yours"))
        self.assertEqual(self.k.cancel(second), CancelOutcome(cancelled=True))
        self.assertEqual(self.k.cancel(second), CancelOutcome(cancelled=True), "cancelling twice answers the same")
        self.assertEqual(cancels, ["Remind order o2", "Remind order o2"])
        outcome = self.k.submit("shop", "order", "o2", "Remind", id=second)
        self.assertEqual((outcome.committed, outcome.code), (False, "cancelled"))
        self.assertFalse(self.k.cancel("nope").cancelled)

        # The first one runs at its time, not before. Asking for its result by its ID waits for it.
        result = self.k.submit("shop", "order", "o1", "Remind", id=id)
        self.assertTrue(result.committed)
        self.assertEqual(len(ran), 1)
        self.assertEqual(ran[0][0], id)
        self.assertGreaterEqual(ran[0][1], at, "not before its time")
        self.assertFalse(self.k.cancel(id).cancelled, "too late once it ran")


if __name__ == "__main__":
    unittest.main()
