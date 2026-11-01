"""Python SDK: apply, fold and a command handler, called directly without a kernel.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_apply
"""

import unittest
from datetime import datetime, timedelta, timezone

from kernable import CommandContext, Rejected, RequestContext, domain
from kernable.signing import derived_id


class ApplyTest(unittest.TestCase):
    """apply: a handler changes a copy of the state in place, or returns a state that replaces it."""

    def test_apply_changes_a_copy_in_place_deletes_or_replaces(self):
        account = domain("crm").aggregate("Account", {"tags": [], "status": "new"})

        @account.apply("Opened")
        def _(state, data):
            state["email"] = data["email"]
            state["tags"].append("open")

        account.apply("Reset", lambda state, data: {"tags": [], "status": "reset"})

        @account.apply("Closed")
        def _(state, data):
            del state["email"]

        opened = account.fold([("Opened", 1, {"email": "a@example.com"})])
        self.assertEqual(opened, {"email": "a@example.com", "tags": ["open"], "status": "new"})
        self.assertEqual(account.initial, {"tags": [], "status": "new"})  # untouched
        closed = account.fold([("Closed", 1, {})], opened)
        self.assertEqual(closed, {"tags": ["open"], "status": "new"})
        self.assertIn("email", opened)  # the state before is untouched
        self.assertEqual(account.fold([("Opened", 1, {"email": "b@example.com"}), ("Reset", 1, {})]),
                         {"tags": [], "status": "reset"})

    def test_a_command_handler_runs_directly_with_a_command_context(self):
        account = domain("crm").aggregate("Account", {"email": None})

        @account.command("Close")
        def _(state, data, cmd, ctx):
            if state["email"] is None:
                raise Rejected("no account", "not_found")
            what = f"{cmd.domain}.{cmd.aggregate_type}.{cmd.type}"
            return [{"type": "Closed", "data": {"id": ctx.id(), "by": cmd.command_id, "what": what, "data": data,
                                                "at": ctx.now().isoformat(), "who": ctx.request.sender_identity}}]

        at = datetime(2026, 1, 2, tzinfo=timezone.utc)
        ctx = CommandContext.new("close-1", RequestContext(sender_identity="alice"), at)
        self.assertEqual(ctx.deadline, at + timedelta(seconds=30))
        with self.assertRaises(Rejected) as rejected:
            account.handle({"email": None}, "Close", None, ctx)
        self.assertEqual(rejected.exception.code, "not_found")
        closed = account.handle({"email": "a@example.com"}, "Close", None, ctx)
        self.assertEqual(closed, [{"type": "Closed", "data": {
            "id": derived_id("close-1", 0), "by": "close-1", "what": "crm.Account.Close", "data": {},
            "at": "2026-01-02T00:00:00+00:00", "who": "alice"}}])
        with self.assertRaisesRegex(ValueError, "no handler for Open on crm.Account"):
            account.handle({}, "Open", {}, ctx)


if __name__ == "__main__":
    unittest.main()
