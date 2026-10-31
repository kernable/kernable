"""Unit tests of the tutorial's Order aggregate, without a kernel: fold events into state, then
call a command handler on that state."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "shop" / "py"))

from kernable import CommandContext, Rejected  # noqa: E402

from domain import order_aggregate  # noqa: E402


# region test
def decide(history, type, data=None):
    """Runs `type` on the state after `history`, as a worker would."""
    state = order_aggregate.fold([(e["type"], 1, e.get("data", {})) for e in history])
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return order_aggregate.handle(state, type, data, CommandContext.new("test", received_at=at))


class OrderTest(unittest.TestCase):
    placed = {"type": "OrderPlaced", "data": {"customerId": "c1"}}

    def test_a_draft_order_with_items_can_be_submitted(self):
        added = {"type": "ItemAdded", "data": {"sku": "apple", "qty": 2}}
        self.assertEqual(decide([self.placed, added], "Submit"), [{"type": "OrderSubmitted"}])

    def test_an_empty_order_cannot_be_submitted(self):
        with self.assertRaises(Rejected) as rejected:
            decide([self.placed], "Submit")
        self.assertEqual(rejected.exception.code, "invalid_state")
# endregion test


if __name__ == "__main__":
    unittest.main()
