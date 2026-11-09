"""The process builder: the design's example builds the shared model, and the local checks."""

import json
import struct
import unittest

from kernable import Aggregate, Command, Event, Query, domain, value
from kernable.processes import condition, duration_ms
from kernable.signing import membership_message, process_message
from kernable.v1 import kernable_pb2 as pb
from tests.kernel import ROOT

FIXTURE = json.load(open(f"{ROOT}/proto/test-vectors/process-model.json"))

shop = domain("shop")
payments = domain("payments")


class OrderSubmitted(Event):
    total: int


class OrderApproved(Event):
    pass


class PaymentReceived(Event):
    order_id: str
    at: int


class Confirm(Command):
    pass


class Ship(Command):
    pass


class NotifyDelay(Command):
    pass


class Cancel(Command):
    reason: str


class NeedsApproval(Query[bool]):
    order_id: str


class Order(Aggregate, domain=shop):
    status: str = "none"


for cmd in (Confirm, Ship, NotifyDelay, Cancel):
    Order.command(cmd)(lambda s: [])
shop.event(OrderSubmitted, OrderApproved)
payments.event(PaymentReceived)
shop.read_model("approvals").query(NeedsApproval)(lambda d: False)


def fulfilment(d=None):
    """The design's example in Python style, with a literal and a mapped field."""
    return (
        (d or domain("shop")).process("fulfilment")
        .start(OrderSubmitted, key="aggregate_id", data={"order_id": "aggregate_id", "total": "data.total"})
        .command("confirm", Confirm, aggregate="case.order_id")
        .decide("large", when="case.total > 1000", then="approval", otherwise="paid")
        .query("approval", NeedsApproval, data={"order_id": "case.order_id"}, paths={True: "approved", False: "paid"})
        .wait("approved", OrderApproved, key="aggregate_id")
        .wait("paid", PaymentReceived, key="data.order_id", data={"paid_at": "data.at"}, timeout="3d", on_timeout="cancel")
        .command("ship", Ship, aggregate="case.order_id", rejected={"out_of_stock": "notify"}, next="done")
        .command("notify", NotifyDelay, aggregate="case.order_id", next="done")
        .command("cancel", Cancel, aggregate="case.order_id", data={"reason": value("unpaid")})
        .end("done")
    )


def small():
    """A process with its start, to add steps to."""
    return domain("shop").process("small").start("shop.OrderSubmitted", key="aggregate_id")


class Builder(unittest.TestCase):
    def test_the_designs_example_builds_the_shared_model(self):
        self.assertEqual(fulfilment().model(), FIXTURE["model"])

    def test_messages_by_qualified_name_build_the_same(self):
        p = (domain("shop").process("fulfilment")
             .start("shop.OrderSubmitted", key="aggregate_id", data={"order_id": "aggregate_id", "total": "data.total"})
             .command("confirm", "shop.order.Confirm", aggregate="case.order_id")
             .decide("large", cases=[("case.total > 1000", "approval")], otherwise="paid")
             .query("approval", "shop.NeedsApproval", data={"order_id": "case.order_id"}, paths={True: "approved", False: "paid"})
             .wait("approved", "shop.OrderApproved", key="aggregate_id")
             .wait("paid", "payments.PaymentReceived", key="data.order_id", data={"paid_at": "data.at"}, timeout=259_200_000,
                   on_timeout="cancel")
             .command("ship", "shop.order.Ship", aggregate="case.order_id", rejected={"out_of_stock": "notify"}, next="done")
             .command("notify", "shop.order.NotifyDelay", aggregate="case.order_id", next="done")
             .command("cancel", "shop.order.Cancel", aggregate="case.order_id", data={"reason": value("unpaid")})
             .end("done"))
        self.assertEqual(p.model(), FIXTURE["model"])

    def test_unqualified_names_are_of_the_process_domain(self):
        p = (domain("shop").process("fulfilment")
             .start("OrderSubmitted", key="aggregate_id", data={"order_id": "aggregate_id", "total": "data.total"})
             .command("confirm", "order.Confirm", aggregate="case.order_id")
             .decide("large", when="case.total > 1000", then="approval", otherwise="paid")
             .query("approval", "NeedsApproval", data={"order_id": "case.order_id"}, paths={True: "approved", False: "paid"})
             .wait("approved", "OrderApproved", key="aggregate_id")
             .wait("paid", "payments.PaymentReceived", key="data.order_id", data={"paid_at": "data.at"}, timeout="3d",
                   on_timeout="cancel")
             .command("ship", "order.Ship", aggregate="case.order_id", rejected={"out_of_stock": "notify"}, next="done")
             .command("notify", "order.NotifyDelay", aggregate="case.order_id", next="done")
             .command("cancel", "order.Cancel", aggregate="case.order_id", data={"reason": value("unpaid")})
             .end("done"))
        self.assertEqual(p.model(), FIXTURE["model"])

    def test_a_domain_keeps_its_processes_once_per_name(self):
        d = domain("shop")
        p = fulfilment(d)
        self.assertEqual((d.processes, p.qualified), ([p], "shop.fulfilment"))
        with self.assertRaisesRegex(ValueError, "already"):
            d.process("fulfilment")

    def test_timers_queries_and_decisions_with_their_options(self):
        p = (small()
             .timer("wait-a-day", after="1d")
             .timer("until-due", at="case.due_at")
             .query("ask", "shop.GetRisk", answer="risk_level", paths={"high": "review", 2: "done"}, into="risk_answer",
                    otherwise="done")
             .decide("review", cases=[{"when": "case.risk_level == 'case.x_y' and not case.is_vip", "then": "done"}])
             .end("done"))
        steps = p.model()["steps"]
        self.assertEqual(steps[0], {"name": "wait-a-day", "kind": "timer", "afterMs": 86_400_000, "next": "until-due"})
        self.assertEqual(steps[1], {"name": "until-due", "kind": "timer", "at": "case.dueAt", "next": "ask"})
        self.assertEqual(steps[2], {"name": "ask", "kind": "query", "query": "shop.GetRisk", "answer": "riskLevel",
                                    "paths": [{"value": "high", "next": "review"}, {"value": 2, "next": "done"}],
                                    "into": "riskAnswer", "otherwise": "done"})
        # Only references are converted: the quoted literal stays as written.
        self.assertEqual(steps[3], {"name": "review", "kind": "decide",
                                    "when": [{"if": "case.riskLevel == 'case.x_y' and not case.isVip", "next": "done"}],
                                    "otherwise": "done"})

    def test_conditions_and_durations(self):
        self.assertEqual(condition('case.order_total >= 10 or case.a.b_c != "data.x_y"', "w"),
                         'case.orderTotal >= 10 or case.a.bC != "data.x_y"')
        self.assertEqual([duration_ms(d) for d in ("30s", "15m", "12h", "3d", 5)], [30_000, 900_000, 43_200_000, 259_200_000, 5])
        for bad in ("3 days", "0s", "1w", "", -1, 1.5):
            with self.assertRaises(ValueError, msg=repr(bad)):
                duration_ms(bad)


class Checks(unittest.TestCase):
    def fails(self, process, reason: str) -> None:
        with self.assertRaisesRegex(ValueError, reason):
            process.model()

    def test_no_start(self):
        self.fails(domain("shop").process("p").end("done"), "shop.p: no start")

    def test_one_start_only(self):
        with self.assertRaisesRegex(ValueError, "one start"):
            small().start("shop.OrderSubmitted", key="aggregate_id")

    def test_duplicate_steps(self):
        self.fails(small().command("a", "shop.order.Confirm", aggregate="case.id").end("a"), "two steps named a")

    def test_invalid_step_names(self):
        self.fails(small().end("not ok"), "invalid step name")

    def test_unknown_targets(self):
        self.fails(small().command("a", "shop.order.Confirm", aggregate="case.id", next="nowhere").end("done"),
                   "step a: 'nowhere' names no step")
        self.fails(small().decide("d", when="case.x", then="nowhere", otherwise="done").end("done"), "names no step")
        self.fails(small().wait("w", "shop.OrderApproved", key="aggregate_id", timeout="1d", on_timeout="nowhere").end("done"),
                   "names no step")
        self.fails(small().command("a", "shop.order.Confirm", aggregate="case.id", rejected={"x": "nowhere"}).end("done"),
                   "names no step")
        self.fails(small().query("q", "shop.Q", paths={True: "nowhere"}).end("done"), "names no step")

    def test_a_step_without_successor(self):
        self.fails(small().command("last", "shop.order.Confirm", aggregate="case.id"), "step last: no next step")
        self.fails(small().decide("last", when="case.x", then="last"), "step last: no otherwise")

    def test_bad_durations(self):
        self.fails(small().wait("w", "shop.OrderApproved", key="aggregate_id", timeout="3 days", on_timeout="done").end("done"),
                   "step w: '3 days' is no duration")
        self.fails(small().timer("t", after="0s").end("done"), "step t: '0s' is no positive duration")
        with self.assertRaisesRegex(ValueError, "go together"):
            small().wait("w", "shop.OrderApproved", key="aggregate_id", timeout="1d")

    def test_malformed_references_and_types(self):
        self.fails(small().command("a", "shop.order.Confirm", aggregate="order_id").end("done"), "step a: 'order_id' is no reference")
        self.fails(small().command("a", "shop.order.Confirm", aggregate="data.id").end("done"), "is no reference")
        self.fails(small().command("a", "Confirm", aggregate="case.id").end("done"), "aggregate.Type or domain.aggregate.Type")
        self.fails(small().command("a", "a.shop.order.Confirm", aggregate="case.id").end("done"), "is no command")
        self.fails(domain("shop").process("p").start("a.b.OrderSubmitted", key="aggregate_id").end("done"), "Type or domain.Type")
        self.fails(small().wait("w", "shop..X", key="aggregate_id").end("done"), "is no event")

        class Unbound(Event):
            pass

        self.fails(domain("shop").process("p").start(Unbound, key="aggregate_id").end("done"), "import the module")


class Signing(unittest.TestCase):
    @staticmethod
    def s(text: str) -> bytes:
        raw = text.encode()
        return struct.pack(">I", len(raw)) + raw

    def test_process_message(self):
        model = b'{"name":"shop.p"}'
        self.assertEqual(process_message(model, 1_700_000_000_000),
                         self.s("kernable.v1.process") + struct.pack(">I", len(model)) + model + struct.pack(">q", 1_700_000_000_000))

    def test_a_membership_signs_its_processes_only_when_it_has_any(self):
        m = pb.Membership(service="shop", issued_at=7)
        plain = membership_message(m)
        self.assertEqual(plain, self.s("kernable.v1.membership") + self.s("shop") + struct.pack(">QQQQq", 0, 0, 0, 0, 7))
        m.processes.extend(["shop.fulfilment", "shop.returns"])
        self.assertEqual(membership_message(m),
                         plain + self.s("processes") + struct.pack(">Q", 2) + self.s("shop.fulfilment") + self.s("shop.returns"))


if __name__ == "__main__":
    unittest.main()
