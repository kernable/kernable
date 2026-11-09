"""The shared authorization fixtures (kits/fixtures/authorization.json): the middleware learns
a world of events, then decides on a check: allowed, or refused with a code. Every language's kit
decides the same way."""

import copy
import json
import re
import unittest
from pathlib import Path
from types import SimpleNamespace

from kernable import Check, ListenerContext, MiddlewareContext, Rejected, RequestContext, Target
from kernable.messages import from_wire

from kit import kit_domains, relation
from kit.authorization.middleware import authorization_listener
from kit.authorization.policy import MemoryKeyValues

# The fixtures: next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
_here = Path(__file__).resolve().parent
_file = next(p for p in (_here.parent / "fixtures" / "authorization.json", _here.parent.parent / "fixtures" / "authorization.json") if p.exists())
fixtures = json.loads(_file.read_text())
for r in fixtures["relations"]:
    relation(r["type"], r["name"]).set(r["event"], lambda e, field=r["field"]: e.data.get(field))
kit_domains()


def learn(store, events) -> None:
    for i, e in enumerate(events):
        handler = authorization_listener._handlers.get(f"{e['domain']}.{e['type']}")
        if handler is None:
            continue  # the middleware does not care about this event
        meta = SimpleNamespace(domain=e["domain"], aggregate_type=e["aggregateType"], type=e["type"], tenant=e["tenant"],
                               aggregate_id=e["aggregateId"], version=1, command_id=f"c{i}", position=i + 1)
        handler(copy.deepcopy(e["data"]), meta, ListenerContext(replaying=False, store=store))


def check_of(c: dict) -> Check:
    request = RequestContext(**from_wire(c.get("request") or {}))
    target = Target(c["target"]["type"], c["target"]["id"]) if c.get("target") else None
    return Check(kind=c["kind"], id="", tenant=c["tenant"], domain=c["domain"], type=c["type"], data=c.get("data"), request=request,
                 aggregate_type=c.get("aggregateType", ""), aggregate_id=c.get("aggregateId", ""), target=target)


class AuthorizationTest(unittest.TestCase):
    pass


def make(case):
    def test(self):
        store = MemoryKeyValues()
        learn(store, fixtures["world"] + case.get("extra", []))
        _, middleware = authorization_listener._middlewares["before"]
        outcome = "allow"
        try:
            middleware(check_of(case["check"]), None, MiddlewareContext(store=store))
        except Rejected as err:
            outcome = err.code
        self.assertEqual(outcome, case["expect"], case["name"])
    return test


for i, case in enumerate(fixtures["cases"]):
    setattr(AuthorizationTest, f"test_{i:03d}_{re.sub(r'[^a-z0-9]+', '_', case['name'].lower())[:60]}", make(case))


if __name__ == "__main__":
    unittest.main()
