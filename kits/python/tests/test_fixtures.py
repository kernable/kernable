"""The shared fixtures (kits/fixtures): given these events, when this command, then these
events (or this rejection), and these unique keys and facts. The TypeScript kit runs the same
cases, so the kits write the same events."""

import json
import re
import unittest
from datetime import datetime, timezone
from pathlib import Path

from kernable import CommandContext, EventMeta, Rejected

from kit import kit_domains

# Next to the kit in a copy (`kernable-sdk add kit`), or in the repository.
DIR = next(p for p in (Path(__file__).parent.parent / "fixtures", Path(__file__).parent.parent.parent / "fixtures") if p.exists())
AGGREGATES = {f"{d.name}.{a.name}": a for d in kit_domains() for a in d.aggregates}


def matches(expected, actual, path="$"):
    """None if `actual` matches `expected`; placeholders `<string>`, `<number>`, `<argon2>`, `<any>`."""
    if isinstance(expected, str) and re.fullmatch(r"<\w+>", expected):
        ok = (expected == "<any>"
              or (expected == "<string>" and isinstance(actual, str) and actual)
              or (expected == "<number>" and isinstance(actual, (int, float)) and not isinstance(actual, bool))
              or (expected == "<argon2>" and isinstance(actual, str) and actual.startswith("$argon2id$")))
        return None if ok else f"{path}: {actual!r} is not {expected}"
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            return f"{path}: {actual!r} is not {expected!r}"
        for i, (e, a) in enumerate(zip(expected, actual)):
            if m := matches(e, a, f"{path}[{i}]"):
                return m
        return None
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return f"{path}: {actual!r} is no object"
        if sorted(expected) != sorted(actual):
            return f"{path}: fields {sorted(actual)}, expected {sorted(expected)}"
        for k in expected:
            if m := matches(expected[k], actual[k], f"{path}.{k}"):
                return m
        return None
    same = expected == actual and type(expected) is type(actual) or (isinstance(expected, (int, float)) and isinstance(actual, (int, float)) and not isinstance(actual, bool) and expected == actual)
    return None if same else f"{path}: {actual!r}, expected {expected!r}"


def index_of(aggregate, state):
    held = aggregate.index(state)
    keys = [{"name": k.name, "value": k.value, "scope": k.scope, **({"subject": k.subject} if k.subject else {})} for k in held.keys]
    return {"keys": keys, "facts": list(held.facts)}


class FixturesTest(unittest.TestCase):
    pass


def make(domain: str, case: dict):
    def test(self):
        aggregate = AGGREGATES.get(f"{domain}.{case['aggregate']}")
        self.assertIsNotNone(aggregate, f"no aggregate {domain}.{case['aggregate']}")
        meta = dict(aggregate_id=case["aggregateId"], tenant=case["tenant"], domain=domain, aggregate_type=case["aggregate"])
        history = [(e["type"], 1, e["data"], EventMeta(e["type"], version=i + 1, **meta)) for i, e in enumerate(case["given"])]
        state = aggregate.fold(history)
        ctx = CommandContext.new(f"fixture-{case['name']}", received_at=datetime.fromtimestamp(case["now"] / 1000, tz=timezone.utc))
        try:
            produced = aggregate.handle(state, case["when"]["type"], case["when"].get("data") or {}, ctx,
                                        aggregate_id=case["aggregateId"], tenant=case["tenant"])
        except Rejected as err:
            self.assertEqual(err.code, case.get("rejected"), f"rejected with {err.code}: {err}")
            return
        self.assertNotIn("rejected", case, f"expected rejected {case.get('rejected')}, got {produced}")
        plain = json.loads(json.dumps([{"type": e["type"], "data": e.get("data") or {}} for e in produced]))
        self.assertIsNone(matches(case["then"], plain))
        if "index" in case:
            after = aggregate.fold([(e["type"], 1, e["data"], EventMeta(e["type"], version=len(history) + i + 1, **meta)) for i, e in enumerate(plain)], state)
            self.assertIsNone(matches(case["index"], index_of(aggregate, after)))
    return test


for file in sorted(DIR.glob("*.json")):
    if file.name == "authorization.json":
        continue
    spec = json.loads(file.read_text())
    for i, case in enumerate(spec["cases"]):
        setattr(FixturesTest, f"test_{spec['domain']}_{i:03d}_{re.sub(r'[^a-z0-9]+', '_', case['name'].lower())[:60]}", make(spec["domain"], case))


if __name__ == "__main__":
    unittest.main()
