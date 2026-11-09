"""Python SDK: messages and signatures match the test vectors.

Run from sdks/python: .venv/bin/python -m unittest tests.unit.test_signing
"""

import json
import os
import unittest

from kernable.signing import (Key, cache_message, cancel_message, challenge_message, command_message, data_message, derived_id,
                              membership_message, query_message, rejection_message, result_message, snapshot_message, vault_ref)
from kernable.v1 import kernable_pb2 as pb
from tests.kernel import ROOT

with open(os.path.join(ROOT, "proto", "test-vectors", "signing.json")) as f:
    VECTORS = json.load(f)


def vector_context(c: dict) -> pb.RequestContext:
    context = dict(c)
    attributes = [pb.Attribute(**a) for a in context.pop("attributes")]
    return pb.RequestContext(**context, attributes=attributes)


def vector_command(c) -> pb.Command:
    fields = ("command_id", "tenant_id", "domain", "aggregate_type", "aggregate_id", "command_type")
    cmd = pb.Command(**{k: c[k] for k in fields}, payload=bytes.fromhex(c["payload_hex"]))
    if "context" in c:
        cmd.context.CopyFrom(vector_context(c["context"]))
    return cmd


class SigningVectors(unittest.TestCase):
    def check(self, name: str, message: bytes):
        key = Key(VECTORS["key"]["seed"])
        self.assertEqual(key.id, VECTORS["key"]["key_id"])
        self.assertEqual(message.hex(), VECTORS[name]["message"])
        self.assertEqual(key.sign(message).sig.hex(), VECTORS[name]["signature"])

    def test_messages_and_signatures_match(self):
        self.check("command", command_message(vector_command(VECTORS["command"]["input"])))
        s = VECTORS["command_scheduled"]["input"]
        scheduled = vector_command(s)
        scheduled.deliver_at, scheduled.deliver_until = s["deliver_at"], s["deliver_until"]
        self.check("command_scheduled", command_message(scheduled))
        c = dict(VECTORS["cancel"]["input"])
        context = vector_context(c.pop("context"))
        self.check("cancel", cancel_message(pb.CancelRequest(**c, context=context)))
        r = VECTORS["result"]["input"]
        events = [pb.NewEvent(event_type=e["event_type"], schema=e["schema"], payload=bytes.fromhex(e["payload_hex"])) for e in r["events"]]
        self.check("result", result_message(vector_command(r["command"]), r["expected_version"], events))
        j = VECTORS["rejection"]["input"]
        self.check("rejection", rejection_message(j["command_id"], pb.Rejected(code=j["code"], message=j["message"])))
        self.check("challenge", challenge_message(bytes.fromhex(VECTORS["challenge"]["input"]["nonce_hex"])))
        n = VECTORS["snapshot"]["input"]
        stream = pb.Command(tenant_id=n["tenant_id"], domain=n["domain"], aggregate_type=n["aggregate_type"],
                            aggregate_id=n["aggregate_id"])
        self.check("snapshot", snapshot_message(stream, n["version"], n["schema"], bytes.fromhex(n["state_hex"])))
        q = dict(VECTORS["query"]["input"])
        context = vector_context(q.pop("context"))
        payload = bytes.fromhex(q.pop("payload_hex"))
        query = pb.QueryRequest(**q, payload=payload, context=context)
        self.check("query", query_message(query))
        q = dict(VECTORS["query_wait_only"]["input"])
        context = vector_context(q.pop("context"))
        payload = bytes.fromhex(q.pop("payload_hex"))
        self.check("query_wait_only", query_message(pb.QueryRequest(**q, payload=payload, context=context)))
        q = dict(VECTORS["query_target"]["input"])
        context = vector_context(q.pop("context"))
        payload = bytes.fromhex(q.pop("payload_hex"))
        self.check("query_target", query_message(pb.QueryRequest(**q, payload=payload, context=context)))
        refs = [vault_ref(bytes.fromhex(i["salt_hex"]), i["subject"], i["secret"], bytes.fromhex(i["value_hex"]))
                for i in VECTORS["vault_ref"]["input"]]
        self.assertEqual(refs, VECTORS["vault_ref"]["refs"])
        c = vector_command(VECTORS["command_vault"]["input"]["command"])
        c.vault_refs.extend(VECTORS["command_vault"]["input"]["vault_refs"])
        self.check("command_vault", command_message(c))
        v = VECTORS["result_vault"]["input"]
        for e, refs_, erases in zip(events, v["vault_refs"], v["erases"]):
            e.vault_refs.extend(refs_)
            e.erases = erases
        self.check("result_vault", result_message(vector_command(r["command"]), r["expected_version"], events))
        d = VECTORS["derived_id"]
        self.assertEqual([derived_id(d["input"]["command_id"], n) for n in d["input"]["n"]], d["ids"])
        p = dict(VECTORS["data_put"]["input"])
        op, data, context = p.pop("op"), bytes.fromhex(p.pop("data_hex")), vector_context(p.pop("context"))
        self.check("data_put", data_message(op, pb.DataRequest(**p, data=data, context=context)))
        s = dict(VECTORS["cache_set"]["input"])  # without a context: encoded like an empty one
        op, value = s.pop("op"), bytes.fromhex(s.pop("value_hex"))
        self.check("cache_set", cache_message(op, pb.CacheRequest(**s, value=value)))
        self.assertEqual(cache_message(op, pb.CacheRequest(**s, value=value, context=pb.RequestContext())).hex(),
                         VECTORS["cache_set"]["message"])
        m = dict(VECTORS["membership"]["input"])
        metadata = [pb.Attribute(**a) for a in m.pop("metadata")]
        self.check("membership", membership_message(pb.Membership(**m, metadata=metadata)))


if __name__ == "__main__":
    unittest.main()
