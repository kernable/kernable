import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type { WireCommand } from "../src/rpc.ts";
import {
  Key,
  challengeMessage,
  checkpointMessage,
  commandMessage,
  derivedId,
  eventHash,
  rejectionMessage,
  resultMessage,
  snapshotMessage,
  subscribeMessage,
} from "../src/signing.ts";

const v = JSON.parse(readFileSync(new URL("../../../proto/test-vectors/signing.json", import.meta.url), "utf8"));
const hex = (s: string) => Buffer.from(s, "hex");
const key = Key.fromHex(v.key.seed);

function check(name: string, message: Buffer) {
  assert.equal(message.toString("hex"), v[name].message, `${name} message`);
  assert.equal(key.sign(message).sig.toString("hex"), v[name].signature, `${name} signature`);
}

function command(c: any): WireCommand {
  return { ...c, payload: hex(c.payload_hex) };
}

test("key ID matches", () => assert.equal(key.id, v.key.key_id));

test("message layouts and signatures match the test vectors", () => {
  check("command", commandMessage(command(v.command.input)));
  const r = v.result.input;
  const events = r.events.map((e: any) => ({ event_type: e.event_type, payload: hex(e.payload_hex) }));
  check("result", resultMessage(command(r.command), r.expected_version, events));
  check("rejection", rejectionMessage(v.rejection.input.command_id, v.rejection.input));
  check("challenge", challengeMessage(hex(v.challenge.input.nonce_hex)));
  const snap = v.snapshot.input;
  check("snapshot", snapshotMessage(snap, snap.version, snap.schema, hex(snap.state_hex)));
  const sub = v.subscribe.input;
  check("subscribe", subscribeMessage(sub.after_position, sub.event_types, sub.issued_at));
  const cp = v.checkpoint.input;
  check("checkpoint", checkpointMessage(cp.global_position, hex(cp.chain_hash_hex), cp.created_at));
});

test("derived IDs match the test vector", () => {
  const d = v.derived_id;
  assert.deepEqual(d.input.n.map((n: number) => derivedId(d.input.command_id, n)), d.ids);
});

test("event hash matches the test vector", () => {
  const e = v.event_hash.input;
  const event = {
    ...e,
    payload: hex(e.payload_hex),
    hash: Buffer.alloc(0),
    producer_sig: { key_id: e.producer_key_id, sig: hex(e.producer_sig_hex) },
  };
  assert.equal(eventHash(hex(e.prev_hex), event).toString("hex"), v.event_hash.hash);
});
