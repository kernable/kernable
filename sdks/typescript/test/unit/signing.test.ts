import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import type { WireCommand } from "../../src/rpc.ts";
import {
  Key,
  cacheMessage,
  membershipMessage,
  processActMessage,
  processMessage,
  challengeMessage,
  checkpointMessage,
  commandMessage,
  cancelMessage,
  dataMessage,
  derivedId,
  queryMessage,
  eventHash,
  indexMessage,
  rejectionMessage,
  resultMessage,
  snapshotMessage,
  subscribeMessage,
  uniqueKeyId,
  valueHash,
  vaultRef,
} from "../../src/signing.ts";

const v = JSON.parse(readFileSync(new URL("../../../../proto/test-vectors/signing.json", import.meta.url), "utf8"));
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
  const events = r.events.map((e: any) => ({ event_type: e.event_type, schema: e.schema, payload: hex(e.payload_hex) }));
  check("result", resultMessage(command(r.command), r.expected_version, events));
  check("rejection", rejectionMessage(v.rejection.input.command_id, v.rejection.input));
  check("challenge", challengeMessage(hex(v.challenge.input.nonce_hex)));
  const q = v.query.input;
  check("query", queryMessage({ ...q, payload: hex(q.payload_hex) }));
  const qw = v.query_wait_only.input;
  check("query_wait_only", queryMessage({ ...qw, payload: hex(qw.payload_hex) }));
  const cs = v.command_scheduled.input;
  check("command_scheduled", commandMessage(command(cs)));
  check("cancel", cancelMessage(v.cancel.input));
  const qt = v.query_target.input;
  check("query_target", queryMessage({ ...qt, payload: hex(qt.payload_hex) }));
  const snap = v.snapshot.input;
  check("snapshot", snapshotMessage(snap, snap.version, snap.schema, hex(snap.state_hex)));
  const sub = v.subscribe.input;
  check("subscribe", subscribeMessage(sub.after_position, sub.event_types, sub.issued_at));
  const cp = v.checkpoint.input;
  check("checkpoint", checkpointMessage(cp.global_position, hex(cp.chain_hash_hex), cp.created_at));
});

test("data and cache messages match the test vectors", () => {
  // A file request with a full context; a cache request without one encodes like an empty one.
  const d = v.data_put.input;
  check("data_put", dataMessage(d.op, { ...d, data: hex(d.data_hex), context: d.context }));
  const c = v.cache_set.input;
  assert.equal(c.context, undefined);
  check("cache_set", cacheMessage(c.op, { ...c, value: hex(c.value_hex), context: c.context }));
});

test("membership messages match the test vectors", () => {
  check("membership", membershipMessage(v.membership.input));
});

test("process messages match the test vectors", () => {
  check("process", processMessage(hex(v.process.input.model_hex), v.process.input.issued_at));
  const act = v.process_act.input;
  check("process_act", processActMessage(act.name, act.context, act.issued_at));
});

test("vault references and vault fields in signed messages match the test vectors", () => {
  const refs = v.vault_ref.input.map((i: any) => vaultRef(hex(i.salt_hex), i.subject, i.secret, hex(i.value_hex)));
  assert.deepEqual(refs, v.vault_ref.refs);
  const c = v.command_vault.input;
  check("command_vault", commandMessage({ ...command(c.command), vault_refs: c.vault_refs }));
  const r = v.result.input;
  const events = r.events.map((e: any, i: number) => ({
    event_type: e.event_type,
    schema: e.schema,
    payload: hex(e.payload_hex),
    vault_refs: v.result_vault.input.vault_refs[i],
    erases: v.result_vault.input.erases[i],
  }));
  check("result_vault", resultMessage(command(r.command), r.expected_version, events));
  const e = v.event_hash.input;
  const event = {
    ...e,
    payload: hex(e.payload_hex),
    hash: Buffer.alloc(0),
    producer_sig: { key_id: e.producer_key_id, sig: hex(e.producer_sig_hex) },
    ...v.event_hash_vault.input,
  };
  assert.equal(eventHash(hex(e.prev_hex), event).toString("hex"), v.event_hash_vault.hash);
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

test("index messages and unique key IDs match the test vectors", () => {
  const i = v.result_index.input;
  const keys = i.keys.map((k: any) => ({ ...k, value_hash: hex(k.value_hash_hex) }));
  const requires = i.requires.map((r: any) => ({ ...r, value_hash: hex(r.value_hash_hex) }));
  check("result_index", indexMessage(command(i.command), i.expected_version, keys, i.facts, requires));
  // The values behind the vectors' hashes.
  assert.deepEqual([valueHash("A-1001"), valueHash("max@firma.de")].map((h) => h.toString("hex")), i.keys.map((k: any) => k.value_hash_hex));
  const u = v.unique_key_id.input;
  const ids = u.keys.map((k: any) => uniqueKeyId(u.tenant_id, u.domain, u.aggregate_type, { ...k, value_hash: hex(k.value_hash_hex) }).toString("hex"));
  assert.deepEqual(ids, v.unique_key_id.ids);
});
