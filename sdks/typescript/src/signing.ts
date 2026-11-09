// Signed messages and the event hash chain, as specified in proto/SIGNING.md.

import { createHash, createPrivateKey, createPublicKey, sign, type KeyObject } from "node:crypto";
import { readFileSync } from "node:fs";

import type {
  WireCacheRequest,
  WireCancelRequest,
  WireCommand,
  WireDataRequest,
  WireEvent,
  WireMembership,
  WireNewEvent,
  WireQueryRequest,
  WireRejected,
  WireRequestContext,
  WireRequirement,
  WireSignature,
  WireUniqueKey,
} from "./rpc.ts";

// PKCS#8 DER prefix for a raw 32-byte Ed25519 seed.
const PKCS8_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");

export class Key {
  readonly id: string;
  private readonly key: KeyObject;

  private constructor(seed: Buffer) {
    this.key = createPrivateKey({ key: Buffer.concat([PKCS8_PREFIX, seed]), format: "der", type: "pkcs8" });
    this.id = createPublicKey(this.key).export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
  }

  /** From a hex-encoded 32-byte seed (the content of a `.key` file). */
  static fromHex(hex: string): Key {
    // Buffer.from would quietly stop at the first character that is not hex.
    if (!/^[0-9a-fA-F]{64}$/.test(hex.trim())) throw new Error("not a 32-byte key in hex");
    return new Key(Buffer.from(hex.trim(), "hex"));
  }

  static fromFile(path: string): Key {
    try {
      return Key.fromHex(readFileSync(path, "utf8"));
    } catch (err) {
      throw new Error(`key ${path}: ${(err as Error).message}`);
    }
  }

  sign(message: Buffer): WireSignature {
    return { key_id: this.id, sig: sign(null, message, this.key) };
  }
}

/** Length-prefixed fields: `u32 BE length || bytes`; integers are 8 bytes BE. */
class Msg {
  private parts: Buffer[] = [];

  constructor(tag: string) {
    this.str(tag);
  }

  bytes(b: Buffer): this {
    const len = Buffer.alloc(4);
    len.writeUInt32BE(b.length);
    this.parts.push(len, b);
    return this;
  }

  str(s: string): this {
    return this.bytes(Buffer.from(s, "utf8"));
  }

  u64(n: number): this {
    const b = Buffer.alloc(8);
    b.writeBigUInt64BE(BigInt(n));
    this.parts.push(b);
    return this;
  }

  i64(n: number): this {
    const b = Buffer.alloc(8);
    b.writeBigInt64BE(BigInt(n));
    this.parts.push(b);
    return this;
  }

  /** A request context; an absent one encodes like an empty one. */
  context(c: WireRequestContext | null | undefined): this {
    this.str(c?.sender_tenant ?? "")
      .str(c?.sender_identity ?? "")
      .str(c?.sender_account ?? "")
      .str(c?.sender_session ?? "")
      .str(c?.on_behalf_of ?? "");
    if (c?.expected_version != null) this.u64(1).u64(c.expected_version);
    else this.u64(0);
    this.str(c?.correlation_id ?? "")
      .str(c?.causation_id ?? "")
      .str(c?.trace_id ?? "")
      .u64(c?.bypass_authorization ? 1 : 0)
      .u64(c?.attributes.length ?? 0);
    for (const a of c?.attributes ?? []) this.str(a.key).str(a.value);
    return this;
  }

  /** Vault references, appended only when there are any (see SIGNING.md). */
  refs(refs: string[] | undefined): this {
    if (!refs?.length) return this;
    this.u64(refs.length);
    for (const r of refs) this.str(r);
    return this;
  }

  done(): Buffer {
    return Buffer.concat(this.parts);
  }
}

type Stream = Pick<WireCommand, "command_id" | "tenant_id" | "domain" | "aggregate_type" | "aggregate_id">;

const stream = (m: Msg, c: Stream) =>
  m.str(c.command_id).str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id);

export function commandMessage(c: WireCommand): Buffer {
  const m = stream(new Msg("kernable.v1.command"), c)
    .str(c.command_type)
    .bytes(c.payload)
    .context(c.context)
    .refs(c.vault_refs);
  // Each only when set, so a command without them signs as before.
  if (c.deliver_at) m.str("deliver_at").i64(c.deliver_at);
  if (c.deliver_until) m.str("deliver_until").i64(c.deliver_until);
  return m.done();
}

export function cancelMessage(c: WireCancelRequest): Buffer {
  return new Msg("kernable.v1.cancel").str(c.command_id).str(c.tenant_id).context(c.context).i64(c.issued_at).done();
}

/** The reference of a value kept in the vault: it commits to salt, subject, kind and value. */
export function vaultRef(salt: Buffer, subject: string, secret: boolean, value: Buffer): string {
  const m = new Msg("kernable.v1.vault").bytes(salt).str(subject).u64(secret ? 1 : 0).bytes(value);
  return createHash("sha256").update(m.done()).digest("hex");
}

export function queryMessage(q: WireQueryRequest): Buffer {
  const m = new Msg("kernable.v1.query")
    .str(q.query_id)
    .str(q.tenant_id)
    .str(q.domain)
    .str(q.query_type)
    .bytes(q.payload)
    .context(q.context)
    .u64(q.min_position)
    .i64(q.issued_at);
  // Each only when set, so a query without them signs as before.
  const w = q.wait_only ? m.str("wait_only") : m;
  return (q.target ? w.str("target").str(q.target.aggregate_type).str(q.target.aggregate_id) : w).done();
}

export function resultMessage(c: Stream, expectedVersion: number, events: WireNewEvent[]): Buffer {
  const m = stream(new Msg("kernable.v1.result"), c).u64(expectedVersion).u64(events.length);
  for (const e of events) m.str(e.event_type).u64(e.schema).bytes(e.payload);
  // Vault fields, appended only when an event has any.
  if (events.some((e) => e.vault_refs?.length || e.erases)) {
    for (const e of events) {
      m.u64(e.vault_refs?.length ?? 0);
      for (const r of e.vault_refs ?? []) m.str(r);
      m.str(e.erases ?? "");
    }
  }
  return m.done();
}

/**
 * What a worker signs for the unique keys, facts and requirements of its result:
 * a message of its own, not part of `resultMessage`, because every event stores the result
 * signature and `kernable verify` checks it against the stored events alone.
 */
export function indexMessage(c: Stream, expectedVersion: number, keys: WireUniqueKey[], facts: string[], requires: WireRequirement[]): Buffer {
  const m = stream(new Msg("kernable.v1.index"), c).u64(expectedVersion).u64(keys.length);
  for (const k of keys) m.str(k.name).u64(k.global ? 1 : 0).bytes(k.value_hash).str(k.subject);
  m.u64(facts.length);
  for (const f of facts) m.str(f);
  m.u64(requires.length);
  for (const r of requires) {
    m.str(r.domain).str(r.aggregate_type).str(r.aggregate_id).str(r.fact).str(r.key).bytes(r.value_hash).u64(r.absent ? 1 : 0);
  }
  return m.done();
}

/** A unique key's `value_hash`: the SHA-256 of the value's UTF-8 bytes. */
export function valueHash(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

/**
 * The identity of a unique key in the kernel's index, from the command's tenant (none for a
 * global key), domain and aggregate type. The kernel computes it; workers send only the value's
 * hash. Here for the test vectors.
 */
export function uniqueKeyId(tenant: string, domain: string, aggregateType: string, key: WireUniqueKey): Buffer {
  const m = new Msg("kernable.v1.key").str(key.global ? "" : tenant).str(domain).str(aggregateType).str(key.name).bytes(key.value_hash);
  return createHash("sha256").update(m.done()).digest();
}

export function snapshotMessage(c: Omit<Stream, "command_id">, version: number, schema: string, state: Buffer): Buffer {
  return new Msg("kernable.v1.snapshot")
    .str(c.tenant_id)
    .str(c.domain)
    .str(c.aggregate_type)
    .str(c.aggregate_id)
    .u64(version)
    .str(schema)
    .bytes(state)
    .done();
}

/**
 * What a client signs to put, get, delete or list files (`op`: `put`, `get`, `delete`, `list`):
 * the data's SHA-256 hash, not the data, the request context, and for a listing its page.
 */
export function dataMessage(op: "put" | "get" | "delete" | "list", r: WireDataRequest): Buffer {
  const msg = new Msg(`kernable.v1.data.${op}`)
    .str(r.tenant_id)
    .str(r.domain)
    .str(r.key)
    .str(r.content_type)
    .bytes(createHash("sha256").update(r.data).digest())
    .i64(r.issued_at)
    .context(r.context);
  return (op === "list" ? msg.str(r.after ?? "") : msg).done();
}

/**
 * What a client signs to set, get or delete a cache value (`op`: `set`, `get`, `delete`), with
 * the value's SHA-256 hash and the request context.
 */
export function cacheMessage(op: "set" | "get" | "delete", r: WireCacheRequest): Buffer {
  return new Msg(`kernable.v1.cache.${op}`)
    .str(r.tenant_id)
    .str(r.domain)
    .str(r.key)
    .bytes(createHash("sha256").update(r.value).digest())
    .u64(r.ttl_ms)
    .i64(r.issued_at)
    .context(r.context)
    .done();
}

export function rejectionMessage(commandId: string, reason: WireRejected): Buffer {
  return new Msg("kernable.v1.rejection").str(commandId).str(reason.code).str(reason.message).done();
}

export function challengeMessage(nonce: Buffer): Buffer {
  return new Msg("kernable.v1.challenge").bytes(nonce).done();
}

export function subscribeMessage(afterPosition: number, eventTypes: string[], issuedAt: number): Buffer {
  const m = new Msg("kernable.v1.subscribe").u64(afterPosition).i64(issuedAt).u64(eventTypes.length);
  for (const t of eventTypes) m.str(t);
  return m.done();
}

/** What a key granted `manage` signs to apply a service's membership. */
export function membershipMessage(m: WireMembership): Buffer {
  const msg = new Msg("kernable.v1.membership").str(m.service);
  for (const list of [m.workers, m.listeners, m.middlewares]) {
    msg.u64(list.length);
    for (const entry of list) msg.str(entry);
  }
  msg.u64(m.metadata.length);
  for (const a of m.metadata) msg.str(a.key).str(a.value);
  return msg.i64(m.issued_at).done();
}

export function checkpointMessage(position: number, chainHash: Buffer, createdAt: number): Buffer {
  return new Msg("kernable.v1.checkpoint").u64(position).bytes(chainHash).i64(createdAt).done();
}

/** The `n`-th ID derived for a command (`ctx.id()`): a version 8 UUID, see SIGNING.md. */
export function derivedId(commandId: string, n: number): string {
  const b = createHash("sha256").update(new Msg("kernable.v1.id").str(commandId).u64(n).done()).digest().subarray(0, 16);
  b[6] = (b[6] & 0x0f) | 0x80;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = b.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

export function eventHash(prev: Buffer, e: WireEvent): Buffer {
  const sig = e.producer_sig ?? { key_id: "", sig: Buffer.alloc(0) };
  const m = new Msg("kernable.v1.event")
    .bytes(prev)
    .u64(e.global_position)
    .str(e.tenant_id)
    .str(e.domain)
    .str(e.aggregate_type)
    .str(e.aggregate_id)
    .u64(e.version)
    .str(e.event_type)
    .u64(e.schema)
    .bytes(e.payload)
    .str(e.command_id)
    .i64(e.created_at)
    .str(sig.key_id)
    .bytes(sig.sig)
    .context(e.context);
  // Vault fields, appended only when set.
  if (e.vault_refs?.length || e.erases) {
    m.u64(e.vault_refs.length);
    for (const r of e.vault_refs) m.str(r);
    m.str(e.erases);
  }
  return createHash("sha256").update(m.done()).digest();
}
