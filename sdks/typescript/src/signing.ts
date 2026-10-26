// Signed messages and the event hash chain, as specified in proto/SIGNING.md.

import { createHash, createPrivateKey, createPublicKey, sign, type KeyObject } from "node:crypto";
import { readFileSync } from "node:fs";

import type { WireCommand, WireEvent, WireNewEvent, WireRejected, WireRequestContext, WireSignature } from "./rpc.ts";

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
    return new Key(Buffer.from(hex.trim(), "hex"));
  }

  static fromFile(path: string): Key {
    return Key.fromHex(readFileSync(path, "utf8"));
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
      .str(c?.on_behalf_of ?? "")
      .str(c?.workspace ?? "");
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

  done(): Buffer {
    return Buffer.concat(this.parts);
  }
}

type Stream = Pick<WireCommand, "command_id" | "tenant_id" | "domain" | "aggregate_type" | "aggregate_id">;

const stream = (m: Msg, c: Stream) =>
  m.str(c.command_id).str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id);

export function commandMessage(c: WireCommand): Buffer {
  return stream(new Msg("kernable.v1.command"), c).str(c.command_type).bytes(c.payload).context(c.context).done();
}

export function resultMessage(c: Stream, expectedVersion: number, events: WireNewEvent[]): Buffer {
  const m = stream(new Msg("kernable.v1.result"), c).u64(expectedVersion).u64(events.length);
  for (const e of events) m.str(e.event_type).bytes(e.payload);
  return m.done();
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
    .bytes(e.payload)
    .str(e.command_id)
    .i64(e.created_at)
    .str(sig.key_id)
    .bytes(sig.sig)
    .context(e.context);
  return createHash("sha256").update(m.done()).digest();
}
