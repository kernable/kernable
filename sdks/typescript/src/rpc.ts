// Wire types and gRPC clients, loaded from proto/kernable/v1/kernable.proto at runtime.

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

export interface WireSignature {
  key_id: string;
  sig: Buffer;
}

export interface WireRequestContext {
  sender_tenant: string;
  sender_identity: string;
  sender_account: string;
  sender_session: string;
  on_behalf_of: string;
  expected_version?: number | null;
  correlation_id: string;
  causation_id: string;
  trace_id: string;
  bypass_authorization: boolean;
  attributes: { key: string; value: string }[];
}

export interface WireCommand {
  command_id: string;
  tenant_id: string;
  domain: string;
  aggregate_type: string;
  aggregate_id: string;
  command_type: string;
  payload: Buffer;
  client_sig?: WireSignature | null;
  context?: WireRequestContext | null;
  vault_refs?: string[];
}

/** A value kept in the vault; see `VaultValue` in kernable.proto. */
export interface WireVaultValue {
  ref: string;
  subject: string;
  secret: boolean;
  salt: Buffer;
  value: Buffer;
}

export interface WireEvent {
  global_position: number;
  tenant_id: string;
  domain: string;
  aggregate_type: string;
  aggregate_id: string;
  version: number;
  event_type: string;
  payload: Buffer;
  command_id: string;
  created_at: number;
  hash: Buffer;
  producer_sig?: WireSignature | null;
  context?: WireRequestContext | null;
  schema: number;
  vault_refs: string[];
  erases: string;
}

export interface WireNewEvent {
  event_type: string;
  payload: Buffer;
  schema: number;
  vault_refs?: string[];
  erases?: string;
}

export interface WireSnapshot {
  version: number;
  schema: string;
  state: Buffer;
  worker_sig: WireSignature;
}

/** A unique key an aggregate holds; see `UniqueKey` in kernable.proto. */
export interface WireUniqueKey {
  name: string;
  global: boolean;
  /** SHA-256 of the value's UTF-8 bytes. */
  value_hash: Buffer;
  /** Personal: the person the value belongs to; empty otherwise. */
  subject: string;
}

/** A fact another aggregate of the command's tenant must hold; see `Requirement` in kernable.proto. */
export interface WireRequirement {
  domain: string;
  aggregate_type: string;
  aggregate_id: string;
  fact: string;
  key: string;
  value_hash: Buffer;
  absent: boolean;
}

export interface WireRejected {
  code: string;
  message: string;
  /** Set by the kernel only: the key of `unique_taken`. */
  key?: string;
  /** Set by the kernel only: the requirement of `requirement_missing`. */
  requirement?: WireRequirement | null;
}

export type WireWait = "WAIT_SETTLED" | "WAIT_COMMITTED" | "WAIT_ACCEPTED";

export interface WireCommandResult {
  command_id: string;
  outcome: "committed" | "rejected" | "accepted";
  committed?: { events: WireEvent[]; settled: boolean } | null;
  rejected?: WireRejected | null;
}

export interface WireSubscribeRequest {
  after_position: number;
  event_types: string[];
  issued_at: number;
  client_sig: WireSignature;
}

/** One message from the kernel on a worker session; exactly one field is set. */
export interface KernelMessage {
  challenge?: { nonce: Buffer } | null;
  registered?: { session_id: string } | null;
  execute?: {
    lease_id: string;
    command: WireCommand;
    history: WireEvent[];
    vault: WireVaultValue[];
    expected_version: number;
    snapshot?: WireSnapshot | null;
    received_at: number;
    deadline: number;
    /** `execute_more` messages of this lease follow with the rest of the history. */
    more?: boolean;
  } | null;
  /** The next part of an Execute's history. */
  execute_more?: {
    lease_id: string;
    history: WireEvent[];
    vault: WireVaultValue[];
    more?: boolean;
  } | null;
}

export type WorkerMessage =
  | {
      register: {
        worker_id: string;
        key_id: string;
        protocol: number;
        aggregates: { aggregate_type: string; command_types: string[] }[];
        max_concurrency: number;
        event_schemas: { event_type: string; schema: number }[];
        service: string;
      };
    }
  | { challenge_response: { sig: Buffer } }
  | {
      result: {
        lease_id: string;
        events: WireNewEvent[];
        worker_sig: WireSignature;
        snapshot?: WireSnapshot;
        vault: WireVaultValue[];
        /** The full set the aggregate holds after the events, and what the command requires. */
        keys?: WireUniqueKey[];
        facts?: string[];
        requires?: WireRequirement[];
        /** Over `kernable.v1.index`; only when keys, facts or requires are set. */
        index_sig?: WireSignature;
      };
    }
  | { rejection: { lease_id: string; reason: WireRejected; worker_sig: WireSignature } };

export type SessionCall = grpc.ClientDuplexStream<WorkerMessage, KernelMessage>;

export interface WireQueryRequest {
  query_id: string;
  tenant_id: string;
  domain: string;
  query_type: string;
  payload: Buffer;
  context?: WireRequestContext | null;
  min_position: number;
  issued_at: number;
  client_sig?: WireSignature | null;
  /** Not signed; see `QueryRequest.listener`. */
  listener?: string;
}

export interface WireQueryResult {
  query_id: string;
  outcome: "response" | "rejected";
  response?: Buffer | null;
  rejected?: WireRejected | null;
}

/** One message from the kernel on a listener stream; exactly one field is set. */
export interface ListenerDelivery {
  challenge?: { nonce: Buffer } | null;
  ready?: { after_position: number; head_position: number } | null;
  batch?: { events: WireEvent[]; up_to: number; vault: WireVaultValue[]; retries?: WireEvent[] } | null;
  query?: { call_id: string; query: WireQueryRequest } | null;
  check?: {
    call_id: string;
    request: "command" | "query" | "data" | "cache";
    command?: WireCommand | null;
    query?: WireQueryRequest | null;
    /** A file request, without its data. */
    data?: WireDataRequest | null;
    /** A cache request, without its value. */
    cache?: WireCacheRequest | null;
    vault: WireVaultValue[];
    /** `WireWhen`: 0 before, 1 after. */
    /** The qualified type being checked, e.g. `shop.PutData`. */
    type: string;
    when: number;
    /** After only: what came of the request. */
    command_result?: WireCommandResult | null;
    query_result?: WireQueryResult | null;
  } | null;
}

export type ListenerMessage =
  | {
      hello: {
        name: string;
        key_id: string;
        protocol: number;
        order: number;
        event_types: string[];
        from_position?: number;
        query_types: string[];
        middlewares: { order: number; types: string[]; when: number }[];
        service: string;
        live: boolean;
      };
    }
  | { challenge_response: { sig: Buffer } }
  | { ack: { position: number; parked?: WireParked[] } }
  | { answer: { call_id: string; response?: Buffer; rejected?: WireRejected } }
  | { verdict: { call_id: string; rejected?: WireRejected } };

export type ListenCall = grpc.ClientDuplexStream<ListenerMessage, ListenerDelivery>;

/** An event a listener gave up on; see `Parked` in kernable.proto. */
export interface WireParked {
  position: number;
  code: string;
  message: string;
  attempts: number;
}

/** A file of a domain; see `DataService` in kernable.proto. */
export interface WireDataRequest {
  tenant_id: string;
  domain: string;
  key: string;
  /** Put only; empty otherwise. */
  data: Buffer;
  /** Put only; empty otherwise. */
  content_type: string;
  issued_at: number;
  client_sig?: WireSignature | null;
  /** Who asks; signed. */
  context?: WireRequestContext | null;
}

export interface WireDataResult {
  found: boolean;
  data: Buffer;
  content_type: string;
}

/** A value of a domain's cache; see `CacheService` in kernable.proto. */
export interface WireCacheRequest {
  tenant_id: string;
  domain: string;
  key: string;
  /** Set only; empty otherwise. */
  value: Buffer;
  /** Set only; 0 = until deleted or the kernel restarts. */
  ttl_ms: number;
  issued_at: number;
  client_sig?: WireSignature | null;
  /** Who asks; signed. */
  context?: WireRequestContext | null;
}

export interface WireCacheResult {
  found: boolean;
  value: Buffer;
}

/** What a service consists of; see `Membership` in kernable.proto. */
export interface WireMembership {
  service: string;
  workers: string[];
  listeners: string[];
  middlewares: string[];
  metadata: { key: string; value: string }[];
  issued_at: number;
}

export type DataOp = "PutData" | "GetData" | "DeleteData";
export type CacheOp = "SetCache" | "GetCache" | "DeleteCache";

export interface Clients {
  /**
   * `listener`: the listener whose handler submits (see `SubmitRequest.listener`); `timeoutMs`
   * replaces the client's deadline for this call.
   */
  submit(command: WireCommand, wait: WireWait, listener?: string, vault?: WireVaultValue[], timeoutMs?: number): Promise<WireCommandResult>;
  session(): SessionCall;
  listen(): ListenCall;
  query(req: WireQueryRequest, timeoutMs?: number): Promise<WireQueryResult>;
  subscribe(req: WireSubscribeRequest): grpc.ClientReadableStream<WireEvent>;
  data(op: DataOp, req: WireDataRequest): Promise<WireDataResult>;
  cache(op: CacheOp, req: WireCacheRequest): Promise<WireCacheResult>;
  apply(membership: WireMembership, sig: WireSignature): Promise<void>;
  close(): void;
}

// Packaged next to `dist/`; in the repository, the shared `proto/` at the root.
const BUNDLED = fileURLToPath(new URL("../proto/kernable/v1/kernable.proto", import.meta.url));
const PROTO = existsSync(BUNDLED) ? BUNDLED : fileURLToPath(new URL("../../../proto/kernable/v1/kernable.proto", import.meta.url));

/** The protocol version this SDK speaks; see "Protocol versions" in `kernable.proto`. */
export const PROTOCOL = 1;

/** Room for a file of 16 MiB, the kernel's limit, plus the rest of the message (grpc-js receives 4 MiB by default). */
const MAX_DATA_MESSAGE = 17 << 20;

let v1: any; // generated client constructors, untyped

/** PEM files for mutual TLS: the CA of the kernel's certificate, and this process's own. */
export interface TlsFiles {
  ca: string;
  cert: string;
  key: string;
}

/** How long a request waits, for the connection and the answer, unless told otherwise. */
export const DEFAULT_TIMEOUT_MS = 60_000;

/**
 * Pings on every connection, also idle ones: a connection the network dropped without a word
 * (a dead host, a NAT that forgot it) ends within 40 s, and the workers and listeners on it
 * reconnect.
 */
const KEEPALIVE = {
  "grpc.keepalive_time_ms": 30_000,
  "grpc.keepalive_timeout_ms": 10_000,
  "grpc.keepalive_permit_without_calls": 1,
};

/** How long a request waits for one address of several before it tries the next. */
const PER_ADDRESS_MS = 1_000;

/**
 * Clients for a kernel at `unix:/path` or `host:port` (with `tls`: over mutual TLS). `grant`,
 * the hex-encoded grant of this process's key, goes with every call (see `Grant` in kernable.proto).
 * A request waits for the connection (after a kernel restart, say) and gives up after
 * `timeoutMs` in all.
 *
 * `url` may name several kernels, comma-separated: the two kernels of a pair (see Run a
 * standby). Requests go to one of them; on `UNAVAILABLE` (a standby answers `not_primary`, a
 * kernel is gone or shutting down) the clients turn to the next, and a request tries the next
 * address within its timeout. Streams that end with `UNAVAILABLE` reconnect to the next one.
 */
export function connect(url: string, tls?: TlsFiles, grant?: string, timeoutMs = DEFAULT_TIMEOUT_MS): Clients {
  const urls = url
    .split(",")
    .map((u) => u.trim().replace(/^https?:\/\//, "").replace(/\/$/, ""))
    .filter((u) => u !== "");
  if (urls.length === 0) throw new Error("no kernel address");
  v1 ??= (
    grpc.loadPackageDefinition(
      protoLoader.loadSync(PROTO, { keepCase: true, longs: Number, defaults: true, oneofs: true }),
    ) as any
  ).kernable.v1;
  const creds = tls
    ? grpc.credentials.createSsl(readFileSync(tls.ca), readFileSync(tls.key), readFileSync(tls.cert))
    : grpc.credentials.createInsecure();
  const targets = urls.map((u) => ({
    commands: new v1.CommandService(u, creds, KEEPALIVE),
    workers: new v1.WorkerGateway(u, creds, KEEPALIVE),
    events: new v1.EventService(u, creds, KEEPALIVE),
    listeners: new v1.ListenerService(u, creds, KEEPALIVE),
    queries: new v1.QueryService(u, creds, KEEPALIVE),
    data: new v1.DataService(u, creds, {
      ...KEEPALIVE,
      "grpc.max_send_message_length": MAX_DATA_MESSAGE,
      "grpc.max_receive_message_length": MAX_DATA_MESSAGE,
    }),
    cache: new v1.CacheService(u, creds, KEEPALIVE),
    memberships: new v1.MembershipService(u, creds, KEEPALIVE),
  }));
  type Target = (typeof targets)[number];
  let current = 0;
  // Turns to the next address, unless another call did so since `from`.
  const next = (from: number) => {
    if (targets.length > 1 && current === from) current = (from + 1) % targets.length;
  };
  const metadata = new grpc.Metadata();
  if (grant) metadata.set("kernable-grant", grant);
  // A unary call with the grant and the protocol version: it waits for the connection, then
  // for the answer, until the deadline; with several addresses, it turns to the next one when
  // one does not connect within a second or answers UNAVAILABLE.
  const unary = async <R>(service: keyof Target, rpc: string, req: object, ms = timeoutMs): Promise<R> => {
    const deadline = Date.now() + ms;
    for (;;) {
      const at = current;
      const client = targets[at][service];
      const several = targets.length > 1;
      const wait = several ? Math.min(deadline, Date.now() + PER_ADDRESS_MS) : deadline;
      const ready = await new Promise<boolean>((resolve) => client.waitForReady(wait, (err?: Error) => resolve(!err)));
      if (!ready) {
        if (Date.now() >= deadline) {
          throw Object.assign(new Error(`no connection to the kernel within ${ms} ms`), { code: grpc.status.DEADLINE_EXCEEDED });
        }
        next(at);
        continue;
      }
      try {
        return await new Promise<R>((resolve, reject) =>
          client[rpc]({ ...req, protocol: PROTOCOL }, metadata, { deadline }, (err: grpc.ServiceError | null, res: R) => (err ? reject(err) : resolve(res))),
        );
      } catch (err) {
        if (!several || (err as grpc.ServiceError).code !== grpc.status.UNAVAILABLE || Date.now() >= deadline) throw err;
        next(at);
      }
    }
  };
  // A stream on the current address; one that ends with UNAVAILABLE turns to the next.
  const stream = <S extends { on(event: "error", f: (e: grpc.ServiceError) => void): unknown }>(open: (t: Target) => S): S => {
    const at = current;
    const call = open(targets[at]);
    call.on("error", (e) => {
      if (e.code === grpc.status.UNAVAILABLE) next(at);
    });
    return call;
  };
  return {
    submit: (command, wait, listener = "", vault = [], ms) =>
      unary<WireCommandResult>("commands", "Submit", { command, wait, listener, vault }, ms),
    session: () => stream((t) => t.workers.Session(metadata)),
    listen: () => stream((t) => t.listeners.Listen(metadata)),
    query: (req, ms) => unary<WireQueryResult>("queries", "Query", req, ms),
    subscribe: (req) => stream((t) => t.events.Subscribe({ ...req, protocol: PROTOCOL }, metadata)),
    data: (op, req) => unary("data", op, req),
    cache: (op, req) => unary("cache", op, req),
    apply: async (membership, sig) => {
      await unary("memberships", "Apply", { membership, sig });
    },
    close: () => {
      for (const t of targets) for (const client of Object.values(t)) client.close();
    },
  };
}
