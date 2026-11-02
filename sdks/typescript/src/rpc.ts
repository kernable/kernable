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
    /** Protocol 3: `execute_more` messages of this lease follow with the rest of the history. */
    more?: boolean;
  } | null;
  /** Protocol 3: the next part of an Execute's history. */
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
        /** Protocol 2: the full set the aggregate holds after the events, and what the command requires. */
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
  batch?: { events: WireEvent[]; up_to: number; vault: WireVaultValue[] } | null;
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
  | { ack: { position: number } }
  | { answer: { call_id: string; response?: Buffer; rejected?: WireRejected } }
  | { verdict: { call_id: string; rejected?: WireRejected } };

export type ListenCall = grpc.ClientDuplexStream<ListenerMessage, ListenerDelivery>;

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
export const PROTOCOL = 3;

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

/**
 * Clients for a kernel at `unix:/path` or `host:port` (with `tls`: over mutual TLS). `grant`,
 * the hex-encoded grant of this process's key, goes with every call (see `Grant` in kernable.proto).
 * A request waits for the connection (after a kernel restart, say) and gives up after
 * `timeoutMs` in all.
 */
export function connect(url: string, tls?: TlsFiles, grant?: string, timeoutMs = DEFAULT_TIMEOUT_MS): Clients {
  url = url.replace(/^https?:\/\//, "").replace(/\/$/, "");
  v1 ??= (
    grpc.loadPackageDefinition(
      protoLoader.loadSync(PROTO, { keepCase: true, longs: Number, defaults: true, oneofs: true }),
    ) as any
  ).kernable.v1;
  const creds = tls
    ? grpc.credentials.createSsl(readFileSync(tls.ca), readFileSync(tls.key), readFileSync(tls.cert))
    : grpc.credentials.createInsecure();
  const commands = new v1.CommandService(url, creds, KEEPALIVE);
  const workers = new v1.WorkerGateway(url, creds, KEEPALIVE);
  const events = new v1.EventService(url, creds, KEEPALIVE);
  const listeners = new v1.ListenerService(url, creds, KEEPALIVE);
  const queries = new v1.QueryService(url, creds, KEEPALIVE);
  const data = new v1.DataService(url, creds, {
    ...KEEPALIVE,
    "grpc.max_send_message_length": MAX_DATA_MESSAGE,
    "grpc.max_receive_message_length": MAX_DATA_MESSAGE,
  });
  const cache = new v1.CacheService(url, creds, KEEPALIVE);
  const memberships = new v1.MembershipService(url, creds, KEEPALIVE);
  const metadata = new grpc.Metadata();
  if (grant) metadata.set("kernable-grant", grant);
  // A unary call with the grant and the protocol version: it waits for the connection, then
  // for the answer, until the deadline.
  const unary = async <R>(client: any, rpc: string, req: object, ms = timeoutMs): Promise<R> => {
    const deadline = Date.now() + ms;
    await new Promise<void>((resolve, reject) =>
      client.waitForReady(deadline, (err?: Error) =>
        err ? reject(Object.assign(new Error(`no connection to the kernel within ${ms} ms`), { code: grpc.status.DEADLINE_EXCEEDED })) : resolve(),
      ),
    );
    return new Promise<R>((resolve, reject) =>
      client[rpc]({ ...req, protocol: PROTOCOL }, metadata, { deadline }, (err: grpc.ServiceError | null, res: R) => (err ? reject(err) : resolve(res))),
    );
  };
  return {
    submit: (command, wait, listener = "", vault = [], ms) =>
      unary<WireCommandResult>(commands, "Submit", { command, wait, listener, vault }, ms),
    session: () => workers.Session(metadata),
    listen: () => listeners.Listen(metadata),
    query: (req, ms) => unary<WireQueryResult>(queries, "Query", req, ms),
    subscribe: (req) => events.Subscribe({ ...req, protocol: PROTOCOL }, metadata),
    data: (op, req) => unary(data, op, req),
    cache: (op, req) => unary(cache, op, req),
    apply: async (membership, sig) => {
      await unary(memberships, "Apply", { membership, sig });
    },
    close: () => {
      commands.close();
      workers.close();
      events.close();
      listeners.close();
      queries.close();
      data.close();
      cache.close();
      memberships.close();
    },
  };
}
