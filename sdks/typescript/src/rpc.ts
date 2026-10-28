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
  workspace: string;
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

export interface WireRejected {
  code: string;
  message: string;
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
  } | null;
}

export type WorkerMessage =
  | {
      register: {
        worker_id: string;
        key_id: string;
        protocol: number;
        command_types: string[];
        max_concurrency: number;
        event_schemas: { event_type: string; schema: number }[];
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
    request: "command" | "query";
    command?: WireCommand | null;
    query?: WireQueryRequest | null;
    vault: WireVaultValue[];
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
        middleware?: { order: number; types: string[] };
      };
    }
  | { challenge_response: { sig: Buffer } }
  | { ack: { position: number } }
  | { answer: { call_id: string; response?: Buffer; rejected?: WireRejected } }
  | { verdict: { call_id: string; rejected?: WireRejected } };

export type ListenCall = grpc.ClientDuplexStream<ListenerMessage, ListenerDelivery>;

export interface Clients {
  /** `listener`: the listener whose handler submits (see `SubmitRequest.listener`). */
  submit(command: WireCommand, wait: WireWait, listener?: string, vault?: WireVaultValue[]): Promise<WireCommandResult>;
  session(): SessionCall;
  listen(): ListenCall;
  query(req: WireQueryRequest): Promise<WireQueryResult>;
  subscribe(req: WireSubscribeRequest): grpc.ClientReadableStream<WireEvent>;
  close(): void;
}

// Packaged next to `dist/`; in the repository, the shared `proto/` at the root.
const BUNDLED = fileURLToPath(new URL("../proto/kernable/v1/kernable.proto", import.meta.url));
const PROTO = existsSync(BUNDLED) ? BUNDLED : fileURLToPath(new URL("../../../proto/kernable/v1/kernable.proto", import.meta.url));

/** The protocol version this SDK speaks; see "Protocol versions" in `kernable.proto`. */
export const PROTOCOL = 1;

let v1: any; // generated client constructors, untyped

/** PEM files for mutual TLS: the CA of the kernel's certificate, and this process's own. */
export interface TlsFiles {
  ca: string;
  cert: string;
  key: string;
}

/**
 * Clients for a kernel at `unix:/path` or `host:port` (with `tls`: over mutual TLS). `grant`,
 * the hex-encoded grant of this process's key, goes with every call (see `Grant` in kernable.proto).
 */
export function connect(url: string, tls?: TlsFiles, grant?: string): Clients {
  url = url.replace(/^https?:\/\//, "").replace(/\/$/, "");
  v1 ??= (
    grpc.loadPackageDefinition(
      protoLoader.loadSync(PROTO, { keepCase: true, longs: Number, defaults: true, oneofs: true }),
    ) as any
  ).kernable.v1;
  const creds = tls
    ? grpc.credentials.createSsl(readFileSync(tls.ca), readFileSync(tls.key), readFileSync(tls.cert))
    : grpc.credentials.createInsecure();
  const commands = new v1.CommandService(url, creds);
  const workers = new v1.WorkerGateway(url, creds);
  const events = new v1.EventService(url, creds);
  const listeners = new v1.ListenerService(url, creds);
  const queries = new v1.QueryService(url, creds);
  const metadata = new grpc.Metadata();
  if (grant) metadata.set("kernable-grant", grant);
  return {
    submit: (command, wait, listener = "", vault = []) =>
      new Promise((resolve, reject) =>
        commands.Submit({ command, wait, protocol: PROTOCOL, listener, vault }, metadata, (err: grpc.ServiceError | null, res: WireCommandResult) =>
          err ? reject(err) : resolve(res),
        ),
      ),
    session: () => workers.Session(metadata),
    listen: () => listeners.Listen(metadata),
    query: (req) =>
      new Promise((resolve, reject) =>
        queries.Query({ ...req, protocol: PROTOCOL }, metadata, (err: grpc.ServiceError | null, res: WireQueryResult) => (err ? reject(err) : resolve(res))),
      ),
    subscribe: (req) => events.Subscribe({ ...req, protocol: PROTOCOL }, metadata),
    close: () => {
      commands.close();
      workers.close();
      events.close();
      listeners.close();
      queries.close();
    },
  };
}
