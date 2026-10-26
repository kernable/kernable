// Wire types and gRPC clients, loaded from proto/kernable/v1/kernable.proto at runtime.

import { readFileSync } from "node:fs";
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
}

export interface WireNewEvent {
  event_type: string;
  payload: Buffer;
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
    expected_version: number;
    snapshot?: WireSnapshot | null;
    received_at: number;
    deadline: number;
  } | null;
}

export type WorkerMessage =
  | { register: { worker_id: string; key_id: string; command_types: string[]; max_concurrency: number } }
  | { challenge_response: { sig: Buffer } }
  | { result: { lease_id: string; events: WireNewEvent[]; worker_sig: WireSignature; snapshot?: WireSnapshot } }
  | { rejection: { lease_id: string; reason: WireRejected; worker_sig: WireSignature } };

export type SessionCall = grpc.ClientDuplexStream<WorkerMessage, KernelMessage>;

/** One message from the kernel on a listener stream; exactly one field is set. */
export interface ListenerDelivery {
  challenge?: { nonce: Buffer } | null;
  ready?: { after_position: number; head_position: number } | null;
  batch?: { events: WireEvent[]; up_to: number } | null;
}

export type ListenerMessage =
  | { hello: { name: string; key_id: string; order: number; event_types: string[]; from_position?: number } }
  | { challenge_response: { sig: Buffer } }
  | { ack: { position: number } };

export type ListenCall = grpc.ClientDuplexStream<ListenerMessage, ListenerDelivery>;

export interface Clients {
  submit(command: WireCommand, wait: WireWait): Promise<WireCommandResult>;
  session(): SessionCall;
  listen(): ListenCall;
  subscribe(req: WireSubscribeRequest): grpc.ClientReadableStream<WireEvent>;
  close(): void;
}

const PROTO = fileURLToPath(new URL("../../../proto/kernable/v1/kernable.proto", import.meta.url));

let v1: any; // generated client constructors, untyped

/** PEM files for mutual TLS: the CA of the kernel's certificate, and this process's own. */
export interface TlsFiles {
  ca: string;
  cert: string;
  key: string;
}

/** Clients for a kernel at `unix:/path` or `host:port` (with `tls`: over mutual TLS). */
export function connect(url: string, tls?: TlsFiles): Clients {
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
  return {
    submit: (command, wait) =>
      new Promise((resolve, reject) =>
        commands.Submit({ command, wait }, (err: grpc.ServiceError | null, res: WireCommandResult) =>
          err ? reject(err) : resolve(res),
        ),
      ),
    session: () => workers.Session(),
    listen: () => listeners.Listen(),
    subscribe: (req) => events.Subscribe(req),
    close: () => {
      commands.close();
      workers.close();
      events.close();
      listeners.close();
    },
  };
}
