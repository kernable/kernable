// @kernable/sdk: write event-sourced business logic in TypeScript; the kernel does the rest.

import { randomUUID } from "node:crypto";

import type { ClientReadableStream } from "@grpc/grpc-js";

import { connect, type Clients, type SessionCall, type KernelMessage, type WireEvent, type WireNewEvent, type WireRejected } from "./rpc.ts";
import { Key, challengeMessage, commandMessage, rejectionMessage, resultMessage, subscribeMessage } from "./signing.ts";

export { Key } from "./signing.ts";

/** Throw from a command handler to reject the command with a business reason. */
export class Rejected extends Error {
  readonly code: string;

  constructor(message: string, code = "rejected") {
    super(message);
    this.code = code;
  }
}

/** An event returned by a command handler; `data` is stored as JSON. */
export interface NewEvent {
  type: string;
  data?: unknown;
}

/** An event as stored by the kernel. */
export interface StoredEvent {
  position: number;
  tenant: string;
  aggregateType: string;
  aggregateId: string;
  version: number;
  type: string;
  data: unknown;
  commandId: string;
}

export type Outcome =
  | { committed: true; events: StoredEvent[] }
  | { committed: false; code: string; message: string };

type Apply<S> = (state: S, data: any) => S;
type Handle<S> = (state: S, data: any) => NewEvent[] | Promise<NewEvent[]>;

/**
 * An aggregate: how events change its state (`apply`) and which events a command produces
 * (`command`). Handlers must be deterministic and free of side effects; they may run again.
 */
export class Aggregate<S> {
  readonly name: string;
  readonly initial: S;
  readonly appliers = new Map<string, Apply<S>>();
  readonly handlers = new Map<string, Handle<S>>();

  constructor(name: string, initial: S) {
    this.name = name;
    this.initial = initial;
  }

  apply<D = any>(eventType: string, fn: (state: S, data: D) => S): this {
    this.appliers.set(eventType, fn);
    return this;
  }

  command<D = any>(commandType: string, fn: (state: S, data: D) => NewEvent[] | Promise<NewEvent[]>): this {
    this.handlers.set(commandType, fn);
    return this;
  }

  /** State after `history`; events without an `apply` are skipped. */
  fold(history: { type: string; data: unknown }[]): S {
    return history.reduce((s, e) => this.appliers.get(e.type)?.(s, e.data) ?? s, this.initial);
  }
}

export function aggregate<S>(name: string, initial: S): Aggregate<S> {
  return new Aggregate(name, initial);
}

export interface Options {
  /** Kernel address: `unix:/path/to.sock` or `host:port`. */
  url: string;
  /** Path to this process's `.key` file; used to sign commands and worker results. */
  key: string;
  /** Default tenant for `submit`. */
  tenant?: string;
}

export interface SubmitOptions {
  aggregateType: string;
  aggregateId: string;
  type: string;
  data?: unknown;
  tenant?: string;
  /** Idempotency key; resubmitting the same ID returns the original outcome. */
  id?: string;
}

export class Kernable {
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly tenant: string;

  constructor(options: Options) {
    this.clients = connect(options.url);
    this.key = Key.fromFile(options.key);
    this.tenant = options.tenant ?? "default";
  }

  worker(...aggregates: Aggregate<any>[]): Worker {
    return new Worker(this.clients, this.key, aggregates);
  }

  async submit(options: SubmitOptions): Promise<Outcome> {
    const cmd = {
      command_id: options.id ?? randomUUID(),
      tenant_id: options.tenant ?? this.tenant,
      aggregate_type: options.aggregateType,
      aggregate_id: options.aggregateId,
      command_type: options.type,
      payload: encode(options.data),
    };
    const res = await this.clients.submit({ ...cmd, client_sig: this.key.sign(commandMessage(cmd)) });
    if (res.outcome === "rejected") {
      return { committed: false, code: res.rejected!.code, message: res.rejected!.message };
    }
    return { committed: true, events: res.committed!.events.map(toStored) };
  }

  /**
   * Streams committed events after `options.after` (backlog first, then live) to `handler`,
   * one at a time. Reconnects and resumes after the last handled event; if the handler throws,
   * that event is delivered again. Persist `position` yourself to resume across restarts.
   */
  subscribe(options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>): Subscription {
    return new Subscription(this.clients, this.key, options, handler);
  }

  close(): void {
    this.clients.close();
  }
}

export interface SubscribeOptions {
  /** Global position of the last event already handled; 0 = from the beginning. */
  after?: number;
  /** Event types to receive; empty = all the key may read. */
  types?: string[];
  reconnectMs?: number;
}

export class Subscription {
  /** Global position of the last handled event. */
  position: number;
  /** Resolves after `stop()`. */
  readonly done: Promise<void>;
  private call?: ClientReadableStream<WireEvent>;
  private stopped = false;

  constructor(clients: Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>) {
    this.position = options.after ?? 0;
    this.done = this.loop(clients, key, options, handler);
  }

  stop(): void {
    this.stopped = true;
    this.call?.cancel();
  }

  private async loop(clients: Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>) {
    const event_types = options.types ?? [];
    while (!this.stopped) {
      const issued_at = Date.now();
      const client_sig = key.sign(subscribeMessage(this.position, event_types, issued_at));
      this.call = clients.subscribe({ after_position: this.position, event_types, issued_at, client_sig });
      try {
        for await (const e of this.call as AsyncIterable<WireEvent>) {
          await handler(toStored(e));
          this.position = e.global_position;
        }
      } catch (err) {
        if (!this.stopped) console.error(`kernable subscription: ${(err as Error).message}`);
      }
      if (!this.stopped) await new Promise((r) => setTimeout(r, options.reconnectMs ?? 1000));
    }
  }
}

export interface WorkerOptions {
  /** Commands executed in parallel. */
  concurrency?: number;
  /** Delay before reconnecting after the session ends. */
  reconnectMs?: number;
}

export class Worker {
  /** Resolves once the kernel accepted the worker. */
  readonly ready: Promise<void>;
  private markReady!: () => void;
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly byCommand = new Map<string, Aggregate<any>>();
  private call?: SessionCall;
  private stopped = false;

  constructor(clients: Clients, key: Key, aggregates: Aggregate<any>[]) {
    this.clients = clients;
    this.key = key;
    for (const agg of aggregates) for (const type of agg.handlers.keys()) this.byCommand.set(type, agg);
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /** Processes commands until `stop()`, reconnecting when the session drops. */
  async run(options: WorkerOptions = {}): Promise<void> {
    while (!this.stopped) {
      try {
        await this.session(options.concurrency ?? 16);
      } catch (err) {
        if (!this.stopped) console.error(`kernable worker: ${(err as Error).message}`);
      }
      if (!this.stopped) await new Promise((r) => setTimeout(r, options.reconnectMs ?? 1000));
    }
  }

  stop(): void {
    this.stopped = true;
    this.call?.cancel();
  }

  private session(concurrency: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const call = this.clients.session();
      this.call = call;
      call.on("data", (m: KernelMessage) => this.onMessage(call, m));
      call.on("error", reject);
      call.on("end", resolve);
      call.write({
        register: {
          worker_id: `ts-${process.pid}`,
          key_id: this.key.id,
          command_types: [...this.byCommand.keys()],
          max_concurrency: concurrency,
        },
      });
    });
  }

  private onMessage(call: SessionCall, m: KernelMessage): void {
    if (m.challenge) {
      call.write({ challenge_response: { sig: this.key.sign(challengeMessage(m.challenge.nonce)).sig } });
    } else if (m.registered) {
      this.markReady();
    } else if (m.execute) {
      void this.execute(call, m.execute);
    }
  }

  private async execute(call: SessionCall, x: NonNullable<KernelMessage["execute"]>): Promise<void> {
    const cmd = x.command;
    let events: WireNewEvent[];
    try {
      const agg = this.byCommand.get(cmd.command_type);
      if (!agg || agg.name !== cmd.aggregate_type) {
        throw new Rejected(`no handler for ${cmd.aggregate_type}.${cmd.command_type}`, "unknown_command");
      }
      const state = agg.fold(x.history.map((e) => ({ type: e.event_type, data: decode(e.payload) })));
      const produced = await agg.handlers.get(cmd.command_type)!(state, decode(cmd.payload));
      events = produced.map((e) => ({ event_type: e.type, payload: encode(e.data) }));
    } catch (err) {
      const reason: WireRejected =
        err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: String(err) };
      const worker_sig = this.key.sign(rejectionMessage(cmd.command_id, reason));
      call.write({ rejection: { lease_id: x.lease_id, reason, worker_sig } });
      return;
    }
    const worker_sig = this.key.sign(resultMessage(cmd, x.expected_version, events));
    call.write({ result: { lease_id: x.lease_id, events, worker_sig } });
  }
}

function encode(data: unknown): Buffer {
  return Buffer.from(JSON.stringify(data ?? {}));
}

function decode(payload: Buffer): unknown {
  return payload.length ? JSON.parse(payload.toString("utf8")) : {};
}

function toStored(e: WireEvent): StoredEvent {
  return {
    position: e.global_position,
    tenant: e.tenant_id,
    aggregateType: e.aggregate_type,
    aggregateId: e.aggregate_id,
    version: e.version,
    type: e.event_type,
    data: decode(e.payload),
    commandId: e.command_id,
  };
}
