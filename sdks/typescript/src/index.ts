// @kernable/sdk: write event-sourced business logic in TypeScript; the kernel does the rest.

import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";

import type { ClientReadableStream } from "@grpc/grpc-js";

import {
  PROTOCOL,
  connect,
  type Clients,
  type KernelMessage,
  type ListenCall,
  type ListenerDelivery,
  type SessionCall,
  type TlsFiles,
  type WireCommand,
  type WireEvent,
  type WireNewEvent,
  type WireQueryRequest,
  type WireRejected,
  type WireRequestContext,
  type WireSnapshot,
  type WireWait,
} from "./rpc.ts";
import {
  Key,
  challengeMessage,
  commandMessage,
  derivedId,
  queryMessage,
  rejectionMessage,
  resultMessage,
  snapshotMessage,
  subscribeMessage,
} from "./signing.ts";

export { Key } from "./signing.ts";

/** Throw from a command handler to reject the command with a business reason. */
export class Rejected extends Error {
  readonly code: string;

  constructor(message: string, code = "rejected") {
    super(message);
    this.code = code;
  }
}

/**
 * The command was committed, but not every listener processed its events within the kernel's
 * `settle_timeout_ms`. Read models may not show the change yet; `outcome` has the events.
 */
export class NotSettled extends Error {
  readonly outcome: { committed: true; events: StoredEvent[] };

  constructor(outcome: { committed: true; events: StoredEvent[] }) {
    super("command committed, but not all listeners have processed its events yet");
    this.outcome = outcome;
  }
}

/** An event returned by a command handler; `data` is stored as JSON. */
export interface NewEvent {
  type: string;
  data?: unknown;
}

/**
 * Who sends a request and in which context: signed with the command and stored with every
 * event it causes. `attributes` are business data only, never security decisions.
 */
export interface RequestContext {
  senderTenant?: string;
  senderIdentity?: string;
  senderAccount?: string;
  senderSession?: string;
  /** Authorize as this identity of the target tenant; the sender stays the real principal. */
  onBehalfOf?: string;
  workspace?: string;
  /** Reject the command (`version_mismatch`) unless the aggregate is at this version. */
  expectedVersion?: number;
  /** Shared by everything one original request causes; defaults to the command ID. */
  correlationId?: string;
  /** The command that caused this one. */
  causationId?: string;
  traceId?: string;
  /** Privileged: only keys in the kernel's `[privileged]` table may set it. */
  bypassAuthorization?: boolean;
  attributes?: Record<string, string>;
}

/** What a command handler gets besides state and data. */
export interface CommandContext {
  commandId: string;
  request: RequestContext;
  /** When the kernel received the command: the same every time the command runs. Use it instead of `new Date()`. */
  now(): Date;
  /** A new ID that is the same every time the command runs (1st call, 2nd call, ...). Use it instead of random IDs. */
  id(): string;
  /** When the kernel hands the command to another worker if this one has not answered. */
  deadline: Date;
}

/** What a query handler gets besides the query's data. */
export interface QueryContext {
  queryId: string;
  /** Who asks: check permissions here. */
  request: RequestContext;
}

export interface QueryOptions {
  domain: string;
  /** The query type within the domain. */
  type: string;
  data?: unknown;
  tenant?: string;
  context?: RequestContext;
  /** Answer only after the read model has processed this global position (e.g. of a `committed` submit). */
  minPosition?: number;
}

/** A command or query a middleware checks before the kernel accepts or routes it. */
export interface Check {
  kind: "command" | "query";
  /** The command or query ID. */
  id: string;
  tenant: string;
  domain: string;
  /** The command or query type within the domain. */
  type: string;
  /** Commands only. */
  aggregateType?: string;
  aggregateId?: string;
  data: unknown;
  /** Who sends it: check permissions here. */
  request: RequestContext;
}

export interface MiddlewareOptions {
  /** Command and query types it checks: `*`, a domain (`orders.*`) or `domain.Type`. */
  types: string[];
  /**
   * Position in the chain: lower runs first, equal orders by listener name (default 0).
   * Negative orders are reserved for kernable.
   */
  order?: number;
}

/** What a listener handler gets besides the event. */
export interface ListenerContext {
  /** The event was committed before the listener connected: it is being replayed. */
  replaying: boolean;
  /** A request context for commands caused by this event: same sender and correlation. */
  follow(): RequestContext;
}

/** An event as stored by the kernel. */
export interface StoredEvent {
  position: number;
  tenant: string;
  /** Always the domain of the command that caused the event. */
  domain: string;
  aggregateType: string;
  aggregateId: string;
  version: number;
  type: string;
  /**
   * Version of the payload format. Listeners get `data` upcast to the version they know (see
   * `Domain.event`), unless the handler is `raw`.
   */
  schema: number;
  data: unknown;
  commandId: string;
  /** The request context of the command that caused the event. */
  request: RequestContext;
}

export type Outcome =
  | { committed: true; events: StoredEvent[] }
  | { committed: false; code: string; message: string };

type Apply<S> = (state: S, data: any) => S;
type Handle<S> = (state: S, data: any, ctx: CommandContext) => NewEvent[] | Promise<NewEvent[]>;

export interface AggregateOptions {
  /** Send a snapshot of the state every this many events (default 100; 0 turns them off). */
  snapshotEvery?: number;
  /**
   * Version of the state format (default "1"). Change it when `apply` produces differently
   * shaped state; older snapshots are then ignored and the state is rebuilt from the events.
   */
  schema?: string;
}

/** The name of the listener whose handler is running, for requests sent from it. */
const handling = new AsyncLocalStorage<string>();

type Upcast = (data: any) => unknown;

/**
 * The payload format of one event type: its current version and the steps that bring older
 * payloads up to it. Events are stored as written and never changed; readers upcast them.
 */
export class EventSchema {
  readonly type: string;
  readonly schema: number;
  readonly steps = new Map<number, Upcast>();

  constructor(type: string, schema: number) {
    if (!/^[^.*]+$/.test(type)) throw new Error(`event types are unqualified within their domain, not ${JSON.stringify(type)}`);
    if (!Number.isInteger(schema) || schema < 1) throw new Error(`a schema is a version from 1, not ${schema}`);
    this.type = type;
    this.schema = schema;
  }

  /** Turns a payload of version `from` into one of version `from + 1`. */
  upcast<D = any>(from: number, fn: (data: D) => unknown): this {
    if (!Number.isInteger(from) || from < 1 || from >= this.schema) {
      throw new Error(`upcast ${this.type} from a version between 1 and ${this.schema - 1}, not ${from}`);
    }
    this.steps.set(from, fn);
    return this;
  }
}

/** An event a process cannot read: its schema is newer than known, or a step is missing. */
class SchemaError extends Error {}

/** `data` of a `type` event stored at `schema`, upcast to the current version in `events`. */
function upcast(events: Map<string, EventSchema> | undefined, domain: string, type: string, schema: number, data: unknown) {
  const def = events?.get(type);
  const current = def?.schema ?? 1;
  if (schema > current) {
    throw new SchemaError(`${domain}.${type} has schema ${schema}, but this process knows it only up to ${current}: update it`);
  }
  for (let v = schema; v < current; v++) {
    const step = def!.steps.get(v);
    if (!step) throw new SchemaError(`no upcast for ${domain}.${type} from schema ${v}`);
    data = step(data);
  }
  return { schema: current, data };
}

/**
 * A domain: a lowercase name (`orders`) that scopes its aggregates, commands and events.
 * Types are unique within a domain; elsewhere they are named `domain.Type`.
 */
export class Domain {
  readonly name: string;
  /** Payload versions and upcasts per event type; types not listed are at version 1. */
  readonly events = new Map<string, EventSchema>();

  constructor(name: string) {
    if (!/^[a-z][a-z0-9_-]*$/.test(name)) throw new Error(`invalid domain name ${JSON.stringify(name)}`);
    this.name = name;
  }

  /**
   * Declares that this domain writes `type` events at version `schema`, with an `upcast` per
   * older version. The domain's aggregates use it; listeners get it with `use(domain)`.
   */
  event(type: string, schema: number): EventSchema {
    const def = new EventSchema(type, schema);
    this.events.set(type, def);
    return def;
  }

  aggregate<S>(name: string, initial: S, options?: AggregateOptions): Aggregate<S> {
    return new Aggregate(this.name, name, initial, options, this.events);
  }
}

export function domain(name: string): Domain {
  return new Domain(name);
}

/**
 * An aggregate: how events change its state (`apply`) and which events a command produces
 * (`command`). Handlers must be deterministic and free of side effects; they may run again.
 * The state must survive a JSON round trip, because snapshots store it as JSON. An aggregate
 * belongs to a domain and produces events only of that domain; create it with
 * `domain(...).aggregate(...)`.
 */
export class Aggregate<S> {
  readonly domain: string;
  readonly name: string;
  readonly initial: S;
  readonly snapshotEvery: number;
  readonly schema: string;
  readonly appliers = new Map<string, Apply<S>>();
  readonly handlers = new Map<string, Handle<S>>();
  /** The domain's event schemas. */
  readonly events: Map<string, EventSchema>;

  constructor(domain: string, name: string, initial: S, options: AggregateOptions = {}, events = new Map<string, EventSchema>()) {
    this.domain = domain;
    this.name = name;
    this.events = events;
    this.initial = initial;
    this.snapshotEvery = options.snapshotEvery ?? 100;
    this.schema = options.schema ?? "1";
  }

  apply<D = any>(eventType: string, fn: (state: S, data: D) => S): this {
    this.appliers.set(eventType, fn);
    return this;
  }

  command<D = any>(commandType: string, fn: (state: S, data: D, ctx: CommandContext) => NewEvent[] | Promise<NewEvent[]>): this {
    this.handlers.set(commandType, fn);
    return this;
  }

  /** The payload version this aggregate writes `type` events at. */
  schemaOf(type: string): number {
    return this.events.get(type)?.schema ?? 1;
  }

  /**
   * State after `history`, starting from `from`. Events are upcast first; events without an
   * `apply` are skipped.
   */
  fold(history: { type: string; schema: number; data: unknown }[], from: S = this.initial): S {
    return history.reduce((s, e) => {
      const apply = this.appliers.get(e.type);
      return apply ? apply(s, upcast(this.events, this.domain, e.type, e.schema, e.data).data) : s;
    }, from);
  }
}


export interface Options {
  /** Kernel address: `unix:/path/to.sock` or `host:port`. */
  url: string;
  /** Path to this process's `.key` file; used to sign commands and worker results. */
  key: string;
  /** Default tenant for `submit`. */
  tenant?: string;
  /** Default request context for `submit`, e.g. this service's identity. */
  context?: RequestContext;
  /** Mutual TLS for `host:port` URLs: paths of the CA certificate and this process's certificate and key. */
  tls?: TlsFiles;
}

export interface SubmitOptions {
  domain: string;
  aggregateType: string;
  aggregateId: string;
  type: string;
  data?: unknown;
  tenant?: string;
  /** Idempotency key; resubmitting the same ID returns the original outcome. */
  id?: string;
  /** Overrides the client's default context; inside a listener, pass `ctx.follow()`. */
  context?: RequestContext;
  /**
   * `"settled"` (default): respond once every listener has processed the events, so read
   * models already show them; throws `NotSettled` on timeout. From a listener's handler, only
   * the listeners with a lower order count. `"committed"`: respond once the events are stored.
   */
  wait?: "settled" | "committed";
}

export class Kernable {
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly tenant: string;
  private readonly context: RequestContext;

  constructor(options: Options) {
    this.clients = connect(options.url, options.tls);
    this.key = Key.fromFile(options.key);
    this.tenant = options.tenant ?? "default";
    this.context = options.context ?? {};
  }

  worker(...aggregates: Aggregate<any>[]): Worker {
    return new Worker(this.clients, this.key, aggregates);
  }

  /** Runs a command and returns its outcome; see `SubmitOptions.wait`. */
  async submit(options: SubmitOptions): Promise<Outcome> {
    const wait = options.wait === "committed" ? "WAIT_COMMITTED" : "WAIT_SETTLED";
    const res = await this.send(options, wait);
    if (res.outcome === "rejected") {
      return { committed: false, code: res.rejected!.code, message: res.rejected!.message };
    }
    const outcome = { committed: true as const, events: res.committed!.events.map(toStored) };
    if (wait === "WAIT_SETTLED" && !res.committed!.settled) throw new NotSettled(outcome);
    return outcome;
  }

  /**
   * Stores the command in the kernel's queue and returns its ID without waiting for it to run.
   * Submit it again with the same `id` to get the outcome.
   */
  async enqueue(options: Omit<SubmitOptions, "wait">): Promise<string> {
    return (await this.send(options, "WAIT_ACCEPTED")).command_id;
  }

  /**
   * Asks the read model that answers `domain.type` and returns its answer. Queries always go
   * through the kernel, also within one process. Throws `Rejected` if the read model rejects it.
   */
  async query<T = unknown>(options: QueryOptions): Promise<T> {
    const request = {
      query_id: randomUUID(),
      tenant_id: options.tenant ?? this.tenant,
      domain: options.domain,
      query_type: options.type,
      payload: encode(options.data),
      context: toWireContext({ ...this.context, ...options.context }),
      min_position: options.minPosition ?? 0,
      issued_at: Date.now(),
    };
    const res = await this.clients.query({
      ...request,
      client_sig: this.key.sign(queryMessage(request)),
      listener: handling.getStore() ?? "",
    });
    if (res.outcome === "rejected") throw new Rejected(res.rejected!.message, res.rejected!.code);
    return decode(res.response!) as T;
  }

  /** A read model: kept in memory and rebuilt from the first event on every start (default). */
  readModel(name: string, options: ListenerOptions = {}): Listener {
    return new Listener(this.clients, this.key, name, { order: 0, start: 0, ...options });
  }

  /**
   * A reactor: triggers side effects; resumes after the last acknowledged event (default).
   * A settling submit from its handler waits for the listeners before it (the read models),
   * not for the reactor itself, which handles one event at a time.
   */
  reactor(name: string, options: ListenerOptions = {}): Listener {
    return new Listener(this.clients, this.key, name, { order: 10, ...options });
  }

  private send(options: SubmitOptions, wait: WireWait) {
    const command_id = options.id ?? randomUUID();
    const context = { ...this.context, ...options.context };
    const cmd = {
      command_id,
      tenant_id: options.tenant ?? this.tenant,
      domain: options.domain,
      aggregate_type: options.aggregateType,
      aggregate_id: options.aggregateId,
      command_type: options.type,
      payload: encode(options.data),
      context: toWireContext({ ...context, correlationId: context.correlationId || command_id }),
    };
    const signed = { ...cmd, client_sig: this.key.sign(commandMessage(cmd)) };
    return this.clients.submit(signed, wait, handling.getStore());
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
  /** Qualified event types (`orders.OrderPlaced`) or domains (`orders.*`); empty = all the key may read. */
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

export interface ListenerOptions {
  /**
   * Listeners with a lower order process each event first; equal orders run in parallel.
   * Defaults: 0 for read models, 10 for reactors. Negative orders are reserved for kernable.
   */
  order?: number;
  /**
   * Position to start after on the first connect: 0 rebuilds from the beginning (default for
   * read models); omitted, the kernel resumes after the last acknowledged event (default for
   * reactors). Reconnects always resume after the last handled event.
   */
  start?: number;
  reconnectMs?: number;
}

type EventHandler = (event: StoredEvent, ctx: ListenerContext) => void | Promise<void>;

export interface HandlerOptions {
  /**
   * Get events as stored, without upcasting, whatever their schema (for archives and exports).
   * By default a listener stops at an event whose schema it does not know.
   */
  raw?: boolean;
}
type QueryHandler = (data: unknown, ctx: QueryContext) => unknown;
type MiddlewareHandler = (check: Check) => void | Promise<void>;

/**
 * A durable, named event listener. Events arrive in order, one at a time, after every listener
 * with a lower order has processed them. Delivery is at least once: if a handler throws, the
 * listener reconnects and the event is delivered again.
 *
 * Handlers get event payloads upcast to the versions of the domains the listener `use`s. An
 * event with a newer schema than known stops the listener (it retries and logs) until it is
 * updated, so it never misreads data.
 */
export class Listener {
  /** Global position of the last handled event (or skipped, for other event types). */
  position = 0;
  /** Resolves once the kernel accepted the listener. */
  readonly ready: Promise<void>;
  private markReady!: () => void;
  private readonly handlers = new Map<string, { handler: EventHandler; raw: boolean }>();
  private readonly domains = new Map<string, Map<string, EventSchema>>();
  private readonly queries = new Map<string, QueryHandler>();
  private checker?: { options: MiddlewareOptions; handler: MiddlewareHandler };
  private call?: ListenCall;
  private stopped = false;
  /** Newest event when the kernel accepted the listener; events up to it are replayed. */
  private replayUpTo = 0;
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly name: string;
  private readonly options: ListenerOptions;

  constructor(clients: Clients, key: Key, name: string, options: ListenerOptions) {
    this.clients = clients;
    this.key = key;
    this.name = name;
    this.options = options;
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /** Upcasts the events of these domains with their event schemas (see `Domain.event`). */
  use(...domains: Domain[]): this {
    for (const d of domains) this.domains.set(d.name, d.events);
    return this;
  }

  /** `eventType` is qualified (`orders.OrderPlaced`) or a whole domain (`orders.*`). */
  on<D = any>(
    eventType: string,
    handler: (event: StoredEvent & { data: D }, ctx: ListenerContext) => void | Promise<void>,
    options: HandlerOptions = {},
  ): this {
    if (!/^[a-z][a-z0-9_-]*\.([^.]+|\*)$/.test(eventType)) {
      throw new Error(`listen to a qualified type (domain.Type) or a domain (domain.*), not ${JSON.stringify(eventType)}`);
    }
    this.handlers.set(eventType, { handler: handler as EventHandler, raw: options.raw ?? false });
    return this;
  }

  /**
   * Answers the qualified query type `queryType` (`orders.GetOrder`) from this listener's
   * state. The kernel delivers queries in order with the events, so an answer reflects every
   * event handled before it. Throw `Rejected` to refuse. A handler cannot query its own
   * listener (the answer would wait behind itself): the kernel refuses it.
   */
  query<D = any, R = unknown>(queryType: string, handler: (data: D, ctx: QueryContext) => R | Promise<R>): this {
    if (!/^[a-z][a-z0-9_-]*\.[^.*]+$/.test(queryType)) {
      throw new Error(`answer a qualified query type (domain.Query), not ${JSON.stringify(queryType)}`);
    }
    this.queries.set(queryType, handler as QueryHandler);
    return this;
  }

  /**
   * Makes this listener a middleware: the kernel asks `handler` about every command and query
   * of `options.types` before accepting or routing it, in chain order, and only once this
   * listener has caught up with the events. Throw `Rejected` to refuse; any other error
   * refuses too. The registration is stored: while this listener is away, the kernel refuses
   * those requests (`kernable listeners remove` unregisters it). The handler cannot submit or
   * query what it checks itself (it would wait behind itself): the kernel refuses it.
   */
  middleware(options: MiddlewareOptions, handler: MiddlewareHandler): this {
    for (const t of options.types) {
      if (t !== "*" && !/^[a-z][a-z0-9_-]*\.([^.]+|\*)$/.test(t)) {
        throw new Error(`check \`*\`, a domain (domain.*) or a qualified type (domain.Type), not ${JSON.stringify(t)}`);
      }
    }
    this.checker = { options, handler };
    return this;
  }

  /** Processes events until `stop()`, reconnecting when the stream drops. */
  async run(): Promise<void> {
    let start = this.options.start;
    while (!this.stopped) {
      try {
        await this.listen(start);
      } catch (err) {
        if (!this.stopped) console.error(`kernable listener ${this.name}: ${(err as Error).message}`);
      }
      start = this.position;
      if (!this.stopped) await new Promise((r) => setTimeout(r, this.options.reconnectMs ?? 1000));
    }
  }

  stop(): void {
    this.stopped = true;
    this.call?.cancel();
  }

  private listen(start: number | undefined): Promise<void> {
    return new Promise((resolve, reject) => {
      const call = this.clients.listen();
      this.call = call;
      // Batches are handled strictly one after another.
      let queue = Promise.resolve();
      call.on("data", (m: ListenerDelivery) => {
        // Requests sent from the handlers name this listener, so the kernel never makes them
        // wait for it (it handles one message at a time).
        queue = queue.then(() => handling.run(this.name, () => this.onMessage(call, m))).catch((err) => {
          call.cancel();
          reject(err);
        });
      });
      call.on("error", reject);
      call.on("end", resolve);
      call.write({
        hello: {
          name: this.name,
          key_id: this.key.id,
          protocol: PROTOCOL,
          order: this.options.order ?? 0,
          event_types: [...this.handlers.keys()],
          query_types: [...this.queries.keys()],
          ...(start === undefined ? {} : { from_position: start }),
          ...(this.checker ? { middleware: { order: this.checker.options.order ?? 0, types: this.checker.options.types } } : {}),
        },
      });
    });
  }

  private async onMessage(call: ListenCall, m: ListenerDelivery): Promise<void> {
    if (m.challenge) {
      call.write({ challenge_response: { sig: this.key.sign(challengeMessage(m.challenge.nonce)).sig } });
    } else if (m.ready) {
      this.position = m.ready.after_position;
      this.replayUpTo = m.ready.head_position;
      this.markReady();
    } else if (m.batch) {
      for (const e of m.batch.events) {
        const entry = this.handlers.get(`${e.domain}.${e.event_type}`) ?? this.handlers.get(`${e.domain}.*`);
        if (entry) {
          const stored = toStored(e);
          const event = entry.raw ? stored : { ...stored, ...upcast(this.domains.get(e.domain), e.domain, e.event_type, e.schema, stored.data) };
          await entry.handler(event, { replaying: e.global_position <= this.replayUpTo, follow: () => follow(event) });
        }
        this.position = e.global_position;
      }
      this.position = m.batch.up_to;
      call.write({ ack: { position: m.batch.up_to } });
    } else if (m.query) {
      call.write({ answer: { call_id: m.query.call_id, ...(await this.answer(m.query.query)) } });
    } else if (m.check) {
      call.write({ verdict: { call_id: m.check.call_id, ...(await this.verdict(m.check)) } });
    }
  }

  private async verdict(c: NonNullable<ListenerDelivery["check"]>): Promise<{ rejected?: WireRejected }> {
    try {
      if (!this.checker) throw new Rejected("not a middleware", "not_a_middleware");
      await this.checker.handler(toCheck(c));
      return {};
    } catch (err) {
      return {
        rejected: err instanceof Rejected ? { code: err.code, message: err.message } : { code: "middleware_error", message: String(err) },
      };
    }
  }

  private async answer(q: WireQueryRequest): Promise<{ response: Buffer } | { rejected: WireRejected }> {
    try {
      const handler = this.queries.get(`${q.domain}.${q.query_type}`);
      if (!handler) throw new Rejected(`no handler for ${q.domain}.${q.query_type}`, "unknown_query");
      const result = await handler(decode(q.payload), { queryId: q.query_id, request: fromWireContext(q.context) });
      return { response: encode(result) };
    } catch (err) {
      return {
        rejected: err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: String(err) },
      };
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
    for (const agg of aggregates) for (const type of agg.handlers.keys()) this.byCommand.set(`${agg.domain}.${type}`, agg);
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
          protocol: PROTOCOL,
          command_types: [...this.byCommand.keys()],
          max_concurrency: concurrency,
          event_schemas: this.eventSchemas(),
        },
      });
    });
  }

  /**
   * The payload versions the worker writes, for every event type its aggregates know: the
   * kernel refuses the worker if the store already holds a newer one.
   */
  private eventSchemas(): { event_type: string; schema: number }[] {
    const schemas = new Map<string, number>();
    for (const agg of new Set(this.byCommand.values())) {
      for (const type of new Set([...agg.appliers.keys(), ...agg.events.keys()])) {
        schemas.set(`${agg.domain}.${type}`, agg.schemaOf(type));
      }
    }
    return [...schemas].map(([event_type, schema]) => ({ event_type, schema }));
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
    let snapshot: WireSnapshot | undefined;
    try {
      const agg = this.byCommand.get(`${cmd.domain}.${cmd.command_type}`);
      if (!agg || agg.name !== cmd.aggregate_type) {
        throw new Rejected(`no handler for ${cmd.command_type} on ${cmd.domain}.${cmd.aggregate_type}`, "unknown_command");
      }
      if (x.snapshot && x.snapshot.schema !== agg.schema) {
        throw new Rejected(`snapshot schema ${x.snapshot.schema} is not ${agg.schema}`, "snapshot_stale");
      }
      const from = x.snapshot ? (decode(x.snapshot.state) as any) : agg.initial;
      const state = agg.fold(x.history.map((e) => ({ type: e.event_type, schema: e.schema, data: decode(e.payload) })), from);
      let ids = 0;
      const ctx: CommandContext = {
        commandId: cmd.command_id,
        request: fromWireContext(cmd.context),
        now: () => new Date(x.received_at),
        id: () => derivedId(cmd.command_id, ids++),
        deadline: new Date(x.deadline),
      };
      const produced = await agg.handlers.get(cmd.command_type)!(state, decode(cmd.payload), ctx);
      events = produced.map((e) => ({ event_type: e.type, schema: agg.schemaOf(e.type), payload: encode(e.data) }));
      snapshot = this.snapshot(agg, cmd, x.expected_version, state, events);
    } catch (err) {
      if (err instanceof SchemaError) {
        // Not the command's fault: this worker cannot read the aggregate. Stop until updated.
        console.error(`kernable worker: ${err.message}`);
        call.cancel();
        return;
      }
      const reason: WireRejected =
        err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: String(err) };
      const worker_sig = this.key.sign(rejectionMessage(cmd.command_id, reason));
      call.write({ rejection: { lease_id: x.lease_id, reason, worker_sig } });
      return;
    }
    const worker_sig = this.key.sign(resultMessage(cmd, x.expected_version, events));
    call.write({ result: { lease_id: x.lease_id, events, worker_sig, ...(snapshot && { snapshot }) } });
  }

  /** A snapshot of the state after `events`, if they cross a multiple of `snapshotEvery`. */
  private snapshot(agg: Aggregate<any>, cmd: WireCommand, before: number, state: unknown, events: WireNewEvent[]) {
    const version = before + events.length;
    const every = agg.snapshotEvery;
    if (every <= 0 || events.length === 0 || Math.floor(version / every) === Math.floor(before / every)) return undefined;
    // Apply the events as they will be read back, so the snapshot equals a replay.
    const after = agg.fold(events.map((e) => ({ type: e.event_type, schema: e.schema, data: decode(e.payload) })), state);
    const snapshot = { version, schema: agg.schema, state: encode(after) };
    const message = snapshotMessage(cmd, version, snapshot.schema, snapshot.state);
    return { ...snapshot, worker_sig: this.key.sign(message) };
  }
}

function encode(data: unknown): Buffer {
  return Buffer.from(JSON.stringify(data ?? {}));
}

function decode(payload: Buffer): unknown {
  return payload.length ? JSON.parse(payload.toString("utf8")) : {};
}

/** A request context for a command caused by `event`. */
function follow(event: StoredEvent): RequestContext {
  const { expectedVersion, bypassAuthorization, ...rest } = event.request;
  return { ...rest, correlationId: rest.correlationId || event.commandId, causationId: event.commandId };
}

function toWireContext(c: RequestContext): WireRequestContext {
  return {
    sender_tenant: c.senderTenant ?? "",
    sender_identity: c.senderIdentity ?? "",
    sender_account: c.senderAccount ?? "",
    sender_session: c.senderSession ?? "",
    on_behalf_of: c.onBehalfOf ?? "",
    workspace: c.workspace ?? "",
    ...(c.expectedVersion === undefined ? {} : { expected_version: c.expectedVersion }),
    correlation_id: c.correlationId ?? "",
    causation_id: c.causationId ?? "",
    trace_id: c.traceId ?? "",
    bypass_authorization: c.bypassAuthorization ?? false,
    attributes: Object.entries(c.attributes ?? {}).map(([key, value]) => ({ key, value })),
  };
}

/** Empty fields are left out. */
function fromWireContext(c: WireRequestContext | null | undefined): RequestContext {
  if (!c) return {};
  const all: RequestContext = {
    senderTenant: c.sender_tenant,
    senderIdentity: c.sender_identity,
    senderAccount: c.sender_account,
    senderSession: c.sender_session,
    onBehalfOf: c.on_behalf_of,
    workspace: c.workspace,
    expectedVersion: c.expected_version ?? undefined,
    correlationId: c.correlation_id,
    causationId: c.causation_id,
    traceId: c.trace_id,
    bypassAuthorization: c.bypass_authorization || undefined,
    attributes: c.attributes.length ? Object.fromEntries(c.attributes.map((a) => [a.key, a.value])) : undefined,
  };
  return Object.fromEntries(Object.entries(all).filter(([, v]) => v !== undefined && v !== "")) as RequestContext;
}

function toCheck(c: NonNullable<ListenerDelivery["check"]>): Check {
  if (c.command) {
    const x = c.command;
    return {
      kind: "command",
      id: x.command_id,
      tenant: x.tenant_id,
      domain: x.domain,
      type: x.command_type,
      aggregateType: x.aggregate_type,
      aggregateId: x.aggregate_id,
      data: decode(x.payload),
      request: fromWireContext(x.context),
    };
  }
  const q = c.query!;
  return {
    kind: "query",
    id: q.query_id,
    tenant: q.tenant_id,
    domain: q.domain,
    type: q.query_type,
    data: decode(q.payload),
    request: fromWireContext(q.context),
  };
}

function toStored(e: WireEvent): StoredEvent {
  return {
    position: e.global_position,
    tenant: e.tenant_id,
    domain: e.domain,
    aggregateType: e.aggregate_type,
    aggregateId: e.aggregate_id,
    version: e.version,
    type: e.event_type,
    schema: e.schema,
    data: decode(e.payload),
    commandId: e.command_id,
    request: fromWireContext(e.context),
  };
}
