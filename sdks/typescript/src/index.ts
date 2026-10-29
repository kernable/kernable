// @kernable/sdk: write event-sourced business logic in TypeScript; the kernel does the rest.

import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { basename } from "node:path";

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
  type WireCacheRequest,
  type WireCommand,
  type WireDataRequest,
  type WireEvent,
  type WireNewEvent,
  type WireQueryRequest,
  type WireRejected,
  type WireRequestContext,
  type WireSnapshot,
  type WireVaultValue,
  type WireWait,
} from "./rpc.ts";
import {
  Key,
  cacheMessage,
  challengeMessage,
  commandMessage,
  dataMessage,
  derivedId,
  membershipMessage,
  queryMessage,
  rejectionMessage,
  resultMessage,
  snapshotMessage,
  subscribeMessage,
  vaultRef,
} from "./signing.ts";
import { Store } from "./store.ts";

export { Key } from "./signing.ts";
export { Store } from "./store.ts";

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
 * `--settle-timeout-ms`. Read models may not show the change yet; `outcome` has the events.
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
  /** Reject the command (`version_mismatch`) unless the aggregate is at this version. */
  expectedVersion?: number;
  /** Shared by everything one original request causes; defaults to the command ID. */
  correlationId?: string;
  /** The command that caused this one. */
  causationId?: string;
  traceId?: string;
  /** Privileged: only keys granted `privileged` for it may set it. */
  bypassAuthorization?: boolean;
  attributes?: Record<string, string>;
}

/** What a command handler gets about the command besides its data. */
export interface CommandMeta {
  commandId: string;
  /** The command type within the domain. */
  type: string;
  domain: string;
  aggregateType: string;
  aggregateId: string;
  tenant: string;
}

/** What a command handler gets besides state, data and `CommandMeta`. */
export interface CommandContext {
  /** Who sends the command: check permissions here. */
  request: RequestContext;
  /** When the kernel received the command: the same every time the command runs. Use it instead of `new Date()`. */
  now(): Date;
  /** A new ID that is the same every time the command runs (1st call, 2nd call, ...). Use it instead of random IDs. */
  id(): string;
  /** When the kernel hands the command to another worker if this one has not answered. */
  deadline: Date;
}

/** What a query handler gets about the query besides its data. */
export interface QueryMeta {
  queryId: string;
  /** The query type within the domain. */
  type: string;
  domain: string;
  tenant: string;
}

/** What a query handler gets besides the query's data and `QueryMeta`. */
export interface QueryContext {
  /** Who asks: check permissions here. */
  request: RequestContext;
  /** The listener's store, if it has one (`ListenerOptions.store`). */
  store?: Store;
}

export interface QueryOptions {
  domain: string;
  /** The query type within the domain. */
  type: string;
  data?: unknown;
  tenant?: string;
  /** Replaces the client's default context. */
  context?: RequestContext;
  /** Answer only after the read model has processed this global position (e.g. of a `committed` submit). */
  minPosition?: number;
}

/** A command or query a middleware checks, before the kernel accepts or routes it or after. */
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
  /**
   * Command and query types it checks: `*` (all), a type of the listener's own domain (`Type`),
   * another domain's type (`domain.Type`) or a whole domain (`domain.*`).
   */
  types: string[];
  /**
   * `before` (default): checks a request before the kernel accepts it; throwing refuses it.
   * `after`: gets the request's outcome too. Throwing refuses a query's response; a command's
   * outcome, and a rejected query, it only observes, best effort (it misses what happens while
   * it is away, and nobody waits for it).
   */
  when?: "before" | "after";
  /**
   * Position in the chain: lower runs first, equal orders by listener name (default 0).
   * Negative orders are reserved for kernable.
   */
  order?: number;
}

/** What a listener handler gets besides the event's data and `StoredEventMeta`. */
export interface ListenerContext {
  /** The event was committed before the listener connected: it is being replayed. */
  replaying: boolean;
  /** A request context for commands caused by this event: same sender and correlation. */
  follow(): RequestContext;
  /** The request context of the command that caused the event. */
  request: RequestContext;
  /** The listener's store, if it has one (`ListenerOptions.store`). */
  store?: Store;
}

/**
 * What `apply` gets about an event besides its data. Only fields known before the kernel stores
 * the event, so a snapshot and a replay compute the same state; put a time into the data
 * (`ctx.now()`) if the state needs one.
 */
export interface EventMeta {
  type: string;
  /** The aggregate's version after this event: 1 for its first event. */
  version: number;
  tenant: string;
  /** Always the domain of the command that caused the event. */
  domain: string;
  aggregateType: string;
  aggregateId: string;
  commandId: string;
}

/**
 * What `apply` gets besides state, data and `EventMeta`. Nothing that differs between runs
 * (no clock, no IDs), so a replay computes the same state.
 */
export interface ApplyContext {
  /** The request context of the command that caused the event. */
  request: RequestContext;
}

/** What a listener handler gets about an event besides its data and request context. */
export interface StoredEventMeta extends EventMeta {
  position: number;
  /**
   * Version of the payload format. Listeners get `data` upcast to the version they know (see
   * `Domain.event`), unless the handler is `raw`.
   */
  schema: number;
  /**
   * Fields kept in the vault whose value is not here (`null` in `data`): erased, or not
   * readable with this key (granted `vault_read`).
   */
  redacted: string[];
}

/** An event as stored by the kernel. */
export interface StoredEvent extends StoredEventMeta {
  data: unknown;
  /** The request context of the command that caused the event. */
  request: RequestContext;
}

export type Outcome =
  | { committed: true; events: StoredEvent[] }
  | { committed: false; code: string; message: string };

/** What came of a query: its response data or why it was rejected. */
export type QueryOutcome =
  | { answered: true; data: unknown }
  | { answered: false; code: string; message: string };

/** An event to fold: type, payload version and data, plus as much of `EventMeta` and its request context as known. */
export type FoldEvent = Partial<EventMeta> & { type: string; schema: number; data: unknown; request?: RequestContext };

type Apply<S> = (state: S, data: any, evt: EventMeta, ctx: ApplyContext) => S | void;
type Handle<S> = (state: S, data: any, cmd: CommandMeta, ctx: CommandContext) => NewEvent[] | Promise<NewEvent[]>;

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

  /** The fields kept in the vault; see `personal`. */
  readonly vault = new VaultFields();
  /** The subject (person) this event erases, if it is an erasing event; see `erases`. */
  eraser?: Subject;

  /**
   * Keeps these top-level fields in the vault, as personal data of the subject `subject`
   * returns from the payload and the aggregate's ID (e.g. `(d) => d.customerId`, or
   * `(_d, id) => id` for the aggregate itself). Events then hold only references; the kernel hands
   * the values to workers and to listeners allowed to read them, until an event erases the
   * subject.
   */
  personal<D = any>(fields: string[], subject: (data: D, aggregateId: string) => string): this {
    this.vault.personal.push({ fields, subject });
    return this;
  }

  /**
   * Makes this an erasing event: it deletes every vault value of the subject `subject`
   * returns, in every stream, and the snapshots that may hold them. Read models should drop
   * the subject's data when they see it; rebuilt, they read `null`.
   */
  erases<D = any>(subject: (data: D, aggregateId: string) => string): this {
    this.eraser = subject;
    return this;
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

/** Whose personal data a payload holds: from the payload and the aggregate's ID. */
type Subject = (data: any, aggregateId: string) => string;

/** Payload fields kept in the vault instead of the signed log. */
class VaultFields {
  readonly personal: { fields: string[]; subject: Subject }[] = [];
  readonly secret: string[] = [];
}

/** How a command's payload is kept: which fields go to the vault. */
export class CommandSchema {
  readonly type: string;
  readonly vault = new VaultFields();

  constructor(type: string) {
    if (!/^[^.*]+$/.test(type)) throw new Error(`command types are unqualified within their domain, not ${JSON.stringify(type)}`);
    this.type = type;
  }

  /** Keeps these top-level fields in the vault as personal data of `subject`; see `EventSchema.personal`. */
  personal<D = any>(fields: string[], subject: (data: D, aggregateId: string) => string): this {
    this.vault.personal.push({ fields, subject });
    return this;
  }

  /**
   * Keeps these top-level fields (passwords, one-time tokens) in the vault only until the
   * command completed. The handler gets them in clear; the stored command keeps a reference.
   */
  secret(fields: string[]): this {
    this.vault.secret.push(...fields);
    return this;
  }
}

/**
 * Moves the vault fields of `data` into vault values: the payload keeps `{"$vault": ref}`.
 * Absent and null fields stay as they are.
 */
function seal(
  data: unknown,
  vault: VaultFields | undefined,
  aggregateId: string,
): { payload: Buffer; refs: string[]; values: WireVaultValue[] } {
  if (!vault || (vault.personal.length === 0 && vault.secret.length === 0) || data === null || typeof data !== "object") {
    return { payload: encode(data), refs: [], values: [] };
  }
  const sealed: Record<string, unknown> = { ...(data as Record<string, unknown>) };
  const values: WireVaultValue[] = [];
  const keep = (field: string, subject: string, secret: boolean) => {
    if (sealed[field] === undefined || sealed[field] === null) return;
    const salt = randomBytes(16);
    const value = Buffer.from(JSON.stringify(sealed[field]));
    const ref = vaultRef(salt, subject, secret, value);
    values.push({ ref, subject, secret, salt, value });
    sealed[field] = { $vault: ref };
  };
  for (const { fields, subject } of vault.personal) {
    const who = String(subject(data, aggregateId) ?? "");
    if (!who) throw new Error(`no subject for the personal fields ${fields.join(", ")}`);
    for (const f of fields) keep(f, who, false);
  }
  for (const f of vault.secret) keep(f, "", true);
  return { payload: encode(sealed), refs: values.map((v) => v.ref), values };
}

/** Puts vault values back into `data`; references without a value become `null`. */
function unseal(data: unknown, vault: WireVaultValue[] | undefined): { data: unknown; redacted: string[] } {
  const values = new Map((vault ?? []).map((v) => [v.ref, v.value]));
  const redacted: string[] = [];
  const walk = (v: unknown, path: string): unknown => {
    if (Array.isArray(v)) return v.map((x, i) => walk(x, `${path}[${i}]`));
    if (v === null || typeof v !== "object") return v;
    const o = v as Record<string, unknown>;
    const keys = Object.keys(o);
    if (keys.length === 1 && keys[0] === "$vault" && typeof o.$vault === "string") {
      const value = values.get(o.$vault);
      if (value === undefined) {
        redacted.push(path);
        return null;
      }
      return JSON.parse(value.toString("utf8"));
    }
    return Object.fromEntries(keys.map((k) => [k, walk(o[k], path ? `${path}.${k}` : k)]));
  };
  return { data: walk(data, ""), redacted };
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
 * A domain: a name that scopes its aggregates, commands and events, lowercase by convention
 * (`orders`) like aggregates, while message types are capitalized (`orders.PlaceOrder`). Both
 * cases are valid; names are case-sensitive. Types are unique within a domain; elsewhere they
 * are named `domain.Type`.
 */
export class Domain {
  readonly name: string;
  /** Payload versions and upcasts per event type; types not listed are at version 1. */
  readonly events = new Map<string, EventSchema>();
  /** Vault fields per command type; see `command`. */
  readonly commands = new Map<string, CommandSchema>();
  /** The aggregates defined on it, which `Kernable.app` runs. */
  readonly aggregates: Aggregate<any>[] = [];
  /** The read models and reactors defined on it, which `Kernable.app` runs. */
  readonly listeners: Listener[] = [];

  constructor(name: string) {
    if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(name)) throw new Error(`invalid domain name ${JSON.stringify(name)}`);
    this.name = name;
  }

  /**
   * Declares that this domain writes `type` events at version `schema`, with an `upcast` per
   * older version. The domain's aggregates and listeners use it; listeners of other domains
   * get it with `use(domain)`.
   */
  event(type: string, schema: number): EventSchema {
    const def = new EventSchema(type, schema);
    this.events.set(type, def);
    return def;
  }

  /**
   * Declares which fields of `type` commands the vault keeps (`personal`, `secret`). Clients
   * that submit them need the domain: `kernable.use(domain)`.
   */
  command(type: string): CommandSchema {
    const def = new CommandSchema(type);
    this.commands.set(type, def);
    return def;
  }

  aggregate<S>(name: string, initial: S, options?: AggregateOptions): Aggregate<S> {
    const aggregate = new Aggregate(this.name, name, initial, options, this.events);
    this.aggregates.push(aggregate);
    return aggregate;
  }

  /**
   * Defines a read model: kept in memory and rebuilt from the first event on every start
   * (defaults: order 0, start 0). Run it with `kernable.listener(definition)`.
   */
  readModel(name: string, options: ListenerOptions = {}): Listener {
    const listener = new Listener(this, name, { order: 0, start: 0, ...options });
    this.listeners.push(listener);
    return listener;
  }

  /**
   * Defines a reactor: triggers side effects; starts at the newest event when it is new, then
   * resumes after the last acknowledged event (defaults: order 10, no start). A settling submit from its handler waits for the listeners
   * before it (the read models), not for the reactor itself, which handles one event at a time.
   * Run it with `kernable.listener(definition)`.
   */
  reactor(name: string, options: ListenerOptions = {}): Listener {
    const listener = new Listener(this, name, { order: 10, ...options });
    this.listeners.push(listener);
    return listener;
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

  /**
   * How an event of `eventType` changes the state: `fn(state, data, evt, ctx)` changes `state` in
   * place and returns nothing, or returns a new state that replaces it. `state` is a copy, so
   * changing it never touches the state it came from.
   */
  apply<D = any>(eventType: string, fn: (state: S, data: D, evt: EventMeta, ctx: ApplyContext) => S | void): this {
    this.appliers.set(eventType, fn);
    return this;
  }

  /** Which events a `commandType` command produces: `fn(state, data, cmd, ctx)` returns them. */
  command<D = any>(
    commandType: string,
    fn: (state: S, data: D, cmd: CommandMeta, ctx: CommandContext) => NewEvent[] | Promise<NewEvent[]>,
  ): this {
    this.handlers.set(commandType, fn);
    return this;
  }

  /** The payload version this aggregate writes `type` events at. */
  schemaOf(type: string): number {
    return this.events.get(type)?.schema ?? 1;
  }

  /**
   * State after `history`, starting from `from`. Events are upcast first; events without an
   * `apply` are skipped. Fields of `EventMeta` an event lacks are empty (0 for `version`), as is
   * its request context.
   */
  fold(history: FoldEvent[], from: S = this.initial): S {
    return history.reduce((s, e) => {
      const apply = this.appliers.get(e.type);
      if (!apply) return s;
      const { schema, data, request, ...meta } = e;
      const evt: EventMeta = { version: 0, tenant: "", domain: this.domain, aggregateType: this.name, aggregateId: "", commandId: "", ...meta };
      const draft = structuredClone(s);
      const next = apply(draft, upcast(this.events, this.domain, e.type, schema, data).data, evt, { request: request ?? {} });
      return next === undefined ? draft : next;
    }, from);
  }
}


/** `shop.ItemAdded` as `ItemAdded` when `shop` is the only domain in `domains`. */
function relative(type: string, domains: Set<string>): string {
  const [domain, name] = type.split(".");
  return domains.size === 1 && domains.has(domain) && name ? name : type;
}

/**
 * What a listener does, one part per entry: where it starts, what it reads, answers and checks.
 * `name` shortens the types (see `relative`).
 */
function listenerDetails(l: Listener, position: number, name: (type: string) => string = (t) => t): string[] {
  const list = (types: Iterable<string>) => [...types].map(name).join(", ");
  const details = [l.handlers.size ? `from ${position} · reads ${list(l.handlers.keys())}` : `from ${position}`];
  if (l.queries.size) details.push(`answers ${list(l.queries.keys())}`);
  for (const [when, m] of l.middlewares) details.push(`checks ${list(m.options.types)} ${when}`);
  return details;
}

/** An app's processes: see `Kernable.app`. */
export class App {
  /** Resolves once every worker and listener of the app is connected. */
  readonly ready: Promise<void>;
  /** The membership `run()` applies. */
  readonly manifest: Manifest;
  readonly worker?: Worker;
  readonly listeners: ListenerHandle[];
  private readonly kernable: Kernable;
  private readonly log: Log;
  /** The names of the app's domains. */
  private readonly domains: Set<string>;

  constructor(kernable: Kernable, log: Log, domains: Domain[], manifest: Manifest, worker: Worker | undefined, listeners: ListenerHandle[]) {
    this.kernable = kernable;
    this.log = log;
    this.domains = new Set(domains.map((d) => d.name));
    this.manifest = manifest;
    this.worker = worker;
    this.listeners = listeners;
    this.ready = Promise.all([worker?.ready, ...listeners.map((l) => l.ready)]).then(() => {});
  }

  /**
   * Applies the membership, then runs the worker and the listeners until `stop()`. Once all
   * are connected, it prints an overview of the app (unless `KERNABLE_LOG` is below `info`).
   */
  async run(options: WorkerOptions = {}): Promise<void> {
    const started = Date.now();
    await this.kernable.apply(this.manifest);
    if (this.worker) this.worker.announce = false;
    for (const l of this.listeners) l.announce = false;
    void this.ready.then(() => this.overview(Date.now() - started));
    await Promise.all([this.worker?.run(options), ...this.listeners.map((l) => l.run())]);
  }

  private overview(ms: number): void {
    const log = this.log;
    if (!log.enabled("info")) return;
    const m = this.manifest;
    const domains = this.domains;
    // A single domain's own names go without it; with several, every name is qualified.
    const short = (domain: string, name: string) => (domains.size === 1 && domains.has(domain) ? name : `${domain}.${name}`);
    const count = (n: number, what: string) => `${n} ${what}${n === 1 ? "" : "s"}`;
    const listed = [count(m.workers?.length ?? 0, "worker"), count(m.listeners?.length ?? 0, "listener")];
    if (m.middlewares?.length) listed.push(count(m.middlewares.length, "middleware"));
    const meta = m.metadata ?? {};
    const rows: [string, string, string[]][] = [];
    for (const a of m.workers ?? []) {
      const types = [...a.handlers.keys()].sort().join(", ");
      rows.push(["worker", short(a.domain, a.name), [`runs ${types}`]]);
    }
    for (const h of this.listeners) {
      const l = h.listener;
      const kind = l.options.live ? "live" : "listener";
      rows.push([kind, short(l.domain.name, l.name), listenerDetails(l, h.startedAt, (t) => relative(t, domains))]);
    }
    const nameWidth = Math.max(0, ...rows.map(([, name]) => name.length));
    const lines = [
      log.style(1, ["kernable", [m.service, meta.version].filter(Boolean).join(" "), meta.description].filter(Boolean).join(" · ")),
      `  ${"kernel".padEnd(10)}  ${log.peer}`,
      `  ${"membership".padEnd(10)}  applied: ${listed.join(", ")}`,
      "",
    ];
    for (const [kind, name, details] of rows) {
      const [first, ...rest] = details;
      lines.push(`  ${kind.padEnd(10)}  ${name.padEnd(nameWidth)}  ${first}`);
      for (const more of rest) lines.push(`  ${"".padEnd(10)}  ${"".padEnd(nameWidth)}  ${more}`);
    }
    lines.push("", `  ${log.style(2, `ready in ${ms} ms`)}`, "");
    console.error(lines.join("\n"));
  }

  stop(): void {
    this.worker?.stop();
    for (const l of this.listeners) l.stop();
  }
}

/** What a service consists of, for `Kernable.apply`; see `Membership` in kernable.proto. */
export interface Manifest {
  /** Default: this process's service (`Options.service`). */
  service?: string;
  /** The aggregates the service runs; the kernel waits for a worker of each. */
  workers?: Aggregate<any>[];
  /** The listeners the kernel waits for; others of the service count for nobody. */
  listeners?: Listener[];
  /** The service's listeners that may check requests; they count like listeners. */
  middlewares?: Listener[];
  /** Free metadata, e.g. `{ version: "1.0.0" }`. */
  metadata?: Record<string, string>;
}

/** An app: one service made of whole domains, for `Kernable.app`. */
export interface AppOptions {
  /** Default: this process's service (`Options.service`). */
  service?: string;
  /** Goes into the membership's metadata, like `description`. */
  version?: string;
  description?: string;
  /** More metadata. */
  metadata?: Record<string, string>;
  /** Every aggregate, read model and reactor defined on them runs in this app. */
  domains: Domain[];
}

export interface Options {
  /**
   * Kernel address: `http://host:port` (`host:port` works too) or, on Unix, `unix:/path/to.sock`.
   * Default: `KERNABLE_URL`, else `http://127.0.0.1:7233`, where `kernable start` listens.
   */
  url?: string;
  /**
   * Path to this process's `.key` file; used to sign commands and worker results. Default:
   * `KERNABLE_KEY`, else `app.key` in the current directory.
   */
  key?: string;
  /**
   * Path to the key's grant (`kernable grant`), sent with every call. Default: `KERNABLE_GRANT`,
   * else the `.grant` file next to the key file, if there is one.
   */
  grant?: string;
  /** Default tenant for `submit`. */
  tenant?: string;
  /**
   * The service this process's workers and listeners belong to. Its membership (see `apply`)
   * decides what the kernel waits for: what it lists counts, the service's other workers and
   * listeners are dynamic, and only listeners it lists as middlewares may check requests.
   * Default: `KERNABLE_SERVICE`, else none (everything registered counts).
   */
  service?: string;
  /** Default request context for `submit` and `query`, e.g. this service's identity; a request's own context replaces it. */
  context?: RequestContext;
  /**
   * Mutual TLS for `host:port` URLs: paths of the CA certificate and this process's certificate
   * and key. Default: `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set.
   */
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
  /** Replaces the client's default context; inside a listener, pass `ctx.follow()`. */
  context?: RequestContext;
  /**
   * `"settled"` (default): respond once every listener has processed the events, so read
   * models already show them; throws `NotSettled` on timeout. From a listener's handler, only
   * the listeners with a lower order count. `"committed"`: respond once the events are stored.
   */
  wait?: "settled" | "committed";
}

const LEVELS = { off: 0, error: 1, warn: 2, info: 3, debug: 4 };
type Level = Exclude<keyof typeof LEVELS, "off">;

/**
 * Connection and activity lines of workers, listeners and subscriptions on stderr, up to the
 * level of `KERNABLE_LOG` (`off`, `error`, `warn`, `info` (default) or `debug`).
 */
class Log {
  private readonly level: number;
  /** Colors on a terminal, unless `NO_COLOR` is set. */
  private readonly color = Boolean(process.stderr.isTTY) && !process.env.NO_COLOR;
  /** `<url> · key <name> (<key id>…)`: the kernel and the key this process connects with. */
  readonly peer: string;

  constructor(url: string, keyPath: string, key: Key) {
    const level = process.env.KERNABLE_LOG?.toLowerCase() ?? "";
    this.level = level in LEVELS ? LEVELS[level as keyof typeof LEVELS] : LEVELS.info;
    this.peer = `${url} · key ${basename(keyPath).replace(/\.key$/, "")} (${key.id.slice(0, 12)}…)`;
  }

  enabled(level: Level): boolean {
    return LEVELS[level] <= this.level;
  }

  /** `text` in an ANSI style (1 bold, 2 dim, 31 red, 33 yellow) on a terminal. */
  style(code: number, text: string): string {
    return this.color ? `\x1b[${code}m${text}\x1b[0m` : text;
  }

  /** `12:04:11  listener carts  <line>`, warnings in yellow and errors in red. */
  print(level: Level, what: string, line: string): void {
    if (!this.enabled(level)) return;
    const time = new Date().toTimeString().slice(0, 8);
    const text = level === "error" ? this.style(31, line) : level === "warn" ? this.style(33, line) : line;
    console.error(`${this.style(2, time)}  ${this.style(1, what)}  ${text}`);
  }

  /** Logs the end of a session that was not stopped and waits `ms` before reconnecting. */
  async reconnect(what: string, err: unknown, ms: number): Promise<void> {
    // A gRPC error by its details only (`Connection dropped`, not `14 UNAVAILABLE: …`).
    const e = err as Error & { details?: string };
    const reason = e.details || e.message;
    this.print("warn", what, `connection lost (${reason}), reconnecting in ${ms / 1000} s`);
    await new Promise((r) => setTimeout(r, ms));
  }
}

export class Kernable {
  /** Files of a domain kept by the kernel, e.g. invoices; see `Data`. */
  readonly data: Data;
  /** The shared cache of a domain; see `Cache`. */
  readonly cache: Cache;
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly log: Log;
  private readonly tenant: string;
  private readonly context: RequestContext;
  private readonly domains = new Map<string, Domain>();
  private readonly service: string;

  constructor(options: Options = {}) {
    const env = process.env;
    const key = options.key ?? env.KERNABLE_KEY ?? "app.key";
    const grant = options.grant ?? env.KERNABLE_GRANT ?? key.replace(/\.key$/, "") + ".grant";
    const explicit = options.grant !== undefined || env.KERNABLE_GRANT !== undefined;
    const url = options.url ?? env.KERNABLE_URL ?? "http://127.0.0.1:7233";
    this.clients = connect(url, options.tls ?? tlsFromEnv(), explicit || existsSync(grant) ? readFileSync(grant, "utf8").trim() : undefined);
    this.key = Key.fromFile(key);
    this.log = new Log(url, key, this.key);
    this.tenant = options.tenant ?? "default";
    this.context = options.context ?? {};
    this.service = options.service ?? env.KERNABLE_SERVICE ?? "";
    this.data = new Data(this.clients, this.key, this.tenant);
    this.cache = new Cache(this.clients, this.key, this.tenant);
  }

  /** Seals the vault fields of these domains' commands when submitting them; see `Domain.command`. */
  use(...domains: Domain[]): this {
    for (const d of domains) this.domains.set(d.name, d);
    return this;
  }

  /**
   * Runs these aggregates' commands over this connection. The kernel registers the aggregates:
   * commands for an aggregate or command type no worker registered are refused.
   */
  worker(...aggregates: Aggregate<any>[]): Worker {
    return new Worker(this.clients, this.key, aggregates, this.log, this.service);
  }

  /**
   * An app made of whole domains: `run()` applies the service's membership (its domains'
   * aggregates as workers, their read models and reactors as listeners, those that check
   * requests as middlewares; live ones are not listed), then runs all of them. One service is
   * one app: two processes with the same service would replace each other's membership.
   */
  app(options: AppOptions): App {
    const service = options.service ?? this.service;
    if (!service) throw new Error("an app needs a service: set `service` in the app or the options");
    const aggregates = options.domains.flatMap((d) => d.aggregates);
    const listeners = options.domains.flatMap((d) => d.listeners);
    const durable = listeners.filter((l) => !l.options.live);
    const manifest: Manifest = {
      service,
      workers: aggregates,
      listeners: durable.filter((l) => l.middlewares.size === 0),
      middlewares: durable.filter((l) => l.middlewares.size > 0),
      metadata: {
        ...(options.version === undefined ? {} : { version: options.version }),
        ...(options.description === undefined ? {} : { description: options.description }),
        ...options.metadata,
      },
    };
    const worker = aggregates.length ? new Worker(this.clients, this.key, aggregates, this.log, service) : undefined;
    const handles = listeners.map((l) => new ListenerHandle(this.clients, this.key, l, this.log, service));
    return new App(this, this.log, options.domains, manifest, worker, handles);
  }

  /**
   * Replaces the membership of a service (default: this process's): the workers, listeners and
   * middlewares the kernel waits for, and free metadata. Apply it once per deploy, e.g. from a
   * deploy script, not from every process: the latest apply wins. The key needs `manage` for
   * the service. A middleware the new membership no longer lists stops checking.
   */
  async apply(manifest: Manifest): Promise<void> {
    const service = manifest.service ?? this.service;
    if (!service) throw new Error("apply needs a service: set `service` in the manifest or the options");
    const membership = {
      service,
      workers: (manifest.workers ?? []).map((agg) => `${agg.domain}.${agg.name}`),
      listeners: (manifest.listeners ?? []).map((l) => l.name),
      middlewares: (manifest.middlewares ?? []).map((l) => l.name),
      metadata: Object.entries(manifest.metadata ?? {}).map(([key, value]) => ({ key, value })),
      issued_at: Date.now(),
    };
    await this.clients.apply(membership, this.key.sign(membershipMessage(membership)));
  }

  /** Runs a command and returns its outcome; see `SubmitOptions.wait`. */
  async submit(options: SubmitOptions): Promise<Outcome> {
    const wait = options.wait === "committed" ? "WAIT_COMMITTED" : "WAIT_SETTLED";
    const res = await this.send(options, wait);
    if (res.outcome === "rejected") {
      return { committed: false, code: res.rejected!.code, message: res.rejected!.message };
    }
    // Events come back with references only: the submitter is no vault reader.
    const outcome = { committed: true as const, events: res.committed!.events.map((e) => toStored(e)) };
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
      context: toWireContext(options.context ?? this.context),
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

  /**
   * Runs a listener defined with `Domain.readModel` or `Domain.reactor` over this connection:
   * `await kernable.listener(Carts).run()`. Handlers added to the definition later still count
   * until `run()` connects.
   */
  listener(listener: Listener): ListenerHandle {
    return new ListenerHandle(this.clients, this.key, listener, this.log, this.service);
  }

  private send(options: SubmitOptions, wait: WireWait) {
    const command_id = options.id ?? randomUUID();
    const schema = this.domains.get(options.domain)?.commands.get(options.type);
    const sealed = seal(options.data, schema?.vault, options.aggregateId);
    const context = options.context ?? this.context;
    const cmd = {
      command_id,
      tenant_id: options.tenant ?? this.tenant,
      domain: options.domain,
      aggregate_type: options.aggregateType,
      aggregate_id: options.aggregateId,
      command_type: options.type,
      payload: sealed.payload,
      context: toWireContext({ ...context, correlationId: context.correlationId || command_id }),
      vault_refs: sealed.refs,
    };
    const signed = { ...cmd, client_sig: this.key.sign(commandMessage(cmd)) };
    return this.clients.submit(signed, wait, handling.getStore(), sealed.values);
  }

  /**
   * Streams committed events after `options.after` (backlog first, then live) to `handler`,
   * one at a time. Reconnects and resumes after the last handled event; if the handler throws,
   * that event is delivered again. Persist `position` yourself to resume across restarts.
   */
  subscribe(options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>): Subscription {
    return new Subscription(this.clients, this.key, options, handler, this.log);
  }

  /** Closes the connection now; only needed in a long-running process that discards the client. */
  close(): void {
    this.clients.close();
  }
}

export interface DataOptions {
  /** Default: the client's tenant. */
  tenant?: string;
}

export interface PutDataOptions extends DataOptions {
  /** Default: `text/plain; charset=utf-8` for a string, else `application/octet-stream`. */
  contentType?: string;
}

/**
 * Files of a domain kept by the kernel under `(tenant, domain, key)`, at most 16 MiB each: PDFs,
 * images, exports. Reading needs a grant of `data_read` for the domain (`shop.*`), putting and
 * deleting one of `data_write`. Files are not part of the signed event log and are not erased
 * with the vault: delete a person's files yourself when erasing them.
 */
export class Data {
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly tenant: string;

  constructor(clients: Clients, key: Key, tenant: string) {
    this.clients = clients;
    this.key = key;
    this.tenant = tenant;
  }

  /** Stores `data` under `key`, replacing a file there; a string is stored as UTF-8. */
  async put(domain: string, key: string, data: Uint8Array | string, options: PutDataOptions = {}): Promise<void> {
    const contentType = options.contentType ?? (typeof data === "string" ? "text/plain; charset=utf-8" : "application/octet-stream");
    await this.call("put", domain, key, options, toBuffer(data), contentType);
  }

  /** The file under `key`, or `undefined` if there is none. */
  async get(domain: string, key: string, options: DataOptions = {}): Promise<{ data: Uint8Array; contentType: string } | undefined> {
    const res = await this.call("get", domain, key, options);
    return res.found ? { data: res.data, contentType: res.content_type } : undefined;
  }

  /** Deletes the file under `key`; returns whether there was one. */
  async delete(domain: string, key: string, options: DataOptions = {}): Promise<boolean> {
    return (await this.call("delete", domain, key, options)).found;
  }

  private call(op: "put" | "get" | "delete", domain: string, key: string, options: DataOptions, data: Buffer = Buffer.alloc(0), contentType = "") {
    const request: WireDataRequest = {
      tenant_id: options.tenant ?? this.tenant,
      domain,
      key,
      data,
      content_type: contentType,
      issued_at: Date.now(),
    };
    const rpc = op === "put" ? "PutData" : op === "get" ? "GetData" : "DeleteData";
    return this.clients.data(rpc, { ...request, client_sig: this.key.sign(dataMessage(op, request)) });
  }
}

export interface SetCacheOptions extends DataOptions {
  /** Milliseconds until the value expires; default: until deleted or the kernel restarts. */
  ttlMs?: number;
}

/**
 * The shared cache of a domain: values of at most 1 MiB under `(tenant, domain, key)` that may
 * expire, e.g. rates or tokens shared by several processes. Needs a grant of `cache` for the
 * domain (`shop.*`). The cache lives in the kernel's memory: a restart empties it. Values are not
 * part of the signed event log and are not erased with the vault.
 */
export class Cache {
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly tenant: string;

  constructor(clients: Clients, key: Key, tenant: string) {
    this.clients = clients;
    this.key = key;
    this.tenant = tenant;
  }

  /** Stores `value` under `key`, replacing a value there; a string is stored as UTF-8. */
  async set(domain: string, key: string, value: Uint8Array | string, options: SetCacheOptions = {}): Promise<void> {
    await this.call("set", domain, key, options, toBuffer(value), options.ttlMs ?? 0);
  }

  /** The live value under `key`, or `undefined` if there is none or it expired. */
  async get(domain: string, key: string, options: DataOptions = {}): Promise<Uint8Array | undefined> {
    const res = await this.call("get", domain, key, options);
    return res.found ? res.value : undefined;
  }

  /** Deletes the value under `key`; returns whether there was a live one. */
  async delete(domain: string, key: string, options: DataOptions = {}): Promise<boolean> {
    return (await this.call("delete", domain, key, options)).found;
  }

  private call(op: "set" | "get" | "delete", domain: string, key: string, options: DataOptions, value: Buffer = Buffer.alloc(0), ttlMs = 0) {
    const request: WireCacheRequest = {
      tenant_id: options.tenant ?? this.tenant,
      domain,
      key,
      value,
      ttl_ms: ttlMs,
      issued_at: Date.now(),
    };
    const rpc = op === "set" ? "SetCache" : op === "get" ? "GetCache" : "DeleteCache";
    return this.clients.cache(rpc, { ...request, client_sig: this.key.sign(cacheMessage(op, request)) });
  }
}

function toBuffer(value: Uint8Array | string): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
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

  constructor(clients: Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>, log: Log) {
    this.position = options.after ?? 0;
    this.done = this.loop(clients, key, options, handler, log);
  }

  stop(): void {
    this.stopped = true;
    this.call?.cancel();
  }

  private async loop(clients: Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>, log: Log) {
    const event_types = options.types ?? [];
    while (!this.stopped) {
      const issued_at = Date.now();
      const client_sig = key.sign(subscribeMessage(this.position, event_types, issued_at));
      this.call = clients.subscribe({ after_position: this.position, event_types, issued_at, client_sig });
      let error: unknown = new Error("stream ended");
      try {
        for await (const e of this.call as AsyncIterable<WireEvent>) {
          await handler(toStored(e));
          this.position = e.global_position;
        }
      } catch (err) {
        error = err;
      }
      if (!this.stopped) await log.reconnect("subscription", error, options.reconnectMs ?? 1000);
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
   * read models); omitted, the kernel resumes after the last acknowledged event, and a new
   * listener starts at the newest event, so it never reacts to what happened before it existed
   * (default for reactors). Reconnects always resume after the last handled event.
   */
  start?: number;
  /**
   * Not registered with the kernel: no stored position, always starts at the newest event,
   * nobody waits for it, and it is forgotten when it disconnects. For reactions that only
   * matter while the process runs, e.g. pushing to connected browsers. Cannot be a middleware.
   */
  live?: boolean;
  reconnectMs?: number;
  /**
   * Keeps the listener's state in a store (`sqlite:<path>` or `memory:`) instead of memory:
   * its data and position are written together per batch, and a restart continues after the
   * stored position (`start` is then ignored). See `ListenerHandle.store`.
   */
  store?: string;
  /**
   * Version of the stored data's format (default "1"). Change it when the handlers store data
   * differently: the store is cleared and the listener rebuilds from the first event.
   */
  schema?: string;
}

type EventHandler = (data: unknown, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>;

export interface HandlerOptions {
  /**
   * Get events as stored, without upcasting, whatever their schema (for archives and exports).
   * By default a listener stops at an event whose schema it does not know.
   */
  raw?: boolean;
}
type QueryHandler = (data: unknown, qry: QueryMeta, ctx: QueryContext) => unknown;
/**
 * Checks a request. After it, `outcome` is what came of it: an `Outcome` for a command, a
 * `QueryOutcome` for a query (`check.kind` tells which); before it, `undefined`.
 */
type MiddlewareHandler = (check: Check, outcome?: Outcome | QueryOutcome) => void | Promise<void>;

/** A type within its domain (`ItemAdded`), not qualified and no wildcard. */
const UNQUALIFIED = /^[^.*]+$/;
/** `domain.Type` or a whole domain, `domain.*`. */
const QUALIFIED_OR_DOMAIN = /^[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*)$/;

/**
 * The definition of a durable, named event listener: which events it handles, which queries it
 * answers and what it checks as a middleware. Define it on a domain with `Domain.readModel` or
 * `Domain.reactor`, like an aggregate, and run it with `kernable.listener(definition)`.
 *
 * Type names without a domain (`ItemAdded`, `GetCart`) are of the listener's own domain; types
 * of other domains are qualified (`identity.AccountRegistered`) or name a whole domain
 * (`identity.*`).
 *
 * Handlers get event payloads upcast to the versions of the listener's own domain and of the
 * domains it `use`s. An event with a newer schema than known stops the listener (it retries and
 * logs) until it is updated, so it never misreads data.
 */
export class Listener {
  /** The domain the listener is defined on: its unqualified types are of this domain. */
  readonly domain: Domain;
  /** The listener's name, unique per tenant; the kernel tracks its position under it. */
  readonly name: string;
  readonly options: ListenerOptions;
  /** Event handlers per qualified event type or domain (`domain.*`). */
  readonly handlers = new Map<string, { handler: EventHandler; raw: boolean }>();
  /** Query handlers per qualified query type. */
  readonly queries = new Map<string, QueryHandler>();
  /** The middleware handlers and their qualified types, before and after requests. */
  readonly middlewares = new Map<"before" | "after", { options: MiddlewareOptions; handler: MiddlewareHandler }>();
  /** The domains whose event schemas upcast events, by name: the own domain and those it `use`s. */
  readonly domains = new Map<string, Domain>();

  constructor(domain: Domain, name: string, options: ListenerOptions = {}) {
    this.domain = domain;
    this.name = name;
    this.options = options;
    this.domains.set(domain.name, domain);
  }

  /** Upcasts the events of these other domains with their event schemas (see `Domain.event`). */
  use(...domains: Domain[]): this {
    for (const d of domains) this.domains.set(d.name, d);
    return this;
  }

  /**
   * Handles events of `eventType`: a type of the own domain (`ItemAdded`), of another domain
   * (`identity.AccountRegistered`) or a whole domain (`identity.*`): `handler(data, evt, ctx)`.
   */
  on<D = any>(
    eventType: string,
    handler: (data: D, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>,
    options: HandlerOptions = {},
  ): this {
    if (!UNQUALIFIED.test(eventType) && !QUALIFIED_OR_DOMAIN.test(eventType)) {
      throw new Error(`listen to a type (Type), a qualified type (domain.Type) or a domain (domain.*), not ${JSON.stringify(eventType)}`);
    }
    this.handlers.set(this.qualify(eventType), { handler: handler as EventHandler, raw: options.raw ?? false });
    return this;
  }

  /**
   * Answers the query type `queryType`, of the own domain (`GetCart`) or another one
   * (`orders.GetOrder`), from this listener's state. The kernel delivers queries in order with
   * the events, so an answer reflects every event handled before it. Throw `Rejected` to refuse.
   * A handler cannot query its own listener (the answer would wait behind itself): the kernel
   * refuses it.
   */
  query<D = any, R = unknown>(queryType: string, handler: (data: D, qry: QueryMeta, ctx: QueryContext) => R | Promise<R>): this {
    if (!UNQUALIFIED.test(queryType) && !/^[A-Za-z][A-Za-z0-9_-]*\.[^.*]+$/.test(queryType)) {
      throw new Error(`answer a query type (Query) or a qualified one (domain.Query), not ${JSON.stringify(queryType)}`);
    }
    this.queries.set(this.qualify(queryType), handler as QueryHandler);
    return this;
  }

  /**
   * Makes this listener a middleware: the kernel asks `handler` about every command and query
   * of `options.types`, before accepting or routing it (or after, see `MiddlewareOptions.when`),
   * in chain order, and only once this listener has caught up with the events. Throw `Rejected`
   * to refuse; any other error refuses too. The registration is stored: while this listener is
   * away, the kernel refuses those requests (`kernable listeners remove` unregisters it). The
   * handler cannot submit or query what it checks itself (it would wait behind itself): the
   * kernel refuses it. A listener has at most one middleware before and one after.
   */
  middleware(options: MiddlewareOptions, handler: MiddlewareHandler): this {
    if (this.options.live) throw new Error(`${this.name} is live: a live listener cannot be a middleware`);
    for (const t of options.types) {
      if (t !== "*" && !UNQUALIFIED.test(t) && !QUALIFIED_OR_DOMAIN.test(t)) {
        throw new Error(`check \`*\`, a type (Type), a qualified type (domain.Type) or a domain (domain.*), not ${JSON.stringify(t)}`);
      }
    }
    const when = options.when ?? "before";
    if (this.middlewares.has(when)) throw new Error(`${this.name} has a middleware ${when} already`);
    const types = options.types.map((t) => (t === "*" ? t : this.qualify(t)));
    this.middlewares.set(when, { options: { ...options, types }, handler });
    return this;
  }

  /** `type` with the own domain if it has none. */
  private qualify(type: string): string {
    return UNQUALIFIED.test(type) ? `${this.domain.name}.${type}` : type;
  }
}

/**
 * A listener running over a connection: `kernable.listener(definition)`. Events arrive in order,
 * one at a time, after every listener with a lower order has processed them. Delivery is at
 * least once: if a handler throws, the listener reconnects and the event is delivered again.
 */
export class ListenerHandle {
  /** Global position of the last handled event (or skipped, for other event types). */
  position = 0;
  private stored?: Store;
  /** Resolves once the kernel accepted the listener. */
  readonly ready: Promise<void>;
  private markReady!: () => void;
  private call?: ListenCall;
  private stopped = false;
  /** Newest event when the kernel accepted the listener; events up to it are replayed. */
  private replayUpTo = 0;
  /** Position after which the kernel delivered on the first connect. */
  startedAt = 0;
  /** Whether the first connect gets its own line; an `App` prints it in its overview instead. */
  announce = true;
  private connects = 0;
  private readonly clients: Clients;
  private readonly key: Key;
  private readonly log: Log;
  /** The service it belongs to (`Options.service`). */
  private readonly service: string;
  /** The definition it runs. */
  readonly listener: Listener;

  constructor(clients: Clients, key: Key, listener: Listener, log: Log, service: string) {
    this.clients = clients;
    this.key = key;
    this.log = log;
    this.service = service;
    this.listener = listener;
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /** The listener's store (`ListenerOptions.store`), once `run()` opened it. */
  get store(): Store {
    if (!this.stored) throw new Error(`listener ${this.listener.name} has no store, or run() has not opened it yet`);
    return this.stored;
  }

  /** Processes events until `stop()`, reconnecting when the stream drops. */
  async run(): Promise<void> {
    const options = this.listener.options;
    let start = options.start;
    if (options.store && !this.stored) {
      this.stored = await Store.open(options.store, options.schema ?? "1");
      this.position = this.stored.committed;
      start = this.position;
    }
    while (!this.stopped) {
      let error: unknown = new Error("stream ended");
      try {
        await this.listen(start);
      } catch (err) {
        error = err;
      }
      start = this.position;
      if (!this.stopped) await this.log.reconnect(this.what, error, options.reconnectMs ?? 1000);
    }
  }

  stop(): void {
    this.stopped = true;
    this.call?.cancel();
  }

  /** `listener <name>`, for log lines. */
  private get what(): string {
    return `listener ${this.listener.name}`;
  }

  private listen(start: number | undefined): Promise<void> {
    const { name, options, handlers, queries, middlewares } = this.listener;
    return new Promise((resolve, reject) => {
      const call = this.clients.listen();
      this.call = call;
      // Batches are handled strictly one after another.
      let queue = Promise.resolve();
      call.on("data", (m: ListenerDelivery) => {
        // Requests sent from the handlers name this listener, so the kernel never makes them
        // wait for it (it handles one message at a time).
        queue = queue.then(() => handling.run(name, () => this.onMessage(call, m))).catch((err) => {
          call.cancel();
          reject(err);
        });
      });
      call.on("error", reject);
      call.on("end", resolve);
      call.write({
        hello: {
          name,
          key_id: this.key.id,
          protocol: PROTOCOL,
          order: options.order ?? 0,
          event_types: [...handlers.keys()],
          query_types: [...queries.keys()],
          ...(start === undefined ? {} : { from_position: start }),
          service: this.service,
          live: options.live ?? false,
          middlewares: [...middlewares].map(([when, m]) => ({
            order: m.options.order ?? 0,
            types: m.options.types,
            when: when === "after" ? 1 : 0,
          })),
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
      this.logConnected();
      this.markReady();
    } else if (m.batch) {
      // With a store, the batch's data and position are committed together, before the ack.
      this.stored?.begin();
      try {
        await this.handleBatch(m.batch);
        this.stored?.commit(m.batch.up_to);
      } catch (err) {
        this.stored?.rollback();
        if (this.stored) this.position = this.stored.committed;
        throw err;
      }
      this.position = m.batch.up_to;
      call.write({ ack: { position: m.batch.up_to } });
    } else if (m.query) {
      call.write({ answer: { call_id: m.query.call_id, ...(await this.answer(m.query.query)) } });
    } else if (m.check) {
      call.write({ verdict: { call_id: m.check.call_id, ...(await this.verdict(m.check)) } });
    }
  }

  private logConnected(): void {
    if (this.connects++ === 0) {
      this.startedAt = this.position;
      if (this.announce) this.log.print("info", this.what, [`connected to ${this.log.peer}`, ...listenerDetails(this.listener, this.position)].join(" · "));
    } else {
      this.log.print("info", this.what, `connected again at position ${this.position}`);
    }
  }

  private async handleBatch(batch: NonNullable<ListenerDelivery["batch"]>): Promise<void> {
    const { handlers, domains } = this.listener;
    for (const e of batch.events) {
      const entry = handlers.get(`${e.domain}.${e.event_type}`) ?? handlers.get(`${e.domain}.*`);
      if (entry) {
        const stored = toStored(e, batch.vault);
        const { data, request, ...evt } = entry.raw
          ? stored
          : { ...stored, ...upcast(domains.get(e.domain)?.events, e.domain, e.event_type, e.schema, stored.data) };
        const ctx = { replaying: e.global_position <= this.replayUpTo, follow: () => follow(stored), request, store: this.stored };
        await entry.handler(data, evt, ctx);
        this.log.print("debug", this.what, `${e.domain}.${e.event_type} at ${e.global_position}`);
      }
      this.position = e.global_position;
    }
  }

  private async verdict(c: NonNullable<ListenerDelivery["check"]>): Promise<{ rejected?: WireRejected }> {
    try {
      const when = c.when === 1 ? "after" : "before";
      const middleware = this.listener.middlewares.get(when);
      if (!middleware) throw new Rejected(`no middleware ${when}`, "not_a_middleware");
      await middleware.handler(toCheck(c), when === "after" ? toOutcome(c) : undefined);
      return {};
    } catch (err) {
      return {
        rejected: err instanceof Rejected ? { code: err.code, message: err.message } : { code: "middleware_error", message: String(err) },
      };
    }
  }

  private async answer(q: WireQueryRequest): Promise<{ response: Buffer } | { rejected: WireRejected }> {
    const answer = await this.tryAnswer(q);
    const type = `${q.domain}.${q.query_type}`;
    this.log.print("debug", this.what, "rejected" in answer ? `rejected ${type} ${answer.rejected.code}` : `answered ${type}`);
    return answer;
  }

  private async tryAnswer(q: WireQueryRequest): Promise<{ response: Buffer } | { rejected: WireRejected }> {
    try {
      const handler = this.listener.queries.get(`${q.domain}.${q.query_type}`);
      if (!handler) throw new Rejected(`no handler for ${q.domain}.${q.query_type}`, "unknown_query");
      const qry: QueryMeta = { queryId: q.query_id, type: q.query_type, domain: q.domain, tenant: q.tenant_id };
      const result = await handler(decode(q.payload), qry, { request: fromWireContext(q.context), store: this.stored });
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
  private readonly log: Log;
  private readonly byCommand = new Map<string, Aggregate<any>>();
  private readonly aggregates: Aggregate<any>[];
  /** The service it belongs to (`Options.service`). */
  private readonly service: string;
  /** Whether the first connect gets its own line; an `App` prints it in its overview instead. */
  announce = true;
  private connects = 0;
  private call?: SessionCall;
  private stopped = false;

  constructor(clients: Clients, key: Key, aggregates: Aggregate<any>[], log: Log, service: string) {
    this.clients = clients;
    this.key = key;
    this.log = log;
    this.aggregates = aggregates;
    this.service = service;
    for (const agg of aggregates) for (const type of agg.handlers.keys()) this.byCommand.set(`${agg.domain}.${type}`, agg);
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /** Processes commands until `stop()`, reconnecting when the session drops. */
  async run(options: WorkerOptions = {}): Promise<void> {
    while (!this.stopped) {
      let error: unknown = new Error("stream ended");
      try {
        await this.session(options.concurrency ?? 16);
      } catch (err) {
        error = err;
      }
      if (!this.stopped) await this.log.reconnect("worker", error, options.reconnectMs ?? 1000);
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
          aggregates: this.aggregates.map((agg) => ({
            aggregate_type: `${agg.domain}.${agg.name}`,
            command_types: [...agg.handlers.keys()].map((type) => `${agg.domain}.${type}`),
          })),
          max_concurrency: concurrency,
          event_schemas: this.eventSchemas(),
          service: this.service,
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
      if (this.connects++ > 0) this.log.print("info", "worker", "connected again");
      else if (this.announce) this.log.print("info", "worker", `connected to ${this.log.peer} · runs ${[...this.byCommand.keys()].sort().join(", ")}`);
      this.markReady();
    } else if (m.execute) {
      void this.execute(call, m.execute);
    }
  }

  private async execute(call: SessionCall, x: NonNullable<KernelMessage["execute"]>): Promise<void> {
    const cmd = x.command;
    const stream = `${cmd.domain}.${cmd.command_type} ${cmd.aggregate_type}/${cmd.aggregate_id}`;
    let events: WireNewEvent[];
    let vault: WireVaultValue[] = [];
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
      const history = x.history.map((e) => ({
        ...metaOf(e.event_type, e.version, cmd, e.command_id, e.context),
        schema: e.schema,
        data: unseal(decode(e.payload), x.vault).data,
      }));
      const state = agg.fold(history, from);
      let ids = 0;
      const meta: CommandMeta = {
        commandId: cmd.command_id,
        type: cmd.command_type,
        domain: cmd.domain,
        aggregateType: cmd.aggregate_type,
        aggregateId: cmd.aggregate_id,
        tenant: cmd.tenant_id,
      };
      const ctx: CommandContext = {
        request: fromWireContext(cmd.context),
        now: () => new Date(x.received_at),
        id: () => derivedId(cmd.command_id, ids++),
        deadline: new Date(x.deadline),
      };
      const produced = await agg.handlers.get(cmd.command_type)!(state, unseal(decode(cmd.payload), x.vault).data, meta, ctx);
      events = produced.map((e) => {
        const def = agg.events.get(e.type);
        const sealed = seal(e.data, def?.vault, cmd.aggregate_id);
        vault.push(...sealed.values);
        const erases = def?.eraser ? String(def.eraser(e.data, cmd.aggregate_id) ?? "") : "";
        return { event_type: e.type, schema: agg.schemaOf(e.type), payload: sealed.payload, vault_refs: sealed.refs, erases };
      });
      snapshot = this.snapshot(agg, cmd, x.expected_version, state, produced);
    } catch (err) {
      if (err instanceof SchemaError) {
        // Not the command's fault: this worker cannot read the aggregate. Stop until updated.
        this.log.print("error", "worker", err.message);
        call.cancel();
        return;
      }
      const reason: WireRejected =
        err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: String(err) };
      const worker_sig = this.key.sign(rejectionMessage(cmd.command_id, reason));
      call.write({ rejection: { lease_id: x.lease_id, reason, worker_sig } });
      this.log.print("debug", "worker", `${stream} rejected ${reason.code}`);
      return;
    }
    const worker_sig = this.key.sign(resultMessage(cmd, x.expected_version, events));
    call.write({ result: { lease_id: x.lease_id, events, worker_sig, vault, ...(snapshot && { snapshot }) } });
    this.log.print("debug", "worker", `${stream} committed ${events.length} events`);
  }

  /** A snapshot of the state after `events`, if they cross a multiple of `snapshotEvery`. */
  private snapshot(agg: Aggregate<any>, cmd: WireCommand, before: number, state: unknown, events: NewEvent[]) {
    const version = before + events.length;
    const every = agg.snapshotEvery;
    if (every <= 0 || events.length === 0 || Math.floor(version / every) === Math.floor(before / every)) return undefined;
    // Apply the events as they will be read back (a JSON round trip), so the snapshot equals a replay.
    const read = events.map((e, i) => ({
      ...metaOf(e.type, before + i + 1, cmd, cmd.command_id, cmd.context),
      schema: agg.schemaOf(e.type),
      data: decode(encode(e.data)),
    }));
    const after = agg.fold(read, state);
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

/** An event of `cmd`'s aggregate to fold, caused by command `commandId`, without its data. */
function metaOf(
  type: string,
  version: number,
  cmd: WireCommand,
  commandId: string,
  context: WireRequestContext | null | undefined,
): EventMeta & { request: RequestContext } {
  return {
    type,
    version,
    tenant: cmd.tenant_id,
    domain: cmd.domain,
    aggregateType: cmd.aggregate_type,
    aggregateId: cmd.aggregate_id,
    commandId,
    request: fromWireContext(context),
  };
}

/** A request context for a command caused by `event`. */
function follow(event: StoredEvent): RequestContext {
  const { expectedVersion, bypassAuthorization, ...rest } = event.request;
  return { ...rest, correlationId: rest.correlationId || event.commandId, causationId: event.commandId };
}

/** Mutual TLS from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`: all or none. */
function tlsFromEnv(): TlsFiles | undefined {
  const { KERNABLE_TLS_CA: ca, KERNABLE_TLS_CERT: cert, KERNABLE_TLS_KEY: key } = process.env;
  if (!ca && !cert && !key) return undefined;
  if (!ca || !cert || !key) throw new Error("set all of KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY, or none");
  return { ca, cert, key };
}

function toWireContext(c: RequestContext): WireRequestContext {
  return {
    sender_tenant: c.senderTenant ?? "",
    sender_identity: c.senderIdentity ?? "",
    sender_account: c.senderAccount ?? "",
    sender_session: c.senderSession ?? "",
    on_behalf_of: c.onBehalfOf ?? "",
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
    expectedVersion: c.expected_version ?? undefined,
    correlationId: c.correlation_id,
    causationId: c.causation_id,
    traceId: c.trace_id,
    bypassAuthorization: c.bypass_authorization || undefined,
    attributes: c.attributes.length ? Object.fromEntries(c.attributes.map((a) => [a.key, a.value])) : undefined,
  };
  return Object.fromEntries(Object.entries(all).filter(([, v]) => v !== undefined && v !== "")) as RequestContext;
}

/** What came of the request a middleware after it checks. */
function toOutcome(c: NonNullable<ListenerDelivery["check"]>): Outcome | QueryOutcome {
  const rejected = (r: WireRejected | null | undefined) => ({ code: r?.code ?? "", message: r?.message ?? "" });
  if (c.command_result) {
    const r = c.command_result;
    if (r.outcome === "committed") return { committed: true, events: r.committed!.events.map((e) => toStored(e)) };
    return { committed: false, ...rejected(r.rejected) };
  }
  const r = c.query_result!;
  if (r.outcome === "response") return { answered: true, data: decode(r.response ?? Buffer.alloc(0)) };
  return { answered: false, ...rejected(r.rejected) };
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
      data: unseal(decode(x.payload), c.vault).data,
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

function toStored(e: WireEvent, vault?: WireVaultValue[]): StoredEvent {
  const { data, redacted } = unseal(decode(e.payload), vault);
  return {
    redacted,
    position: e.global_position,
    tenant: e.tenant_id,
    domain: e.domain,
    aggregateType: e.aggregate_type,
    aggregateId: e.aggregate_id,
    version: e.version,
    type: e.event_type,
    schema: e.schema,
    data,
    commandId: e.command_id,
    request: fromWireContext(e.context),
  };
}
