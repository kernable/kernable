// @kernable/sdk: write event-sourced business logic in TypeScript; the kernel does the rest.

import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, readFileSync, statSync } from "node:fs";
import { basename } from "node:path";

import type { ClientReadableStream } from "@grpc/grpc-js";

import {
  PROTOCOL,
  Partitioned,
  connect,
  partitionOf,
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
  type WireParked,
  type WirePartitionMap,
  type WireQueryRequest,
  type WireRejected,
  type WireRequestContext,
  type WireRequirement,
  type WireSkipped,
  type WireSnapshot,
  type WireUniqueKey,
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
  indexMessage,
  membershipMessage,
  processActMessage,
  processMessage,
  queryMessage,
  cancelMessage,
  rejectionMessage,
  resultMessage,
  snapshotMessage,
  subscribeMessage,
  valueHash,
  vaultRef,
} from "./signing.ts";
import {
  Command, Event, Query, bind, boundOrThrow, dataOf, typeOf as messageType,
  type AnswerOf, type CommandClass, type EventClass, type Fields, type MessageStatics, type QueryClass, type QueryTarget,
} from "./messages.ts";
import { Store } from "./store.ts";
import { ProcessBuilder, type ProcessModel, type ProcessVersion } from "./process.ts";

export { Key } from "./signing.ts";
export { Store } from "./store.ts";
export { Command, Event, Query } from "./messages.ts";
export type { AnswerOf, CommandClass, EventClass, Fields, PersonalFields, QueryClass, QueryTarget, SubjectOf } from "./messages.ts";
export { ProcessBuilder, value } from "./process.ts";
export type {
  CancelOnOptions, CommandStepOptions, DecideCase, DecideOptions, Duration, Literal, Mapping, ProcessModel, ProcessStep, ProcessVersion,
  QueryStepOptions, StartOptions, TimerOptions, TimerWhen, WaitOptions,
} from "./process.ts";

/**
 * A refusal with a business reason: throw it from a command handler, a query handler or a
 * middleware. `code` is a short machine-readable reason (default `"rejected"`), `message` the
 * text for people.
 *
 * - From a command handler: the command is rejected and stored as rejected, with no events;
 *   `submit` returns `{ committed: false, code, message }`, it does not throw.
 * - From a query handler or a middleware: the request is refused; `Kernable.query` throws a
 *   `Rejected` with the same code and message.
 *
 * Any other error a handler throws rejects too, with the code `handler_error` (a middleware's:
 * `middleware_error`).
 *
 * ```ts
 * if (state.checkedOut) throw new Rejected("Cart is checked out"); // code: "rejected"
 * if (!state.open) throw new Rejected("Cart was never opened", reason); // reason: a code of your own
 * ```
 */
export class Rejected extends Error {
  /** The reason as a code, e.g. `not_found`; `"rejected"` unless given. */
  readonly code: string;
  /** For a rejected follow-up command (`ListenerContext.submit`): the key the kernel named (`unique_taken`). */
  key?: string;
  /** For a rejected follow-up command: the requirement the kernel named (`requirement_missing`). */
  requirement?: Requirement;

  constructor(message: string, code = "rejected") {
    super(message);
    this.code = code;
  }
}

/**
 * Thrown by a settling `submit` (`wait: "settled"`, the default) when the command was committed
 * but not every listener that counts processed its events within the kernel's
 * `--settle-timeout-ms` (default 5 s). The command is not lost, so do not submit it again under a
 * new ID: read models may just not show the change yet. Submitting again with the same `id` returns the
 * same outcome; a query with `minPosition` waits for the events.
 *
 * ```ts
 * try {
 *   await kernable.submit({ domain: "shop", aggregateType: "cart", aggregateId: "c1", id: commandId, type: "AddItem", data });
 * } catch (err) {
 *   if (!(err instanceof NotSettled)) throw err;
 *   const position = err.outcome.events.at(-1)?.position ?? 0; // committed, not yet settled
 * }
 * ```
 */
export class NotSettled extends Error {
  /** The committed outcome, with the stored events. */
  readonly outcome: { committed: true; events: StoredEvent[] };

  constructor(outcome: { committed: true; events: StoredEvent[] }) {
    super("command committed, but not all listeners have processed its events yet");
    this.outcome = outcome;
  }
}

/**
 * Thrown by `submit` for the ID of a scheduled command (`enqueue` with `at`) that is not due
 * yet: it has no outcome to wait for. Submit its ID again after its time.
 */
export class NotDue extends Error {
  /** The scheduled command's ID. */
  readonly commandId: string;

  constructor(commandId: string) {
    super(`command ${commandId} is scheduled and not due yet: ask again after its time`);
    this.commandId = commandId;
  }
}

/**
 * An event a command handler returns: `{ type: "ItemAdded", data: { sku, qty } }`. The worker
 * stores it at the payload version declared with `Domain.event` (1 if not declared) and moves
 * fields declared `personal` to the vault.
 */
export interface NewEvent {
  /** The event type within the aggregate's domain, unqualified: `ItemAdded`. */
  type: string;
  /** The payload; must survive a JSON round trip. Omitted, it is stored as `{}`. */
  data?: unknown;
}

/**
 * Who sends a request and in which context: signed with the command or query and stored with
 * every event a command causes. Middleware decides on it; `ListenerContext.follow()` passes it on
 * to follow-up commands. All fields are optional; what the sender fields hold is up to your
 * application. `attributes` are business data only, never security decisions. See
 * [The request context](../concepts/queries-and-middleware#the-request-context).
 */
export interface RequestContext {
  /** The sender's tenant, as your application defines it. */
  senderTenant?: string;
  /** The person or system the request is for: who middleware checks. */
  senderIdentity?: string;
  /** The sender's account, as your application defines it. */
  senderAccount?: string;
  /** The sender's session, as your application defines it. */
  senderSession?: string;
  /** Authorize as this identity of the target tenant; the sender stays the real principal. */
  onBehalfOf?: string;
  /** Reject the command (`version_mismatch`) unless the aggregate is at this version. */
  expectedVersion?: number;
  /** Shared by everything one original request causes; defaults to the command ID. */
  correlationId?: string;
  /** The command that caused this one (`follow()` sets it). */
  causationId?: string;
  /** Your tracing system's ID. */
  traceId?: string;
  /**
   * Privileged: only keys granted `privileged` for `bypass_authorization` may set it; what it
   * means, middleware decides.
   */
  bypassAuthorization?: boolean;
  /** Free key-value pairs for business data, never for security decisions. */
  attributes?: Record<string, string>;
}

/** What a command handler gets about the command besides its data (its third argument, `cmd`). */
export interface CommandMeta {
  /** The command's ID: the idempotency key (`SubmitOptions.id`) or a generated UUID. */
  commandId: string;
  /** The command type within the domain. */
  type: string;
  domain: string;
  /** The aggregate type, unqualified: `cart`. */
  aggregateType: string;
  aggregateId: string;
  tenant: string;
}

/**
 * What a command handler gets besides state, data and `CommandMeta` (its fourth argument, `ctx`).
 * The worker creates it; create one yourself to call a handler directly, e.g. in a unit test with
 * `Aggregate.handle`: `new CommandContext("close-1", {}, new Date(0))`.
 */
export class CommandContext {
  /** The command's ID, as in `CommandMeta.commandId`: what `id()` derives its IDs from. */
  readonly commandId: string;
  /** Who sends the command: check permissions here. */
  request: RequestContext;
  /** When the kernel received the command: the same every time the command runs. Use it instead of `new Date()`. */
  readonly now: () => Date;
  /** A new ID that is the same every time the command runs (1st call, 2nd call, ...). Use it instead of random IDs. */
  readonly id: () => string;
  /** When the kernel hands the command to another worker if this one has not answered. */
  deadline: Date;

  /**
   * A context for command `commandId`, sent with `request` and received at `receivedAt` (default:
   * the epoch); its deadline is 30 s after `receivedAt` unless given.
   */
  constructor(commandId: string, request: RequestContext = {}, receivedAt = new Date(0), deadline = new Date(receivedAt.getTime() + 30_000)) {
    const at = receivedAt.getTime();
    let ids = 0;
    this.commandId = commandId;
    this.request = request;
    this.now = () => new Date(at);
    this.id = () => derivedId(commandId, ids++);
    this.deadline = deadline;
  }
}

/** What a query handler gets about the query besides its data (its second argument, `qry`). */
export interface QueryMeta {
  /** A new UUID per query. */
  queryId: string;
  /** The query type within the domain. */
  type: string;
  domain: string;
  tenant: string;
}

/** What a query handler gets besides the query's data and `QueryMeta` (its third argument, `ctx`). */
export interface QueryContext {
  /** Who asks: check permissions here. */
  request: RequestContext;
  /** The listener's store, if it has one (`ListenerOptions.store`). */
  store?: Store;
}

/**
 * A query for `Kernable.query`: which query type to ask, with which data. The kernel routes it
 * to the read model that answers `domain.type`.
 *
 * ```ts
 * const cart = await kernable.query<Cart>({ domain: "shop", type: "GetCart", data: { id: "c1" } });
 * ```
 */
export interface QueryOptions {
  /** The domain of the query type: `shop`. */
  domain: string;
  /** The query type within the domain: `GetCart`. */
  type: string;
  /** The query's data, sent as JSON; the handler gets it as its first argument. */
  data?: unknown;
  /** Default: the client's tenant (`Options.tenant`). */
  tenant?: string;
  /** Replaces the client's default context (`Options.context`). */
  context?: RequestContext;
  /**
   * Answer only after the read model has processed this global position, e.g. the last event's
   * `position` of a `committed` submit. Default 0: answer at once.
   */
  minPosition?: number;
  /** How long to wait for the connection and the answer; default `Options.timeoutMs`. */
  timeoutMs?: number;
  /**
   * For a read model across tenants with partitions: answer only once the read model has
   * processed every partition's events up to the query, not only those of the tenant's
   * partition. Costs a wait-only query to each partition first.
   */
  allPartitions?: boolean;
}

/**
 * A request a middleware checks (the handler's first argument): a command or query, before the
 * kernel accepts or routes it, or after; or a file or cache request (`Kernable.data`,
 * `Kernable.cache`), before it runs. Vault fields of a command's data are filled in only for a
 * middleware before the request whose key is granted `vault_read`; else they are `null`.
 */
export interface Check {
  /**
   * What is checked: a command, a query, a file request (`data`) or a cache request (`cache`);
   * the fields below depend on it.
   */
  kind: "command" | "query" | "data" | "cache" | "cancel";
  /** The command or query ID; empty for a file or cache request. */
  id: string;
  tenant: string;
  domain: string;
  /**
   * The type within the domain: the command or query type, or for a file request `PutData`,
   * `GetData`, `DeleteData` or `ListData`, for a cache request `SetCache`, `GetCache` or
   * `DeleteCache`.
   */
  type: string;
  /** Commands only. */
  aggregateType?: string;
  /** Commands only. */
  aggregateId?: string;
  /**
   * The aggregate the request is about, qualified: a command's aggregate (`shop.order`, `o-17`),
   * or the one a query declares (`Query.target`, `QuerySchema.target`). Absent for queries that
   * declare none, and for file and cache requests.
   */
  target?: { type: string; id: string };
  /** File and cache requests only: the file's or value's key. */
  key?: string;
  /**
   * The command's or query's data. `undefined` for a file or cache request: the middleware
   * decides on who asks for which key, never sees the file or value.
   */
  data: unknown;
  /** Who sends it: check permissions here. */
  request: RequestContext;
}

/**
 * What a middleware checks and where it sits in the chain: the first argument of
 * `Listener.middleware`.
 *
 * ```ts
 * policy.middleware({ types: ["shop.*"] }, (check) => {
 *   if (!check.request.senderIdentity) throw new Rejected("sign in first");
 * });
 * ```
 */
export interface MiddlewareOptions {
  /**
   * Request types it checks: `*` (all), a type of the listener's own domain (`Type`), another
   * domain's type (`domain.Type`) or a whole domain (`domain.*`). Besides command and query
   * types, these cover the file and cache requests of a domain (`Kernable.data`,
   * `Kernable.cache`) under the types `PutData`, `GetData`, `DeleteData`, `ListData`,
   * `SetCache`, `GetCache` and `DeleteCache`, checked before they run: `shop.*` includes
   * `shop.PutData`.
   */
  types: string[];
  /**
   * `before` (default): checks a request before the kernel accepts it; throwing refuses it.
   * `after`: gets the request's outcome too. Throwing refuses a query's response; a command's
   * outcome, and a rejected query, it only observes, best effort (it misses what happens while
   * it is away, and nobody waits for it). File and cache requests are checked only before.
   */
  when?: "before" | "after";
  /**
   * Position in the chain: lower runs first, equal orders by listener name (default 0).
   * Negative orders are reserved for kernable's kits (a grant of `reserved_order`).
   */
  order?: number;
}

/** What a listener handler gets besides the event's data and `StoredEventMeta` (its third argument, `ctx`). */
export interface ListenerContext {
  /**
   * The event was committed before this connection began: a read model rebuilding from the
   * start, and equally the backlog a reactor catches up on after a restart or a lost connection.
   * A reactor must not skip side effects because of it (those events are new to it); submit
   * follow-ups with fixed command IDs instead, so they are safe to repeat. Use it to skip work
   * that only matters live, e.g. a notification about something long past in a listener that
   * starts at 0.
   */
  replaying: boolean;
  /**
   * A request context for commands caused by this event: same sender and correlation, the
   * event's command as `causationId`, without `expectedVersion` and `bypassAuthorization`. Pass
   * it as `SubmitOptions.context`.
   */
  follow(): RequestContext;
  /** The request context of the command that caused the event. */
  request: RequestContext;
  /** The listener's store, if it has one (`ListenerOptions.store`). */
  store?: Store;
  /**
   * Sets as whom the process `name` acts (`Kernable.processAct`), on the listener's connection:
   * how an authorization kit hands a process its identity.
   */
  processAct(name: string, context: RequestContext): Promise<{ changed: boolean }>;
  /**
   * Submits a command this event causes, on the listener's connection, and returns its events.
   * It needs a fixed `id` (`confirm-<order>`), so a redelivered event runs it once. The domain
   * defaults to the listener's, the context to `follow()`, the tenant to the event's. Throws
   * `Rejected` (with the kernel's `key` or `requirement`, if any) if the command is rejected: a
   * reactor then parks the event at once (see `ListenerOptions.parkAfter`); catch it to accept
   * a rejection. Throws like `Kernable.submit` otherwise.
   *
   * ```ts
   * await ctx.submit({ aggregateType: "order", aggregateId: evt.aggregateId, type: "Confirm", id: `confirm-${evt.aggregateId}` });
   * ```
   */
  submit(command: FollowUp): Promise<StoredEvent[]>;
  /** The same with a command class instance: `ctx.submit(new Confirm({ … }), { id, aggregateId })`. */
  submit(command: Command, target: CommandTarget & { id: string }): Promise<StoredEvent[]>;
  /**
   * Asks a query on the listener's connection, like `Kernable.query`: `type` is of the
   * listener's domain (`GetOrder`) or qualified (`crm.GetCustomer`). The tenant defaults to the
   * event's. It is the listener's own read: it keeps the client's context, not the event's sender.
   */
  query<T = unknown>(type: string, data?: unknown, options?: Omit<QueryOptions, "domain" | "type" | "data">): Promise<T>;
  /** The same with a query class instance, typed by its answer: `ctx.query(new GetOrder({ id }))`. */
  query<Q extends Query<any>>(query: Q, options?: Omit<QueryOptions, "domain" | "type" | "data">): Promise<AnswerOf<Q>>;
}

/** Where a command class instance goes: `SubmitOptions` without what the class says. */
export type CommandTarget = Omit<SubmitOptions, "domain" | "aggregateType" | "type" | "data">;

/** A command a listener's handler submits (`ListenerContext.submit`): `SubmitOptions` with a required `id`, and the domain optional. */
export type FollowUp = Omit<SubmitOptions, "domain" | "id"> & { domain?: string; id: string };

/**
 * What `apply` gets about an event besides its data. Only fields known before the kernel stores
 * the event, so a snapshot and a replay compute the same state; put a time into the data
 * (`ctx.now()`) if the state needs one.
 */
export interface EventMeta {
  /** The event type within its domain: `ItemAdded`. */
  type: string;
  /** The aggregate's version after this event: 1 for its first event. */
  version: number;
  tenant: string;
  /** Always the domain of the command that caused the event. */
  domain: string;
  /** The aggregate type, unqualified: `cart`. */
  aggregateType: string;
  aggregateId: string;
  /** The command that produced the event. */
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

/**
 * What a listener handler gets about an event besides its data and request context (its second
 * argument, `evt`): the `EventMeta` fields plus where the event is in the log.
 */
export interface StoredEventMeta extends EventMeta {
  /** The event's global position: its place in the one order of all domains and tenants. */
  position: number;
  /** When the kernel committed the event. */
  createdAt: Date;
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
  /** The code version (`Options.deployment`) of the worker that wrote it; empty if none. */
  deployment: string;
}

/**
 * An event as stored by the kernel, with its data and request context: in a submit's `Outcome`
 * and in a subscription (`Kernable.subscribe`). Its `data` is as stored, not upcast; vault fields
 * are `null` here and listed in `redacted`.
 */
export interface StoredEvent extends StoredEventMeta {
  /** The payload as stored. */
  data: unknown;
  /** The request context of the command that caused the event. */
  request: RequestContext;
}

/**
 * What came of a command (`Kernable.submit`): committed with the events it stored, or rejected
 * with a code and message (by its handler, a middleware or the kernel, e.g. `version_mismatch`).
 * The kernel's codes for unique keys and requirements (see `Aggregate.unique`):
 * `unique_taken` (another aggregate holds one of the keys the command would leave its aggregate
 * with: `orders.Order key number is taken`, never the value), `requirement_missing` (an aggregate
 * the command requires does not hold the fact: `orders.Vehicle v1 does not hold open`) and
 * `invalid_index` (too many keys, facts or requirements, or a malformed name: a bug in the
 * aggregate's definition). With deployments (`Options.deployment`), `unknown_history`: the
 * aggregate holds an event the worker's deployment does not know (a newer schema, or a type
 * a newer deployment wrote), e.g. after a rollback; the worker never sees the command. A rejection is a result, not an exception; check `committed`:
 *
 * ```ts
 * const outcome = await kernable.submit({ domain: "shop", aggregateType: "cart", aggregateId: "c1", type: "CheckOut" });
 * if (!outcome.committed) console.log(outcome.code, outcome.message);
 * ```
 *
 * For `unique_taken`, `key` names the key (`number`); for `requirement_missing`, `requirement` is
 * the first requirement that does not hold. Both are set by the kernel only, and stored with the
 * rejection.
 */
export type Outcome =
  | { committed: true; events: StoredEvent[] }
  | { committed: false; code: string; message: string; key?: string; requirement?: Requirement };

/** The outcome of a rejected command, with the key or requirement the kernel named. */
let issuedLast = 0;

/** A request's `issued_at`: now, but later than every one this process issued before. Two calls in the same
 * millisecond would otherwise carry the same signature, and the kernel refuses the second as a replay. */
function issuedNow(): number {
  issuedLast = Math.max(Date.now(), issuedLast + 1);
  return issuedLast;
}

function rejectedOutcome(r: WireRejected): Outcome {
  const outcome: Extract<Outcome, { committed: false }> = { committed: false, code: r.code, message: r.message };
  if (r.key) outcome.key = r.key;
  const q = r.requirement;
  if (q) {
    // Named by a key: its name only, never the value.
    const named = q.key ? { key: q.key } : { aggregateId: q.aggregate_id };
    outcome.requirement = { domain: q.domain, aggregateType: q.aggregate_type, ...named, fact: q.fact, ...(q.absent ? { absent: true } : {}) };
  }
  return outcome;
}

/** What came of a query: its response data or why it was rejected. */
export type QueryOutcome =
  | { answered: true; data: unknown }
  | { answered: false; code: string; message: string };

/** An event to fold: type, payload version and data, plus as much of `EventMeta` and its request context as known. */
export type FoldEvent = Partial<EventMeta> & { type: string; schema: number; data: unknown; request?: RequestContext };

type Apply<S> = (state: S, data: any, evt: EventMeta, ctx: ApplyContext) => S | void;
type Handle<S> = (state: S, data: any, cmd: CommandMeta, ctx: CommandContext) => NewEvent[] | Promise<NewEvent[]>;
/** What a command handler returns: `NewEvent`s or event class instances. */
export type Produced = (NewEvent | Event)[];

/** The event and command schemas of a domain, as a class binding carries them. */
function schemasOf(domain: string, events: Map<string, EventSchema>, commands: Map<string, CommandSchema>) {
  return { name: domain, events, commands };
}

/** Declares an event class in a domain (its statics, unless declared by name already); its type. */
function declareEvent(domain: string, events: Map<string, EventSchema>, cls: EventClass<unknown>): string {
  const type = messageType(cls);
  bind(cls, { domain, schemas: schemasOf(domain, events, new Map()) });
  if (!events.has(type)) {
    const statics = cls as MessageStatics;
    const def = new EventSchema(type, statics.version ?? 1);
    for (const [from, fn] of Object.entries(statics.upcasts ?? {})) def.upcast(Number(from), fn);
    for (const p of statics.personal ?? []) def.personal([...p.fields], p.subject);
    if (statics.erases) def.erases(statics.erases);
    events.set(type, def);
  }
  return type;
}

/** Declares a command class of an aggregate (its vault statics, unless declared by name); its type. */
function declareCommand(domain: string, aggregateType: string, events: Map<string, EventSchema>, commands: Map<string, CommandSchema>, cls: CommandClass<unknown>): string {
  const type = messageType(cls);
  bind(cls, { domain, aggregateType, schemas: schemasOf(domain, events, commands) });
  if (!commands.has(type)) {
    const statics = cls as MessageStatics;
    const def = new CommandSchema(type);
    for (const p of statics.personal ?? []) def.personal([...p.fields], p.subject);
    if (statics.secret?.length) def.secret([...statics.secret]);
    commands.set(type, def);
  }
  return type;
}

/**
 * A fact another aggregate of the command's tenant must hold (or, `absent`, must not hold) when
 * the command commits: what `CommandOptions.requires` returns. The kernel checks it atomically
 * with the commit; if it does not hold, the command is rejected with `requirement_missing`. The
 * aggregate is named by its ID, or by a unique key of its type (`key` and `value`, not a
 * personal key), which the kernel resolves at commit: no aggregate holds the key, none holds the
 * fact. An aggregate without events holds no fact either.
 *
 * ```ts
 * { aggregateType: "Vehicle", aggregateId: data.vehicleId, fact: "open" }
 * { domain: "funds", aggregateType: "Vehicle", key: "isin", value: normalize(data.vehicle), fact: "open" }
 * { aggregateType: "Vehicle", aggregateId: data.vehicleId, fact: "woundUp", absent: true }
 * ```
 */
export interface Requirement {
  /** The domain of the aggregate: `funds`; default: the command's own. */
  domain?: string;
  /** The aggregate type within the domain, unqualified: `Vehicle`. */
  aggregateType: string;
  /** The aggregate's ID; omitted when it is named by `key`. */
  aggregateId?: string;
  /** A unique key of the aggregate type that names the aggregate instead of its ID. */
  key?: string;
  /**
   * The key's value, normalised as the aggregate type's key function does: the kernel gets only
   * its SHA-256. Absent in a rejection, which never carries the value.
   */
  value?: string;
  /** The fact it must hold, as declared with `Aggregate.fact`: `open`. */
  fact: string;
  /** The aggregate must not hold the fact. */
  absent?: boolean;
}

/** Options of one command handler: the third argument of `Aggregate.command`. */
export interface CommandOptions<S, D = any> {
  /**
   * The facts other aggregates of the command's tenant must hold for the command to commit,
   * from the command's data and the state before it: `(data, state) => Requirement[]`. Checked
   * by the kernel at commit, so no command in between can remove a fact unseen; a missing one
   * rejects the command with `requirement_missing`. If it throws, the command is rejected as for
   * a handler error.
   */
  requires?: (data: D, state: S) => Requirement[];
}

/** Options of a unique key: the third argument of `Aggregate.unique`. */
export interface UniqueOptions<S> {
  /**
   * `tenant` (default): no two aggregates of this type in the same tenant hold the same value;
   * `global`: none in any tenant. A global key tells a worker of any tenant whether a value is
   * taken.
   */
  scope?: "tenant" | "global";
  /**
   * Makes the key personal: the person (subject) its values belong to, from the state, e.g.
   * `(state) => state.customerId`. The kernel keeps the key in the vault, and erasing the
   * subject frees it. An empty or missing subject: the aggregate holds no value of the key.
   */
  personal?: (state: S) => string | null | undefined;
}

/** A unique key an aggregate holds, as `Aggregate.index` returns it. */
export interface HeldKey {
  /** The key's name, as declared with `Aggregate.unique`. */
  name: string;
  /** The value, as the key's function returned it. The kernel gets only its SHA-256 hash. */
  value: string;
  scope: "tenant" | "global";
  /** For a personal key: the person the value belongs to. */
  subject?: string;
}

/** The unique keys and facts an aggregate holds in a state: what `Aggregate.index` returns. */
export interface Index {
  /** In the order of declaration, the values of a key in the order its function returned them. */
  keys: HeldKey[];
  /** The facts that are true, in the order of declaration. */
  facts: string[];
}

/** A unique key as declared with `Aggregate.unique`. */
interface KeyDefinition<S> {
  values: (state: S) => string | string[] | null | undefined;
  scope: "tenant" | "global";
  personal?: (state: S) => string | null | undefined;
}

/** What the built-in reindex commands start with; reserved for them. */
const REINDEX = "_reindex_";

/**
 * The command an aggregate type with unique keys or facts runs itself, one per aggregate type
 * because command types are unique within a domain: `_reindex_Customer`. See `Kernable.reindex`.
 */
export function reindexType(aggregateType: string): string {
  return `${REINDEX}${aggregateType}`;
}

/**
 * Snapshot settings of an aggregate: the third argument of `Domain.aggregate`. Snapshots are a
 * cache of the state; without one, the worker folds all events of the aggregate.
 */
export interface AggregateOptions {
  /** Send a snapshot of the state every this many events (default 100; 0 turns them off). */
  snapshotEvery?: number;
  /**
   * Version of the state format (default "1"). Change it when `apply` produces differently
   * shaped state; older snapshots are then ignored and the state is rebuilt from the events.
   */
  schema?: string;
}

/**
 * A field kept in the vault (`EventSchema.personal`), as a listener reads it: `null` after its
 * subject was erased, or for a reader whose key is not granted `vault_read`. Type such fields
 * with it, so handlers deal with the `null`: `data: { email: Personal<string> }`.
 */
export type Personal<T> = T | null;

/** The name of the listener whose handler is running, for requests sent from it. */
const handling = new AsyncLocalStorage<string>();

type Upcast = (data: any) => unknown;

/**
 * The payload format of one event type: its current version, the steps that bring older
 * payloads up to it, and which fields go to the vault. Create it with `Domain.event`; its
 * methods return it, so they chain. Events are stored as written and never changed; readers
 * upcast them. See [Data over time](../concepts/data).
 *
 * ```ts
 * shopDomain.event("ItemAdded", 2).upcast(1, (d) => ({ sku: d.sku, count: d.qty }));
 * identityDomain.event("AccountRegistered", 1).personal(["name", "email"], (_d, id) => id);
 * identityDomain.event("AccountErased", 1).erases((_d, id) => id);
 * accounts.on("AccountRegistered", (data: { name: Personal<string>; email: Personal<string> }) => { … });
 * ```
 */
export class EventSchema {
  /** The event type within its domain, unqualified. */
  readonly type: string;
  /** The current payload version (from 1): what workers write and readers get. */
  readonly schema: number;
  /** The upcasts by the version they start from. */
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

  /**
   * Turns a payload of version `from` into one of version `from + 1`; `fn` gets the old payload
   * and returns the new one. Steps chain: version 1 reaches 3 through `upcast(1, …)` and
   * `upcast(2, …)`. Throws unless `from` is between 1 and `schema - 1`. A reader that lacks a
   * step stops at such an event instead of misreading it.
   */
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

/**
 * How a command's payload is kept: which fields go to the vault instead of the signed log.
 * Create it with `Domain.command`; its methods return it, so they chain. The submitting client
 * seals the fields, so it needs the domain: `kernable.use(domain)`.
 *
 * ```ts
 * identityDomain.command("Register").personal(["name", "email"], (_d, id) => id).secret(["password"]);
 * ```
 */
export class CommandSchema {
  /** The command type within its domain, unqualified. */
  readonly type: string;
  /** The fields kept in the vault; see `personal` and `secret`. */
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
 * What a domain declares about a query type given by name (`domain.query("GetOrder")`): the
 * aggregate it is about. Query classes declare it as `static target` instead.
 *
 * ```ts
 * shopDomain.query("GetOrder").target("order", "id"); // GetOrder { id: "o-17" } is about order o-17
 * ```
 */
export class QuerySchema {
  /** The query type within its domain, unqualified. */
  readonly type: string;
  /** The aggregate the query is about; see `target`. */
  targetOf?: QueryTarget;

  constructor(type: string) {
    if (!/^[^.*]+$/.test(type)) throw new Error(`query types are unqualified within their domain, not ${JSON.stringify(type)}`);
    this.type = type;
  }

  /**
   * The query is about one aggregate of `aggregateType` (`order`, or qualified `shop.order`),
   * whose ID is in the data's `field`. The SDK sends it with the query, signed, and a middleware
   * decides on it as on a command's aggregate. Clients that ask it need the domain:
   * `kernable.use(domain)`.
   */
  target(aggregateType: string, field: string): this {
    this.targetOf = { aggregateType, field };
    return this;
  }
}

/** The wire target of a query: the declared aggregate type, qualified, and the ID from `data`. */
function queryTarget(def: QueryTarget | undefined, domain: string, data: unknown): { aggregate_type: string; aggregate_id: string } | undefined {
  if (!def) return undefined;
  const id = data !== null && typeof data === "object" ? (data as Record<string, unknown>)[def.field] : undefined;
  if (typeof id !== "string" || !id) return undefined; // no ID: no target, so a rule that needs one refuses
  return { aggregate_type: def.aggregateType.includes(".") ? def.aggregateType : `${domain}.${def.aggregateType}`, aggregate_id: id };
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

/** `deployment` if empty or valid (1 to 128 letters, digits, `-`, `_` and `.`); throws otherwise. */
function validDeployment(deployment: string): string {
  if (deployment && !/^[A-Za-z0-9_.-]{1,128}$/.test(deployment)) {
    throw new Error(`a deployment has 1 to 128 letters, digits, \`-\`, \`_\` and \`.\`, not ${JSON.stringify(deployment)}`);
  }
  return deployment;
}

/** An event a process cannot read: its schema is newer than known, or a step is missing. */
class SchemaError extends Error {}

/**
 * The newest schema of a `type` event this code knows (see `Domain.event`), if `schema` is
 * newer: a newer deployment wrote it. Undefined if the code knows it.
 */
function newerThanKnown(events: Map<string, EventSchema> | undefined, type: string, schema: number): number | undefined {
  const known = events?.get(type)?.schema ?? 1;
  return schema > known ? known : undefined;
}

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
 * are named `domain.Type`. Create it with `domain(name)`.
 *
 * Everything is defined on it: aggregates (`aggregate`), read models (`readModel`), reactors
 * (`reactor`), business processes (`process`), event versions and vault fields (`event`,
 * `command`). `Kernable.app` runs all of it and registers its processes; `Kernable.worker` and
 * `Kernable.listener` run parts. See [Domains](../concepts/domains).
 *
 * ```ts
 * const shopDomain = domain("shop");
 * const cart = shopDomain.aggregate("cart", { items: 0 });
 * const carts = shopDomain.readModel("carts");
 * const mailer = shopDomain.reactor("mailer");
 * ```
 */
export class Domain {
  /** The domain's name: `shop`. */
  readonly name: string;
  /** Payload versions and upcasts per event type; types not listed are at version 1. */
  readonly events = new Map<string, EventSchema>();
  /** Vault fields per command type; see `command`. */
  readonly commands = new Map<string, CommandSchema>();
  /** What query types given by name are about; see `query`. */
  readonly queries = new Map<string, QuerySchema>();
  /** The aggregates defined on it, which `Kernable.app` runs. */
  readonly aggregates: Aggregate<any>[] = [];
  /** The read models and reactors defined on it, which `Kernable.app` runs. */
  readonly listeners: Listener[] = [];
  /** The business processes defined on it, which `Kernable.app` registers. */
  readonly processes: ProcessBuilder[] = [];

  constructor(name: string) {
    if (!/^[A-Za-z][A-Za-z0-9_-]*$/.test(name)) throw new Error(`invalid domain name ${JSON.stringify(name)}`);
    this.name = name;
  }

  /**
   * Declares the payload format of `type` events (unqualified): this domain writes them at
   * version `schema` (from 1), with an `upcast` per older version, and may keep fields in the
   * vault (`personal`) or erase a subject (`erases`). Returns the `EventSchema` to chain on.
   * Types not declared are at version 1. The domain's aggregates and listeners use it;
   * listeners of other domains get it with `use(domain)`. Throws for a qualified type or a
   * version below 1.
   */
  event(type: string, schema: number): EventSchema;
  /**
   * Declares event classes of this domain (their statics: `version`, `upcasts`, `personal`,
   * `erases`), e.g. in a process that only reads them. An aggregate's `apply` declares them too.
   */
  event(...classes: EventClass<unknown>[]): void;
  event(type: string | EventClass<unknown>, ...rest: (number | EventClass<unknown>)[]): EventSchema | void {
    if (typeof type !== "string") {
      for (const cls of [type, ...rest] as EventClass<unknown>[]) declareEvent(this.name, this.events, cls);
      return;
    }
    const def = new EventSchema(type, rest[0] as number);
    this.events.set(type, def);
    return def;
  }

  /**
   * Declares which fields of `type` commands (unqualified) the vault keeps: returns the
   * `CommandSchema` to chain `personal` and `secret` on. Only needed for commands with such
   * fields. Clients that submit them need the domain: `kernable.use(domain)`.
   */
  command(type: string): CommandSchema {
    const def = new CommandSchema(type);
    this.commands.set(type, def);
    return def;
  }

  /**
   * Declares what `type` queries (unqualified) are about: returns the `QuerySchema` to chain
   * `target` on. Query classes declare it as `static target` instead. Clients that ask such
   * queries by name need the domain: `kernable.use(domain)`.
   */
  query(type: string): QuerySchema {
    const def = new QuerySchema(type);
    this.queries.set(type, def);
    return def;
  }

  /**
   * Defines an aggregate type `name` (`cart`) of this domain, with the state `initial` every
   * aggregate of it starts from: `shopDomain.aggregate("cart", { items: 0 })`. Add its event
   * appliers and command handlers with `apply` and `command`; run it with `kernable.worker(cart)`
   * or `kernable.app`. `options`: snapshots, see `AggregateOptions`. Throws if the domain has an
   * aggregate `name` already.
   */
  aggregate<S>(name: string, initial: S, options?: AggregateOptions): Aggregate<S> {
    if (this.aggregates.some((a) => a.name === name)) throw new Error(`${this.name} has an aggregate ${name} already`);
    const aggregate = new Aggregate(this.name, name, initial, options, this.events, this.commands);
    this.aggregates.push(aggregate);
    return aggregate;
  }

  /**
   * Defines a read model named `name`: a listener that builds a view from events (`on`) and
   * answers queries from it (`query`). Without a store it is kept in memory and rebuilt from
   * the first event on every start (defaults: order 0, start 0); with `options.store` it
   * continues where it left off. `options` override the defaults, see `ListenerOptions`. Run it
   * with `kernable.listener(definition)` or `kernable.app`. Throws if the domain has a listener
   * `name` already.
   */
  readModel(name: string, options: ListenerOptions = {}): Listener {
    return this.listen(new Listener(this, name, { order: 0, start: 0, ...options }));
  }

  /**
   * Defines a reactor named `name`: a listener that triggers side effects and follow-up
   * commands. It starts at the newest event when it is new, then resumes after the last
   * acknowledged event (defaults: order 10, no start), so it never reacts to what happened before
   * it existed. Submit follow-ups with `ctx.submit` and a fixed command `id`, so a redelivered
   * event is harmless; a rejected one parks the event (see `ListenerOptions.parkAfter`). A settling submit from its handler waits for the listeners before it (the
   * read models), not for the reactor itself, which handles one event at a time. Run it with
   * `kernable.listener(definition)` or `kernable.app`. Throws if the domain has a listener `name`
   * already.
   */
  reactor(name: string, options: ListenerOptions = {}): Listener {
    return this.listen(new Listener(this, name, { order: 10, parkAfter: PARK_AFTER, single: true, ...options }));
  }

  /**
   * Defines a business process `name` (`fulfilment`, qualified `shop.fulfilment`): returns the
   * `ProcessBuilder` to chain its start and steps on. The kernel runs it once registered:
   * `Kernable.app` registers the processes of its domains when its workers are connected, and
   * lists them in its membership; `Kernable.registerProcess` registers one alone. Throws if the
   * domain has a process `name` already, or for a name other than letters, digits, `_` and `-`.
   */
  process(name: string): ProcessBuilder {
    if (this.processes.some((p) => p.name === name)) throw new Error(`${this.name} has a process ${name} already`);
    const process = new ProcessBuilder(this.name, name);
    this.processes.push(process);
    return process;
  }

  /** Adds `listener`, unless the domain has one of its name: a read model or a reactor. */
  private listen(listener: Listener): Listener {
    if (this.listeners.some((l) => l.name === listener.name)) throw new Error(`${this.name} has a listener ${listener.name} already`);
    this.listeners.push(listener);
    return listener;
  }
}

/**
 * Creates a domain named `name`: `const shopDomain = domain("shop")`. A name starts with a
 * letter, followed by letters, digits, `_` or `-`; anything else throws. Processes that share a
 * domain (a worker and a read model in another process) each create it with the same name and
 * the same `event` and `command` declarations.
 */
export function domain(name: string): Domain {
  return new Domain(name);
}

/**
 * An aggregate: how events change its state (`apply`) and which events a command produces
 * (`command`). Handlers must be deterministic and free of side effects; they may run again.
 * The state must survive a JSON round trip, because snapshots store it as JSON. An aggregate
 * belongs to a domain and produces events only of that domain; create it with
 * `domain(...).aggregate(...)`. `apply` and `command` return the aggregate, so they chain.
 *
 * A worker runs a command like this: it folds the aggregate's events (from the latest snapshot,
 * if any) into the state, calls the command's handler, and sends the events it returns. The
 * kernel commits them only on the version the state was folded from; if another command came
 * first, the command runs again on the new state.
 *
 * An aggregate may hold unique keys (`unique`: no other aggregate holds the same value) and facts
 * (`fact`), both computed from its state; a command may require facts of other aggregates
 * (`CommandOptions.requires`). The kernel checks both atomically with the commit, so neither a
 * read model nor a registry of claims is needed to keep a value unique or a condition true.
 *
 * ```ts
 * const cart = shopDomain.aggregate("cart", { items: 0, checkedOut: false });
 * cart.apply("ItemAdded", (state, data: { qty: number }) => { state.items += data.qty; });
 * cart.command("AddItem", (state, data: { sku: string; qty: number }) => {
 *   if (state.checkedOut) throw new Rejected("Cart is checked out");
 *   return [{ type: "ItemAdded", data }];
 * });
 * ```
 */
export class Aggregate<S> {
  /** The name of the domain it belongs to. */
  readonly domain: string;
  /** The aggregate type, unqualified: `cart`. */
  readonly name: string;
  /** The state every aggregate of this type starts from. */
  readonly initial: S;
  /** See `AggregateOptions.snapshotEvery`. */
  readonly snapshotEvery: number;
  /** The state format's version; see `AggregateOptions.schema`. */
  readonly schema: string;
  /** The `apply` functions per event type. */
  readonly appliers = new Map<string, Apply<S>>();
  /** The command handlers per command type. */
  readonly handlers = new Map<string, Handle<S>>();
  /** The domain's event schemas. */
  readonly events: Map<string, EventSchema>;
  /** The domain's command schemas: which fields of a command are personal. */
  readonly commands: Map<string, CommandSchema>;
  /** The unique keys by name; see `unique`. */
  readonly keys = new Map<string, KeyDefinition<S>>();
  /** The facts by name; see `fact`. */
  readonly facts = new Map<string, (state: S) => boolean>();
  /** What the commands require, per command type; see `CommandOptions.requires`. */
  readonly requirements = new Map<string, (data: any, state: S) => Requirement[]>();

  constructor(
    domain: string,
    name: string,
    initial: S,
    options: AggregateOptions = {},
    events = new Map<string, EventSchema>(),
    commands = new Map<string, CommandSchema>(),
  ) {
    this.domain = domain;
    this.name = name;
    this.events = events;
    this.commands = commands;
    this.initial = initial;
    this.snapshotEvery = options.snapshotEvery ?? 100;
    this.schema = options.schema ?? "1";
  }

  /**
   * How an event of `eventType` (unqualified, of this domain) changes the state:
   * `fn(state, data, evt, ctx)` changes `state` in place and returns nothing, or returns a new
   * state that replaces it. `state` is a copy, so changing it never touches the state it came
   * from. `data` is upcast to the version declared with `Domain.event`. Events without an `apply`
   * leave the state as it is. Must be deterministic: no clock, no random values, no I/O.
   */
  apply<D = any>(eventType: string, fn: (state: S, data: D, evt: EventMeta, ctx: ApplyContext) => S | void): this;
  /** The same with an event class: `apply(ItemAdded, (state, data) => …)`, `data` typed by its fields. */
  apply<E>(event: EventClass<E>, fn: (state: S, data: Fields<E>, evt: EventMeta, ctx: ApplyContext) => S | void): this;
  apply(event: string | EventClass<unknown>, fn: Apply<S>): this {
    const type = typeof event === "string" ? event : declareEvent(this.domain, this.events, event);
    this.appliers.set(type, fn);
    return this;
  }

  /**
   * Which events a `commandType` command (unqualified, of this domain) produces:
   * `fn(state, data, cmd, ctx)` gets the current state and the command's data and returns the
   * events (`NewEvent[]`, possibly empty), or a promise of them. Throw `Rejected` to reject the
   * command. It may run more than once for the same command, so it must decide the same way
   * every time: use `ctx.now()` and `ctx.id()` instead of the clock and random IDs, and leave
   * side effects to reactors. `options.requires`: facts other aggregates must hold for the
   * command to commit, see `CommandOptions`. Throws for a type starting with `_reindex_`: the
   * built-in commands aggregates with unique keys or facts run themselves (`_reindex_Customer`,
   * see `Kernable.reindex`).
   */
  command<D = any>(
    commandType: string,
    fn: (state: S, data: D, cmd: CommandMeta, ctx: CommandContext) => Produced | Promise<Produced>,
    options?: CommandOptions<S, D>,
  ): this;
  /**
   * The same with a command class: `command(AddItem, (state, data) => [new ItemAdded({ … })])`,
   * `data` typed by its fields; its statics declare its vault fields.
   */
  command<C>(
    command: CommandClass<C>,
    fn: (state: S, data: Fields<C>, cmd: CommandMeta, ctx: CommandContext) => Produced | Promise<Produced>,
    options?: CommandOptions<S, Fields<C>>,
  ): this;
  command(
    command: string | CommandClass<unknown>,
    fn: (state: S, data: any, cmd: CommandMeta, ctx: CommandContext) => Produced | Promise<Produced>,
    options: CommandOptions<S, any> = {},
  ): this {
    const commandType = typeof command === "string" ? command : declareCommand(this.domain, this.name, this.events, this.commands, command);
    if (commandType.startsWith(REINDEX)) {
      throw new Error(`${commandType}: command types starting with ${REINDEX} are reserved for the reindex commands aggregates with unique keys or facts run themselves`);
    }
    // Event class instances become `NewEvent`s, declaring their classes.
    const plain = (events: Produced) => events.map((e) => (e instanceof Event ? this.newEvent(e) : e));
    this.handlers.set(commandType, (state, data, cmd, ctx) => {
      const produced = fn(state, data, cmd, ctx);
      return produced instanceof Promise ? produced.then(plain) : plain(produced);
    });
    if (options.requires) this.requirements.set(commandType, options.requires);
    else this.requirements.delete(commandType);
    return this;
  }

  /**
   * Declares a unique key `name`: `fn(state)` returns the value (or values) this aggregate holds,
   * or `null`/`undefined` for none, and no other aggregate of this type holds the same value
   * within the tenant (`options.scope`, default `tenant`) or in any tenant (`global`). After
   * every command the aggregate holds exactly the values its new state yields: a changed value
   * frees the old one in the same commit. A command that would leave the aggregate with a value
   * another one holds is rejected with `unique_taken`, and nothing is written. Normalise the
   * value yourself (`email.trim().toLowerCase()`); the kernel gets only its SHA-256 hash.
   * `options.personal` keeps the key in the vault, freed by erasing its subject.
   *
   * Aggregates that existed before the key was declared hold it from their next command on; run
   * `Kernable.reindex` to give it to them at once. Two of them with the same value: the second to
   * run a command is rejected with `unique_taken` until its value changes. `fn` must be
   * deterministic, as `apply` must; if it throws, the command is rejected as for a handler error.
   * Throws for a name declared already, an empty name, or one with a dot or over 128 bytes.
   *
   * ```ts
   * customer.unique("email", (state) => state.email?.toLowerCase(), { personal: (state) => state.id });
   * ```
   */
  unique(name: string, fn: (state: S) => string | string[] | null | undefined, options: UniqueOptions<S> = {}): this {
    this.named("key", name, this.keys);
    this.keys.set(name, { values: fn, scope: options.scope ?? "tenant", personal: options.personal });
    return this;
  }

  /**
   * Declares a fact `name` this aggregate holds while `fn(state)` is true: what other aggregates'
   * commands require (`CommandOptions.requires`). The kernel keeps the facts each aggregate
   * holds after its last command. `fn` must be deterministic, as `apply` must. Throws for a name
   * declared already, an empty name, or one with a dot or over 128 bytes.
   *
   * ```ts
   * vehicle.fact("open", (state) => state.status === "open");
   * ```
   */
  fact(name: string, fn: (state: S) => boolean): this {
    this.named("fact", name, this.facts);
    this.facts.set(name, fn);
    return this;
  }

  /** Checks a key's or fact's name, and adds the built-in reindex command (`_reindex_<name>`). */
  private named(what: "key" | "fact", name: string, declared: Map<string, unknown>): void {
    if (!name || name.includes(".") || Buffer.byteLength(name) > 128) {
      throw new Error(`a ${what} name is non-empty, without dots and at most 128 bytes, not ${JSON.stringify(name)}`);
    }
    if (declared.has(name)) throw new Error(`${this.name} has a ${what} ${name} already`);
    this.handlers.set(reindexType(this.name), () => []);
  }

  /**
   * The unique keys and facts this aggregate holds in `state`, as a worker computes them after a
   * command; no kernel needed, so it suits tests next to `handle`:
   * `cart.index(cart.fold(history))`. A key's values come once each; a personal key without a
   * subject holds none. Throws what a key's or fact's function throws, or if a key's function
   * returns something other than a string, a list of strings, `null` or `undefined`.
   */
  index(state: S): Index {
    const keys: HeldKey[] = [];
    for (const [name, def] of this.keys) {
      const returned = def.values(state);
      if (returned === null || returned === undefined) continue;
      const values = Array.isArray(returned) ? returned : [returned];
      if (!values.every((v) => typeof v === "string")) {
        throw new TypeError(`key ${name} of ${this.name}: return a string, a list of strings, null or undefined`);
      }
      const subject = def.personal ? def.personal(state) : undefined;
      if (def.personal && !subject) continue;
      for (const value of new Set(values)) keys.push({ name, value, scope: def.scope, ...(subject ? { subject } : {}) });
    }
    const facts = [...this.facts].filter(([, fn]) => fn(state)).map(([name]) => name);
    return { keys, facts };
  }

  /**
   * Runs the `commandType` handler on a copy of `state`, as a worker does, e.g. in a unit test,
   * and returns the events it produces: `await cart.handle(state, "AddItem", { sku: "apple", qty: 2 }, new CommandContext("c-1"))`.
   * The handler's `CommandMeta` has the command ID of `ctx`, this aggregate's domain and name,
   * and the aggregate ID and tenant of `target` (default empty); `data` goes through JSON as it would on the wire.
   * Rejects with the handler's `Rejected` (or other error), or with an error if there is no
   * handler for `commandType`.
   */
  async handle(state: S, commandType: string, data: unknown, ctx: CommandContext, target: { aggregateId?: string; tenant?: string } = {}): Promise<NewEvent[]> {
    const handler = this.handlers.get(commandType);
    if (!handler) throw new Error(`no handler for ${commandType} on ${this.name}`);
    const cmd: CommandMeta = {
      commandId: ctx.commandId, type: commandType, domain: this.domain, aggregateType: this.name,
      aggregateId: target.aggregateId ?? "", tenant: target.tenant ?? "",
    };
    return handler(structuredClone(state), decode(encode(data)), cmd, ctx);
  }

  /** An event class instance as the `NewEvent` it stands for. */
  private newEvent(e: Event): NewEvent {
    return { type: declareEvent(this.domain, this.events, e.constructor as EventClass<unknown>), data: dataOf(e) };
  }

  /** The payload version this aggregate writes `type` events at. */
  schemaOf(type: string): number {
    return this.events.get(type)?.schema ?? 1;
  }

  /**
   * State after `history`, starting from `from` (default: `initial`), as a worker computes it;
   * no kernel needed, so it suits tests: `cart.fold([{ type: "ItemAdded", schema: 1, data: { qty: 2 } }])`.
   * Events are upcast first; events without an `apply` are skipped. Fields of `EventMeta` an
   * event lacks are empty (0 for `version`), as is its request context. Throws if an event's
   * schema is newer than declared or an upcast step is missing.
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


/**
 * What a listener is handling, and after how long that is too long: a query or check after 5 s
 * (the kernel's default `--query-timeout-ms` and `--middleware-timeout-ms`, after which the
 * request has failed), a batch after 30 s (every batch, query and check behind it waits).
 */
function handlingOf(m: ListenerDelivery): { what: string; ms: number } | undefined {
  if (m.batch) return { what: `the batch up to ${m.batch.up_to}`, ms: 30_000 };
  if (m.query) return { what: `the query ${m.query.query.domain}.${m.query.query.query_type}`, ms: 5_000 };
  if (m.check) return { what: `a middleware check`, ms: 5_000 };
  return undefined;
}

/** Runs `f`, logging an error if it is still running after `handling.ms`: a handler cannot be stopped from outside, but a stuck one shows. */
async function watched<T>(log: Log, what: string, handling: { what: string; ms: number } | undefined, f: () => Promise<T>): Promise<T> {
  if (!handling) return f();
  const started = Date.now();
  const hung = setTimeout(
    () => log.print("error", what, `still handling ${handling.what} after ${Math.round((Date.now() - started) / 1000)} s: everything behind it waits; is a handler stuck?`),
    handling.ms,
  );
  hung.unref();
  try {
    return await f();
  } finally {
    clearTimeout(hung);
  }
}

/** The type of what was thrown: `TypeError`, or `string` for a thrown string. */
function errorType(err: unknown): string {
  return err instanceof Error ? err.name : err === null ? "null" : typeof err;
}

/** An error as text, even one whose own conversion throws. */
function describe(err: unknown): string {
  try {
    return String(err);
  } catch {
    return "an error that cannot be shown as text";
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

/**
 * A service made of whole domains, ready to run: one worker for all their aggregates, one
 * `ListenerHandle` per read model and reactor, and their business processes, registered once
 * the worker and listeners are connected. Create it with `Kernable.app`, start it with `run()`.
 *
 * ```ts
 * const app = new Kernable({ key: "app.key" }).app({ service: "shop", version: "1.0.0", domains: [shopDomain] });
 * await app.run(); // until app.stop()
 * ```
 */
export class App {
  /**
   * Resolves once every worker and listener of the app is connected, its deployment serves the
   * service (`serving`) and its processes are registered; rejects if a process cannot be
   * registered (`run()` rejects then too).
   */
  readonly ready: Promise<void>;
  /** Resolves once every worker and listener of the app is connected; while another deployment serves, they warm up beside it. */
  readonly connected: Promise<void>;
  /**
   * Resolves once the app's membership is applied: its deployment serves the service. Right at
   * the start, unless another deployment serves; then once the app's deployment has caught up
   * and taken over (`AppOptions.switch`), or an operator switched to it.
   */
  readonly serving: Promise<void>;
  private servingNow!: () => void;
  private stopped = false;
  /** The app's processes as the kernel registered them (`version`, `new`); filled once `ready`. */
  readonly registered: ProcessVersion[] = [];
  /** The membership `run()` applies. */
  readonly manifest: Manifest;
  /** The worker of all the app's aggregates; none if its domains define no aggregate. */
  readonly worker?: Worker;
  /** One handle per read model and reactor of the app's domains, live ones included. */
  readonly listeners: ListenerHandle[];
  private readonly kernable: Kernable;
  private readonly log: Log;
  /** The names of the app's domains. */
  private readonly domains: Set<string>;
  /** The processes of the app's domains. */
  private readonly processes: ProcessBuilder[];
  /** Hands `run()`'s registration to `ready`. */
  private registering!: (done: Promise<void>) => void;

  constructor(kernable: Kernable, log: Log, domains: Domain[], manifest: Manifest, worker: Worker | undefined, listeners: ListenerHandle[]) {
    this.kernable = kernable;
    this.log = log;
    this.domains = new Set(domains.map((d) => d.name));
    this.processes = domains.flatMap((d) => d.processes);
    this.manifest = manifest;
    this.worker = worker;
    this.listeners = listeners;
    this.connected = Promise.all([worker?.ready, ...listeners.map((l) => l.ready)]).then(() => {});
    this.serving = new Promise<void>((resolve) => (this.servingNow = resolve));
    this.ready = new Promise<Promise<void>>((resolve) => (this.registering = resolve)).then((done) => done);
    // Whoever awaits `ready` sees a failed registration; `run()` reports it anyway.
    this.ready.catch(() => {});
  }

  /**
   * Applies the membership (see `Kernable.apply`; the key needs `manage` for the service), then
   * runs the worker and the listeners until `stop()`, reconnecting when a connection drops.
   * `options` go to the worker. If another deployment serves the service, the kernel switches
   * to this one only as `AppOptions.switch` says: until then the app runs beside it (workers
   * idle, read models building) and applies its membership again every second. Once all are
   * connected and it serves, it registers the domains' processes (see
   * `Kernable.registerProcess`; the key needs `process` for each, and `submit` for their
   * commands) and prints an overview of the app on stderr (unless `KERNABLE_LOG` is below
   * `info`). Rejects if the kernel refuses the membership for another reason than the switch,
   * or if a process cannot be registered (an invalid model, a missing grant): it stops the app
   * then, so the deploy fails.
   */
  async run(options: WorkerOptions = {}): Promise<void> {
    const started = Date.now();
    this.stopped = false;
    let applied = true;
    try {
      await this.kernable.apply(this.manifest);
    } catch (err) {
      if (!waitsForSwitch(err)) throw err;
      applied = false;
      this.log.print("info", "app", `deployment ${this.manifest.deployment} runs beside the one that serves (${(err as { details: string }).details})`);
    }
    if (this.worker) this.worker.announce = false;
    for (const l of this.listeners) l.announce = false;
    const running = Promise.all([this.worker?.run(options), ...this.listeners.map((l) => l.run())]);
    const switched = applied ? Promise.resolve(true) : this.switchLater();
    switched.then((yes) => yes && this.servingNow());
    // Once all are connected and it serves; not at all if the app stops before.
    const go = Promise.all([this.connected, switched]).then(([, yes]) => yes);
    const registered = Promise.race([go, running.then(() => false)]).then((yes) => (yes ? this.register() : undefined));
    this.registering(this.connected.then(() => registered));
    registered.then(
      () => this.overview(Date.now() - started),
      () => this.stop(),
    );
    await Promise.all([running, registered]);
  }

  /** Applies the membership every second until the kernel takes it; false if the app stops first. */
  private async switchLater(): Promise<boolean> {
    for (;;) {
      await new Promise((r) => setTimeout(r, 1_000));
      if (this.stopped) return false;
      try {
        await this.kernable.apply(this.manifest);
        this.log.print("info", "app", `deployment ${this.manifest.deployment} serves ${this.manifest.service} now`);
        return true;
      } catch (err) {
        if (!waitsForSwitch(err)) this.log.print("warn", "app", `applying the membership failed: ${(err as Error).message}`);
      }
    }
  }

  /** Registers every process of the app's domains, in order. */
  private async register(): Promise<void> {
    this.registered.length = 0;
    for (const p of this.processes) this.registered.push(await this.kernable.registerProcess(p));
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
    if (m.processes?.length) listed.push(`${m.processes.length} process${m.processes.length === 1 ? "" : "es"}`);
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
    for (const r of this.registered) {
      const [domain, name] = [r.name.slice(0, r.name.indexOf(".")), r.name.slice(r.name.indexOf(".") + 1)];
      rows.push(["process", short(domain, name), [`version ${r.version}${r.new ? " (new)" : ""}`]]);
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

  /** Stops the worker and every listener of the app, so `run()` resolves. */
  stop(): void {
    this.stopped = true;
    this.worker?.stop();
    for (const l of this.listeners) l.stop();
  }
}

/**
 * What a service consists of, for `Kernable.apply`: its membership, which tells the kernel what
 * to wait for. What it lists counts (also before it ever connected); the service's other workers
 * and listeners are dynamic: they run, but nobody waits for them. See
 * [Who counts](../concepts/ordering#who-counts) and `Membership` in kernable.proto.
 *
 * ```ts
 * await kernable.apply({ service: "shop", workers: [cart], listeners: [carts], middlewares: [policy] });
 * ```
 */
export interface Manifest {
  /** Default: this process's service (`Options.service`). */
  service?: string;
  /** The aggregates the service runs; the kernel waits for a worker of each. */
  workers?: Aggregate<any>[];
  /** The listeners the kernel waits for, by name; the service's others are dynamic. */
  listeners?: Listener[];
  /**
   * The service's listeners that may check requests (`Listener.middleware`); they count like
   * listeners. Only middlewares listed here check requests.
   */
  middlewares?: Listener[];
  /** Free metadata, e.g. `{ version: "1.0.0" }`. */
  metadata?: Record<string, string>;
  /**
   * The service's business processes, by builder or qualified name (`shop.fulfilment`). One the
   * membership no longer lists starts no new cases; its running cases finish.
   */
  processes?: (ProcessBuilder | string)[];
  /**
   * The deployment (code version) that serves the service: its workers run the commands, its
   * listeners count, its middlewares check and its read models answer queries first. The
   * service's workers and listeners of other deployments stay connected: workers idle, read
   * models keep catching up. To roll back, apply a membership naming the earlier deployment.
   * Default: this process's (`Options.deployment`); empty: every deployment serves.
   */
  deployment?: string;
  /**
   * When a membership that names another deployment than the one serving the service switches
   * to it (see `Switch`). Default `"caught-up"`.
   */
  switch?: Switch;
}

/**
 * When a membership switches its service to its deployment while another one serves:
 * - `"caught-up"` (default): once that deployment has caught up, and only to a deployment that
 *   never served before: its listed workers connected and its read models within the kernel's
 *   `--deployment-switch-lag` of the newest event. Until then the kernel refuses it
 *   (`not_caught_up`); an app runs beside the serving deployment meanwhile and applies again
 *   every second until it switches. A deployment that served before (it wrote events) is never
 *   switched back to this way (`served_before`): its app runs beside, as with `"never"`, until
 *   an operator rolls back (`kernable apply --deployment`). So an older deployment's app that
 *   restarts during a rolling deploy does not take the service back.
 * - `"now"`: at once, caught up or not (`kernable apply --force`).
 * - `"never"`: never by itself: it applies only while its deployment serves already (or the
 *   service has no membership yet), else the kernel refuses it (`not_serving`). An app runs
 *   beside the serving deployment until an operator switches (`kernable apply --deployment`).
 */
export type Switch = "caught-up" | "now" | "never";

/**
 * Whether the kernel refused a membership because it would switch its service to a deployment
 * that has not caught up yet (`not_caught_up`), back to one that served before with
 * `switch: "caught-up"` (`served_before`), or at all with `switch: "never"` (`not_serving`):
 * applying it again may succeed later, once the deployment caught up or an operator switched
 * to it.
 */
export function waitsForSwitch(err: unknown): boolean {
  const e = err as { code?: number; details?: unknown };
  return (
    e?.code === 9 /* FAILED_PRECONDITION */ &&
    typeof e.details === "string" &&
    /^(not_caught_up|not_serving|served_before)/.test(e.details)
  );
}

/** `KERNABLE_SWITCH`, else `"caught-up"`. */
function switchFromEnv(): Switch {
  const v = process.env.KERNABLE_SWITCH ?? "";
  if (v === "" || v === "caught-up") return "caught-up";
  if (v === "now" || v === "never") return v;
  throw new Error(`KERNABLE_SWITCH=${v}: caught-up, now or never`);
}

/**
 * An app: one service made of whole domains, for `Kernable.app`. `version`, `description` and
 * `metadata` go into the membership's metadata and the app's overview.
 */
export interface AppOptions {
  /** The service's name. Default: this process's service (`Options.service`); one of them is required. */
  service?: string;
  /** The app's version, e.g. `1.0.0`. */
  version?: string;
  /** One line about the app. */
  description?: string;
  /** More metadata. */
  metadata?: Record<string, string>;
  /** Every aggregate, read model and reactor defined on them runs in this app; their processes are registered. */
  domains: Domain[];
  /**
   * When the app's deployment takes the service over from another one that serves it (see
   * `Switch`): by default once its read models have caught up, if the deployment never served
   * before; meanwhile it runs beside the serving deployment, its workers idle and its read
   * models building. One that served before (an older deployment's app restarting) runs beside
   * until an operator rolls back. Default:
   * `KERNABLE_SWITCH`, else `"caught-up"`.
   */
  switch?: Switch;
}

/**
 * How a `Kernable` client connects: kernel address, key, grant and defaults for its requests.
 * Every option has a default, most from an environment variable, so `new Kernable()` works with
 * the files `kernable start --dev` creates. The constructor reads the key, grant and TLS files:
 * a missing key file, an explicitly named grant file or a missing TLS file throws.
 */
export interface Options {
  /**
   * Kernel address: `http://host:port` (`host:port` works too) or, on Unix, `unix:/path/to.sock`.
   * For a pair of kernels with a standby, both, comma-separated (`http://a:7233,http://b:7233`):
   * the client turns to the other one when a kernel answers `UNAVAILABLE` (a standby says
   * `not_primary`) or cannot be reached. Default: `KERNABLE_URL`, else `http://127.0.0.1:7233`,
   * where `kernable start` listens.
   */
  url?: string;
  /**
   * Path to this process's `.key` file; it signs every request, worker result and listener
   * handshake. Default:
   * `KERNABLE_KEY`, else `app.key` in the current directory.
   */
  key?: string;
  /**
   * Path to the key's grant (`kernable grant`), sent with every call. Default: `KERNABLE_GRANT`,
   * else the key file's path with `.grant` for `.key` (`app.grant` for `app.key`), if it exists.
   */
  grant?: string;
  /** Default tenant for `submit`, `enqueue`, `query`, `data` and `cache`. Default: `default`. */
  tenant?: string;
  /**
   * The service this process's workers and listeners belong to. Its membership (see `apply`)
   * decides what the kernel waits for: what it lists counts, the service's other workers and
   * listeners are dynamic, and only listeners it lists as middlewares may check requests.
   * Default: `KERNABLE_SERVICE`, else none (everything registered counts).
   */
  service?: string;
  /**
   * This process's code version, e.g. a build ID: 1 to 128 letters, digits, `-`, `_` and `.`.
   * Events its workers write carry it, its read models and middlewares are kept apart from
   * those of other deployments (a new one builds its own from the first event, beside the old
   * one), and its queries go to read models of the same deployment first. The service's
   * membership (`Manifest.deployment`) decides which deployment serves. Default:
   * `KERNABLE_DEPLOYMENT`, else none. The constructor throws for an invalid one.
   */
  deployment?: string;
  /**
   * Default request context for `submit`, `enqueue`, `query` and the requests of `data` and
   * `cache`, e.g. this service's identity; a request's own `context` replaces it as a whole.
   * Default: empty.
   */
  context?: RequestContext;
  /**
   * Mutual TLS for `host:port` URLs: paths of the CA certificate and this process's certificate
   * and key. Default: `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set.
   */
  tls?: TlsFiles;
  /**
   * Allows plain text to a kernel that is not on this machine. Without TLS, grants, signed
   * requests and vault values cross the network readable, so the client refuses such a URL
   * unless this is set. Default: `KERNABLE_INSECURE=1`, else false.
   */
  insecure?: boolean;
  /**
   * How long a request (`submit`, `query`, files, cache, `apply`) waits in all: for the
   * connection, e.g. while the kernel restarts, and for its answer. Default 60 000 ms, above
   * the kernel's own submit and settle timeouts. A submit (by its command ID), a cancel, a
   * query or a data or cache read (get, list) that a kernel answers `UNAVAILABLE` (shutting
   * down, handing over, a middleware or read model not connected) is asked again with backoff
   * within this time; any other request (a write) may have run, and fails at once.
   */
  timeoutMs?: number;
}

/**
 * A command for `Kernable.submit` and `Kernable.enqueue`: the aggregate it goes to (domain,
 * aggregate type and ID), the command type and its data.
 *
 * ```ts
 * await kernable.submit({ domain: "shop", aggregateType: "cart", aggregateId: "c1", type: "AddItem", data: { sku: "apple", qty: 2 } });
 * ```
 */
export interface SubmitOptions {
  /** The domain of the aggregate and the command type: `shop`. */
  domain: string;
  /** The aggregate type within the domain: `cart`. */
  aggregateType: string;
  /** Which aggregate: the stream the command's events go to. A new ID starts a new aggregate. */
  aggregateId: string;
  /** The command type within the domain: `AddItem`. */
  type: string;
  /** The command's data, sent as JSON; fields declared with `Domain.command` go to the vault. */
  data?: unknown;
  /** Default: the client's tenant (`Options.tenant`). */
  tenant?: string;
  /**
   * The command ID and idempotency key: submitting the same ID again returns the original
   * outcome instead of running the command twice. Default: a new UUID. Give retried commands,
   * and every command a reactor submits, a fixed ID.
   */
  id?: string;
  /**
   * Replaces the client's default context (`Options.context`); inside a listener, pass
   * `ctx.follow()`. Its `correlationId` defaults to the command ID.
   */
  context?: RequestContext;
  /**
   * When `submit` returns. `"settled"` (default): once every listener that counts has processed
   * the events, so the next query sees them; throws `NotSettled` after `--settle-timeout-ms`.
   * From a listener's handler, only the listeners with a lower order count. `"committed"`: once
   * the events are stored; read with `QueryOptions.minPosition` to see them. To return once the
   * command is only queued, use `Kernable.enqueue`. See
   * [Settling](../concepts/ordering#settling-reading-your-own-writes).
   */
  wait?: "settled" | "committed";
  /** How long to wait for the connection and the outcome; default `Options.timeoutMs`. */
  timeoutMs?: number;
  /**
   * `enqueue` only: a scheduled command, run at this time and not before (a `Date` or ms since
   * epoch). The kernel stores it at once; the aggregate's other commands pass meanwhile, and
   * middlewares check it now and again when it is due. Cancel it with `Kernable.cancel`.
   */
  at?: Date | number;
  /**
   * `enqueue` only, with `at`: if it cannot run by this time (after an outage), it is rejected
   * `expired`. Without `at`, the call throws.
   */
  until?: Date | number;
}

/**
 * Which aggregates `Kernable.reindex` reindexes: domain, aggregate type and IDs.
 *
 * ```ts
 * await kernable.reindex({ domain: "crm", aggregateType: "Customer", aggregateIds: ["k1", "k2"] });
 * ```
 */
export interface ReindexOptions {
  /** The domain of the aggregate type: `crm`. */
  domain: string;
  /** The aggregate type within the domain, unqualified: `Customer`. */
  aggregateType: string;
  /** The aggregates to reindex; each ID once. */
  aggregateIds: string[];
  /** Default: the client's tenant (`Options.tenant`). */
  tenant?: string;
  /** Replaces the client's default context (`Options.context`). */
  context?: RequestContext;
  /** How long to wait for each outcome; default `Options.timeoutMs`. */
  timeoutMs?: number;
}

const LEVELS = { off: 0, error: 1, warn: 2, info: 3, debug: 4 };

/**
 * The partitions of `clients`, read again (backing off like a reconnect) while no kernel answers;
 * `undefined` once `stopped`.
 */
async function partitionsOf(
  clients: Partitioned,
  what: string,
  log: Log,
  ms: number,
  wake: Wake,
  stopped: () => boolean,
): Promise<{ name: string; clients: Clients }[] | undefined> {
  for (let attempt = 1; ; attempt++) {
    try {
      // Briefly per attempt, as a session would fail; `stop()` ends the wait at once.
      const parts = await Promise.race([clients.all(500), wake.stopped.then(() => undefined)]);
      if (parts === undefined || stopped()) return undefined;
      return parts;
    } catch (err) {
      if (stopped()) return undefined;
      await log.reconnect(what, err, ms, attempt, wake);
      if (stopped()) return undefined;
    }
  }
}

/** A wait before reconnecting that `stop()` cuts short, so a stopped stream ends at once. */
class Wake {
  private wake?: () => void;
  private woken = false;
  private markStopped!: () => void;
  /** Resolves once `now()` is called: for waits other than `sleep` to end with it. */
  readonly stopped = new Promise<void>((resolve) => (this.markStopped = resolve));

  sleep(ms: number): Promise<void> {
    if (this.woken) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms);
      this.wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }

  now(): void {
    this.woken = true;
    this.wake?.();
    this.markStopped();
  }
}

/** gRPC codes that a retry does not change: INVALID_ARGUMENT, PERMISSION_DENIED, FAILED_PRECONDITION, UNIMPLEMENTED, UNAUTHENTICATED. */
const REFUSALS = new Set([3, 7, 9, 12, 16]);
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
  /**
   * Logs the end of a session that was not stopped and waits before reconnecting: `ms` after
   * the first failed attempt, twice as long after each further one, at most 30 s, give or take
   * a fifth, so processes do not reconnect in lockstep after a kernel restart. A refusal that
   * will not go away by itself (no grant, an invalid registration) is an error, not a warning.
   */
  async reconnect(what: string, err: unknown, ms: number, attempt: number, stopped: Wake): Promise<void> {
    // A gRPC error by its details only (`Connection dropped`, not `14 UNAVAILABLE: …`).
    const e = err as Error & { details?: string; code?: number };
    const reason = e.details || e.message;
    // A kernel of a pair that is not the primary right now may be in a moment.
    const delay = switchingRoles(reason) ? Math.min(backoff(ms, attempt), PAIR_RETRY_MS) : backoff(ms, attempt);
    const refused = e.code !== undefined && REFUSALS.has(e.code);
    this.print(refused ? "error" : "warn", what, `${refused ? "refused" : "connection lost"} (${reason}), reconnecting in ${delay / 1000} s`);
    await stopped.sleep(delay);
  }

  /** Logs that a store could not be opened, and waits before the next try. */
  async storeFailed(what: string, err: unknown, ms: number, attempt: number, stopped: Wake): Promise<void> {
    const delay = backoff(ms, attempt);
    this.print("error", what, `store: ${(err as Error).message}; trying again in ${delay / 1000} s`);
    await stopped.sleep(delay);
  }
}

/** The longest wait before trying a kernel of a pair again that was not the primary. */
const PAIR_RETRY_MS = 1000;

/** Whether `reason` says the kernel is one of a pair that is not the primary right now. */
function switchingRoles(reason: string): boolean {
  return reason.startsWith("not_primary") || reason.includes("lost its lease at the witness") || reason.includes("hands its role over");
}

/** The pause before try `attempt` (from 1): `ms`, doubled each time up to 30 s, ±20% jitter. */
function backoff(ms: number, attempt: number): number {
  return Math.round(Math.min(ms * 2 ** (attempt - 1), 30_000) * (0.8 + Math.random() * 0.4));
}

/**
 * A client of one kernel: it submits commands, asks queries, and runs workers and listeners
 * over its connection. Create one per process; the constructor reads the key, grant and TLS
 * files (see `Options`) and connects on the first call. Every request is signed with the key; the kernel checks it against the
 * key's grant (see [Trust](../concepts/trust)).
 *
 * ```ts
 * const kernable = new Kernable({ key: "app.key" }); // or new Kernable() with the defaults
 * const outcome = await kernable.submit({ domain: "shop", aggregateType: "cart", aggregateId: "c1", type: "AddItem", data: { sku: "apple", qty: 2 } });
 * const cart = await kernable.query({ domain: "shop", type: "GetCart", data: { id: "c1" } });
 * ```
 *
 * Calls the kernel refuses itself (not signed, not granted, no read model for a query, the kernel
 * unreachable) reject with the gRPC error, whose `details` say why. Business rejections come
 * differently: `submit` returns them in its `Outcome`, `query` throws `Rejected`.
 */
export class Kernable {
  /** Files of a domain kept by the kernel, e.g. invoices; see `Data`. */
  readonly data: Data;
  /** The shared cache of a domain; see `Cache`. */
  readonly cache: Cache;
  private readonly clients: Partitioned;
  private readonly key: Key;
  private readonly log: Log;
  private readonly tenant: string;
  private readonly context: RequestContext;
  private readonly domains = new Map<string, Domain>();
  private readonly service: string;
  /** This process's code version (`Options.deployment`); empty: none. */
  private readonly deployment: string;

  constructor(options: Options = {}) {
    const env = process.env;
    const key = options.key ?? env.KERNABLE_KEY ?? "app.key";
    const grant = options.grant ?? env.KERNABLE_GRANT ?? key.replace(/\.key$/, "") + ".grant";
    const explicit = options.grant !== undefined || env.KERNABLE_GRANT !== undefined;
    const url = options.url ?? env.KERNABLE_URL ?? "http://127.0.0.1:7233";
    const tls = options.tls ?? tlsFromEnv();
    if (!tls && !(options.insecure ?? env.KERNABLE_INSECURE === "1") && !local(url)) {
      throw new Error(`${url} is not on this machine: connect with TLS (Options.tls), or set Options.insecure to send in plain text`);
    }
    const granted = explicit || existsSync(grant) ? readFileSync(grant, "utf8").trim() : undefined;
    this.clients = new Partitioned(url, (u) => connect(u, tls, granted, options.timeoutMs));
    this.key = Key.fromFile(key);
    this.log = new Log(url, key, this.key);
    warnIfShared(this.log, key);
    this.tenant = options.tenant ?? "default";
    this.context = options.context ?? {};
    this.service = options.service ?? env.KERNABLE_SERVICE ?? "";
    this.deployment = validDeployment(options.deployment ?? env.KERNABLE_DEPLOYMENT ?? "");
    this.data = new Data(this.clients, this.key, this.tenant, this.context);
    this.cache = new Cache(this.clients, this.key, this.tenant, this.context);
  }

  /**
   * Seals the vault fields of these domains' commands when submitting them (see
   * `Domain.command`); without it, those fields go into the signed log in clear. Returns the
   * client: `new Kernable().use(identityDomain)`.
   */
  use(...domains: Domain[]): this {
    for (const d of domains) this.domains.set(d.name, d);
    return this;
  }

  /**
   * A worker for these aggregates: `await kernable.worker(cart).run()` executes their commands
   * until `stop()`. The kernel registers the aggregates when it connects: commands for an
   * aggregate or command type no worker registered are refused. Start as many workers as you
   * like; the kernel spreads commands over them, one at a time per aggregate. Without
   * `Options.service` or a membership, the aggregates count as soon as they are registered.
   */
  worker(...aggregates: Aggregate<any>[]): Worker {
    return new Worker(this.clients, this.key, aggregates, this.log, this.service, this.deployment);
  }

  /**
   * An app made of whole domains: `run()` applies the service's membership (its domains'
   * aggregates as workers, their read models and reactors as listeners, those that check
   * requests as middlewares, live ones not listed; and their processes), then runs all of them
   * and, once they are connected, registers the processes:
   * `await kernable.app({ service: "shop", domains: [shopDomain] }).run()`. Throws if neither
   * `options.service` nor `Options.service` names a service. One service is one app: two
   * processes with the same service would replace each other's membership. To split a service
   * over several processes, run `worker` and `listener` and `apply` the membership yourself.
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
      processes: options.domains.flatMap((d) => d.processes),
      metadata: {
        ...(options.version === undefined ? {} : { version: options.version }),
        ...(options.description === undefined ? {} : { description: options.description }),
        ...options.metadata,
      },
      deployment: this.deployment,
      switch: options.switch ?? switchFromEnv(),
    };
    const worker = aggregates.length ? new Worker(this.clients, this.key, aggregates, this.log, service, this.deployment) : undefined;
    const handles = listeners.map((l) => new ListenerHandle(this, this.clients, this.key, l, this.log, service, this.deployment));
    return new App(this, this.log, options.domains, manifest, worker, handles);
  }

  /**
   * Replaces the membership of a service (default: this process's): the workers, listeners and
   * middlewares the kernel waits for, its processes, and free metadata. Apply it once per deploy, e.g. from a
   * deploy script, not from every process: the newest apply wins, and an older one that arrives
   * late is refused. The key needs `manage` for
   * the service. A middleware the new membership no longer lists stops checking. Throws if
   * neither `manifest.service` nor `Options.service` names a service, or for an invalid
   * `manifest.deployment`. See
   * [Remove a listener](../how-to/remove-listener).
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
      issued_at: issuedNow(),
      processes: (manifest.processes ?? []).map((p) => (typeof p === "string" ? p : p.qualified)),
      deployment: validDeployment(manifest.deployment ?? this.deployment),
    };
    const flags = {
      force: manifest.switch === "now",
      no_switch: manifest.switch === "never",
      no_rollback: (manifest.switch ?? "caught-up") === "caught-up",
    };
    await (await this.clients.control()).apply(membership, this.key.sign(membershipMessage(membership)), flags);
  }

  /**
   * Registers a business process with the kernel: its builder (`Domain.process`), whose
   * `model()` it sends, or a model as is. Signed with this client's key, which needs a grant
   * `process` for the process, `submit` for each command, `query` for each query and `listen`
   * for each event the model names. A model equal to the newest version registers nothing
   * (`new: false`); a different one becomes the next version, while running cases finish on
   * theirs. `Kernable.app` registers the processes of its domains itself. Throws what
   * `ProcessBuilder.model` throws, and the kernel's `INVALID_ARGUMENT` for a model it refuses,
   * naming the process, the step and why.
   *
   * ```ts
   * const { version, new: changed } = await kernable.registerProcess(fulfilment);
   * ```
   */
  async registerProcess(process: ProcessBuilder | ProcessModel): Promise<ProcessVersion> {
    const model = Buffer.from(JSON.stringify(process instanceof ProcessBuilder ? process.model() : process), "utf8");
    const issued_at = issuedNow();
    const sig = this.key.sign(processMessage(model, issued_at));
    const r = await (await this.clients.control()).registerProcess({ model, issued_at, sig });
    return { name: r.name, version: r.version, new: r.new };
  }

  /**
   * Sets as whom the process `name` (qualified: `shop.fulfilment`) acts: the sender tenant,
   * identity and session of `context` go with every command and query the kernel's engine sends
   * for it from now on; its correlation and causation stay the engine's. Without it a process
   * acts as `process:<name>`. The identity kit calls it for each process it gives an identity;
   * operator tooling may too. Idempotent: `changed` is false if the process acted so already.
   * The key needs `process` for the process. Rejects with `NOT_FOUND` for a process never
   * registered.
   *
   * ```ts
   * await kernable.processAct("shop.fulfilment", { senderTenant: "platform", senderIdentity, senderSession: keyId });
   * ```
   */
  async processAct(name: string, context: RequestContext): Promise<{ changed: boolean }> {
    const req = { name, context: toWireContext(context), issued_at: issuedNow() };
    const sig = this.key.sign(processActMessage(req.name, req.context, req.issued_at));
    const r = await (await this.clients.control()).processAct({ ...req, sig });
    return { changed: r.changed };
  }

  /**
   * Runs a command and returns its outcome: `{ committed: true, events }`, or
   * `{ committed: false, code, message }` if it was rejected (a rejection does not throw). By
   * default it returns once the command settled, so a query right after it sees the events; see
   * `SubmitOptions.wait`. Throws `NotSettled` if the command committed but did not settle in time.
   * Idempotent on `options.id`. A scheduled command (`at`) has no outcome yet: enqueue it.
   * Submitted again by its ID without `at`, it waits for the scheduled command's outcome;
   * throws `NotDue` if the kernel answers that it is not due yet.
   */
  async submit(options: SubmitOptions): Promise<Outcome>;
  /**
   * The same with a command class instance: `submit(new AddItem({ sku, qty }), { aggregateId })`.
   * Its class says the domain, aggregate type, type and vault fields (no `use` needed).
   */
  async submit(command: Command, target: CommandTarget): Promise<Outcome>;
  async submit(command: SubmitOptions | Command, target?: CommandTarget): Promise<Outcome> {
    const options = this.commandOptions(command, target);
    if (options.at !== undefined) throw new Error("a scheduled command (`at`) has no outcome yet: use `enqueue`");
    const wait = options.wait === "committed" ? "WAIT_COMMITTED" : "WAIT_SETTLED";
    const res = await this.send(options, wait);
    if (res.outcome === "rejected") {
      return rejectedOutcome(res.rejected!);
    }
    if (res.outcome === "accepted") throw new NotDue(res.command_id);
    // Events come back with references only: the submitter is no vault reader.
    const outcome = { committed: true as const, events: res.committed!.events.map((e) => toStored(e)) };
    if (wait === "WAIT_SETTLED" && !res.committed!.settled) throw new NotSettled(outcome);
    return outcome;
  }

  /**
   * Stores the command in the kernel's queue and returns its ID without waiting for it to run
   * (fire and forget). Submit it again with the same `id` to get the outcome. With `at`, a
   * scheduled command: it runs at that time, e.g. a reminder in 24 hours,
   * `enqueue(Remind.new(), { aggregateId, at: new Date(Date.now() + 86_400_000) })`.
   */
  async enqueue(options: Omit<SubmitOptions, "wait">): Promise<string>;
  async enqueue(command: Command, target: Omit<CommandTarget, "wait">): Promise<string>;
  async enqueue(command: Omit<SubmitOptions, "wait"> | Command, target?: Omit<CommandTarget, "wait">): Promise<string> {
    return (await this.send(this.commandOptions(command, target), "WAIT_ACCEPTED")).command_id;
  }

  /** A follow-up command (`ListenerContext.submit`) from a command class instance and its target. */
  followUpOf(command: Command, target: CommandTarget & { id: string }): FollowUp {
    return this.commandOptions(command, target) as unknown as FollowUp;
  }

  /** `SubmitOptions` from options as they are, or from a command class instance and its target. */
  private commandOptions<O extends Omit<SubmitOptions, "wait">>(command: O | Command, target?: Partial<SubmitOptions>): O {
    if (!(command instanceof Command)) return command;
    if (!target?.aggregateId) throw new Error(`${command.constructor.name}: say which aggregate (aggregateId)`);
    const cls = command.constructor as CommandClass<unknown>;
    const binding = boundOrThrow(cls, "handled by an aggregate");
    if (!this.domains.has(binding.domain) && binding.schemas) this.domains.set(binding.domain, binding.schemas as Domain);
    return { ...target, domain: binding.domain, aggregateType: binding.aggregateType!, type: messageType(cls), data: dataOf(command) } as unknown as O;
  }

  /**
   * Has these aggregates store the unique keys and facts their current state yields, e.g. after
   * declaring a key on an aggregate type with existing aggregates: each runs the built-in
   * reindex command of its type (`_reindex_<aggregateType>`, see `reindexType`), which produces
   * no events. Returns the outcome per ID, once committed:
   * committed (with no events), or rejected, e.g. with `unique_taken` if another aggregate holds
   * the same value. An ID without events holds nothing. The key needs `run` for
   * `<domain>._reindex_<aggregateType>` on the worker and `submit` here.
   *
   * ```ts
   * const outcomes = await kernable.reindex({ domain: "crm", aggregateType: "Customer", aggregateIds: ["k1", "k2"] });
   * ```
   */
  async reindex(options: ReindexOptions): Promise<Map<string, Outcome>> {
    const outcomes = new Map<string, Outcome>();
    const { aggregateIds, ...command } = options;
    const ids = [...new Set(aggregateIds)];
    // A few at a time, so a long list does not open thousands of calls at once.
    for (let i = 0; i < ids.length; i += 16) {
      const batch = ids.slice(i, i + 16);
      const done = await Promise.all(
        batch.map((aggregateId) =>
          this.submit({ ...command, aggregateId, type: reindexType(options.aggregateType), data: {}, wait: "committed" }),
        ),
      );
      batch.forEach((id, j) => outcomes.set(id, done[j]));
    }
    return outcomes;
  }

  /**
   * Asks the read model that answers `domain.type` and returns its answer, typed as `T`
   * (unchecked): `await kernable.query<Cart>({ domain: "shop", type: "GetCart", data: { id: "c1" } })`.
   * Queries always go through the kernel, also within one process, so grants and middleware
   * apply. The kernel waits until the read model has caught up (and reached `minPosition`).
   * With `Options.deployment`, a read model of that deployment answers if one is connected,
   * else one of the deployment the service's membership names.
   * Throws `Rejected` if the read model or a middleware rejects it.
   */
  async query<T = unknown>(options: QueryOptions): Promise<T>;
  /** The same with a query class instance, typed by its answer: `query(new GetCart({ id }), { tenant })`. */
  async query<Q extends Query<any>>(query: Q, options?: Omit<QueryOptions, "domain" | "type" | "data">): Promise<AnswerOf<Q>>;
  async query(query: QueryOptions | Query<any>, more?: Omit<QueryOptions, "domain" | "type" | "data">): Promise<unknown> {
    let options: QueryOptions;
    let declared: QueryTarget | undefined;
    if (query instanceof Query) {
      const cls = query.constructor as QueryClass<unknown>;
      options = { ...more, domain: boundOrThrow(cls, "answered by a read model").domain, type: messageType(cls), data: dataOf(query) };
      declared = (cls as MessageStatics).target;
    } else {
      options = query;
      declared = this.domains.get(options.domain)?.queries?.get(options.type)?.targetOf;
    }
    const target = queryTarget(declared, options.domain, options.data);
    const request = {
      query_id: randomUUID(),
      tenant_id: options.tenant ?? this.tenant,
      domain: options.domain,
      query_type: options.type,
      payload: encode(options.data),
      context: toWireContext(options.context ?? this.context),
      min_position: options.minPosition ?? 0,
      issued_at: issuedNow(),
      ...(target ? { target } : {}),
      // Not signed: read models of this deployment answer first.
      deployment: this.deployment,
    };
    if (options.allPartitions) {
      // A wait-only query to every partition at once: each returns once the read model has
      // processed that partition's head.
      const wait = { ...request, payload: Buffer.alloc(0), min_position: 0, wait_only: true };
      const signedWait = { ...wait, client_sig: this.key.sign(queryMessage(wait)), listener: handling.getStore() ?? "" };
      const parts = await this.clients.all(options.timeoutMs);
      await Promise.all(parts.map((p) => p.clients.query(signedWait, options.timeoutMs)));
      request.issued_at = issuedNow();
    }
    const signed = {
      ...request,
      client_sig: this.key.sign(queryMessage(request)),
      listener: handling.getStore() ?? "",
    };
    const res = await this.clients.route(request.tenant_id, (c) => c.query(signed, options.timeoutMs));
    if (res.outcome === "rejected") throw new Rejected(res.rejected!.message, res.rejected!.code);
    return decode(res.response!);
  }

  /**
   * Runs a listener defined with `Domain.readModel` or `Domain.reactor` over this connection:
   * `await kernable.listener(carts).run()`. Handlers added to the definition later still count
   * until `run()` connects.
   */
  listener(listener: Listener): ListenerHandle {
    return new ListenerHandle(this, this.clients, this.key, listener, this.log, this.service, this.deployment);
  }

  private send(options: SubmitOptions, wait: WireWait) {
    if (options.until !== undefined && options.at === undefined) throw new Error("`until` needs `at`");
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
      ...(options.at !== undefined ? { deliver_at: new Date(options.at).getTime() } : {}),
      ...(options.until !== undefined ? { deliver_until: new Date(options.until).getTime() } : {}),
    };
    const signed = { ...cmd, client_sig: this.key.sign(commandMessage(cmd)) };
    const listener = handling.getStore();
    return this.clients.route(signed.tenant_id, (c) => c.submit(signed, wait, listener, sealed.values, options.timeoutMs));
  }

  /**
   * Cancels a scheduled command (`enqueue` with `at`) before it is due: it completes as rejected
   * `cancelled`. Needs `submit` for the command's type; middlewares check it as a cancel of that
   * type. Returns `{ cancelled: false, code }` with `too_late` (due, running or done already),
   * `not_found`, or a middleware's code.
   */
  async cancel(commandId: string, options: { tenant?: string; context?: RequestContext; timeoutMs?: number } = {}): Promise<{ cancelled: true } | { cancelled: false; code: string; message: string }> {
    const req = { command_id: commandId, tenant_id: options.tenant ?? this.tenant, context: toWireContext(options.context ?? this.context), issued_at: issuedNow() };
    const signed = { ...req, client_sig: this.key.sign(cancelMessage(req)) };
    const res = await this.clients.route(signed.tenant_id, (c) => c.cancel(signed, options.timeoutMs));
    return res.outcome === "cancelled" ? { cancelled: true } : { cancelled: false, code: res.rejected!.code, message: res.rejected!.message };
  }

  /**
   * Streams committed events after `options.after` (backlog first, then live) to `handler`,
   * one at a time, e.g. to export them: `kernable.subscribe({ types: ["shop.*"] }, (e) => ...)`.
   * Reconnects and resumes after the last handled event; if the handler throws, that event is
   * delivered again. Persist `position` yourself to resume across restarts. Unlike a listener, a
   * subscription is not registered: the kernel keeps no position, nobody waits for it, and events
   * arrive as stored (not upcast, vault fields `null`). The key needs `subscribe` for the types.
   */
  subscribe(options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>): Subscription {
    return new Subscription(this.clients, this.key, options, handler, this.log);
  }

  /** Closes the connection now; only needed in a long-running process that discards the client. */
  close(): void {
    this.clients.close();
  }
}

/** Options of `Data.get`, `Data.delete`, `Cache.get` and `Cache.delete`: which tenant's file or value, and who asks. */
export interface DataOptions {
  /** Default: the client's tenant (`Options.tenant`). */
  tenant?: string;
  /**
   * Who asks; signed, and what middlewares checking the request decide on. Replaces the
   * client's default context (`Options.context`).
   */
  context?: RequestContext;
}

/** Options of `Data.put`: the tenant, and the content type stored with the file. */
export interface PutDataOptions extends DataOptions {
  /** Default: `text/plain; charset=utf-8` for a string, else `application/octet-stream`. */
  contentType?: string;
}

/**
 * Files of a domain kept by the kernel under `(tenant, domain, key)`, at most 16 MiB each (the
 * kernel's `--max-data-bytes`): PDFs,
 * images, exports. Reading needs a grant of `data_read` for the domain (`shop.*`), putting and
 * deleting one of `data_write`. Files are not part of the signed event log and are not erased
 * with the vault: delete a person's files yourself when erasing them. Use it as `kernable.data`.
 *
 * Each request carries a request context (`DataOptions.context`, default `Options.context`) and
 * passes the middleware chain first, as `<domain>.PutData`, `.GetData`, `.DeleteData` or
 * `.ListData` (see `Check`). A middleware that refuses it makes the call throw a gRPC error with code
 * `PERMISSION_DENIED` and the message `<code>: <message>`.
 *
 * ```ts
 * await kernable.data.put("shop", "invoices/o1.pdf", pdfBytes, { contentType: "application/pdf" });
 * const file = await kernable.data.get("shop", "invoices/o1.pdf"); // { data, contentType, sha256 } or undefined
 * const { files, next } = await kernable.data.list("shop", "invoices/"); // up to 1,000; then { after: next }
 * ```
 *
 * See [Store files and cache values](../how-to/data-and-cache).
 */
export class Data {
  private readonly clients: Partitioned;
  private readonly key: Key;
  private readonly tenant: string;
  private readonly context: RequestContext;

  constructor(clients: Partitioned, key: Key, tenant: string, context: RequestContext = {}) {
    this.clients = clients;
    this.key = key;
    this.tenant = tenant;
    this.context = context;
  }

  /**
   * Stores `data` under `key`, replacing a file there; a string is stored as UTF-8. Resolves to
   * the SHA-256 of the data (hex) as the kernel computed it: for an event to name.
   */
  async put(domain: string, key: string, data: Uint8Array | string, options: PutDataOptions = {}): Promise<string> {
    const contentType = options.contentType ?? (typeof data === "string" ? "text/plain; charset=utf-8" : "application/octet-stream");
    const res = await this.call("put", domain, key, options, toBuffer(data), contentType);
    return Buffer.from(res.sha256 ?? []).toString("hex");
  }

  /** The file under `key` with its SHA-256 (hex), or `undefined` if there is none. */
  async get(domain: string, key: string, options: DataOptions = {}): Promise<{ data: Uint8Array; contentType: string; sha256: string } | undefined> {
    const res = await this.call("get", domain, key, options);
    return res.found ? { data: res.data, contentType: res.content_type, sha256: Buffer.from(res.sha256 ?? []).toString("hex") } : undefined;
  }

  /**
   * The files whose keys start with `prefix` (needs `data_read`), in the order of their keys'
   * bytes, up to 1,000 after `options.after`; `next`, if set, is the `after` of the next page.
   * To find files no event names, or what a tenant stores.
   */
  async list(domain: string, prefix = "", options: ListDataOptions = {}): Promise<{ files: { key: string; contentType: string; size: number }[]; next?: string }> {
    const request: WireDataRequest = {
      tenant_id: options.tenant ?? this.tenant,
      domain,
      key: prefix,
      data: Buffer.alloc(0),
      content_type: "",
      issued_at: issuedNow(),
      context: toWireContext(options.context ?? this.context),
      after: options.after ?? "",
    };
    const signed = { ...request, client_sig: this.key.sign(dataMessage("list", request)) };
    const res = await this.clients.route(request.tenant_id, (c) => c.listData(signed));
    return {
      files: (res.files ?? []).map((f) => ({ key: f.key, contentType: f.content_type, size: Number(f.size) })),
      ...(res.next ? { next: res.next } : {}),
    };
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
      issued_at: issuedNow(),
      context: toWireContext(options.context ?? this.context),
    };
    const rpc = op === "put" ? "PutData" : op === "get" ? "GetData" : "DeleteData";
    const signed = { ...request, client_sig: this.key.sign(dataMessage(op, request)) };
    return this.clients.route(request.tenant_id, (c) => c.data(rpc, signed));
  }
}

/** Options of `Data.list`: the tenant, and where the page starts. */
export interface ListDataOptions extends DataOptions {
  /** The `next` of the page before; default: the first page. */
  after?: string;
}

/** Options of `Cache.set`: the tenant, and when the value expires. */
export interface SetCacheOptions extends DataOptions {
  /** Milliseconds until the value expires; default: until deleted or the kernel restarts. */
  ttlMs?: number;
}

/**
 * The shared cache of a domain: values of at most 1 MiB under `(tenant, domain, key)` that may
 * expire, e.g. rates or tokens shared by several processes. Needs a grant of `cache` for the
 * domain (`shop.*`). The cache lives in the kernel's memory: a restart empties it. Values are not
 * part of the signed event log and are not erased with the vault. Use it as `kernable.cache`.
 *
 * Each request carries a request context (`DataOptions.context`, default `Options.context`) and
 * passes the middleware chain first, as `<domain>.SetCache`, `.GetCache` or `.DeleteCache` (see
 * `Check`). A middleware that refuses it makes the call throw a gRPC error with code
 * `PERMISSION_DENIED` and the message `<code>: <message>`.
 *
 * ```ts
 * await kernable.cache.set("shop", "rates/EUR", JSON.stringify(rates), { ttlMs: 60_000 });
 * const value = await kernable.cache.get("shop", "rates/EUR"); // Uint8Array or undefined
 * if (value) rates = JSON.parse(new TextDecoder().decode(value));
 * ```
 *
 * See [Store files and cache values](../how-to/data-and-cache).
 */
export class Cache {
  private readonly clients: Partitioned;
  private readonly key: Key;
  private readonly tenant: string;
  private readonly context: RequestContext;

  constructor(clients: Partitioned, key: Key, tenant: string, context: RequestContext = {}) {
    this.clients = clients;
    this.key = key;
    this.tenant = tenant;
    this.context = context;
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
      issued_at: issuedNow(),
      context: toWireContext(options.context ?? this.context),
    };
    const rpc = op === "set" ? "SetCache" : op === "get" ? "GetCache" : "DeleteCache";
    const signed = { ...request, client_sig: this.key.sign(cacheMessage(op, request)) };
    return this.clients.route(request.tenant_id, (c) => c.cache(rpc, signed));
  }
}

function toBuffer(value: Uint8Array | string): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

/** What `Kernable.subscribe` streams: from which position, and which event types. */
export interface SubscribeOptions {
  /** Global position of the last event already handled; 0 = from the beginning (default). */
  after?: number;
  /** Qualified event types (`orders.OrderPlaced`) or domains (`orders.*`); empty = all the key may read. */
  types?: string[];
  /**
   * Milliseconds to wait before reconnecting after the stream ends (default 1000); twice as
   * long after each further failed attempt, at most 30 s.
   */
  reconnectMs?: number;
}

/**
 * A running subscription, from `Kernable.subscribe`. It streams until `stop()`, reconnecting
 * after `position` when the stream drops; `done` resolves after `stop()`.
 */
export class Subscription {
  /** Global position of the last handled event. */
  position: number;
  /** Resolves after `stop()`. */
  readonly done: Promise<void>;
  private call?: ClientReadableStream<WireEvent>;
  private stopped = false;
  private readonly wake = new Wake();

  /** With partitions: one stream per partition. */
  private readonly calls = new Set<ClientReadableStream<WireEvent>>();
  /** With partitions: the handler takes one event at a time, of whichever partition. */
  private turns: Promise<void> = Promise.resolve();

  constructor(clients: Partitioned | Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>, log: Log) {
    this.position = options.after ?? 0;
    // A failing stream or handler reconnects; anything else ends the subscription with an
    // error line, so `done` never rejects (nobody needs to wait for it).
    this.done = this.start(clients, key, options, handler, log).catch((err: unknown) => {
      log.print("error", "subscription", `ended: ${(err as Error)?.message ?? String(err)}`);
    });
  }

  /** Stops the subscription: ends its stream; `done` then resolves. */
  stop(): void {
    this.stopped = true;
    this.call?.cancel();
    for (const call of this.calls) call.cancel();
    this.wake.now();
  }

  private async start(clients: Partitioned | Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>, log: Log) {
    if (!(clients instanceof Partitioned)) return this.loop(clients, key, options, handler, log, "");
    const parts = await partitionsOf(clients, "subscription", log, options.reconnectMs ?? 1000, this.wake, () => this.stopped);
    if (!parts) return;
    if (parts.length === 1 && parts[0].name === "") return this.loop(parts[0].clients, key, options, handler, log, "");
    // A stream per partition, each from the start (`after` names a position of one kernel);
    // events are in order within a partition, not across them.
    const inTurn = (event: StoredEvent) => {
      const run = this.turns.then(() => handler(event));
      this.turns = run.then(
        () => undefined,
        () => undefined,
      );
      return run;
    };
    await Promise.all(parts.map((p) => this.loop(p.clients, key, { ...options, after: 0 }, inTurn, log, p.name)));
  }

  private async loop(clients: Clients, key: Key, options: SubscribeOptions, handler: (event: StoredEvent) => void | Promise<void>, log: Log, part: string) {
    const event_types = options.types ?? [];
    let position = part === "" ? this.position : (options.after ?? 0);
    let attempt = 0;
    while (!this.stopped) {
      const from = position;
      let error: unknown = new Error("stream ended");
      let call: ClientReadableStream<WireEvent> | undefined;
      try {
        // Opening the stream may throw too (a client closed meanwhile): that is reconnected
        // like a dropped stream.
        const issued_at = issuedNow();
        const client_sig = key.sign(subscribeMessage(position, event_types, issued_at));
        call = clients.subscribe({ after_position: position, event_types, issued_at, client_sig });
        if (part === "") this.call = call;
        else this.calls.add(call);
        for await (const e of call as AsyncIterable<WireEvent>) {
          await handler(toStored(e));
          position = e.global_position;
          if (part === "") this.position = position;
        }
      } catch (err) {
        error = err;
      }
      if (call) this.calls.delete(call);
      attempt = position > from ? 1 : attempt + 1;
      if (!this.stopped) await log.reconnect(part === "" ? "subscription" : `subscription ${part}`, error, options.reconnectMs ?? 1000, attempt, this.wake);
    }
  }
}

/**
 * How a listener runs: its place in the order, where it starts, whether it is live, and where it
 * keeps its state. The second argument of `Domain.readModel` and `Domain.reactor`, which set the
 * defaults. See [Order and consistency](../concepts/ordering).
 *
 * ```ts
 * const carts = shopDomain.readModel("carts", { store: "sqlite:carts.db" });
 * const push = shopDomain.reactor("push", { live: true });
 * ```
 */
export interface ListenerOptions {
  /**
   * Listeners with a lower order process each event first; equal orders run in parallel.
   * Defaults: 0 for read models, 10 for reactors. Negative orders are reserved for kernable's kits (a grant of `reserved_order`).
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
  /**
   * Runs this process's listener as one instance of a group under the listener's name, e.g. one
   * per node of a pair: 1 to 128 letters, digits, `-`, `_` and `.`. Default: `KERNABLE_INSTANCE`,
   * which `kernable node` sets; without either, the name has one connection. The group counts
   * as one listener: it is through an event once one instance has handled it.
   */
  instance?: string;
  /**
   * Whether the instances take turns (default for reactors): one gets events, the others wait
   * and the first of them takes over when it leaves, so a side effect happens once. `false`
   * (default for read models): every instance gets every event, from its own position, and
   * answers queries; such a listener cannot park events. Live listeners are never single.
   */
  single?: boolean;
  /**
   * Milliseconds to wait before reconnecting after the stream ends (default 1000); twice as
   * long after each further failed attempt, at most 30 s.
   */
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
  /**
   * How often a handler may fail on one event before the listener parks it and moves on
   * (default 5 for reactors, 0 for read models; 0: never, the listener keeps getting the event
   * and everything behind it waits). A `Rejected` (also from `ctx.submit`) parks at once; the
   * kernel being unreachable or busy (`UNAVAILABLE`, `DEADLINE_EXCEEDED`,
   * `RESOURCE_EXHAUSTED`), `NotSettled` or the store failing never does: the listener waits and
   * tries again. The kernel keeps parked events with the rejection's code and message (for
   * other errors `handler_error` and the error's type; the log has the whole error) until an
   * operator retries or dismisses them (`kernable listeners retry`). A read model should not
   * park: it would miss the event's change. With a store, a parked event's writes are dropped,
   * the rest of its batch kept. Live listeners never park, and no listener parks an event that
   * erases a subject because its handler failed: the handler must get past it, so no personal
   * data stays behind. An event newer than the code (`unknown_event`) is parked, erasing or
   * not: no handler ran on it.
   */
  parkAfter?: number;
}

type EventHandler = (data: unknown, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>;

/** Options of one event handler: the third argument of `Listener.on`. */
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
/** What a middleware handler gets besides the check: the listener's store, if it has one. */
export interface MiddlewareContext {
  store?: Store;
}

type MiddlewareHandler = (check: Check, outcome: Outcome | QueryOutcome | undefined, ctx: MiddlewareContext) => void | Promise<void>;

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
 *
 * `use`, `on`, `query` and `middleware` return the listener, so they chain:
 *
 * ```ts
 * const carts = shopDomain.readModel("carts");
 * const rows = new Map<string, { items: number }>();
 * carts
 *   .on("ItemAdded", (data: { qty: number }, evt) => {
 *     const row = rows.get(evt.aggregateId) ?? { items: 0 };
 *     row.items += data.qty;
 *     rows.set(evt.aggregateId, row);
 *   })
 *   .query("GetCart", (data: { id: string }) => {
 *     const row = rows.get(data.id);
 *     if (!row) throw new Rejected("no such cart");
 *     return row;
 *   });
 * ```
 */
export class Listener {
  /** The domain the listener is defined on: its unqualified types are of this domain. */
  readonly domain: Domain;
  /** The listener's name, unique per tenant; the kernel tracks its position under it. */
  readonly name: string;
  /** The options it was defined with, including the defaults of `readModel` or `reactor`. */
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
   * (`identity.AccountRegistered`) or a whole domain (`identity.*`): `handler(data, evt, ctx)`
   * gets the upcast payload, the event's `StoredEventMeta` and a `ListenerContext`. A handler
   * for the exact type wins over one for its domain; one handler per type (a second replaces the
   * first). The listener acknowledges an event only after the handler finished (a returned
   * promise resolved); if it throws, the event is delivered again. With `options.raw`, it gets
   * the payload as stored. Throws for a malformed type.
   */
  on<D = any>(
    eventType: string,
    handler: (data: D, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>,
    options?: HandlerOptions,
  ): this;
  /**
   * The same with an event class, of this domain or another: `on(ItemAdded, (data) => …)`, `data`
   * typed by its fields. The class must be declared (an aggregate's `apply` or `Domain.event`);
   * its domain's event schemas upcast its events.
   */
  on<E>(
    event: EventClass<E>,
    handler: (data: Fields<E>, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>,
    options?: HandlerOptions,
  ): this;
  on(event: string | EventClass<unknown>, handler: (data: any, evt: StoredEventMeta, ctx: ListenerContext) => void | Promise<void>, options: HandlerOptions = {}): this {
    let eventType: string;
    if (typeof event === "string") {
      eventType = event;
    } else {
      const binding = boundOrThrow(event, "declared (an aggregate's apply, or Domain.event)");
      eventType = `${binding.domain}.${messageType(event)}`;
      if (!this.domains.has(binding.domain) && binding.schemas) this.domains.set(binding.domain, binding.schemas as Domain);
    }
    if (!UNQUALIFIED.test(eventType) && !QUALIFIED_OR_DOMAIN.test(eventType)) {
      throw new Error(`listen to a type (Type), a qualified type (domain.Type) or a domain (domain.*), not ${JSON.stringify(eventType)}`);
    }
    this.handlers.set(this.qualify(eventType), { handler: handler as EventHandler, raw: options.raw ?? false });
    return this;
  }

  /**
   * Answers the query type `queryType`, of the own domain (`GetCart`) or another one
   * (`orders.GetOrder`), from this listener's state. The kernel delivers queries in order with
   * the events, so an answer reflects every event handled before it. `handler(data, qry, ctx)`
   * gets the query's data, its `QueryMeta` and a `QueryContext`, and returns the answer (or a
   * promise of it), sent as JSON. Throw `Rejected` to refuse; any other error refuses with
   * `handler_error`. A handler cannot query its own listener (the answer would wait behind
   * itself): the kernel refuses it. Throws for a malformed type.
   */
  query<D = any, R = unknown>(queryType: string, handler: (data: D, qry: QueryMeta, ctx: QueryContext) => R | Promise<R>): this;
  /** The same with a query class: `query(GetCart, (data) => …)`, typed by its fields and its answer. */
  query<Q>(query: QueryClass<Q>, handler: (data: Fields<Q>, qry: QueryMeta, ctx: QueryContext) => AnswerOf<Q> | Promise<AnswerOf<Q>>): this;
  query(query: string | QueryClass<unknown>, handler: (data: any, qry: QueryMeta, ctx: QueryContext) => unknown): this {
    let queryType: string;
    if (typeof query === "string") {
      queryType = query;
    } else {
      bind(query, { domain: this.domain.name, schemas: this.domain });
      queryType = messageType(query);
      // Its target counts also when the query is asked by name, by clients that `use` the domain.
      const target = (query as MessageStatics).target;
      if (target && !this.domain.queries.has(queryType)) this.domain.queries.set(queryType, new QuerySchema(queryType).target(target.aggregateType, target.field));
    }
    if (!UNQUALIFIED.test(queryType) && !/^[A-Za-z][A-Za-z0-9_-]*\.[^.*]+$/.test(queryType)) {
      throw new Error(`answer a query type (Query) or a qualified one (domain.Query), not ${JSON.stringify(queryType)}`);
    }
    this.queries.set(this.qualify(queryType), handler as QueryHandler);
    return this;
  }

  /**
   * Makes this listener a middleware: the kernel asks `handler` about every command, query, file
   * and cache request of `options.types`, before accepting or routing it (or after, see `MiddlewareOptions.when`),
   * in chain order, and only once this listener has caught up with the events. Throw `Rejected`
   * to refuse; any other error refuses too. The registration is stored: while this listener is
   * away, the kernel refuses those requests (`kernable listeners remove` unregisters it). The
   * handler cannot submit or query what it checks itself (it would wait behind itself): the
   * kernel refuses it. A listener has at most one middleware before and one after.
   * `handler(check, outcome, ctx)` gets the `Check`; after the request, also its `Outcome` (a command)
   * or `QueryOutcome` (a query); and a `MiddlewareContext` with the listener's store, if it has one. The key needs `check` for the types; if its service has a
   * membership, the membership must list it as a middleware (`Kernable.app` does that). Throws for a live listener, a malformed type, or a second
   * middleware at the same `when`. See [Middleware](../concepts/queries-and-middleware#middleware).
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
 * least once: if a handler throws, the listener reconnects and the event is delivered again,
 * until a reactor gives up on it and parks it (`ListenerOptions.parkAfter`).
 * With a store, a batch's data and position are written together, so it takes effect once.
 *
 * ```ts
 * const handle = kernable.listener(carts);
 * void handle.run(); // until handle.stop()
 * await handle.ready;
 * ```
 */
export class ListenerHandle {
  /** Global position of the last handled event (or skipped, for other event types). */
  position = 0;
  /**
   * Events skipped because this code does not know them: a schema newer than the listener's
   * domains declare, written by a newer deployment (see `Options.deployment`). Read models,
   * live and replicated listeners skip them (with one warning per type and schema); a reactor
   * that parks parks them instead (`unknown_event`). Counted since this process started.
   */
  skipped = 0;
  private stored?: Store;
  /** Resolves once the kernel accepted the listener. */
  readonly ready: Promise<void>;
  private markReady!: () => void;
  private call?: ListenCall;
  private stopped = false;
  private readonly wake = new Wake();
  /** Newest event when the kernel accepted the listener; events up to it are replayed. */
  private replayUpTo = 0;
  /** Position after which the kernel delivered on the first connect. */
  startedAt = 0;
  /** Whether the first connect gets its own line; an `App` prints it in its overview instead. */
  announce = true;
  private connects = 0;
  private readonly clients: Partitioned | Clients;
  private readonly key: Key;
  private readonly log: Log;
  /** The service it belongs to (`Options.service`). */
  private readonly service: string;
  /** Its code version (`Options.deployment`); empty: none. */
  private readonly deployment: string;
  /** The definition it runs. */
  readonly listener: Listener;
  /** For handlers' follow-up commands and queries. */
  private readonly kernable: Kernable;
  /** How often the handler failed on an event, by position, until it gets past it. */
  private readonly attempts = new Map<number, number>();

  /** With partitions: one stream per partition, sharing this handle's store. */
  private children: ListenerHandle[] = [];
  /** The clients of the one kernel it listens to (without partitions, or as a partition's stream). */
  private single?: Clients;
  /** As a partition's stream: its partition, whose position the store keeps apart. */
  private part = "";
  /** As a partition's stream: the handle whose store it shares. */
  private parent?: ListenerHandle;
  /** Batches of all partitions take turns on the shared store: one at a time. */
  private turns: Promise<void> = Promise.resolve();
  /** With partitions: how far each partition's stream has handled, for the order across moves. */
  private progress = new Map<string, number>();
  private waiting: { part: string; at: number; resume: () => void }[] = [];
  /** As a partition's stream: tenants it read the map again for. */
  private checked = new Set<string>();

  constructor(
    kernable: Kernable,
    clients: Partitioned | Clients,
    key: Key,
    listener: Listener,
    log: Log,
    service: string,
    deployment: string,
    part?: { name: string; parent: ListenerHandle },
  ) {
    this.kernable = kernable;
    this.clients = clients;
    if (part) {
      this.part = part.name;
      this.parent = part.parent;
    }
    this.key = key;
    this.log = log;
    this.service = service;
    this.deployment = deployment;
    this.listener = listener;
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /**
   * The listener's store (`ListenerOptions.store`) while `run()` runs: it opens the store and
   * closes it when it ends. Throws outside of that, or if the listener has no store. Handlers get
   * it as `ctx.store`.
   */
  get store(): Store {
    if (!this.stored) throw new Error(`listener ${this.listener.name} has no store, or run() has not opened it yet`);
    return this.stored;
  }

  /**
   * Opens the store (if any; trying again while it cannot be opened, e.g. on a full disk),
   * connects, and processes events, queries and checks until `stop()`, reconnecting after the
   * last handled event when the stream drops or a handler throws (after
   * `ListenerOptions.reconnectMs`). Resolves after `stop()`, having closed the store.
   */
  async run(): Promise<void> {
    try {
      await this.runUntilStopped();
    } finally {
      // Stopped: the store is released; a later run() opens it again. A partition's stream
      // leaves it to the handle that owns it.
      if (!this.parent) this.stored?.close();
      this.stored = undefined;
    }
  }

  private async runUntilStopped(): Promise<void> {
    const options = this.listener.options;
    if (this.clients instanceof Partitioned) {
      const parts = await partitionsOf(this.clients, this.what, this.log, options.reconnectMs ?? 1000, this.wake, () => this.stopped);
      if (!parts) return;
      if (parts.length > 1 || parts[0].name !== "") {
        await this.openStore();
        if (this.stopped) return;
        // A stream per partition, each from its own position in the shared store.
        this.children = parts.map((p) => {
          const child = new ListenerHandle(this.kernable, p.clients, this.key, this.listener, this.log, this.service, this.deployment, { name: p.name, parent: this });
          child.announce = this.announce;
          child.stored = this.stored;
          return child;
        });
        void Promise.all(this.children.map((c) => c.ready)).then(() => {
          this.startedAt = Math.min(...this.children.map((c) => c.startedAt));
          this.markReady();
        });
        await Promise.all(this.children.map((c) => c.run()));
        return;
      }
      this.single = parts[0].clients;
    } else {
      this.single = this.clients;
    }
    let start = options.start;
    if (this.parent && this.stored) {
      this.position = this.stored.committedFor(this.part);
      if (this.position > 0) start = this.position;
    }
    if (!this.parent && options.store && !this.stored) {
      // A store that cannot be opened (a full disk) is tried again, backing off like a reconnect.
      for (let attempt = 1; !this.stored; attempt++) {
        try {
          this.stored = await Store.open(options.store, options.schema ?? "1");
        } catch (err) {
          if (this.stopped) return;
          await this.log.storeFailed(this.what, err, options.reconnectMs ?? 1000, attempt, this.wake);
          if (this.stopped) return;
        }
      }
      this.position = this.stored.committed;
      // A new (or cleared) store says nothing: the listener's own start applies (a reactor
      // starts at the newest event).
      if (this.position > 0) start = this.position;
    }
    let attempt = 0;
    while (!this.stopped) {
      const connects = this.connects;
      let error: unknown = new Error("stream ended");
      try {
        await this.listen(start);
      } catch (err) {
        error = err;
      }
      attempt = this.connects > connects ? 1 : attempt + 1;
      // Resume after the last handled event, once there is one: before the first `ready`, the
      // position says nothing (a new reactor would replay everything from 0).
      if (this.connects > 0) start = this.position;
      if (!this.stopped) await this.log.reconnect(this.what, error, options.reconnectMs ?? 1000, attempt, this.wake);
    }
  }

  /** Stops the listener: ends its stream, so `run()` resolves. */
  stop(): void {
    this.stopped = true;
    this.call?.cancel();
    this.wake.now();
    for (const child of this.children) child.stop();
  }

  /** Opens the listener's store, if it has one, trying again while it cannot be opened. */
  private async openStore(): Promise<void> {
    const options = this.listener.options;
    for (let attempt = 1; options.store && !this.stored; attempt++) {
      try {
        this.stored = await Store.open(options.store, options.schema ?? "1");
      } catch (err) {
        if (this.stopped) return;
        await this.log.storeFailed(this.what, err, options.reconnectMs ?? 1000, attempt, this.wake);
        if (this.stopped) return;
      }
    }
  }

  /** `listener <name>`, for log lines. */
  private get what(): string {
    return `listener ${this.listener.name}`;
  }

  private listen(start: number | undefined): Promise<void> {
    const { name, options, handlers, queries, middlewares } = this.listener;
    const instance = options.instance ?? process.env.KERNABLE_INSTANCE ?? "";
    const single = instance !== "" && (options.single ?? false) && !(options.live ?? false);
    return new Promise((resolve, reject) => {
      const call = this.single!.listen();
      this.call = call;
      // Batches are handled strictly one after another. Once one fails or the stream is over,
      // the messages queued behind it are dropped (the next connect resumes after the last
      // handled event), and the connection ends only after the one being handled: two batches
      // never run at once.
      let queue = Promise.resolve();
      let over = false;
      call.on("data", (m: ListenerDelivery) => {
        // Requests sent from the handlers name this listener, so the kernel never makes them
        // wait for it (it handles one message at a time).
        queue = queue
          .then(() => (over ? undefined : watched(this.log, this.what, handlingOf(m), () => handling.run(name, () => this.onMessage(call, m)))))
          .catch((err) => {
            over = true;
            call.cancel();
            reject(err);
          });
      });
      const end = (settle: () => void) => {
        over = true;
        void queue.then(settle);
      };
      call.on("error", (err) => end(() => reject(err)));
      call.on("end", () => end(resolve));
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
          instance,
          single,
          deployment: this.deployment,
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
      this.parent?.advanced(this.part, Number(m.ready.after_position));
      this.replayUpTo = m.ready.head_position;
      this.logConnected();
      this.markReady();
    } else if (m.waiting) {
      // Another instance runs the group: this one is ready to take over.
      this.log.print("info", this.what, `waiting: instance ${m.waiting.active} is active`);
      this.markReady();
    } else if (m.batch) {
      // A kernel that goes back (a bug, or not the kernel) must not make a store apply events
      // twice: the session ends, and the next resumes after what was handled.
      if (m.batch.up_to < this.position) {
        throw new Error(`the kernel sent a batch up to ${m.batch.up_to}, behind position ${this.position}`);
      }
      // With a store, the batch's data and position are committed together, before the ack;
      // the partitions' batches take turns on a shared store.
      const batch = m.batch;
      await this.inOrder(batch.events);
      const { parked, skipped } = await this.inTurn(async () => {
        this.stored?.begin();
        try {
          const done = await this.handleBatch(batch);
          // After an erasure, nothing the handlers removed may stay in the store's files.
          this.stored?.commit(batch.up_to, [...batch.events, ...(batch.retries ?? [])].some((e) => e.erases), this.part);
          return done;
        } catch (err) {
          this.stored?.rollback();
          if (this.stored) this.position = this.stored.committedFor(this.part);
          throw err;
        }
      });
      this.position = m.batch.up_to;
      this.parent?.advanced(this.part, Number(m.batch.up_to));
      this.skipped += skipped.length;
      if (this.parent) this.parent.skipped += skipped.length;
      call.write({ ack: { position: m.batch.up_to, parked, skipped } });
    } else if (m.query) {
      call.write({ answer: { call_id: m.query.call_id, ...(await this.answer(m.query.query)) } });
    } else if (m.check) {
      call.write({ verdict: { call_id: m.check.call_id, ...(await this.verdict(m.check)) } });
    }
  }

  /** On the parent: partition `part`'s stream has handled everything up to `position`. */
  private advanced(part: string, position: number): void {
    if ((this.progress.get(part) ?? -1) >= position) return;
    this.progress.set(part, position);
    this.waiting = this.waiting.filter((w) => {
      if (w.part !== part || position < w.at) return true;
      w.resume();
      return false;
    });
  }

  /**
   * Before a batch of a partition's stream: the events of a tenant that moved here
   * after its move wait until the stream of the partition it came from has passed the tenant's
   * last event there, so a tenant's events keep their order across the move. Waits at most
   * 30 s (a partition that is down), then goes on with a warning.
   */
  private async inOrder(events: WireEvent[]): Promise<void> {
    const parent = this.parent;
    if (!parent || !(parent.clients instanceof Partitioned) || events.length === 0) return;
    const clients = parent.clients;
    let map = await clients.current();
    if (!map) return;
    const known = (m: WirePartitionMap, tenant: string) =>
      partitionOf(m, tenant) === this.part || (m.moved ?? []).some((x) => x.tenant === tenant && (x.to === this.part || x.from === this.part));
    for (const e of events) {
      if (!known(map, e.tenant_id) && !this.checked.has(e.tenant_id)) {
        // The tenant came here after the map was read; another kernel may take a moment to hold the new one.
        this.checked.add(e.tenant_id);
        for (let i = 0; i < 3 && !known(map, e.tenant_id); i++) {
          if (i > 0) await this.wake.sleep(500);
          map = (await clients.current(true)) ?? map;
        }
      }
      for (const m of map.moved ?? []) {
        if (m.tenant !== e.tenant_id || m.to !== this.part || Number(e.global_position) <= Number(m.target_after)) continue;
        if (!map.partitions.some((p) => p.name === m.from)) continue;
        await this.reached(parent, m.from, Number(m.source_head), e.tenant_id);
      }
    }
  }

  /** Resolves once partition `part`'s stream (of `parent`) has handled up to `position`, or after 30 s. */
  private async reached(parent: ListenerHandle, part: string, position: number, tenant: string): Promise<void> {
    if ((parent.progress.get(part) ?? -1) >= position) return;
    let resume!: () => void;
    const passed = new Promise<boolean>((resolve) => (resume = () => resolve(true)));
    const waiter = { part, at: position, resume };
    parent.waiting.push(waiter);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<boolean>((resolve) => (timer = setTimeout(() => resolve(false), 30_000)));
    const ok = await Promise.race([passed, late, this.wake.stopped.then(() => true)]);
    clearTimeout(timer);
    parent.waiting = parent.waiting.filter((w) => w !== waiter);
    if (this.stopped) throw new Error("stopped");
    if (!ok) {
      this.log.print("warn", this.what, `tenant ${tenant} moved here from partition ${part}, whose stream has not passed position ${position} in 30 s; going on without its order`);
    }
  }

  /** Runs `batch` once the batches before it, of every partition, are done. */
  private inTurn<T>(batch: () => Promise<T>): Promise<T> {
    const owner = this.parent ?? this;
    const run = owner.turns.then(batch);
    owner.turns = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private logConnected(): void {
    if (this.connects++ === 0) {
      this.startedAt = this.position;
      if (this.announce) this.log.print("info", this.what, [`connected to ${this.log.peer}`, ...listenerDetails(this.listener, this.position)].join(" · "));
    } else {
      this.log.print("info", this.what, `connected again at position ${this.position}`);
    }
  }

  /**
   * Handles a batch: first the retries, then the events after the position. Returns the events
   * it parked and those it skipped as unknown; throws if a handler failed on one it does not park.
   */
  private async handleBatch(batch: NonNullable<ListenerDelivery["batch"]>): Promise<{ parked: WireParked[]; skipped: WireSkipped[] }> {
    const { handlers, domains, options } = this.listener;
    const parkAfter = options.live ? 0 : (options.parkAfter ?? 0);
    // Replicated instances (not `single`) cannot park.
    const replicated = (options.instance ?? process.env.KERNABLE_INSTANCE ?? "") !== "" && !(options.single ?? false);
    const parked: WireParked[] = [];
    const skipped: WireSkipped[] = [];
    const retries = (batch.retries ?? []).map((e) => ({ e, retry: true }));
    for (const { e, retry } of [...retries, ...batch.events.map((e) => ({ e, retry: false }))]) {
      if (!retry && e.global_position <= this.position) continue; // handled already
      const entry = handlers.get(`${e.domain}.${e.event_type}`) ?? handlers.get(`${e.domain}.*`);
      const known = entry && !entry.raw ? newerThanKnown(domains.get(e.domain)?.events, e.event_type, e.schema) : undefined;
      if (entry && known !== undefined) {
        // Newer than this code: a newer deployment wrote it. No handler call.
        const type = `${e.domain}.${e.event_type}`;
        if (parkAfter > 0 && !replicated) {
          const message = `${type} schema ${e.schema}: this code reads it up to ${known}; a newer deployment wrote it`;
          this.log.print("warn", this.what, `${type} at ${e.global_position} parked: ${message}`);
          parked.push({ position: e.global_position, code: "unknown_event", message, attempts: 1 });
        } else {
          skipped.push({ position: e.global_position, event_type: type, schema: e.schema });
          const once = `${type}@${e.schema}`;
          if (!warnedUnknown.has(once)) {
            warnedUnknown.add(once);
            this.log.print("warn", this.what, `skipped ${type} schema ${e.schema}: this code reads it up to ${known}; a newer deployment wrote it`);
          }
        }
      } else if (entry) {
        const stored = toStored(e, batch.vault);
        const { data, request, ...evt } = entry.raw
          ? stored
          : { ...stored, ...upcast(domains.get(e.domain)?.events, e.domain, e.event_type, e.schema, stored.data) };
        const ctx: ListenerContext = {
          replaying: e.global_position <= this.replayUpTo,
          follow: () => follow(stored),
          processAct: (name: string, context: RequestContext) => this.kernable.processAct(name, context),
          request,
          store: this.stored,
          submit: (command: FollowUp | Command, target?: CommandTarget & { id: string }) =>
            this.followUp(command instanceof Command ? this.kernable.followUpOf(command, target!) : command, stored),
          query: (query: string | Query<any>, data?: unknown, options?: Omit<QueryOptions, "domain" | "type" | "data">) =>
            typeof query === "string"
              ? this.kernable.query({ ...options, ...qualifiedType(query, this.listener.domain.name), data, tenant: options?.tenant ?? stored.tenant })
              : this.kernable.query(query, { ...(data as object), tenant: (data as QueryOptions | undefined)?.tenant ?? stored.tenant }),
        };
        const type = `${e.domain}.${e.event_type}`;
        const saved = this.stored !== undefined && parkAfter > 0;
        if (saved) this.stored!.savepoint();
        try {
          await entry.handler(data, evt, ctx);
          if (saved) this.stored!.release();
          this.attempts.delete(e.global_position);
          this.log.print("debug", this.what, `${type} at ${e.global_position}`);
        } catch (err) {
          // An erasing event is never parked: its handler must get past it, or what it removes would stay behind.
          parked.push(this.giveUp(type, e.global_position, err, e.erases ? 0 : parkAfter));
          if (saved) this.stored!.rollbackEvent();
        }
      }
      if (!retry) this.position = e.global_position;
    }
    return { parked, skipped };
  }

  /** `ListenerContext.submit`: a follow-up of `event`, its rejection thrown. */
  private async followUp(command: FollowUp, event: StoredEvent): Promise<StoredEvent[]> {
    if (!command.id) throw new Error(`${command.type}: a follow-up command needs a fixed id, so a redelivered event runs it once`);
    const outcome = await this.kernable.submit({
      ...command,
      domain: command.domain ?? this.listener.domain.name,
      tenant: command.tenant ?? event.tenant,
      context: command.context ?? follow(event),
    });
    if (outcome.committed) return outcome.events;
    const rejected = new Rejected(outcome.message, outcome.code);
    if (outcome.key) rejected.key = outcome.key;
    if (outcome.requirement) rejected.requirement = outcome.requirement;
    throw rejected;
  }

  /**
   * What to do after the handler failed on the event at `position`: park it (a rejection at
   * once, another error after `parkAfter` attempts), or throw `err`, so the listener gets the
   * event again (the kernel or the store away, or attempts left).
   */
  private giveUp(type: string, position: number, err: unknown, parkAfter: number): WireParked {
    if (parkAfter <= 0 || transient(err)) throw err;
    const attempts = (this.attempts.get(position) ?? 0) + 1;
    this.attempts.set(position, attempts);
    if (!(err instanceof Rejected) && attempts < parkAfter) throw err;
    this.attempts.delete(position);
    if (err instanceof Rejected) {
      this.log.print("warn", this.what, `${type} at ${position} rejected (${err.code}), parked: ${err.message}`);
      return { position, code: err.code, message: err.message, attempts };
    }
    this.log.print("error", this.what, `${type} at ${position} failed ${attempts} times, parked: ${err instanceof Error ? (err.stack ?? err.message) : String(err)}`);
    return { position, code: "handler_error", message: errorType(err), attempts };
  }

  private async verdict(c: NonNullable<ListenerDelivery["check"]>): Promise<{ rejected?: WireRejected }> {
    try {
      const when = c.when === 1 ? "after" : "before";
      const middleware = this.listener.middlewares.get(when);
      if (!middleware) throw new Rejected(`no middleware ${when}`, "not_a_middleware");
      await middleware.handler(toCheck(c), when === "after" ? toOutcome(c) : undefined, { store: this.stored });
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
      // No answer is `null`, not `{}` as for commands and events without data.
      return { response: Buffer.from(JSON.stringify(result ?? null)) };
    } catch (err) {
      return {
        rejected: err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: String(err) },
      };
    }
  }
}

/** The unknown event types and schemas a listener warned about, as `domain.Type@schema`: once per process. */
const warnedUnknown = new Set<string>();

/** How a worker runs: the argument of `Worker.run` and `App.run`. */
export interface WorkerOptions {
  /** Commands executed in parallel, of different aggregates (default 16). */
  concurrency?: number;
  /**
   * Milliseconds to wait before reconnecting after the session ends (default 1000); twice as
   * long after each further failed attempt, at most 30 s.
   */
  reconnectMs?: number;
}

/**
 * A worker: runs the commands of its aggregates over a connection. Create it with
 * `kernable.worker(...aggregates)` (or let `Kernable.app` do it) and start it with `run()`.
 * The kernel leases it one command at a time per aggregate, with the aggregate's events (and
 * latest snapshot); the worker folds them, calls the command's handler, and sends back the
 * signed events, or the rejection. A command whose handler throws `Rejected` is rejected with
 * its code; any other error rejects with `handler_error`. An event whose schema the worker does
 * not know ends the session instead (it logs and reconnects until it is updated).
 *
 * ```ts
 * await kernable.worker(cart).run({ concurrency: 32 });
 * ```
 */
export class Worker {
  /** Resolves once the kernel accepted the worker. */
  readonly ready: Promise<void>;
  private markReady!: () => void;
  private readonly clients: Partitioned | Clients;
  private readonly key: Key;
  private readonly log: Log;
  private readonly byCommand = new Map<string, Aggregate<any>>();
  /** By qualified aggregate type, the command's aggregate type decides which aggregate runs it. */
  private readonly byAggregate = new Map<string, Aggregate<any>>();
  private readonly aggregates: Aggregate<any>[];
  /** The service it belongs to (`Options.service`). */
  private readonly service: string;
  /** Its code version (`Options.deployment`); empty: none. */
  private readonly deployment: string;
  /** Whether the first connect gets its own line; an `App` prints it in its overview instead. */
  announce = true;
  private connects = 0;
  private call?: SessionCall;
  private stopped = false;
  private readonly wake = new Wake();

  /** With partitions: one worker per partition, run together. */
  private children: Worker[] = [];
  /** The clients of the one kernel it runs on (without partitions, or as a partition's worker). */
  private single?: Clients;

  constructor(clients: Partitioned | Clients, key: Key, aggregates: Aggregate<any>[], log: Log, service: string, deployment = "") {
    this.clients = clients;
    this.key = key;
    this.log = log;
    this.aggregates = aggregates;
    this.service = service;
    this.deployment = deployment;
    for (const agg of aggregates) {
      for (const type of agg.handlers.keys()) this.byCommand.set(`${agg.domain}.${type}`, agg);
      this.byAggregate.set(`${agg.domain}.${agg.name}`, agg);
    }
    this.ready = new Promise((resolve) => (this.markReady = resolve));
  }

  /**
   * Registers the aggregates and processes commands until `stop()`, reconnecting when the
   * session drops. Resolves after `stop()`.
   */
  async run(options: WorkerOptions = {}): Promise<void> {
    if (this.clients instanceof Partitioned) {
      const parts = await partitionsOf(this.clients, "worker", this.log, options.reconnectMs ?? 1000, this.wake, () => this.stopped);
      if (!parts) return;
      if (parts.length > 1 || parts[0].name !== "") {
        // A session per partition: commands of every partition's tenants run here.
        this.children = parts.map((p) => {
          const worker = new Worker(p.clients, this.key, this.aggregates, this.log, this.service, this.deployment);
          worker.announce = this.announce;
          return worker;
        });
        void Promise.all(this.children.map((w) => w.ready)).then(() => this.markReady());
        if (this.stopped) return;
        await Promise.all(this.children.map((w) => w.run(options)));
        return;
      }
      this.single = parts[0].clients;
    } else {
      this.single = this.clients;
    }
    let attempt = 0;
    while (!this.stopped) {
      const connects = this.connects;
      let error: unknown = new Error("stream ended");
      try {
        await this.session(options.concurrency ?? 16);
      } catch (err) {
        error = err;
      }
      attempt = this.connects > connects ? 1 : attempt + 1;
      if (!this.stopped) await this.log.reconnect("worker", error, options.reconnectMs ?? 1000, attempt, this.wake);
    }
  }

  /** Stops the worker: ends its session, so `run()` resolves. */
  stop(): void {
    this.stopped = true;
    this.call?.cancel();
    this.wake.now();
    for (const worker of this.children) worker.stop();
  }

  private session(concurrency: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const call = this.single!.session();
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
          deployment: this.deployment,
        },
      });
    });
  }

  /**
   * The payload versions the worker reads and writes, for every event type its aggregates know
   * (those they apply and those their domains declare): without a deployment, the kernel
   * refuses the worker if the store already holds a newer one; with one, it rejects commands
   * on aggregates holding an event newer than declared here, or of a type not declared here
   * that a newer deployment wrote (`unknown_history`).
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

  /** Executes whose history is still coming in parts, by session and lease. */
  private partial = new WeakMap<SessionCall, Map<string, NonNullable<KernelMessage["execute"]>>>();

  private onMessage(call: SessionCall, m: KernelMessage): void {
    if (m.challenge) {
      call.write({ challenge_response: { sig: this.key.sign(challengeMessage(m.challenge.nonce)).sig } });
    } else if (m.registered) {
      if (this.connects++ > 0) this.log.print("info", "worker", "connected again");
      else if (this.announce) this.log.print("info", "worker", `connected to ${this.log.peer} · runs ${[...this.byCommand.keys()].sort().join(", ")}`);
      this.markReady();
    } else if (m.execute || m.execute_more) {
      // A long history comes in parts; run the command once the last one is here.
      const partial = this.partial.get(call) ?? new Map<string, NonNullable<KernelMessage["execute"]>>();
      this.partial.set(call, partial);
      let x = m.execute;
      if (m.execute_more) {
        const rest = m.execute_more;
        x = partial.get(rest.lease_id);
        partial.delete(rest.lease_id);
        if (!x) return;
        x.history.push(...rest.history);
        x.vault.push(...rest.vault);
        x.more = rest.more;
      }
      if (!x) return;
      if (x.more) {
        partial.set(x.lease_id, x);
        return;
      }
      // Whatever goes wrong outside the handler (a malformed message) ends the session, not
      // the process.
      this.execute(call, x).catch((err) => {
        this.log.print("error", "worker", `cannot execute: ${describe(err)}`);
        call.cancel();
      });
    }
  }

  private async execute(call: SessionCall, x: NonNullable<KernelMessage["execute"]>): Promise<void> {
    const cmd = x.command;
    const stream = `${cmd.domain}.${cmd.command_type} ${cmd.aggregate_type}/${cmd.aggregate_id}`;
    // A handler cannot be stopped from outside; one that runs past its lease at least shows.
    const hung = setTimeout(
      () => this.log.print("error", "worker", `${stream} still running after its lease ended: the kernel gives it to another worker; is the handler stuck?`),
      Math.max(0, x.deadline - Date.now()),
    );
    hung.unref();
    try {
      await this.handle(call, x, cmd, stream);
    } finally {
      clearTimeout(hung);
    }
  }

  private async handle(call: SessionCall, x: NonNullable<KernelMessage["execute"]>, cmd: WireCommand, stream: string): Promise<void> {
    let events: WireNewEvent[];
    let vault: WireVaultValue[] = [];
    let snapshot: WireSnapshot | undefined;
    let index: { keys: WireUniqueKey[]; facts: string[]; requires: WireRequirement[] } | undefined;
    try {
      const agg = this.byAggregate.get(`${cmd.domain}.${cmd.aggregate_type}`);
      if (!agg?.handlers.has(cmd.command_type)) {
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
      const meta: CommandMeta = {
        commandId: cmd.command_id,
        type: cmd.command_type,
        domain: cmd.domain,
        aggregateType: cmd.aggregate_type,
        aggregateId: cmd.aggregate_id,
        tenant: cmd.tenant_id,
      };
      const ctx = new CommandContext(cmd.command_id, fromWireContext(cmd.context), new Date(x.received_at), new Date(x.deadline));
      // A copy: a handler that changes it must not change the snapshot, nor `initial`.
      const produced = await agg.handlers.get(cmd.command_type)!(structuredClone(state), unseal(decode(cmd.payload), x.vault).data, meta, ctx);
      inClear(agg, cmd.command_type, produced);
      events = produced.map((e) => {
        const def = agg.events.get(e.type);
        const sealed = seal(e.data, def?.vault, cmd.aggregate_id);
        vault.push(...sealed.values);
        const erases = def?.eraser ? String(def.eraser(e.data, cmd.aggregate_id) ?? "") : "";
        return { event_type: e.type, schema: agg.schemaOf(e.type), payload: sealed.payload, vault_refs: sealed.refs, erases };
      });
      let folded: { state: unknown } | undefined;
      const after = () => (folded ??= { state: this.after(agg, cmd, x.expected_version, state, produced) }).state;
      snapshot = this.snapshot(agg, cmd, x.expected_version, produced, after);
      // The requirements from the data and the state before; keys and facts from the state after.
      const requires = agg.requirements.get(cmd.command_type)?.(unseal(decode(cmd.payload), x.vault).data, structuredClone(state)) ?? [];
      if (agg.keys.size || agg.facts.size || requires.length) {
        const held = agg.keys.size || agg.facts.size ? agg.index(after()) : { keys: [], facts: [] };
        index = {
          keys: held.keys.map((k) => ({
            name: k.name,
            global: k.scope === "global",
            value_hash: valueHash(k.value),
            subject: k.subject ?? "",
          })),
          facts: held.facts,
          requires: requires.map((r) => ({
            domain: r.domain ?? cmd.domain,
            aggregate_type: r.aggregateType,
            aggregate_id: r.aggregateId ?? "",
            fact: r.fact,
            key: r.key ?? "",
            value_hash: r.key === undefined ? Buffer.alloc(0) : valueHash(r.value ?? ""),
            absent: r.absent ?? false,
          })),
        };
      }
    } catch (err) {
      if (err instanceof SchemaError) {
        // Not the command's fault: this worker cannot read the aggregate. End the session; run()
        // reconnects and meets the same event again until the worker is updated.
        this.log.print("error", "worker", err.message);
        call.cancel();
        return;
      }
      // An unexpected error's text may hold personal data or internals, and a rejection is
      // kept for good: it names only the error's type; the log has the rest.
      if (!(err instanceof Rejected)) this.log.print("error", "worker", `${stream} failed: ${err instanceof Error && err.stack ? err.stack : describe(err)}`);
      const reason: WireRejected =
        err instanceof Rejected ? { code: err.code, message: err.message } : { code: "handler_error", message: errorType(err) };
      const worker_sig = this.key.sign(rejectionMessage(cmd.command_id, reason));
      call.write({ rejection: { lease_id: x.lease_id, reason, worker_sig } });
      this.log.print("debug", "worker", `${stream} rejected ${reason.code}`);
      return;
    }
    const worker_sig = this.key.sign(resultMessage(cmd, x.expected_version, events));
    // An aggregate that holds nothing and requires nothing sends no index, and no signature over it.
    const indexed =
      index && (index.keys.length || index.facts.length || index.requires.length)
        ? { ...index, index_sig: this.key.sign(indexMessage(cmd, x.expected_version, index.keys, index.facts, index.requires)) }
        : {};
    call.write({ result: { lease_id: x.lease_id, events, worker_sig, vault, ...(snapshot && { snapshot }), ...indexed } });
    this.log.print("debug", "worker", `${stream} committed ${events.length} events`);
  }

  /**
   * The state after `events`, applied as they will be read back (a JSON round trip), so it
   * equals a replay: what a snapshot stores and keys and facts are computed from.
   */
  private after(agg: Aggregate<any>, cmd: WireCommand, before: number, state: unknown, events: NewEvent[]): unknown {
    const read = events.map((e, i) => ({
      ...metaOf(e.type, before + i + 1, cmd, cmd.command_id, cmd.context),
      schema: agg.schemaOf(e.type),
      data: decode(encode(e.data)),
    }));
    return agg.fold(read, state);
  }

  /** A snapshot of the state `after` the events, if they cross a multiple of `snapshotEvery`. */
  private snapshot(agg: Aggregate<any>, cmd: WireCommand, before: number, events: NewEvent[], after: () => unknown) {
    const version = before + events.length;
    const every = agg.snapshotEvery;
    if (every <= 0 || events.length === 0 || Math.floor(version / every) === Math.floor(before / every)) return undefined;
    const snapshot = { version, schema: agg.schema, state: encode(after()) };
    const message = snapshotMessage(cmd, version, snapshot.schema, snapshot.state);
    return { ...snapshot, worker_sig: this.key.sign(message) };
  }
}

/**
 * Rejects the command (`personal_in_clear`) if an event carries a field that the command
 * declares personal but the event does not: forgotten on the event, the value would go into the
 * signed log in clear, for good.
 */
function inClear(agg: Aggregate<any>, commandType: string, events: NewEvent[]): void {
  const personal = agg.commands.get(commandType)?.vault.personal.flatMap((p) => p.fields) ?? [];
  if (personal.length === 0) return;
  for (const e of events) {
    if (e.data === null || typeof e.data !== "object") continue;
    const data = e.data as Record<string, unknown>;
    const sealed = new Set(agg.events.get(e.type)?.vault.personal.flatMap((p) => p.fields));
    for (const field of personal) {
      if (data[field] !== undefined && data[field] !== null && !sealed.has(field)) {
        throw new Rejected(
          `${commandType} declares ${field} personal, but ${e.type} does not: it would be stored in clear (declare it with Domain.event(${JSON.stringify(e.type)}, …).personal)`,
          "personal_in_clear",
        );
      }
    }
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
/** Attempts before a reactor parks an event its handler keeps failing on. */
const PARK_AFTER = 5;

/** gRPC codes of a kernel that is away or busy: CANCELLED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, INTERNAL, UNAVAILABLE. */
const BUSY = new Set([1, 4, 8, 10, 13, 14]);

/**
 * Whether a failed handler should simply try again: the kernel was unreachable or busy, a
 * submit did not settle in time, or the store failed. Never parks.
 */
function transient(err: unknown): boolean {
  if (err instanceof NotSettled) return true;
  const code = (err as { code?: unknown } | null)?.code;
  return (typeof code === "number" && BUSY.has(code)) || code === "ERR_SQLITE_ERROR";
}

/** `{ domain, type }` of a query type of `domain` (`GetOrder`) or qualified (`crm.GetCustomer`). */
function qualifiedType(type: string, domain: string): { domain: string; type: string } {
  const dot = type.indexOf(".");
  return dot < 0 ? { domain, type } : { domain: type.slice(0, dot), type: type.slice(dot + 1) };
}

function follow(event: StoredEvent): RequestContext {
  const { expectedVersion, bypassAuthorization, ...rest } = event.request;
  return { ...rest, correlationId: rest.correlationId || event.commandId, causationId: event.commandId };
}

/** Warns if other users can read the key file: whoever reads it can sign as this process. */
function warnIfShared(log: Log, key: string): void {
  if (process.platform === "win32") return;
  const mode = statSync(key).mode & 0o777;
  if (mode & 0o077) log.print("warn", "key", `${key} can be read by other users (mode ${mode.toString(8)}): chmod 600 ${key}`);
}

/**
 * Whether `url` (or every address of a comma-separated list) is a Unix socket or a loopback
 * address: plain text never leaves the machine.
 */
function local(url: string): boolean {
  if (url.includes(",")) return url.split(",").every((u) => local(u.trim()));
  if (url.startsWith("unix:")) return true;
  const host = url.replace(/^https?:\/\//, "").replace(/:\d+\/?$/, "").replace(/^\[|\]$/g, "");
  return host === "localhost" || host === "::1" || /^127\.\d+\.\d+\.\d+$/.test(host);
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

/** The type within its domain of a qualified one: `shop.PutData` → `PutData`. */
function typeOf(qualified: string, domain: string): string {
  return qualified.startsWith(`${domain}.`) ? qualified.slice(domain.length + 1) : qualified;
}

function toCheck(c: NonNullable<ListenerDelivery["check"]>): Check {
  const store = c.data ?? c.cache;
  if (store) {
    return {
      kind: c.data ? "data" : "cache",
      id: "",
      tenant: store.tenant_id,
      domain: store.domain,
      type: typeOf(c.type, store.domain),
      key: store.key,
      data: undefined,
      request: fromWireContext(store.context),
    };
  }
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
      target: { type: `${x.domain}.${x.aggregate_type}`, id: x.aggregate_id },
      data: unseal(decode(x.payload), c.vault).data,
      request: fromWireContext(x.context),
    };
  }
  if (c.cancel) {
    // A cancel of a scheduled command: its type, aggregate and data are the scheduled command's.
    const x = c.scheduled!;
    return {
      kind: "cancel",
      id: x.command_id,
      tenant: c.cancel.tenant_id,
      domain: x.domain,
      type: x.command_type,
      aggregateType: x.aggregate_type,
      aggregateId: x.aggregate_id,
      target: { type: `${x.domain}.${x.aggregate_type}`, id: x.aggregate_id },
      data: unseal(decode(x.payload), []).data,
      request: fromWireContext(c.cancel.context),
    };
  }
  const q = c.query!;
  return {
    kind: "query",
    id: q.query_id,
    tenant: q.tenant_id,
    domain: q.domain,
    type: q.query_type,
    ...(q.target?.aggregate_id ? { target: { type: q.target.aggregate_type, id: q.target.aggregate_id } } : {}),
    data: decode(q.payload),
    request: fromWireContext(q.context),
  };
}

function toStored(e: WireEvent, vault?: WireVaultValue[]): StoredEvent {
  const { data, redacted } = unseal(decode(e.payload), vault);
  return {
    redacted,
    position: e.global_position,
    createdAt: new Date(e.created_at),
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
    deployment: e.deployment ?? "",
  };
}
