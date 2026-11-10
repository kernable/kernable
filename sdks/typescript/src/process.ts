// Business processes: a builder for the model the kernel runs (design/processes.md, "The model
// format"). The SDK only builds and checks what it can; the kernel parses conditions, checks
// types, fields and grants, and runs the cases.

import { boundOrThrow, typeOf as messageType, type CommandClass, type EventClass, type QueryClass } from "./messages.ts";

/** A literal in a mapping or a model: `{ value: <json> }`; build it with `value(…)`. */
export interface Literal {
  value: unknown;
}

/**
 * A literal for a mapping, where a plain string would be a reference:
 * `data: { reason: value("unpaid") }` sends the string `unpaid`, while `"case.reason"` sends
 * the case's field. Any JSON value; throws for `undefined`.
 */
export function value(x: unknown): Literal {
  if (x === undefined) throw new Error("value(undefined): a literal is a JSON value");
  return { value: x };
}

/**
 * Field → reference or literal: `{ orderId: "case.orderId", reason: value("unpaid") }`. A
 * reference is `aggregateId` or `data.<path>` (the event being handled, in `start` and `wait`),
 * `case.<path>` (the case's data), or in a query step `answer` / `answer.<path>`. Paths are wire
 * names (camelCase), as the messages' fields.
 */
export type Mapping = Record<string, string | Literal>;

/** A duration: a positive whole number of seconds, minutes, hours or days: `30s`, `15m`, `12h`, `3d`. */
export type Duration = `${number}${"s" | "m" | "h" | "d"}`;

/** A message given by class or by name; see `ProcessBuilder`. */
type Named<C> = C | string;

/** The options of `ProcessBuilder.start`. */
export interface StartOptions {
  /** Where the case's key is in the event: `aggregateId` or `data.<field>`. One case per key. */
  key: string;
  /** The case's data, from the event: `{ orderId: "aggregateId", total: "data.total" }`. */
  data?: Mapping;
}

/** The options of `ProcessBuilder.cancelOn`. */
export interface CancelOnOptions {
  /** Where the case's key is in the event: `aggregateId` or `data.<field>`. */
  key: string;
  /** The end step the case ends at: `closed`. */
  end: string;
}

/** The options of `ProcessBuilder.command`. */
export interface CommandStepOptions {
  /** The aggregate the command goes to, a reference: `case.orderId`. */
  aggregate: string;
  /** The command's data, from the case: `{ orderId: "case.orderId", reason: value("unpaid") }`. */
  data?: Mapping;
  /** The step to go to per rejection code: `{ out_of_stock: "notify" }`. Any other rejection is an incident. */
  rejected?: Record<string, string>;
  /** The step after the command committed. Default: the next step written. */
  next?: string;
}

/** The options of `ProcessBuilder.wait`. */
export interface WaitOptions {
  /** Where the case's key is in the awaited event: `aggregateId` or `data.<field>`. */
  key: string;
  /** Merged into the case's data, from the event: `{ paidAt: "data.at" }`. */
  data?: Mapping;
  /** How long to wait (`3d`); with `onTimeout`, the step to go to then. */
  timeout?: Duration;
  /** The step after `timeout`; required with it. */
  onTimeout?: string;
  /** The step after the event came. Default: the next step written. */
  next?: string;
}

/** When a timer step fires: `{ after: "1d" }`, or `{ at: "case.dueAt" }` (a reference to a time in ms since epoch). */
export type TimerWhen = { after: Duration; at?: never } | { at: string; after?: never };

/** The options of `ProcessBuilder.timer`. */
export interface TimerOptions {
  /** The step after the timer fired. Default: the next step written. */
  next?: string;
}

/** One case of a decision: if the condition `when` holds, go to `then`. */
export interface DecideCase {
  /** A condition on the case's data: `case.total > 1000`. */
  when: string;
  /** The step to go to if it holds. */
  then: string;
}

/**
 * The options of `ProcessBuilder.decide`: one condition (`when`, `then`) or several in order
 * (`cases`, the first that holds wins); `otherwise` if none holds, by default the step written next.
 */
export type DecideOptions =
  | { when: string; then: string; otherwise?: string; cases?: never }
  | { cases: DecideCase[]; otherwise?: string; when?: never; then?: never };

/** The options of `ProcessBuilder.query`. */
export interface QueryStepOptions {
  /** The query's data, from the case: `{ orderId: "case.orderId" }`. */
  data?: Mapping;
  /** For an answer that is an object: the path of the field to match `paths` against. Default: the answer itself. */
  answer?: string;
  /**
   * The step per answer: `{ true: "approved", false: "paid" }`. Keys `true` and `false` stand for
   * the booleans, numeric keys for numbers, others for strings. For a string answer that looks
   * like a boolean or a number, give pairs instead: `[["true", "approved"], ["1", "paid"]]`.
   */
  paths: Record<string, string> | [unknown, string][];
  /** A case field to keep the answer in: `approval` (read as `case.approval` later). */
  into?: string;
  /** The step for an answer no path matches. Without it, such an answer is an incident. */
  otherwise?: string;
}

/** A process model, as the kernel registers it; see "The model format" in design/processes.md. */
export interface ProcessModel {
  /** Qualified: `shop.fulfilment`. */
  name: string;
  start: { on: string; key: string; data?: Mapping };
  /** In the order written. */
  steps: ProcessStep[];
  /** Events that end a running case at once, whatever step it is at. */
  cancelOn?: { event: string; key: string; end: string }[];
}

/** One step of a `ProcessModel`; every step but `end` names what comes next. */
export type ProcessStep =
  | { name: string; kind: "command"; command: string; aggregate: string; data?: Mapping; rejected?: Record<string, string>; next: string }
  | { name: string; kind: "wait"; event: string; key: string; data?: Mapping; timeoutMs?: number; onTimeout?: string; next: string }
  | { name: string; kind: "timer"; afterMs?: number; at?: string; next: string }
  | { name: string; kind: "decide"; when: { if: string; next: string }[]; otherwise: string }
  | { name: string; kind: "query"; query: string; data?: Mapping; answer?: string; paths: { value: unknown; next: string }[]; into?: string; otherwise?: string }
  | { name: string; kind: "end" };

/** What the kernel answered to a registration: `version` from 1; `new` false if the model equals the newest version. */
export interface ProcessVersion {
  /** Qualified: `shop.fulfilment`. */
  name: string;
  version: number;
  /** False: the model equals the newest version, nothing was registered. */
  new: boolean;
}

/** A step as written, before `model()` resolves the order and the types. */
type Written =
  | { name: string; kind: "command"; command: Named<CommandClass<unknown>>; options: CommandStepOptions }
  | { name: string; kind: "wait"; event: Named<EventClass<unknown>>; options: WaitOptions }
  | { name: string; kind: "timer"; when: TimerWhen; options: TimerOptions }
  | { name: string; kind: "decide"; options: DecideOptions }
  | { name: string; kind: "query"; query: Named<QueryClass<unknown>>; options: QueryStepOptions }
  | { name: string; kind: "end" };

const NAME = /^[A-Za-z0-9_-]+$/;
const DOMAIN = /^[A-Za-z][A-Za-z0-9_-]*$/;
const DURATION = /^([0-9]+)(s|m|h|d)$/;
const UNIT_MS = { s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 } as const;

/**
 * A business process of a domain, built in code and run by the kernel: a case starts when an
 * event commits, and moves through steps (commands, waits for events, timers, decisions by the
 * case's data or by a query) to an end. Create it with `Domain.process(name)`; `Kernable.app`
 * registers the processes of its domains.
 *
 * Steps run in the order written, unless a step names `next`; branching steps (`decide`,
 * `query`) and error paths (`rejected`, `onTimeout`) name their targets. Step names are
 * required and stable: versions, the UI and the statistics know steps by them. References are
 * strings (`aggregateId`, `data.total`, `case.orderId`, `answer.approved`), literals `value(…)`,
 * conditions short expressions (`case.total > 1000`) that the kernel parses when registering.
 *
 * Messages are classes or names. A command class gives `<domain>.<aggregateType>.<Type>` from
 * the aggregate that handles it, an event or query class `<domain>.<Type>`; by name, an
 * unqualified one is of the process's domain (`OrderApproved`, `order.Ship`), others qualified
 * (`payments.PaymentReceived`, `shop.order.Ship`). Classes are resolved by `model()`, so they
 * may be bound to their aggregates and read models after the process is defined.
 *
 * ```ts
 * export const fulfilment = shop.process("fulfilment")
 *   .start(OrderSubmitted, { key: "aggregateId", data: { orderId: "aggregateId", total: "data.total" } })
 *   .command("confirm", Confirm, { aggregate: "case.orderId" })
 *   .decide("large", { when: "case.total > 1000", then: "approval", otherwise: "paid" })
 *   .query("approval", NeedsApproval, { data: { orderId: "case.orderId" }, paths: { true: "approved", false: "paid" } })
 *   .wait("approved", OrderApproved, { key: "aggregateId" })
 *   .wait("paid", "payments.PaymentReceived", { key: "data.orderId", timeout: "3d", onTimeout: "cancel" })
 *   .command("ship", Ship, { aggregate: "case.orderId", rejected: { out_of_stock: "notify" }, next: "done" })
 *   .command("notify", NotifyDelay, { aggregate: "case.orderId", next: "done" })
 *   .command("cancel", Cancel, { aggregate: "case.orderId", data: { reason: value("unpaid") } })
 *   .end("done");
 * ```
 */
export class ProcessBuilder {
  /** The name of the domain it belongs to: `shop`. */
  readonly domain: string;
  /** The process's name within its domain: `fulfilment`. */
  readonly name: string;
  private started?: { event: Named<EventClass<unknown>>; options: StartOptions };
  private readonly written: Written[] = [];
  private readonly cancels: { event: Named<EventClass<unknown>>; options: CancelOnOptions }[] = [];

  /** Use `Domain.process(name)`. Throws for a name other than letters, digits, `_` and `-`. */
  constructor(domain: string, name: string) {
    if (!NAME.test(name)) throw new Error(`invalid process name ${JSON.stringify(name)}: letters, digits, _ and - only`);
    this.domain = domain;
    this.name = name;
  }

  /** The qualified name, as the kernel knows it: `shop.fulfilment`. */
  get qualified(): string {
    return `${this.domain}.${this.name}`;
  }

  /**
   * Starts a case when an `event` commits: `key` says where the case's key is in the event
   * (`aggregateId`, or `data.orderId`; one case per key), `data` maps the event to the case's
   * first data. A process has one start; it goes to the first step written.
   */
  start<E>(event: EventClass<E> | string, options: StartOptions): this {
    if (this.started) throw new Error(`process ${this.qualified}: start is given already`);
    this.started = { event, options };
    return this;
  }

  /**
   * Sends a `command` as the process, to the aggregate `options.aggregate` names, with
   * `options.data` built from the case. A rejection goes to the step `options.rejected` names for
   * its code; any other rejection is an incident. Then `options.next`, else the next step written.
   */
  command<C>(name: string, command: CommandClass<C> | string, options: CommandStepOptions): this {
    return this.add({ name, kind: "command", command, options });
  }

  /**
   * Waits for an `event` of this case (`options.key` says where the case's key is in it) and
   * merges `options.data` from it into the case's data. With `timeout`, goes to `onTimeout` if it
   * does not come in time. Then `options.next`, else the next step written.
   */
  wait<E>(name: string, event: EventClass<E> | string, options: WaitOptions): this {
    return this.add({ name, kind: "wait", event, options });
  }

  /**
   * Waits for a time: `{ after: "1d" }` from when the case reached the step, or
   * `{ at: "case.dueAt" }` (a reference to a time in ms since epoch). Then `options.next`, else
   * the next step written.
   */
  timer(name: string, when: TimerWhen, options: TimerOptions = {}): this {
    return this.add({ name, kind: "timer", when, options });
  }

  /**
   * Picks a path by the case's data: `{ when: "case.total > 1000", then: "approval", otherwise: "paid" }`,
   * or several conditions in order, `{ cases: [{ when, then }, …], otherwise }`. Conditions are
   * references to the case (`case.<field>`), literals, `== != < <= > >=`, `and`, `or`, `not` and
   * parentheses; anything more is a query.
   */
  decide(name: string, options: DecideOptions): this {
    return this.add({ name, kind: "decide", options });
  }

  /**
   * Asks a read model a `query` with `options.data` built from the case; the answer (or its field
   * `options.answer`) picks the step in `options.paths`, else `options.otherwise`. `options.into`
   * keeps the answer in the case's data.
   */
  query<Q>(name: string, query: QueryClass<Q> | string, options: QueryStepOptions): this {
    return this.add({ name, kind: "query", query, options });
  }

  /**
   * Ends a running case at once when an `event` of its key commits, whatever step it is at:
   * `.cancelOn("AccountClosed", { key: "aggregateId", end: "closed" })`. Its timer is withdrawn, an
   * outcome it still awaits is ignored, an incident ends with it. Events the case's own commands
   * cause do not count. `end` must be an `end` step.
   */
  cancelOn<E>(event: EventClass<E> | string, options: CancelOnOptions): this {
    this.cancels.push({ event, options });
    return this;
  }

  /** Ends the case. A process may have several ends, e.g. `shipped` and `cancelled`. */
  end(name: string): this {
    return this.add({ name, kind: "end" });
  }

  private add(step: Written): this {
    this.written.push(step);
    return this;
  }

  /**
   * The model the kernel registers (see "The model format"): types qualified, durations in ms,
   * the order written resolved into explicit `next`s. Checks what it can before the kernel does,
   * and throws naming the process and the step: no start, no steps, a malformed or repeated step
   * name, a target that names no step, a step other than `end` without a successor, a malformed
   * duration, `timeout` without `onTimeout` or the other way round. Conditions and references are
   * the kernel's to check.
   */
  model(): ProcessModel {
    const process = this.qualified;
    const fail = (step: string | undefined, reason: string): never => {
      throw new Error(`process ${process}${step === undefined ? "" : `, step ${JSON.stringify(step)}`}: ${reason}`);
    };
    if (!this.started) fail(undefined, "no start: begin with .start(Event, { key })");
    if (this.written.length === 0) fail(undefined, "no steps");
    const names = new Set<string>();
    for (const s of this.written) {
      if (!NAME.test(s.name)) fail(s.name, "a step name has letters, digits, _ and - only");
      if (names.has(s.name)) fail(s.name, "a second step of this name");
      names.add(s.name);
    }
    const target = (step: string, what: string, to: string | undefined): string => {
      if (to === undefined || to === "") fail(step, `${what} names no step`);
      if (!names.has(to!)) fail(step, `${what} names an unknown step ${JSON.stringify(to)}`);
      return to!;
    };
    const steps = this.written.map((s, i): ProcessStep => {
      const following = (next: string | undefined) => {
        if (next !== undefined) return target(s.name, "next", next);
        const after = this.written[i + 1];
        if (!after) fail(s.name, "the last step is not an end: add .end(…) or name its next");
        return after.name;
      };
      switch (s.kind) {
        case "command": {
          const o = s.options;
          const rejected = o.rejected && Object.fromEntries(Object.entries(o.rejected).map(([code, to]) => [code, target(s.name, `rejected.${code}`, to)]));
          return {
            name: s.name,
            kind: "command",
            command: this.commandType(s.name, s.command),
            aggregate: o.aggregate,
            ...(o.data && { data: o.data }),
            ...(rejected && { rejected }),
            next: following(o.next),
          };
        }
        case "wait": {
          const o = s.options;
          if ((o.timeout === undefined) !== (o.onTimeout === undefined)) fail(s.name, "timeout and onTimeout go together");
          return {
            name: s.name,
            kind: "wait",
            event: this.messageName(s.name, s.event, "declared (an aggregate's apply, or Domain.event)"),
            key: o.key,
            ...(o.data && { data: o.data }),
            ...(o.timeout !== undefined && { timeoutMs: durationMs(o.timeout, (reason) => fail(s.name, reason)) }),
            ...(o.onTimeout !== undefined && { onTimeout: target(s.name, "onTimeout", o.onTimeout) }),
            next: following(o.next),
          };
        }
        case "timer": {
          const w = s.when as { after?: Duration; at?: string };
          if ((w.after === undefined) === (w.at === undefined)) fail(s.name, "a timer waits either after a duration or until a time (at)");
          return {
            name: s.name,
            kind: "timer",
            ...(w.after !== undefined ? { afterMs: durationMs(w.after, (reason) => fail(s.name, reason)) } : { at: w.at }),
            next: following(s.options.next),
          };
        }
        case "decide": {
          const o = s.options;
          const cases = o.cases ?? (o.when === undefined ? [] : [{ when: o.when, then: o.then! }]);
          if (cases.length === 0) fail(s.name, "a decision needs a condition: when and then, or cases");
          return {
            name: s.name,
            kind: "decide",
            when: cases.map((c, n) => ({ if: c.when, next: target(s.name, cases.length === 1 && !o.cases ? "then" : `cases[${n}].then`, c.then) })),
            otherwise: o.otherwise !== undefined ? target(s.name, "otherwise", o.otherwise) : following(undefined),
          };
        }
        case "query": {
          const o = s.options;
          const pairs: [unknown, string][] = Array.isArray(o.paths) ? o.paths : Object.entries(o.paths).map(([k, to]) => [answerValue(k), to]);
          if (pairs.length === 0) fail(s.name, "a query step needs paths");
          return {
            name: s.name,
            kind: "query",
            query: this.messageName(s.name, s.query, "answered by a read model"),
            ...(o.data && { data: o.data }),
            ...(o.answer !== undefined && { answer: o.answer }),
            paths: pairs.map(([v, to]) => ({ value: v, next: target(s.name, `paths[${JSON.stringify(v)}]`, to) })),
            ...(o.into !== undefined && { into: o.into }),
            ...(o.otherwise !== undefined && { otherwise: target(s.name, "otherwise", o.otherwise) }),
          };
        }
        case "end":
          return { name: s.name, kind: "end" };
      }
    });
    const start = this.started!;
    return {
      name: process,
      start: {
        on: this.messageName(undefined, start.event, "declared (an aggregate's apply, or Domain.event)"),
        key: start.options.key,
        ...(start.options.data && { data: start.options.data }),
      },
      steps,
      ...(this.cancels.length > 0 && {
        cancelOn: this.cancels.map((c) => {
          const end = this.written.find((s) => s.name === c.options.end);
          if (!end) fail(undefined, `cancelOn names an unknown step ${JSON.stringify(c.options.end)}`);
          if (end!.kind !== "end") fail(undefined, `cancelOn ends at ${JSON.stringify(c.options.end)}, which is no end step`);
          return { event: this.messageName(undefined, c.event, "declared (an aggregate's apply, or Domain.event)"), key: c.options.key, end: c.options.end };
        }),
      }),
    };
  }

  /** An event or query type: `<domain>.<Type>`; an unqualified name is of the process's domain. */
  private messageName(step: string | undefined, message: Named<Function>, what: string): string {
    const where = `process ${this.qualified}${step === undefined ? "" : `, step ${JSON.stringify(step)}`}`;
    if (typeof message !== "string") return `${boundOrThrow(message, what).domain}.${messageType(message)}`;
    const parts = message.split(".");
    if (parts.length === 1 && NAME.test(message)) return `${this.domain}.${message}`;
    if (parts.length === 2 && DOMAIN.test(parts[0]) && NAME.test(parts[1])) return message;
    throw new Error(`${where}: ${JSON.stringify(message)} is no type: Type or domain.Type`);
  }

  /** A command type: `<domain>.<aggregateType>.<Type>`; `aggregate.Type` is of the process's domain. */
  private commandType(step: string, command: Named<Function>): string {
    if (typeof command !== "string") {
      const b = boundOrThrow(command, "handled by an aggregate");
      return `${b.domain}.${b.aggregateType}.${messageType(command)}`;
    }
    const parts = command.split(".");
    if (parts.length === 2 && parts.every((p) => NAME.test(p))) return `${this.domain}.${command}`;
    if (parts.length === 3 && DOMAIN.test(parts[0]) && NAME.test(parts[1]) && NAME.test(parts[2])) return command;
    throw new Error(`process ${this.qualified}, step ${JSON.stringify(step)}: ${JSON.stringify(command)} is no command type: aggregate.Type or domain.aggregate.Type`);
  }
}

/** `30s`, `15m`, `12h`, `3d` in ms; `fail` with the reason otherwise. */
export function durationMs(duration: string, fail: (reason: string) => never = (r) => { throw new Error(r); }): number {
  const m = typeof duration === "string" ? DURATION.exec(duration) : null;
  if (!m || Number(m[1]) === 0) return fail(`${JSON.stringify(duration)} is no duration: a positive number with s, m, h or d (30s, 15m, 12h, 3d)`);
  return Number(m[1]) * UNIT_MS[m[2] as keyof typeof UNIT_MS];
}

/** A key of `paths` as the value it matches: `true`, `false`, a number, else the string. */
function answerValue(key: string): unknown {
  if (key === "true") return true;
  if (key === "false") return false;
  if (key.trim() !== "" && Number.isFinite(Number(key)) && String(Number(key)) === key) return Number(key);
  return key;
}
