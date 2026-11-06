// Events, commands and queries as classes: defined once, typed, and used by reference instead of
// by name (`apply(TenantCreated, …)`, `new TenantCreated({ … })`). Strings keep working.

/** A message's data: the instance fields of its class, without methods. */
export type Fields<T> = {
  [K in keyof T as K extends `__${string}` ? never : T[K] extends (...args: any[]) => any ? never : K]: T[K];
};

/** A message without fields takes no data: `new TenantActivated()`. */
type Args<T> = {} extends Fields<T> ? [data?: Fields<T>] : [data: Fields<T>];

/** Whose personal data fields hold: from the payload and the aggregate's ID. */
export type SubjectOf<T> = (data: Fields<T>, aggregateId: string) => string;

/** Fields kept in the vault as personal data of `subject`. */
export interface PersonalFields<T = any> {
  fields: readonly (keyof Fields<T> & string)[];
  subject: SubjectOf<T>;
}

abstract class Message {
  /** Protected: create a message with `Type.new({ … })`, which checks its fields. */
  protected constructor() {}

  /**
   * A new message of this class with `data` as its fields: `TenantCreated.new({ slug, name })`.
   * A class without fields takes no data. The fields are typed by the class, so a missing or
   * unknown field does not compile. Field defaults (`status = "active"`) apply where `data`
   * leaves a field out.
   */
  static new<T extends Message>(this: { prototype: T }, ...[data]: Args<T>): T {
    const message = new (this as unknown as new () => T)();
    if (data) Object.assign(message, data);
    return message;
  }
}

/**
 * An event: what happened, in the past tense. Its class name is its type within the domain of the
 * aggregate that writes it; its fields are its payload.
 *
 * ```ts
 * export class TenantCreated extends Event {
 *   slug!: string;
 *   name!: string;
 * }
 * tenant.apply(TenantCreated, (state, data) => { state.slug = data.slug; });
 * return [TenantCreated.new({ slug, name })];
 * ```
 *
 * Statics declare what `Domain.event` declares: `version` (default 1), `upcasts` (from each older
 * version), `personal` fields and `erases`. `type` overrides the class name, e.g. if a bundler
 * renames classes.
 */
export abstract class Event extends Message {}

/**
 * A command: what should happen, imperative. Its class name is its type within the domain of the
 * aggregate that handles it; its fields are its data. Statics: `personal` and `secret` fields,
 * `type`.
 *
 * ```ts
 * export class SignIn extends Command {
 *   static secret: (keyof SignIn)[] = ["password"];
 *   email!: string;
 *   password!: string;
 * }
 * account.command(SignIn, (state, data) => …);
 * await kernable.submit(SignIn.new({ email, password }), { aggregateId });
 * ```
 */
export abstract class Command extends Message {}

/**
 * A query: what is asked, with `R` its answer. Its class name is its type within the domain of
 * the read model that answers it.
 *
 * ```ts
 * export class GetTenant extends Query<TenantRow> {
 *   tenantId!: string;
 * }
 * tenants.query(GetTenant, (data) => rows.get(data.tenantId));
 * const row = await kernable.query(GetTenant.new({ tenantId }), { tenant });   // a TenantRow
 * ```
 */
export abstract class Query<R = unknown> extends Message {
  /** Carries the answer's type; never set. */
  declare readonly __answer?: R;
}

/** The statics a message class may declare. */
export interface MessageStatics {
  /** The type, if not the class name. */
  type?: string;
  version?: number;
  upcasts?: Record<number, (data: any) => unknown>;
  personal?: readonly { fields: readonly string[]; subject: (data: any, aggregateId: string) => string }[];
  erases?: (data: any, aggregateId: string) => string;
  secret?: readonly string[];
}

/** A message class, by its instances' type (its constructor is protected: create with `.new`). */
type MessageClass<M> = Function & { prototype: M } & MessageStatics;
export type EventClass<E> = MessageClass<E>;
export type CommandClass<C> = MessageClass<C>;
export type QueryClass<Q> = MessageClass<Q>;
export type AnswerOf<Q> = Q extends Query<infer R> ? R : unknown;

/** Where a class was put to use: its domain, and for a command its aggregate type. */
export interface Binding {
  domain: string;
  aggregateType?: string;
  /** The domain's event and command schemas: what upcasts and seals its messages. */
  schemas?: unknown;
}

const BINDING = Symbol.for("kernable.binding");

/** Whether `value` is a message class rather than a type name. */
export function isMessageClass(value: unknown): value is EventClass<unknown> {
  return typeof value === "function";
}

/**
 * The type a class stands for: `static type`, else its name. A name that is no type (a bundler
 * renamed the class) throws, so a renamed class never writes events under a wrong type.
 */
export function typeOf(cls: Function & MessageStatics): string {
  const type = Object.prototype.hasOwnProperty.call(cls, "type") ? cls.type : cls.name;
  if (typeof type !== "string" || !/^[A-Z][A-Za-z0-9_]{1,}$/.test(type)) {
    throw new Error(`${String(type)} is no message type (a class renamed by a bundler?): set \`static type = "…"\` on it`);
  }
  return type;
}

/** Records where `cls` is used; a class belongs to one domain (and one aggregate type). */
export function bind(cls: Function, binding: Binding): void {
  const known = binding as Binding;
  const own = Object.prototype.hasOwnProperty.call(cls, BINDING) ? (cls as any)[BINDING] as Binding : undefined;
  if (own && (own.domain !== known.domain || (known.aggregateType && own.aggregateType && own.aggregateType !== known.aggregateType))) {
    throw new Error(`${cls.name} is used by ${own.domain}${own.aggregateType ? `.${own.aggregateType}` : ""} already`);
  }
  Object.defineProperty(cls, BINDING, { value: { ...own, ...known }, configurable: true });
}

/** Where `cls` is used, if anywhere yet. */
export function bindingOf(cls: Function): Binding | undefined {
  return Object.prototype.hasOwnProperty.call(cls, BINDING) ? (cls as any)[BINDING] : undefined;
}

/** `cls`'s binding, or an error that names what to do. */
export function boundOrThrow(cls: Function, what: string): Binding {
  const b = bindingOf(cls);
  if (!b) throw new Error(`${cls.name} is not ${what} yet: import the module that registers it before using it`);
  return b;
}

/** A message instance's data, as plain JSON fields. */
export function dataOf(message: object): Record<string, unknown> {
  return Object.fromEntries(Object.entries(message).filter(([, v]) => v !== undefined));
}
