// Relations derived from events: who stands in which relation to an aggregate. Declare them
// next to your aggregates, before the kit's authorization listener starts:
//
//   relation("shop.order", "owner").set("shop.OrderPlaced", (e) => e.data.customer);
//   parent("funds.vehicle").from("funds.VehicleCreated", (e) => ({ type: "funds.fund", id: e.data.fund }));

export interface RelationEvent {
  data: any;
  aggregateId: string;
  tenant: string;
}

type Ids = (e: RelationEvent) => string | string[] | null | undefined;

export interface RelationRule {
  /** The aggregate type, qualified: `shop.order`. */
  type: string;
  name: string;
  /** The event type, qualified: `shop.OrderPlaced`. */
  event: string;
  op: "set" | "unset";
  ids: Ids;
}

export interface ParentRule {
  type: string;
  event: string;
  parent: (e: RelationEvent) => { type: string; id: string } | null | undefined;
}

export const relationRules: RelationRule[] = [];
export const parentRules: ParentRule[] = [];

const qualified = /^[a-z][\w-]*\.[\w-]+$/;

function check(type: string, event: string): void {
  if (!qualified.test(type)) throw new Error(`a relation's aggregate type is qualified (shop.order): ${type}`);
  if (!qualified.test(event)) throw new Error(`a relation's event type is qualified (shop.OrderPlaced): ${event}`);
}

/** Declares relation `name` of aggregates of `type`, set and unset by events of that aggregate. */
export function relation(type: string, name: string) {
  if (!/^[a-z][\w]*$/.test(name)) throw new Error(`a relation's name is lowercase: ${name}`);
  if (["self", "owners"].includes(name)) throw new Error(`${name} is built in`);
  const builder = {
    /** `ids(event)` gains the relation: one identity ID, a list, or none. */
    set(event: string, ids: Ids) {
      check(type, event);
      relationRules.push({ type, name, event, op: "set", ids });
      return builder;
    },
    /** `ids(event)` loses it. */
    unset(event: string, ids: Ids) {
      check(type, event);
      relationRules.push({ type, name, event, op: "unset", ids });
      return builder;
    },
  };
  return builder;
}

/** Declares the parent of aggregates of `type`, for permissions `viaParent`. */
export function parent(type: string) {
  return {
    from(event: string, parent: ParentRule["parent"]) {
      check(type, event);
      parentRules.push({ type, event, parent });
    },
  };
}

/** The relation names declared for `type`. */
export function declared(type?: string): Set<string> {
  return new Set(relationRules.filter((r) => !type || r.type === type).map((r) => r.name));
}

/** Every event type a relation or parent is derived from. */
export function relationEvents(): string[] {
  return [...new Set([...relationRules.map((r) => r.event), ...parentRules.map((p) => p.event)])];
}
