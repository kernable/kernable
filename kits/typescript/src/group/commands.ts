// The group commands: each class with its handler.

import { Command, Rejected } from "@kernable/sdk";

import { groupAggregate } from "./aggregates.ts";
import type { Permission } from "./domain.ts";
import { GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated } from "./events.ts";

export function validPattern(p: unknown): p is string {
  return typeof p === "string" && (p === "*" || /^[a-z][\w-]*\.(\*|[A-Za-z]\w*)$/.test(p));
}

/** The permissions, checked and normalised. */
export function permissions(value: unknown): Permission[] {
  if (!Array.isArray(value)) throw new Rejected("permissions is a list", "invalid_permissions");
  return value.map((p: Permission) => {
    if (!p || !validPattern(p.pattern)) throw new Rejected(`Not a type pattern: ${p?.pattern}`, "invalid_permissions");
    if (p.when !== undefined && (typeof p.when !== "string" || !/^[a-z]\w*$/.test(p.when))) {
      throw new Rejected(`Not a relation: ${p.when}`, "invalid_permissions");
    }
    const out: Permission = { pattern: p.pattern };
    if (p.when) out.when = p.when;
    if (p.when && p.viaParent) out.viaParent = true;
    return out;
  });
}

export function name(value: unknown): string {
  if (typeof value !== "string" || !value.trim() || value.length > 100 || value.trim().toLowerCase() === "owners") {
    throw new Rejected("A group needs a name other than owners", "invalid_name");
  }
  return value.trim();
}

/** The middleware lets the sender hand out only permissions it holds. */
export class CreateGroup extends Command {
  name!: string;
  description?: string;
  permissions!: Permission[];
}

groupAggregate.command(CreateGroup, (s, d) => {
  if (s.status !== "none") throw new Rejected("The group exists", "exists");
  return [GroupCreated.new({ name: name(d.name), description: d.description ?? "", permissions: permissions(d.permissions) })];
});

/** Its memberships stop counting at once (the middleware); a reactor removes them later. */
export class DeleteGroup extends Command {}

groupAggregate.command(DeleteGroup, (s) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [GroupDeleted.new()];
});

export class SetPermissions extends Command {
  permissions!: Permission[];
}

groupAggregate.command(SetPermissions, (s, d) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [GroupPermissionsSet.new({ permissions: permissions(d.permissions) })];
});

export class UpdateGroup extends Command {
  name?: string;
  description?: string;
}

groupAggregate.command(UpdateGroup, (s, d) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [GroupUpdated.new({ name: d.name === undefined ? undefined : name(d.name), description: d.description })];
});
