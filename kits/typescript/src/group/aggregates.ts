// The group aggregates: state, how each event changes it, unique keys and facts.

import { groupDomain, type Permission } from "./domain.ts";
import { GroupCreated, GroupDeleted, GroupPermissionsSet, GroupUpdated } from "./events.ts";

export interface Group {
  status: "none" | "active" | "deleted";
  name?: string;
  description?: string;
  permissions: Permission[];
}

export const groupAggregate = groupDomain
  .aggregate<Group>("group", { status: "none", permissions: [] })
  .apply(GroupCreated, (s, d) => {
    s.status = "active";
    s.name = d.name;
    s.description = d.description;
    s.permissions = d.permissions;
  })
  .apply(GroupUpdated, (s, d) => {
    if (d.name !== undefined) s.name = d.name;
    if (d.description !== undefined) s.description = d.description;
  })
  .apply(GroupPermissionsSet, (s, d) => void (s.permissions = d.permissions))
  .apply(GroupDeleted, (s) => void (s.status = "deleted"))
  .unique("name", (s) => (s.status === "active" ? s.name?.trim().toLowerCase() : null))
  .fact("active", (s) => s.status === "active");
