// The Group aggregate: state, how events change it, its unique key and fact. Commands are in
// ../command/.

import { groupDomain, type Group, type GroupCreated, type Permission } from "../domain.ts";

export const groupAggregate = groupDomain
  .aggregate<Group>("group", { status: "none", permissions: [] })
  .apply("GroupCreated", (s, d: GroupCreated) => {
    s.status = "active";
    s.name = d.name;
    s.description = d.description;
    s.permissions = d.permissions;
  })
  .apply("GroupUpdated", (s, d: { name?: string; description?: string }) => {
    if (d.name !== undefined) s.name = d.name;
    if (d.description !== undefined) s.description = d.description;
  })
  .apply("GroupPermissionsSet", (s, d: { permissions: Permission[] }) => void (s.permissions = d.permissions))
  .apply("GroupDeleted", (s) => void (s.status = "deleted"))
  .unique("name", (s) => (s.status === "active" ? s.name?.trim().toLowerCase() : null))
  .fact("active", (s) => s.status === "active");
