import { Rejected } from "@kernable/sdk";

import { groupAggregate } from "../aggregate/group.ts";
import type { Permission } from "../domain.ts";
import { permissions } from "./checks.ts";

// The whole list in one event: simpler to read in the audit.
groupAggregate.command("SetPermissions", (s, d: { permissions: Permission[] }) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [{ type: "GroupPermissionsSet", data: { permissions: permissions(d.permissions) } }];
});
