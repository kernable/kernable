import { Rejected } from "@kernable/sdk";

import { groupAggregate } from "../aggregate/group.ts";
import type { Permission } from "../domain.ts";
import { name, permissions } from "./checks.ts";

// The middleware lets the sender hand out only permissions it holds.
groupAggregate.command("CreateGroup", (s, d: { name: string; description?: string; permissions: Permission[] }) => {
  if (s.status !== "none") throw new Rejected("The group exists", "exists");
  return [{ type: "GroupCreated", data: { name: name(d.name), description: d.description ?? "", permissions: permissions(d.permissions) } }];
});
