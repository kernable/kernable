import { Rejected } from "@kernable/sdk";

import { groupAggregate } from "../aggregate/group.ts";
import { name } from "./checks.ts";

groupAggregate.command("UpdateGroup", (s, d: { name?: string; description?: string }) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [{ type: "GroupUpdated", data: { name: d.name === undefined ? undefined : name(d.name), description: d.description } }];
});
