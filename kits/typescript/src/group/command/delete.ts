import { Rejected } from "@kernable/sdk";

import { groupAggregate } from "../aggregate/group.ts";

// Its memberships stop counting at once (the middleware); a reactor removes them later.
groupAggregate.command("DeleteGroup", (s) => {
  if (s.status !== "active") throw new Rejected("No such group", "not_found");
  return [{ type: "GroupDeleted" }];
});
