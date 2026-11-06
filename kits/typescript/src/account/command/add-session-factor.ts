import { Rejected } from "@kernable/sdk";

import { sessionAggregate } from "../aggregate/session.ts";

// Internal: after a second factor was proved on the account.
sessionAggregate.command("AddSessionFactor", (s, d: { factor: string }) => {
  if (s.status !== "active") throw new Rejected("The session has ended", "invalid_state");
  return s.factors.includes(d.factor) ? [] : [{ type: "SessionFactorAdded", data: { factor: d.factor } }];
});
