import { Rejected } from "@kernable/sdk";

import { sessionAggregate } from "../aggregate/session.ts";
import type { SessionEnd } from "../domain.ts";

sessionAggregate.command("EndSession", (s, d: { reason?: SessionEnd }) => {
  if (s.status !== "active") throw new Rejected("The session has ended", "invalid_state");
  return [{ type: "SessionEnded", data: { reason: d?.reason ?? "signed_out" } }];
});
