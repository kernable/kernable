import { Rejected } from "@kernable/sdk";

import { sessionAggregate } from "../aggregate/session.ts";
import type { SessionOpened } from "../domain.ts";

// Internal: the sessions reactor opens it after SignedIn.
sessionAggregate.command("OpenSession", (s, d: SessionOpened) => {
  if (s.status !== "none") throw new Rejected("The session exists", "exists");
  return [{ type: "SessionOpened", data: d }];
});
