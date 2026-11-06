import { Rejected } from "@kernable/sdk";

import { accountAggregate } from "../aggregate/account.ts";
import { verificationRequested } from "./register.ts";

accountAggregate.command("RequestVerification", (s, _d, _cmd, ctx) => {
  if (s.status !== "pending") throw new Rejected("Only a pending account is verified", "invalid_state");
  return [verificationRequested(s.email!, ctx.now().getTime())];
});
