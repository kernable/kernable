import { Rejected } from "@kernable/sdk";

import { equalSecrets, parseToken } from "../../shared/secrets.ts";
import { accountAggregate } from "../aggregate/account.ts";

// Public: the token is the proof.
accountAggregate.command("VerifyEmail", (s, d: { token: string }, cmd, ctx) => {
  const parts = parseToken(d.token);
  const open = s.openTokens.find((t) => t.purpose === "verify" && parts && t.tokenId === parts[1]);
  if (!parts || parts[0] !== cmd.aggregateId || !open?.token || !equalSecrets(open.token, parts[2])) {
    throw new Rejected("The link is not valid", "invalid_token");
  }
  if (ctx.now().getTime() > open.expiresAt) throw new Rejected("The link has expired", "expired_token");
  return [{ type: "EmailVerified", data: { tokenId: open.tokenId } }];
});
