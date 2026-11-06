import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

// Internal: from an accepted invitation or a self-service tenant.
identityAggregate.command("CreateIdentity", (s, d: { accountId: string; displayName?: string; title?: string; email?: string }) => {
  if (s.status !== "none") throw new Rejected("The identity exists", "exists");
  if (typeof d.accountId !== "string" || !d.accountId) throw new Rejected("Which account?", "invalid_account");
  return [{
    type: "IdentityCreated",
    data: { kind: "person", accountId: d.accountId, displayName: d.displayName ?? "", title: d.title ?? null, email: d.email ?? null },
  }];
});
