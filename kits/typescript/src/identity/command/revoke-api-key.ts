import { Rejected } from "@kernable/sdk";

import { identityAggregate } from "../aggregate/identity.ts";

identityAggregate.command("RevokeApiKey", (s, d: { keyId: string }) => {
  const k = s.apiKeys.find((x) => x.keyId === d.keyId);
  if (!k || k.revoked) throw new Rejected("No such key", "not_found");
  return [{ type: "ApiKeyRevoked", data: { keyId: d.keyId } }];
});
