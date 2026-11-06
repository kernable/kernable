import { Rejected } from "@kernable/sdk";

import { sha256 } from "../../shared/secrets.ts";
import { identityAggregate } from "../aggregate/identity.ts";
import { apiKey } from "../domain.ts";
import { active } from "./checks.ts";

// The API generates the key and shows it once; only its hash stays.
identityAggregate.command("CreateApiKey", (s, d: { keyId: string; secret: string; label?: string; expiresAt?: number | null }) => {
  active(s);
  if (s.kind !== "service") throw new Rejected("Only a service has API keys", "not_a_service");
  if (typeof d.keyId !== "string" || !/^[A-Za-z0-9]{8,32}$/.test(d.keyId) || typeof d.secret !== "string" || d.secret.length < 32) {
    throw new Rejected("A key ID of 8 to 32 letters and digits, and a secret of at least 32 characters", "invalid_key");
  }
  if (s.apiKeys.some((k) => k.keyId === d.keyId)) throw new Rejected("The key exists", "exists");
  return [{
    type: "ApiKeyCreated",
    data: { keyId: d.keyId, hash: sha256(apiKey(d.keyId, d.secret)), label: d.label ?? "", expiresAt: d.expiresAt ?? null },
  }];
});
