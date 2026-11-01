// Test helper: the identity domain of the vault tests, with a personal field and secrets.

import { createHash } from "node:crypto";

import { Rejected, domain } from "../src/index.ts";

export const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export function identityDomain() {
  const identity = domain("identity");
  identity.command("RegisterAccount").personal(["email"], (d: { id: string }) => d.id).secret(["password"]);
  identity.event("AccountRegistered", 1).personal(["email"], (d: { accountId: string }) => d.accountId);
  identity.event("AccountErased", 1).erases((d: { accountId: string }) => d.accountId);
  const account = identity.aggregate("Account", { email: null as string | null, passwordHash: "" })
    .apply("AccountRegistered", (_s, data: { email: string; passwordHash: string }) => ({ email: data.email, passwordHash: data.passwordHash }))
    .apply("AccountErased", () => ({ email: null, passwordHash: "" }))
    .command("RegisterAccount", (_s, data: { id: string; email: string; password: string }) => [
      // The handler gets the secret in clear and keeps only a hash.
      { type: "AccountRegistered", data: { accountId: data.id, email: data.email, passwordHash: sha256(data.password) } },
    ])
    .command("Login", (s, data: { password: string }) => {
      if (sha256(data.password) !== s.passwordHash) throw new Rejected("wrong password", "denied");
      return [{ type: "LoggedIn" }];
    })
    .command("EraseAccount", (_s, data: { id: string }) => [{ type: "AccountErased", data: { accountId: data.id } }]);
  identity.command("Login").secret(["password"]);
  return { identity, account };
}
