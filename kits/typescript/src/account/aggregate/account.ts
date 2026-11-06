// The Account aggregate: state, how events change it, its unique key and fact. Commands are in
// ../command/.

import { LOCKOUT_AFTER, LOCKOUT_MAX_MINUTES, MINUTE } from "../../shared/constants.ts";
import { accountDomain, type Account, type AccountRegistered, type VerificationRequested } from "../domain.ts";

/** Until when the account is locked after failed sign-ins: computed, never stored. */
export function lockedUntil(s: Account): number {
  if (s.failures.count < LOCKOUT_AFTER) return 0;
  const minutes = Math.min(2 ** (s.failures.count - LOCKOUT_AFTER), LOCKOUT_MAX_MINUTES);
  return s.failures.lastAt + minutes * MINUTE;
}

export const accountAggregate = accountDomain
  .aggregate<Account>("account", {
    status: "none",
    emailVerified: false,
    password: null,
    failures: { count: 0, lastAt: 0 },
    openTokens: [],
  })
  .apply("AccountRegistered", (s, d: AccountRegistered, evt) => {
    s.id = evt.aggregateId;
    s.status = d.verified ? "active" : "pending";
    s.email = d.email;
    s.emailVerified = d.verified;
    s.name = d.name;
    s.password = d.passwordHash ? { hash: d.passwordHash, setAt: d.at } : null;
  })
  .apply("VerificationRequested", (s, d: VerificationRequested) => {
    s.openTokens = s.openTokens.filter((t) => t.purpose !== "verify");
    s.openTokens.push({ tokenId: d.tokenId, purpose: "verify", token: d.token, expiresAt: d.expiresAt });
  })
  .apply("EmailVerified", (s, d: { tokenId: string }) => {
    s.emailVerified = true;
    if (s.status === "pending") s.status = "active";
    s.openTokens = s.openTokens.filter((t) => t.tokenId !== d.tokenId);
  })
  .apply("SignedIn", (s) => void (s.failures = { count: 0, lastAt: 0 }))
  .apply("SignInFailed", (s, d: { reason: string; at: number }) => {
    if (d.reason === "wrong_password") s.failures = { count: s.failures.count + 1, lastAt: d.at };
  })
  // Held from registration on, while not deleted: nobody registers an email twice.
  .unique("email", (s) => (s.status !== "none" && s.status !== "deleted" ? s.email : null), { personal: (s) => s.id })
  .fact("active", (s) => s.status === "active");
