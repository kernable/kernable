// The account aggregates: state, how each event changes it, unique keys and facts.

import { LOCKOUT_AFTER, LOCKOUT_MAX_MINUTES, MINUTE } from "../shared/constants.ts";
import { accountDomain, type AccountStatus, type SessionEnd, type TokenPurpose } from "./domain.ts";
import { AccountRegistered, EmailVerified, SessionEnded, SessionFactorAdded, SessionOpened, SessionRefreshed, SignedIn, SignInFailed, VerificationRequested } from "./events.ts";

export interface Account {
  id?: string;
  status: AccountStatus;
  email?: string;
  emailVerified: boolean;
  name?: string;
  password: { hash: string; setAt: number } | null;
  failures: { count: number; lastAt: number };
  /** Open emailed tokens; the token itself is in the vault until it is used. */
  openTokens: { tokenId: string; purpose: TokenPurpose; token: string | null; expiresAt: number }[];
}

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
  .apply(AccountRegistered, (s, d, evt) => {
    s.id = evt.aggregateId;
    s.status = d.verified ? "active" : "pending";
    s.email = d.email;
    s.emailVerified = d.verified;
    s.name = d.name;
    s.password = d.passwordHash ? { hash: d.passwordHash, setAt: d.at } : null;
  })
  .apply(VerificationRequested, (s, d) => {
    s.openTokens = s.openTokens.filter((t) => t.purpose !== "verify");
    s.openTokens.push({ tokenId: d.tokenId, purpose: "verify", token: d.token, expiresAt: d.expiresAt });
  })
  .apply(EmailVerified, (s, d) => {
    s.emailVerified = true;
    if (s.status === "pending") s.status = "active";
    s.openTokens = s.openTokens.filter((t) => t.tokenId !== d.tokenId);
  })
  .apply(SignedIn, (s) => void (s.failures = { count: 0, lastAt: 0 }))
  .apply(SignInFailed, (s, d) => {
    if (d.reason === "wrong_password") s.failures = { count: s.failures.count + 1, lastAt: d.at };
  })
  // Held from registration on, while not deleted: nobody registers an email twice.
  .unique("email", (s) => (s.status !== "none" && s.status !== "deleted" ? s.email : null), { personal: (s) => s.id })
  .fact("active", (s) => s.status === "active");

export interface Session {
  status: "none" | "active" | "ended";
  accountId?: string;
  method?: string;
  factors: string[];
  generation: number;
  refreshTokenHash?: string;
  openedAt: number;
  lastRefreshAt: number;
  endReason?: SessionEnd;
}

export const sessionAggregate = accountDomain
  .aggregate<Session>("session", { status: "none", factors: [], generation: 0, openedAt: 0, lastRefreshAt: 0 })
  .apply(SessionOpened, (s, d) => {
    s.status = "active";
    s.accountId = d.accountId;
    s.method = d.method;
    s.factors = d.factors;
    s.generation = 1;
    s.refreshTokenHash = d.refreshTokenHash;
    s.openedAt = d.at;
    s.lastRefreshAt = d.at;
  })
  .apply(SessionRefreshed, (s, d) => {
    s.generation = d.generation;
    s.refreshTokenHash = d.refreshTokenHash;
    s.lastRefreshAt = d.at;
  })
  .apply(SessionFactorAdded, (s, d) => void s.factors.push(d.factor))
  .apply(SessionEnded, (s, d) => {
    s.status = "ended";
    s.endReason = d.reason;
  });
