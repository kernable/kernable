// account: one Account per person and a Session per sign-in, in the platform tenant
// (design/kits.md "account"). The domain object, its types and vault declarations; `index.ts`
// registers the parts.

import { domain } from "@kernable/sdk";

export const accountDomain = domain("account");

// Vault fields: what a command brings in clear but must not keep (`secret`), and what
// belongs to the person (`personal`, the account as subject) or to one emailed token.
const own = (_d: unknown, id: string) => id;
const ofToken = (d: { tokenId: string }) => `token:${d.tokenId}`;
const ofAccount = (d: { accountId: string }) => d.accountId;
accountDomain.command("RegisterAccount").personal(["email", "name"], own).secret(["password"]);
accountDomain.command("VerifyEmail").secret(["token"]);
// The IP and user agent stay with the events (personal); the command keeps them only until it completes.
accountDomain.command("SignIn").secret(["password", "refreshToken", "ip", "userAgent"]);
accountDomain.command("RefreshSession").secret(["refreshToken", "newRefreshToken", "ip"]);
accountDomain.command("OpenSession").secret(["ip", "userAgent"]);
accountDomain.event("AccountRegistered", 1).personal(["email", "name", "passwordHash"], own);
accountDomain.event("VerificationRequested", 1).personal(["email"], own).personal(["token"], ofToken);
accountDomain.event("EmailVerified", 1).erases(ofToken);
accountDomain.event("SignedIn", 1).personal(["ip", "userAgent"], own);
accountDomain.event("SignInFailed", 1).personal(["ip"], own);
accountDomain.event("SessionOpened", 1).personal(["ip", "userAgent"], ofAccount);
accountDomain.event("SessionRefreshed", 1).personal(["ip"], ofAccount);

export type AccountStatus = "none" | "pending" | "active" | "suspended" | "deleted";
export type TokenPurpose = "verify" | "reset" | "email_change";

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

export type SessionEnd = "signed_out" | "revoked" | "account_suspended" | "password_changed" | "account_deleted" | "reuse" | "idle" | "expired";

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

// Event payloads.
export interface AccountRegistered { email: string; name: string; passwordHash: string | null; verified: boolean; at: number }
export interface VerificationRequested { tokenId: string; token: string | null; email: string | null; expiresAt: number }
export interface SignedIn {
  sessionId: string; method: string; factors: string[]; refreshTokenHash: string;
  ip: string | null; userAgent: string | null; at: number;
}
export interface SessionOpened {
  accountId: string; method: string; factors: string[]; refreshTokenHash: string;
  ip: string | null; userAgent: string | null; at: number;
}
