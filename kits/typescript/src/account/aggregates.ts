// The account aggregates: state, how each event changes it, unique keys and facts.

import { LOCKOUT_AFTER, LOCKOUT_MAX_MINUTES, MINUTE } from "../shared/constants.ts";
import { accountDomain, type AccountStatus, type AccountSuspensionReason, type SessionEnd, type TokenPurpose } from "./domain.ts";
import { ProviderLinked, ProviderUnlinked, SsoCreated, SsoDeleted, SsoDisabled, SsoDomainAdded, SsoDomainRemoved, SsoDomainVerified, SsoEnabled, SsoEnforcementChanged, SsoUpdated, PasskeyAdded, PasskeyRemoved, PasskeyUsed, RecoveryCodesGenerated, SecondFactorFailed, SecondFactorProved, TotpConfirmed, TotpDisabled, TotpStarted, EmailChangeRequested, EmailChanged, PasswordChanged, PasswordReset, PasswordResetRequested, AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailVerified, SessionEnded, SessionFactorAdded, SessionOpened, SessionRefreshed, SignedIn, SignInFailed, VerificationRequested } from "./events.ts";

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
  suspension: { reason: AccountSuspensionReason; at: number } | null;
  /** TOTP: its secret (vault), whether confirmed, the last step used (no code twice). */
  totp: { secret: string | null; confirmed: boolean; lastStep: number } | null;
  /** Passkeys: credential ID (vault), COSE public key (base64url), signature counter, label (vault). */
  passkeys: { credentialId: string | null; publicKey: string; counter: number; label: string | null }[];
  /** External logins: provider and subject (vault). */
  links: { provider: string; subject: string | null }[];
  /** SHA-256 of the unused recovery codes. */
  recoveryCodes: string[];
  /** A new address waiting for its confirmation: it holds no key yet. */
  pendingEmail?: string | null;
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
    suspension: null,
    totp: null,
    recoveryCodes: [],
    passkeys: [],
    links: [],
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
  .apply(AccountSuspended, (s, d) => {
    s.status = "suspended";
    s.suspension = { reason: d.reason, at: d.at };
  })
  .apply(AccountReactivated, (s) => {
    s.status = "active";
    s.suspension = null;
  })
  .apply(AccountDeleted, (s) => {
    s.status = "deleted";
    s.password = null;
    s.openTokens = [];
  })
  .apply(PasswordChanged, (s, d) => void (s.password = { hash: d.hash, setAt: d.at }))
  .apply(PasswordResetRequested, (s, d) => {
    s.openTokens = s.openTokens.filter((t) => t.purpose !== "reset");
    s.openTokens.push({ tokenId: d.tokenId, purpose: "reset", token: d.token, expiresAt: d.expiresAt });
  })
  .apply(PasswordReset, (s, d) => {
    s.password = { hash: d.hash, setAt: d.at };
    s.failures = { count: 0, lastAt: 0 };
    s.openTokens = s.openTokens.filter((t) => t.tokenId !== d.tokenId);
  })
  .apply(EmailChangeRequested, (s, d) => {
    s.pendingEmail = d.email;
    s.openTokens = s.openTokens.filter((t) => t.purpose !== "email_change");
    s.openTokens.push({ tokenId: d.tokenId, purpose: "email_change", token: d.token, expiresAt: d.expiresAt });
  })
  .apply(EmailChanged, (s, d) => {
    s.email = d.email;
    s.emailVerified = true;
    s.pendingEmail = null;
    s.openTokens = s.openTokens.filter((t) => t.tokenId !== d.tokenId);
  })
  .apply(TotpStarted, (s, d) => void (s.totp = { secret: d.secret, confirmed: false, lastStep: 0 }))
  .apply(TotpConfirmed, (s, d) => {
    if (s.totp) {
      s.totp.confirmed = true;
      s.totp.lastStep = d.step;
    }
  })
  .apply(TotpDisabled, (s) => {
    s.totp = null;
    s.recoveryCodes = [];
  })
  .apply(RecoveryCodesGenerated, (s, d) => void (s.recoveryCodes = d.hashes))
  .apply(SecondFactorProved, (s, d) => {
    if (d.step !== null && s.totp) s.totp.lastStep = d.step;
    if (d.recoveryCodeHash) s.recoveryCodes = s.recoveryCodes.filter((h) => h !== d.recoveryCodeHash);
    s.failures = { count: 0, lastAt: 0 };
  })
  .apply(SecondFactorFailed, (s, d) => void (s.failures = { count: s.failures.count + 1, lastAt: d.at }))
  .apply(PasskeyAdded, (s, d) => void s.passkeys.push({ credentialId: d.credentialId, publicKey: d.publicKey, counter: d.counter, label: d.label }))
  .apply(PasskeyRemoved, (s, d) => void (s.passkeys = s.passkeys.filter((p) => p.credentialId !== d.credentialId)))
  .apply(PasskeyUsed, (s, d) => {
    const p = s.passkeys.find((x) => x.credentialId === d.credentialId);
    if (p) p.counter = d.counter;
  })
  .apply(ProviderLinked, (s, d) => void s.links.push({ provider: d.provider, subject: d.subject }))
  .apply(ProviderUnlinked, (s, d) => void (s.links = s.links.filter((l) => !(l.provider === d.provider && l.subject === d.subject))))
  .apply(SignedIn, (s) => void (s.failures = { count: 0, lastAt: 0 }))
  .apply(SignInFailed, (s, d) => {
    if (d.reason === "wrong_password") s.failures = { count: s.failures.count + 1, lastAt: d.at };
  })
  // Held from registration on, while not deleted: nobody registers an email twice.
  // Each external login belongs to one account.
  .unique("link", (s) => (s.status === "deleted" ? null : s.links.filter((l) => l.subject).map((l) => `${l.provider}:${l.subject}`)), { personal: (s) => s.id })
  .unique("email", (s) => (s.status !== "none" && s.status !== "deleted" ? s.email : null), { personal: (s) => s.id })
  .fact("active", (s) => s.status === "active")
  .fact("mfa", (s) => s.totp?.confirmed === true);

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

// --- account.sso: a tenant's single sign-on, under the tenant's ID ---

export interface Sso {
  status: "none" | "active" | "disabled" | "deleted";
  issuer?: string;
  clientId?: string;
  clientSecret: string | null;
  domains: { domain: string; txt: string; verified: boolean }[];
  enforced: boolean;
}

export const ssoAggregate = accountDomain
  .aggregate<Sso>("sso", { status: "none", clientSecret: null, domains: [], enforced: false })
  .apply(SsoCreated, (s, d) => {
    s.status = "active";
    s.issuer = d.issuer;
    s.clientId = d.clientId;
    s.clientSecret = d.clientSecret;
  })
  .apply(SsoUpdated, (s, d) => {
    s.issuer = d.issuer;
    s.clientId = d.clientId;
    s.clientSecret = d.clientSecret;
  })
  .apply(SsoDomainAdded, (s, d) => void s.domains.push({ domain: d.domain, txt: d.txt, verified: false }))
  .apply(SsoDomainVerified, (s, d) => {
    const x = s.domains.find((y) => y.domain === d.domain);
    if (x) x.verified = true;
  })
  .apply(SsoDomainRemoved, (s, d) => void (s.domains = s.domains.filter((x) => x.domain !== d.domain)))
  .apply(SsoEnforcementChanged, (s, d) => void (s.enforced = d.enforced))
  .apply(SsoDisabled, (s) => void (s.status = "disabled"))
  .apply(SsoEnabled, (s) => void (s.status = "active"))
  .apply(SsoDeleted, (s) => {
    s.status = "deleted";
    s.clientSecret = null;
    s.domains = [];
  })
  // A verified domain belongs to one tenant's SSO.
  .unique("domain", (s) => (s.status === "deleted" ? null : s.domains.filter((x) => x.verified).map((x) => x.domain)), { scope: "global" });
