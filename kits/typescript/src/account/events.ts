// The account events: what happened to accounts and sessions, the vocabulary other domains read.

import { Event, type PersonalFields } from "@kernable/sdk";

import { accountDomain, type AccountSuspensionReason, type SessionEnd } from "./domain.ts";

export class AccountRegistered extends Event {
  static personal: PersonalFields<AccountRegistered>[] = [{ fields: ["email", "name", "passwordHash"], subject: (_d, id) => id }];
  email!: string;
  name!: string;
  /** argon2id; null for an account without a password (passkeys, single sign-on). */
  passwordHash!: string | null;
  /** True when an accepted invitation proved the email: active at once. */
  verified!: boolean;
  at!: number;
}

/** The token stays in the vault under its own subject until it is used. */
export class VerificationRequested extends Event {
  static personal: PersonalFields<VerificationRequested>[] = [
    { fields: ["email"], subject: (_d, id) => id },
    { fields: ["token"], subject: (d) => `token:${d.tokenId}` },
  ];
  tokenId!: string;
  token!: string | null;
  email!: string | null;
  expiresAt!: number;
}

/** Erases the used token. */
export class EmailVerified extends Event {
  static erases = (d: { tokenId: string }) => `token:${d.tokenId}`;
  tokenId!: string;
}

export class SignedIn extends Event {
  static personal: PersonalFields<SignedIn>[] = [{ fields: ["ip", "userAgent"], subject: (_d, id) => id }];
  sessionId!: string;
  method!: string;
  factors!: string[];
  refreshTokenHash!: string;
  ip!: string | null;
  userAgent!: string | null;
  /** The account has a second factor on: the session must prove it before anything else. */
  mfa!: boolean;
  at!: number;
}

/** An event, not a rejection: audited, and it counts toward the lockout. */
export class SignInFailed extends Event {
  static personal: PersonalFields<SignInFailed>[] = [{ fields: ["ip"], subject: (_d, id) => id }];
  method!: string;
  reason!: "wrong_password" | "locked" | "not_verified" | "not_active" | "not_linked";
  ip!: string | null;
  at!: number;
}

export class SessionOpened extends Event {
  static personal: PersonalFields<SessionOpened>[] = [{ fields: ["ip", "userAgent"], subject: (d) => d.accountId }];
  accountId!: string;
  method!: string;
  factors!: string[];
  refreshTokenHash!: string;
  ip!: string | null;
  userAgent!: string | null;
  at!: number;
}

export class SessionRefreshed extends Event {
  static personal: PersonalFields<SessionRefreshed>[] = [{ fields: ["ip"], subject: (d) => d.accountId }];
  generation!: number;
  refreshTokenHash!: string;
  ip!: string | null;
  accountId!: string;
  /** The session's factors, so the API issues the new access token without asking. */
  factors!: string[];
  at!: number;
}

export class SessionFactorAdded extends Event {
  factor!: string;
}

export class SessionEnded extends Event {
  reason!: SessionEnd;
}

/** Ends every session (a reactor); sign-in is refused. */
export class AccountSuspended extends Event {
  reason!: AccountSuspensionReason;
  at!: number;
}

export class AccountReactivated extends Event {}

/** Erases the account's personal data, in its events and its sessions'. */
export class AccountDeleted extends Event {
  static erases = (_d: unknown, id: string) => id;
}

/** Ends the account's other sessions (a reactor); `keepSessionId` is the one it was changed from. */
export class PasswordChanged extends Event {
  static personal: PersonalFields<PasswordChanged>[] = [{ fields: ["hash"], subject: (_d, id) => id }];
  hash!: string;
  keepSessionId!: string | null;
  at!: number;
}

/** The reset token sits in the vault under its own subject until used. */
export class PasswordResetRequested extends Event {
  static personal: PersonalFields<PasswordResetRequested>[] = [
    { fields: ["email"], subject: (_d, id) => id },
    { fields: ["token"], subject: (d) => `token:${d.tokenId}` },
  ];
  tokenId!: string;
  token!: string | null;
  email!: string | null;
  expiresAt!: number;
}

/** Erases the used token; ends every session (a reactor). */
export class PasswordReset extends Event {
  static personal: PersonalFields<PasswordReset>[] = [{ fields: ["hash"], subject: (_d, id) => id }];
  static erases = (d: { tokenId: string }) => `token:${d.tokenId}`;
  tokenId!: string;
  hash!: string;
  at!: number;
}

/** The new address gets a token; it holds nothing until confirmed. */
export class EmailChangeRequested extends Event {
  static personal: PersonalFields<EmailChangeRequested>[] = [
    { fields: ["email"], subject: (_d, id) => id },
    { fields: ["token"], subject: (d) => `token:${d.tokenId}` },
  ];
  tokenId!: string;
  token!: string | null;
  email!: string | null;
  expiresAt!: number;
}

/** Erases the used token; the account now holds the new address. */
export class EmailChanged extends Event {
  static personal: PersonalFields<EmailChanged>[] = [{ fields: ["email"], subject: (_d, id) => id }];
  static erases = (d: { tokenId: string }) => `token:${d.tokenId}`;
  tokenId!: string;
  email!: string;
}

/** Someone registered with this account's address: it gets a mail saying it exists. */
export class RegistrationAttempted extends Event {
  static personal: PersonalFields<RegistrationAttempted>[] = [{ fields: ["email"], subject: (_d, id) => id }];
  email!: string | null;
  at!: number;
}

/** The TOTP secret the API generated and showed: personal, in the vault. Not in use until confirmed. */
export class TotpStarted extends Event {
  static personal: PersonalFields<TotpStarted>[] = [{ fields: ["secret"], subject: (_d, id) => id }];
  secret!: string | null;
}

/** TOTP is the account's second factor from now on (the first code's step: no code twice). */
export class TotpConfirmed extends Event {
  step!: number;
  at!: number;
}

export class TotpDisabled extends Event {}

/** The recovery codes' hashes; the codes were shown once. */
export class RecoveryCodesGenerated extends Event {
  hashes!: string[];
}

/** A session proved a second factor; a reactor adds it to the session. */
export class SecondFactorProved extends Event {
  sessionId!: string;
  method!: "totp" | "recovery" | "passkey";
  /** The TOTP code's step (no code twice), or null. */
  step!: number | null;
  /** The used recovery code's hash, or null. */
  recoveryCodeHash!: string | null;
}

/** Counts toward the lockout, like a wrong password. */
export class SecondFactorFailed extends Event {
  sessionId!: string;
  at!: number;
}

/** A passkey: its credential ID and label are personal; the public key (COSE, base64url) is not. */
export class PasskeyAdded extends Event {
  static personal: PersonalFields<PasskeyAdded>[] = [{ fields: ["credentialId", "label"], subject: (_d, id) => id }];
  credentialId!: string | null;
  publicKey!: string;
  counter!: number;
  label!: string | null;
  at!: number;
}

export class PasskeyRemoved extends Event {
  static personal: PersonalFields<PasskeyRemoved>[] = [{ fields: ["credentialId"], subject: (_d, id) => id }];
  credentialId!: string | null;
}

/** A passkey signed: its counter moves on (a cloned authenticator would fall behind). */
export class PasskeyUsed extends Event {
  static personal: PersonalFields<PasskeyUsed>[] = [{ fields: ["credentialId"], subject: (_d, id) => id }];
  credentialId!: string | null;
  counter!: number;
}

/** An external login (OAuth/OIDC, or a tenant's SSO as `sso:<id>`) linked to the account. */
export class ProviderLinked extends Event {
  static personal: PersonalFields<ProviderLinked>[] = [{ fields: ["subject", "email"], subject: (_d, id) => id }];
  provider!: string;
  subject!: string | null;
  email!: string | null;
  at!: number;
}

export class ProviderUnlinked extends Event {
  static personal: PersonalFields<ProviderUnlinked>[] = [{ fields: ["subject"], subject: (_d, id) => id }];
  provider!: string;
  subject!: string | null;
}

// --- A tenant's single sign-on (account.sso, under the tenant's ID) ---

/** The client secret is personal data of the configuration (`sso:<id>`), erased with it. */
export class SsoCreated extends Event {
  static personal: PersonalFields<SsoCreated>[] = [{ fields: ["clientSecret"], subject: (_d, id) => `sso:${id}` }];
  issuer!: string;
  clientId!: string;
  clientSecret!: string | null;
}

export class SsoUpdated extends Event {
  static personal: PersonalFields<SsoUpdated>[] = [{ fields: ["clientSecret"], subject: (_d, id) => `sso:${id}` }];
  issuer!: string;
  clientId!: string;
  clientSecret!: string | null;
}

/** `txt`: the value the domain's DNS must carry at `_kernable-sso.<domain>`. */
export class SsoDomainAdded extends Event {
  domain!: string;
  txt!: string;
}

export class SsoDomainVerified extends Event {
  domain!: string;
}

export class SsoDomainRemoved extends Event {
  domain!: string;
}

/** Enforced: in this tenant, only sessions signed in through this SSO count (owners excepted). */
export class SsoEnforcementChanged extends Event {
  enforced!: boolean;
}

export class SsoDisabled extends Event {}

export class SsoEnabled extends Event {}

export class SsoDeleted extends Event {
  static erases = (_d: unknown, id: string) => `sso:${id}`;
}

// Declared on the domain, so a process that only reads them knows them too.
accountDomain.event(ProviderLinked, ProviderUnlinked, SsoCreated, SsoUpdated, SsoDomainAdded, SsoDomainVerified, SsoDomainRemoved, SsoEnforcementChanged, SsoDisabled, SsoEnabled, SsoDeleted, PasskeyAdded, PasskeyRemoved, PasskeyUsed, TotpStarted, TotpConfirmed, TotpDisabled, RecoveryCodesGenerated, SecondFactorProved, SecondFactorFailed, AccountRegistered, VerificationRequested, EmailVerified, SignedIn, SignInFailed, SessionOpened, SessionRefreshed, SessionFactorAdded, SessionEnded, AccountSuspended, AccountReactivated, AccountDeleted, PasswordChanged, PasswordResetRequested, PasswordReset, EmailChangeRequested, EmailChanged, RegistrationAttempted);
