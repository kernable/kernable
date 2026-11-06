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
  at!: number;
}

/** An event, not a rejection: audited, and it counts toward the lockout. */
export class SignInFailed extends Event {
  static personal: PersonalFields<SignInFailed>[] = [{ fields: ["ip"], subject: (_d, id) => id }];
  method!: string;
  reason!: "wrong_password" | "locked" | "not_verified" | "not_active";
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

// Declared on the domain, so a process that only reads them knows them too.
accountDomain.event(AccountRegistered, VerificationRequested, EmailVerified, SignedIn, SignInFailed, SessionOpened, SessionRefreshed, SessionFactorAdded, SessionEnded, AccountSuspended, AccountReactivated, AccountDeleted, PasswordChanged, PasswordResetRequested, PasswordReset, EmailChangeRequested, EmailChanged, RegistrationAttempted);
