// The account events: what happened to accounts and sessions, the vocabulary other domains read.

import { Event, type PersonalFields } from "@kernable/sdk";

import type { AccountSuspensionReason, SessionEnd } from "./domain.ts";

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
