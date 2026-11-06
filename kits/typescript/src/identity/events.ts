// The identity events: what happened to identities, the vocabulary other domains read.

import { Event, type PersonalFields } from "@kernable/sdk";

import type { Scope } from "./domain.ts";

export class IdentityCreated extends Event {
  static personal: PersonalFields<IdentityCreated>[] = [{ fields: ["displayName", "title", "email"], subject: (_d, id) => `identity:${id}` }];
  kind!: "person" | "service";
  accountId!: string | null;
  displayName!: string | null;
  title!: string | null;
  email!: string | null;
}

export class ProfileUpdated extends Event {
  static personal: PersonalFields<ProfileUpdated>[] = [{ fields: ["displayName", "title", "email"], subject: (_d, id) => `identity:${id}` }];
  displayName!: string | null;
  title!: string | null;
  email!: string | null;
}

export class MembershipGranted extends Event {
  membershipId!: string;
  group!: string;
  /** null: the whole tenant; else one aggregate (or what has it as parent). */
  scope!: Scope | null;
  expiresAt!: number | null;
}

export class MembershipSuspended extends Event {
  membershipId!: string;
}

export class MembershipResumed extends Event {
  membershipId!: string;
}

export class MembershipRevoked extends Event {
  membershipId!: string;
}

export class ApiKeyCreated extends Event {
  keyId!: string;
  /** SHA-256 of the whole key: it has high entropy, so the hash needs no vault. */
  hash!: string;
  label!: string;
  expiresAt!: number | null;
}

export class ApiKeyRevoked extends Event {
  keyId!: string;
}

export class IdentitySuspended extends Event {
  reason!: string;
}

export class IdentityReactivated extends Event {}

/** Erases the identity's personal data. */
export class IdentityDeleted extends Event {
  static erases = (_d: unknown, id: string) => `identity:${id}`;
}
