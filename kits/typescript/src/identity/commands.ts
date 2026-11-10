// The identity commands: each class with its handler.

import { Command, type PersonalFields, Rejected } from "@kernable/sdk";

import { chars, sha256, trimSpace } from "../shared/secrets.ts";
import { type Identity, identityAggregate } from "./aggregates.ts";
import { apiKey, type Membership, type Scope } from "./domain.ts";
import { ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended, MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated } from "./events.ts";

export function active(s: Identity): void {
  if (s.status !== "active") throw new Rejected("The identity is not active", "invalid_state");
}

export function scope(value: unknown): Scope | null {
  if (value === null || value === undefined) return null;
  const sc = value as Scope;
  if (typeof sc.type === "string" && /^[a-z][\w-]*\.[\w-]+$/.test(sc.type) && typeof sc.id === "string" && sc.id) return { type: sc.type, id: sc.id };
  throw new Rejected("A scope is { type: 'domain.aggregate', id }", "invalid_scope");
}

/** The API generates the key and shows it once; only its hash stays. */
export class CreateApiKey extends Command {
  static secret: (keyof CreateApiKey)[] = ["secret"];
  keyId!: string;
  secret!: string;
  label?: string;
  expiresAt?: number | null;
}

identityAggregate.command(CreateApiKey, (s, d) => {
  active(s);
  if (s.kind !== "service" && s.kind !== "process") throw new Rejected("Only a service or a process has API keys", "not_a_service");
  if (typeof d.keyId !== "string" || !/^[A-Za-z0-9]{8,32}$/.test(d.keyId) || typeof d.secret !== "string" || chars(d.secret) < 32) {
    throw new Rejected("A key ID of 8 to 32 letters and digits, and a secret of at least 32 characters", "invalid_key");
  }
  if (s.apiKeys.some((k) => k.keyId === d.keyId)) throw new Rejected("The key exists", "exists");
  return [ApiKeyCreated.new({ keyId: d.keyId, hash: sha256(apiKey(d.keyId, d.secret)), label: d.label ?? "", expiresAt: d.expiresAt ?? null })];
});

export class CreateServiceIdentity extends Command {
  static personal: PersonalFields<CreateServiceIdentity>[] = [{ fields: ["displayName"], subject: (_d, id) => `identity:${id}` }];
  displayName!: string;
}

identityAggregate.command(CreateServiceIdentity, (s, d) => {
  if (s.status !== "none") throw new Rejected("The identity exists", "exists");
  if (typeof d.displayName !== "string" || !trimSpace(d.displayName)) throw new Rejected("A service needs a name", "invalid_name");
  return [IdentityCreated.new({ kind: "service", accountId: null, displayName: trimSpace(d.displayName), title: null, email: null })];
});

/**
 * Internal: the identity of a business process (`shop.fulfilment`) the kernel runs, created by
 * the `identity.processes` reactor in the platform tenant when the process is registered. It
 * holds no permissions until a platform admin grants it groups.
 */
export class CreateProcessIdentity extends Command {
  process!: string;
}

identityAggregate.command(CreateProcessIdentity, (s, d) => {
  if (s.status !== "none") throw new Rejected("The identity exists", "exists");
  if (typeof d.process !== "string" || !/^[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z0-9_-]+$/.test(d.process)) {
    throw new Rejected("A process is named <domain>.<name>", "invalid_process");
  }
  return [IdentityCreated.new({ kind: "process", accountId: null, displayName: d.process, title: null, email: null })];
});

/** Internal: from an accepted invitation or a self-service tenant. */
export class CreateIdentity extends Command {
  static personal: PersonalFields<CreateIdentity>[] = [{ fields: ["displayName", "title", "email"], subject: (_d, id) => `identity:${id}` }];
  accountId!: string;
  displayName?: string;
  title?: string;
  email?: string;
}

identityAggregate.command(CreateIdentity, (s, d) => {
  if (s.status !== "none") throw new Rejected("The identity exists", "exists");
  if (typeof d.accountId !== "string" || !d.accountId) throw new Rejected("Which account?", "invalid_account");
  return [IdentityCreated.new({ kind: "person", accountId: d.accountId, displayName: d.displayName ?? "", title: d.title ?? null, email: d.email ?? null })];
});

/**
 * Erases the identity's personal data. Never while a tenant holds it as owner: atomic,
 * through the tenant's `owner` key (an absent requirement).
 */
export class DeleteIdentity extends Command {}

identityAggregate.command(DeleteIdentity, (s) => {
  if (s.status === "none" || s.status === "deleted") throw new Rejected("No such identity", "not_found");
  return [IdentityDeleted.new()];
}, {
  requires: (_d, s) =>
    s.id ? [{ domain: "tenancy", aggregateType: "tenant", key: "owner", value: s.id, fact: "exists", absent: true }] : [],
});

/** The middleware lets the sender grant only groups whose permissions it holds. */
export class GrantMembership extends Command {
  membershipId!: string;
  group!: string;
  scope?: Scope | null;
  expiresAt?: number | null;
}

identityAggregate.command(GrantMembership, (s, d) => {
  active(s);
  if (typeof d.membershipId !== "string" || !d.membershipId || typeof d.group !== "string" || !d.group) {
    throw new Rejected("A membership needs an ID and a group", "invalid_membership");
  }
  const sc = scope(d.scope);
  const same = (m: Membership) => m.group === d.group && JSON.stringify(m.scope) === JSON.stringify(sc);
  if (s.memberships.some((m) => m.membershipId === d.membershipId || same(m))) throw new Rejected("Already a member", "exists");
  return [MembershipGranted.new({ membershipId: d.membershipId, group: d.group, scope: sc, expiresAt: d.expiresAt ?? null })];
}, {
  // The group must be active, in this tenant, at commit.
  requires: (d) => [{ domain: "group", aggregateType: "group", aggregateId: d.group, fact: "active" }],
});

export class ReactivateIdentity extends Command {}

identityAggregate.command(ReactivateIdentity, (s) => {
  if (s.status !== "suspended") throw new Rejected("Only a suspended identity", "invalid_state");
  return [IdentityReactivated.new()];
});

export class ResumeMembership extends Command {
  membershipId!: string;
}

identityAggregate.command(ResumeMembership, (s, d) => {
  const m = s.memberships.find((x) => x.membershipId === d.membershipId);
  if (!m || m.status !== "suspended") throw new Rejected("No such suspended membership", "not_found");
  return [MembershipResumed.new({ membershipId: d.membershipId })];
});

export class RevokeApiKey extends Command {
  keyId!: string;
}

identityAggregate.command(RevokeApiKey, (s, d) => {
  const k = s.apiKeys.find((x) => x.keyId === d.keyId);
  if (!k || k.revoked) throw new Rejected("No such key", "not_found");
  return [ApiKeyRevoked.new({ keyId: d.keyId })];
});

export class RevokeMembership extends Command {
  membershipId!: string;
}

identityAggregate.command(RevokeMembership, (s, d) => {
  const m = s.memberships.find((x) => x.membershipId === d.membershipId);
  if (!m) throw new Rejected("No such membership", "not_found");
  return [MembershipRevoked.new({ membershipId: d.membershipId })];
});

export class SuspendMembership extends Command {
  membershipId!: string;
}

identityAggregate.command(SuspendMembership, (s, d) => {
  const m = s.memberships.find((x) => x.membershipId === d.membershipId);
  if (!m || m.status !== "active") throw new Rejected("No such active membership", "not_found");
  return [MembershipSuspended.new({ membershipId: d.membershipId })];
});

export class SuspendIdentity extends Command {
  reason?: string;
}

identityAggregate.command(SuspendIdentity, (s, d) => {
  active(s);
  return [IdentitySuspended.new({ reason: d?.reason ?? "by_group" })];
});

export class UpdateProfile extends Command {
  static personal: PersonalFields<UpdateProfile>[] = [{ fields: ["displayName", "title", "email"], subject: (_d, id) => `identity:${id}` }];
  displayName?: string;
  title?: string;
  email?: string;
}

identityAggregate.command(UpdateProfile, (s, d) => {
  active(s);
  return [ProfileUpdated.new({ displayName: d.displayName ?? null, title: d.title ?? null, email: d.email ?? null })];
});
