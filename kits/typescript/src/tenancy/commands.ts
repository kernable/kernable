// The tenancy commands: each class with its handler.

import { Command, type CommandMeta, Rejected } from "@kernable/sdk";

import { DAY, DELETE_AFTER_DAYS, PLATFORM_TENANT, RESERVED_SLUGS } from "../shared/constants.ts";
import { chars, normalEmail, trimSpace, validEmail } from "../shared/secrets.ts";
import { type Tenant, tenantAggregate } from "./aggregates.ts";
import { type Owner, SUSPENSION_REASONS, type SuspensionReason } from "./domain.ts";
import { MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated, TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended } from "./events.ts";

export function slug(value: unknown, tenantId: string): string {
  const shaped = typeof value === "string" && value.length >= 3 && value.length <= 63 && /^[a-z0-9](-?[a-z0-9])*$/.test(value);
  if (!shaped || (tenantId !== PLATFORM_TENANT && RESERVED_SLUGS.includes(value))) {
    throw new Rejected("A slug is 3 to 63 lowercase letters, digits and single dashes, and not reserved", "invalid_slug");
  }
  return value;
}

export function name(value: unknown): string {
  if (typeof value !== "string" || !trimSpace(value) || chars(value) > 200) throw new Rejected("A tenant needs a name", "invalid_name");
  return trimSpace(value);
}

/** A tenant's aggregate ID is its tenant ID. */
export function itself(cmd: CommandMeta): void {
  if (cmd.aggregateId !== cmd.tenant) throw new Rejected("A tenant lives under its own tenant ID", "invalid_tenant");
}

export function exists(s: Tenant): void {
  if (s.status === "none" || s.status === "deleted") throw new Rejected("No such tenant", "not_found");
}

export class ActivateTenant extends Command {}

tenantAggregate.command(ActivateTenant, (s) => {
  if (s.status !== "pending") throw new Rejected("Only a pending tenant can be activated", "invalid_state");
  return [TenantActivated.new()];
});

export class AddOwner extends Command {
  identityId!: string;
}

tenantAggregate.command(AddOwner, (s, d) => {
  exists(s);
  if (typeof d.identityId !== "string" || !d.identityId) throw new Rejected("Which identity?", "invalid_identity");
  if (s.owners.includes(d.identityId)) throw new Rejected("Already an owner", "exists");
  return [OwnerAdded.new({ identityId: d.identityId })];
}, {
  // The identity must be active in this tenant, checked by the kernel at commit.
  requires: (d) => [{ domain: "identity", aggregateType: "identity", aggregateId: d.identityId, fact: "active" }],
});

export class ChangeSignUpPolicy extends Command {
  tenants!: "off" | "pending" | "active";
  accounts!: "off" | "open";
}

tenantAggregate.command(ChangeSignUpPolicy, (s, d, cmd) => {
  if (cmd.aggregateId !== PLATFORM_TENANT) throw new Rejected("Only the platform tenant has a sign-up policy", "platform");
  exists(s);
  if (!["off", "pending", "active"].includes(d.tenants) || !["off", "open"].includes(d.accounts)) {
    throw new Rejected("tenants: off, pending or active; accounts: off or open", "invalid_policy");
  }
  return [SignUpPolicyChanged.new({ tenants: d.tenants, accounts: d.accounts })];
});

export class ChangeTenantSlug extends Command {
  slug!: string;
}

tenantAggregate.command(ChangeTenantSlug, (s, d, cmd) => {
  if (s.status !== "active") throw new Rejected("Only an active tenant changes its slug", "invalid_state");
  return [TenantSlugChanged.new({ slug: slug(d.slug, cmd.aggregateId) })];
});

export class CreateTenant extends Command {
  slug!: string;
  name!: string;
  /** `{ email }` when the platform creates it (an owner invitation follows), `{ accountId }` by self-service. */
  owner!: Owner;
  /** `active`, or `pending` for a self-service tenant waiting for approval (the middleware holds it to the sign-up policy). */
  status?: "active" | "pending";
}

tenantAggregate.command(CreateTenant, (s, d, cmd) => {
  itself(cmd);
  if (s.status !== "none") throw new Rejected("The tenant exists", "exists");
  const owner = d.owner as Partial<{ email: string; accountId: string }> | undefined;
  let first: Owner;
  if (typeof owner?.accountId === "string" && owner.accountId) first = { accountId: owner.accountId };
  else if (validEmail(normalEmail(owner?.email))) first = { email: normalEmail(owner?.email) };
  else if (cmd.aggregateId === PLATFORM_TENANT) first = { email: "" }; // its owners come from the bootstrap
  else throw new Rejected("A tenant needs an owner: an email or an account", "invalid_owner");
  return [TenantCreated.new({
    slug: slug(d.slug, cmd.aggregateId), name: name(d.name), owner: first, status: d.status === "pending" ? "pending" : "active",
  })];
});

export class DeleteTenant extends Command {}

// Final: the read models drop the tenant, reactors erase its identities, the slug becomes free;
// the events stay.
tenantAggregate.command(DeleteTenant, (s, _d, cmd, ctx) => {
  if (cmd.aggregateId === PLATFORM_TENANT) throw new Rejected("The platform tenant cannot be deleted", "platform");
  if (s.status !== "suspended" || !s.suspension) throw new Rejected("Only a suspended tenant is deleted", "invalid_state");
  if (ctx.now().getTime() < s.suspension.at + DELETE_AFTER_DAYS * DAY) {
    throw new Rejected(`A tenant is deleted ${DELETE_AFTER_DAYS} days after its suspension`, "too_early");
  }
  return [TenantDeleted.new()];
});

export class ReactivateTenant extends Command {}

tenantAggregate.command(ReactivateTenant, (s) => {
  if (s.status !== "suspended" || s.suspension?.reason === "rejected") {
    throw new Rejected("Only a suspended tenant is reactivated", "invalid_state");
  }
  return [TenantReactivated.new()];
});

export class RemoveOwner extends Command {
  identityId!: string;
}

tenantAggregate.command(RemoveOwner, (s, d) => {
  exists(s);
  if (!s.owners.includes(d.identityId)) throw new Rejected("Not an owner", "not_found");
  if (s.owners.length === 1) throw new Rejected("The last owner cannot be removed", "last_owner");
  return [OwnerRemoved.new({ identityId: d.identityId })];
});

export class RenameTenant extends Command {
  name!: string;
}

tenantAggregate.command(RenameTenant, (s, d) => {
  if (s.status !== "pending" && s.status !== "active") throw new Rejected("Cannot rename this tenant", "invalid_state");
  return [TenantRenamed.new({ name: name(d.name) })];
});

export class RequireMfa extends Command {
  required!: boolean;
}

tenantAggregate.command(RequireMfa, (s, d) => {
  if (s.status !== "active") throw new Rejected("Only an active tenant", "invalid_state");
  return [MfaRequirementChanged.new({ required: d.required === true })];
});

export class SuspendTenant extends Command {
  reason!: SuspensionReason;
}

tenantAggregate.command(SuspendTenant, (s, d, cmd, ctx) => {
  if (cmd.aggregateId === PLATFORM_TENANT) throw new Rejected("The platform tenant cannot be suspended", "platform");
  if (s.status !== "pending" && s.status !== "active") throw new Rejected("Cannot suspend this tenant", "invalid_state");
  if (!SUSPENSION_REASONS.includes(d.reason)) throw new Rejected("Unknown reason", "invalid_reason");
  if ((d.reason === "rejected") !== (s.status === "pending")) {
    throw new Rejected("A pending tenant is rejected; an active one suspended for another reason", "invalid_reason");
  }
  return [TenantSuspended.new({ reason: d.reason, at: ctx.now().getTime() })];
});
