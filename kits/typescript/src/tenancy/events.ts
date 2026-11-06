// The tenancy events: what happened to tenants, the vocabulary other domains read.

import { Event } from "@kernable/sdk";

import { tenancyDomain, type Owner, type SuspensionReason } from "./domain.ts";

export class TenantCreated extends Event {
  slug!: string;
  name!: string;
  /** `{ email }` when the platform created it, `{ accountId }` by self-service. */
  owner!: Owner;
  status!: "active" | "pending";
}

export class TenantActivated extends Event {}

export class TenantRenamed extends Event {
  name!: string;
}

export class TenantSlugChanged extends Event {
  slug!: string;
}

export class TenantSuspended extends Event {
  reason!: SuspensionReason;
  /** In the payload: the retention before deletion is computed from it. */
  at!: number;
}

export class TenantReactivated extends Event {}

export class TenantDeleted extends Event {}

export class OwnerAdded extends Event {
  identityId!: string;
}

export class OwnerRemoved extends Event {
  identityId!: string;
}

export class MfaRequirementChanged extends Event {
  required!: boolean;
}

export class SignUpPolicyChanged extends Event {
  tenants!: "off" | "pending" | "active";
  accounts!: "off" | "open";
}

// Declared on the domain, so a process that only reads them knows them too.
tenancyDomain.event(TenantCreated, TenantActivated, TenantRenamed, TenantSlugChanged, TenantSuspended, TenantReactivated, TenantDeleted, OwnerAdded, OwnerRemoved, MfaRequirementChanged, SignUpPolicyChanged);
