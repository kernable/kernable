"""The tenancy commands: each class with its handler."""

import re
from typing import Literal

from kernable import Command, Rejected, Requirement

from ..shared.constants import DAY, DELETE_AFTER_DAYS, PLATFORM_TENANT, RESERVED_SLUGS, now_ms
from ..shared.secrets import normal_email, trim_space, valid_email
from .aggregates import Tenant
from .domain import SUSPENSION_REASONS, Owner, SignUpPolicy, SuspensionReason
from .events import (MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated,
                     TenantDeleted, TenantReactivated, TenantRenamed, TenantSlugChanged, TenantSuspended)


def slug(value: object, tenant_id: str) -> str:
    shaped = isinstance(value, str) and 3 <= len(value) <= 63 and re.fullmatch(r"[a-z0-9](-?[a-z0-9])*", value)
    if not shaped or (tenant_id != PLATFORM_TENANT and value in RESERVED_SLUGS):
        raise Rejected("A slug is 3 to 63 lowercase letters, digits and single dashes, and not reserved", "invalid_slug")
    return value  # type: ignore[return-value]


def name(value: object) -> str:
    if not isinstance(value, str) or not trim_space(value) or len(value) > 200:
        raise Rejected("A tenant needs a name", "invalid_name")
    return trim_space(value)


def itself(cmd) -> None:
    """A tenant's aggregate ID is its tenant ID."""
    if cmd.aggregate_id != cmd.tenant:
        raise Rejected("A tenant lives under its own tenant ID", "invalid_tenant")


def exists(s: Tenant) -> None:
    if s.status in ("none", "deleted"):
        raise Rejected("No such tenant", "not_found")


class CreateTenant(Command):
    slug: str
    name: str
    #: `{"email": ...}` when the platform creates it (an owner invitation follows), `{"account_id": ...}` by self-service.
    owner: Owner
    #: `active`, or `pending` for a self-service tenant waiting for approval (the middleware holds it to the sign-up policy).
    status: Literal["active", "pending"] | None = None


@Tenant.command(CreateTenant)
def _(s: Tenant, d: CreateTenant, cmd):
    itself(cmd)
    if s.status != "none":
        raise Rejected("The tenant exists", "exists")
    owner = d.owner if isinstance(d.owner, dict) else {}
    if isinstance(owner.get("account_id"), str) and owner["account_id"]:
        first: Owner = {"account_id": owner["account_id"]}
    elif valid_email(normal_email(owner.get("email"))):
        first = {"email": normal_email(owner.get("email"))}
    elif cmd.aggregate_id == PLATFORM_TENANT:
        first = {"email": ""}  # its owners come from the bootstrap
    else:
        raise Rejected("A tenant needs an owner: an email or an account", "invalid_owner")
    return [TenantCreated(slug=slug(d.slug, cmd.aggregate_id), name=name(d.name), owner=first,
                          status="pending" if d.status == "pending" else "active")]


class ActivateTenant(Command):
    pass


@Tenant.command(ActivateTenant)
def _(s: Tenant):
    if s.status != "pending":
        raise Rejected("Only a pending tenant can be activated", "invalid_state")
    return [TenantActivated()]


class RenameTenant(Command):
    name: str


@Tenant.command(RenameTenant)
def _(s: Tenant, d: RenameTenant):
    if s.status not in ("pending", "active"):
        raise Rejected("Cannot rename this tenant", "invalid_state")
    return [TenantRenamed(name=name(d.name))]


class ChangeTenantSlug(Command):
    slug: str


@Tenant.command(ChangeTenantSlug)
def _(s: Tenant, d: ChangeTenantSlug, cmd):
    if s.status != "active":
        raise Rejected("Only an active tenant changes its slug", "invalid_state")
    return [TenantSlugChanged(slug=slug(d.slug, cmd.aggregate_id))]


class SuspendTenant(Command):
    reason: SuspensionReason


@Tenant.command(SuspendTenant)
def _(s: Tenant, d: SuspendTenant, cmd, ctx):
    if cmd.aggregate_id == PLATFORM_TENANT:
        raise Rejected("The platform tenant cannot be suspended", "platform")
    if s.status not in ("pending", "active"):
        raise Rejected("Cannot suspend this tenant", "invalid_state")
    if d.reason not in SUSPENSION_REASONS:
        raise Rejected("Unknown reason", "invalid_reason")
    if (d.reason == "rejected") != (s.status == "pending"):
        raise Rejected("A pending tenant is rejected; an active one suspended for another reason", "invalid_reason")
    return [TenantSuspended(reason=d.reason, at=now_ms(ctx))]


class ReactivateTenant(Command):
    pass


@Tenant.command(ReactivateTenant)
def _(s: Tenant):
    if s.status != "suspended" or (s.suspension or {}).get("reason") == "rejected":
        raise Rejected("Only a suspended tenant is reactivated", "invalid_state")
    return [TenantReactivated()]


class DeleteTenant(Command):
    pass


# Final: the read models drop the tenant, reactors erase its identities, the slug becomes free;
# the events stay.
@Tenant.command(DeleteTenant)
def _(s: Tenant, d, cmd, ctx):
    if cmd.aggregate_id == PLATFORM_TENANT:
        raise Rejected("The platform tenant cannot be deleted", "platform")
    if s.status != "suspended" or not s.suspension:
        raise Rejected("Only a suspended tenant is deleted", "invalid_state")
    if now_ms(ctx) < s.suspension["at"] + DELETE_AFTER_DAYS * DAY:
        raise Rejected(f"A tenant is deleted {DELETE_AFTER_DAYS} days after its suspension", "too_early")
    return [TenantDeleted()]


class AddOwner(Command):
    identity_id: str


@Tenant.command(AddOwner, requires=lambda d, s: [Requirement("identity", "identity", d.identity_id, "active")])
def _(s: Tenant, d: AddOwner):
    exists(s)
    if not isinstance(d.identity_id, str) or not d.identity_id:
        raise Rejected("Which identity?", "invalid_identity")
    if d.identity_id in s.owners:
        raise Rejected("Already an owner", "exists")
    return [OwnerAdded(identity_id=d.identity_id)]


class RemoveOwner(Command):
    identity_id: str


@Tenant.command(RemoveOwner)
def _(s: Tenant, d: RemoveOwner):
    exists(s)
    if d.identity_id not in s.owners:
        raise Rejected("Not an owner", "not_found")
    if len(s.owners) == 1:
        raise Rejected("The last owner cannot be removed", "last_owner")
    return [OwnerRemoved(identity_id=d.identity_id)]


class RequireMfa(Command):
    required: bool


@Tenant.command(RequireMfa)
def _(s: Tenant, d: RequireMfa):
    if s.status != "active":
        raise Rejected("Only an active tenant", "invalid_state")
    return [MfaRequirementChanged(required=d.required is True)]


class ChangeSignUpPolicy(Command):
    tenants: Literal["off", "pending", "active"]
    accounts: Literal["off", "open"]


@Tenant.command(ChangeSignUpPolicy)
def _(s: Tenant, d: ChangeSignUpPolicy, cmd):
    if cmd.aggregate_id != PLATFORM_TENANT:
        raise Rejected("Only the platform tenant has a sign-up policy", "platform")
    exists(s)
    if d.tenants not in ("off", "pending", "active") or d.accounts not in ("off", "open"):
        raise Rejected("tenants: off, pending or active; accounts: off or open", "invalid_policy")
    return [SignUpPolicyChanged(tenants=d.tenants, accounts=d.accounts)]
