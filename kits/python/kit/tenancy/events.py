"""The tenancy events: what happened to tenants, the vocabulary other domains read."""

from typing import Literal

from kernable import Event

from .domain import Owner, SuspensionReason, tenancy_domain


class TenantCreated(Event):
    slug: str
    name: str
    owner: Owner
    status: Literal["active", "pending"]


class TenantActivated(Event):
    pass


class TenantRenamed(Event):
    name: str


class TenantSlugChanged(Event):
    slug: str


class TenantSuspended(Event):
    reason: SuspensionReason
    #: In the payload: the retention before deletion is computed from it.
    at: int


class TenantReactivated(Event):
    pass


class TenantDeleted(Event):
    pass


class OwnerAdded(Event):
    identity_id: str


class OwnerRemoved(Event):
    identity_id: str


class MfaRequirementChanged(Event):
    required: bool


class SignUpPolicyChanged(Event):
    tenants: Literal["off", "pending", "active"]
    accounts: Literal["off", "open"]


# Declared on the domain, so a process that only reads them knows them too.
tenancy_domain.event(TenantCreated, TenantActivated, TenantRenamed, TenantSlugChanged, TenantSuspended, TenantReactivated,
                     TenantDeleted, OwnerAdded, OwnerRemoved, MfaRequirementChanged, SignUpPolicyChanged)
