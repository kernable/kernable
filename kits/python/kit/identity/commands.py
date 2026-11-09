"""The identity commands: each class with its handler."""

import json
import re

from kernable import Command, Rejected, Requirement

from ..shared.secrets import sha256
from .aggregates import Identity
from .domain import Scope, api_key, own
from .events import (ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended,
                     MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated)


def _active(s: Identity) -> None:
    if s.status != "active":
        raise Rejected("The identity is not active", "invalid_state")


def _scope(value) -> Scope | None:
    if value is None:
        return None
    if isinstance(value, dict) and isinstance(value.get("type"), str) and re.fullmatch(r"[a-z][\w-]*\.[\w-]+", value["type"]) \
            and isinstance(value.get("id"), str) and value["id"]:
        return {"type": value["type"], "id": value["id"]}
    raise Rejected("A scope is {type: 'domain.aggregate', id}", "invalid_scope")


class CreateIdentity(Command):
    """Internal: from an accepted invitation or a self-service tenant."""
    vault = {"personal": [{"fields": ["display_name", "title", "email"], "subject": own}]}
    account_id: str
    display_name: str | None = None
    title: str | None = None
    email: str | None = None


@Identity.command(CreateIdentity)
def _(s: Identity, d: CreateIdentity):
    if s.status != "none":
        raise Rejected("The identity exists", "exists")
    if not isinstance(d.account_id, str) or not d.account_id:
        raise Rejected("Which account?", "invalid_account")
    return [IdentityCreated(kind="person", account_id=d.account_id, display_name=d.display_name or "", title=d.title, email=d.email)]


class CreateServiceIdentity(Command):
    vault = {"personal": [{"fields": ["display_name"], "subject": own}]}
    display_name: str


@Identity.command(CreateServiceIdentity)
def _(s: Identity, d: CreateServiceIdentity):
    if s.status != "none":
        raise Rejected("The identity exists", "exists")
    if not isinstance(d.display_name, str) or not d.display_name.strip():
        raise Rejected("A service needs a name", "invalid_name")
    return [IdentityCreated(kind="service", account_id=None, display_name=d.display_name.strip(), title=None, email=None)]


class CreateProcessIdentity(Command):
    """Internal: the identity a business process acts as (`process`: qualified, `shop.fulfilment`),
    created by the `identity.processes` reactor in the platform tenant, without permissions."""
    process: str


@Identity.command(CreateProcessIdentity)
def _(s: Identity, d: CreateProcessIdentity):
    if s.status != "none":
        raise Rejected("The identity exists", "exists")
    if not isinstance(d.process, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z0-9_-]+", d.process):
        raise Rejected("A qualified process name: domain.process", "invalid_process")
    return [IdentityCreated(kind="process", account_id=None, display_name=d.process, title=None, email=None)]


class UpdateProfile(Command):
    vault = {"personal": [{"fields": ["display_name", "title", "email"], "subject": own}]}
    display_name: str | None = None
    title: str | None = None
    email: str | None = None


@Identity.command(UpdateProfile)
def _(s: Identity, d: UpdateProfile):
    _active(s)
    return [ProfileUpdated(display_name=d.display_name, title=d.title, email=d.email)]


class GrantMembership(Command):
    """The middleware lets the sender grant only groups whose permissions it holds."""
    membership_id: str
    group: str
    scope: Scope | None = None
    expires_at: int | None = None


@Identity.command(GrantMembership, requires=lambda d, s: [Requirement("group", "group", d.group, "active")])
def _(s: Identity, d: GrantMembership):
    _active(s)
    if not d.membership_id or not d.group:
        raise Rejected("A membership needs an ID and a group", "invalid_membership")
    scope = _scope(d.scope)
    same = [m for m in s.memberships if m["membership_id"] == d.membership_id or (m["group"] == d.group and json.dumps(m["scope"]) == json.dumps(scope))]
    if same:
        raise Rejected("Already a member", "exists")
    return [MembershipGranted(membership_id=d.membership_id, group=d.group, scope=scope, expires_at=d.expires_at)]


def _membership(s: Identity, membership_id: str, status: str | None, message: str) -> None:
    m = next((x for x in s.memberships if x["membership_id"] == membership_id), None)
    if not m or (status and m["status"] != status):
        raise Rejected(message, "not_found")


class SuspendMembership(Command):
    membership_id: str


@Identity.command(SuspendMembership)
def _(s: Identity, d: SuspendMembership):
    _membership(s, d.membership_id, "active", "No such active membership")
    return [MembershipSuspended(membership_id=d.membership_id)]


class ResumeMembership(Command):
    membership_id: str


@Identity.command(ResumeMembership)
def _(s: Identity, d: ResumeMembership):
    _membership(s, d.membership_id, "suspended", "No such suspended membership")
    return [MembershipResumed(membership_id=d.membership_id)]


class RevokeMembership(Command):
    membership_id: str


@Identity.command(RevokeMembership)
def _(s: Identity, d: RevokeMembership):
    _membership(s, d.membership_id, None, "No such membership")
    return [MembershipRevoked(membership_id=d.membership_id)]


class CreateApiKey(Command):
    """The API generates the key and shows it once; only its hash stays."""
    vault = {"secret": ["secret"]}
    key_id: str
    secret: str
    label: str | None = None
    expires_at: int | None = None


@Identity.command(CreateApiKey)
def _(s: Identity, d: CreateApiKey):
    _active(s)
    if s.kind not in ("service", "process"):
        raise Rejected("Only a service or a process has API keys", "not_a_service")
    if not isinstance(d.key_id, str) or not re.fullmatch(r"[A-Za-z0-9]{8,32}", d.key_id) or not isinstance(d.secret, str) or len(d.secret) < 32:
        raise Rejected("A key ID of 8 to 32 letters and digits, and a secret of at least 32 characters", "invalid_key")
    if any(k["key_id"] == d.key_id for k in s.api_keys):
        raise Rejected("The key exists", "exists")
    return [ApiKeyCreated(key_id=d.key_id, hash=sha256(api_key(d.key_id, d.secret)), label=d.label or "", expires_at=d.expires_at)]


class RevokeApiKey(Command):
    key_id: str


@Identity.command(RevokeApiKey)
def _(s: Identity, d: RevokeApiKey):
    k = next((x for x in s.api_keys if x["key_id"] == d.key_id), None)
    if not k or k["revoked"]:
        raise Rejected("No such key", "not_found")
    return [ApiKeyRevoked(key_id=d.key_id)]


class SuspendIdentity(Command):
    reason: str | None = None


@Identity.command(SuspendIdentity)
def _(s: Identity, d: SuspendIdentity):
    _active(s)
    return [IdentitySuspended(reason=d.reason or "by_group")]


class ReactivateIdentity(Command):
    pass


@Identity.command(ReactivateIdentity)
def _(s: Identity):
    if s.status != "suspended":
        raise Rejected("Only a suspended identity", "invalid_state")
    return [IdentityReactivated()]


class DeleteIdentity(Command):
    """Erases the identity's personal data. Never while a tenant holds it as owner: atomic,
    through the tenant's `owner` key (an absent requirement)."""


def _not_an_owner(d, s: Identity):
    return [Requirement("tenancy", "tenant", "", "exists", key="owner", value=s.id, absent=True)] if s.id else []


@Identity.command(DeleteIdentity, requires=_not_an_owner)
def _(s: Identity):
    if s.status in ("none", "deleted"):
        raise Rejected("No such identity", "not_found")
    return [IdentityDeleted()]
