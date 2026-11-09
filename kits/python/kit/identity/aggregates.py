"""The identity aggregates: state, how each event changes it, unique keys and facts."""

from kernable import Aggregate

from .domain import IdentityStatus, identity_domain
from .events import (ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended,
                     MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated)


class Identity(Aggregate, domain=identity_domain, schema="py-1"):
    """A person or service in one tenant."""
    id: str | None = None
    status: IdentityStatus = "none"
    kind: str | None = None
    account_id: str | None = None
    #: A process identity's process (qualified: `shop.fulfilment`), its display name.
    process: str | None = None
    memberships: list = []
    api_keys: list = []


def _membership(s: Identity, membership_id: str) -> dict | None:
    return next((m for m in s.memberships if m["membership_id"] == membership_id), None)


@Identity.apply(IdentityCreated)
def _(s: Identity, d: IdentityCreated, evt):
    s.update(id=evt.aggregate_id, status="active", kind=d.kind, account_id=d.account_id,
             process=d.display_name if d.kind == "process" else None)


@Identity.apply(ProfileUpdated)
def _(s: Identity):
    pass


@Identity.apply(MembershipGranted)
def _(s: Identity, d: MembershipGranted):
    s.memberships.append({"membership_id": d.membership_id, "group": d.group, "scope": d.scope, "expires_at": d.expires_at, "status": "active"})


@Identity.apply(MembershipSuspended)
def _(s: Identity, d: MembershipSuspended):
    if m := _membership(s, d.membership_id):
        m["status"] = "suspended"


@Identity.apply(MembershipResumed)
def _(s: Identity, d: MembershipResumed):
    if m := _membership(s, d.membership_id):
        m["status"] = "active"


@Identity.apply(MembershipRevoked)
def _(s: Identity, d: MembershipRevoked):
    s.memberships = [m for m in s.memberships if m["membership_id"] != d.membership_id]


@Identity.apply(ApiKeyCreated)
def _(s: Identity, d: ApiKeyCreated):
    s.api_keys.append({"key_id": d.key_id, "hash": d.hash, "label": d.label, "expires_at": d.expires_at, "revoked": False})


@Identity.apply(ApiKeyRevoked)
def _(s: Identity, d: ApiKeyRevoked):
    for k in s.api_keys:
        if k["key_id"] == d.key_id:
            k["revoked"] = True


@Identity.apply(IdentitySuspended)
def _(s: Identity):
    s.status = "suspended"


@Identity.apply(IdentityReactivated)
def _(s: Identity):
    s.status = "active"


@Identity.apply(IdentityDeleted)
def _(s: Identity):
    s.update(status="deleted", memberships=[], api_keys=[])


# One identity per account and tenant.
Identity.unique("account", lambda s: s.account_id if s.status not in ("none", "deleted") else None)
# One identity per process and tenant (the platform tenant's, in practice).
Identity.unique("process", lambda s: s.process if s.status not in ("none", "deleted") else None)
Identity.fact("active", lambda s: s.status == "active")

#: The aggregate type, for workers and `requires`.
identity_aggregate = Identity.__aggregate__
