"""The identity read models: identities with their memberships and API keys."""

from typing import TypedDict

from ..shared.constants import ORDERS
from ..tenancy.events import TenantDeleted
from .domain import IdentityStatus, identity_domain
from .events import (ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended,
                     MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended, ProfileUpdated)


class IdentityRow(TypedDict):
    id: str
    tenant: str
    kind: str
    account_id: str | None
    display_name: str | None
    status: IdentityStatus
    memberships: list[dict]
    api_keys: list[dict]


#: `tenant/identityId`
identity_rows: dict[str, IdentityRow] = {}
#: keyId → what the API checks a key against (FindApiKey).
api_key_rows: dict[str, dict] = {}


def row_key(tenant: str, identity_id: str) -> str:
    return f"{tenant}/{identity_id}"


def _row(evt) -> IdentityRow | None:
    return identity_rows.get(row_key(evt.tenant, evt.aggregate_id))


identities_readmodel = identity_domain.read_model("identity.identities", order=ORDERS.read_models)


@identities_readmodel.on(IdentityCreated)
def _(d: IdentityCreated, evt):
    identity_rows[row_key(evt.tenant, evt.aggregate_id)] = IdentityRow(
        id=evt.aggregate_id, tenant=evt.tenant, kind=d.kind, account_id=d.account_id, display_name=d.display_name,
        status="active", memberships=[], api_keys=[])


@identities_readmodel.on(ProfileUpdated)
def _(d: ProfileUpdated, evt):
    if (r := _row(evt)) and d.display_name is not None:
        r["display_name"] = d.display_name


@identities_readmodel.on(MembershipGranted)
def _(d: MembershipGranted, evt, ctx):
    """Who granted it and when come from the event's request context and time, not its payload."""
    if r := _row(evt):
        r["memberships"].append({"membership_id": d.membership_id, "group": d.group, "scope": d.scope, "expires_at": d.expires_at,
                                 "status": "active", "granted_by": ctx.request.sender_identity or None,
                                 "granted_at": int(evt.created_at.timestamp() * 1000)})


def _set_membership(status: str):
    def handler(d, evt):
        for m in (_row(evt) or {"memberships": []})["memberships"]:
            if m["membership_id"] == d.membership_id:
                m["status"] = status
    return handler


identities_readmodel.on(MembershipSuspended, _set_membership("suspended"))
identities_readmodel.on(MembershipResumed, _set_membership("active"))


@identities_readmodel.on(MembershipRevoked)
def _(d: MembershipRevoked, evt):
    if r := _row(evt):
        r["memberships"] = [m for m in r["memberships"] if m["membership_id"] != d.membership_id]


@identities_readmodel.on(ApiKeyCreated)
def _(d: ApiKeyCreated, evt):
    api_key_rows[d.key_id] = {"tenant": evt.tenant, "identity_id": evt.aggregate_id, "hash": d.hash, "expires_at": d.expires_at, "revoked": False}
    if r := _row(evt):
        r["api_keys"].append({"key_id": d.key_id, "label": d.label, "expires_at": d.expires_at, "revoked": False})


@identities_readmodel.on(ApiKeyRevoked)
def _(d: ApiKeyRevoked, evt):
    if key := api_key_rows.get(d.key_id):
        key["revoked"] = True
    for k in (_row(evt) or {"api_keys": []})["api_keys"]:
        if k["key_id"] == d.key_id:
            k["revoked"] = True


def _set_status(status: str):
    def handler(d, evt):
        if r := _row(evt):
            r["status"] = status
    return handler


identities_readmodel.on(IdentitySuspended, _set_status("suspended"))
identities_readmodel.on(IdentityReactivated, _set_status("active"))


@identities_readmodel.on(IdentityDeleted)
def _(d, evt):
    identity_rows.pop(row_key(evt.tenant, evt.aggregate_id), None)


@identities_readmodel.on(TenantDeleted)
def _(d, evt):
    for k in [k for k, r in identity_rows.items() if r["tenant"] == evt.aggregate_id]:
        del identity_rows[k]
