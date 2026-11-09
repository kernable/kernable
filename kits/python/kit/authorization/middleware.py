"""The authorization middleware: one listener at order -30 that learns from events and
checks every request before the kernel accepts or routes it. Its knowledge lives in its store."""

import time

from kernable import Check, Query, Rejected, Target

from ..account.events import (AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailVerified, SessionEnded,
                              SessionFactorAdded, SessionOpened, SsoDeleted, SsoDisabled, SsoEnforcementChanged, TotpConfirmed, TotpDisabled)
from ..group.domain import group_domain
from ..group.events import GroupCreated, GroupDeleted, GroupPermissionsSet
from ..identity.events import (ApiKeyCreated, ApiKeyRevoked, IdentityCreated, IdentityDeleted, IdentityReactivated, IdentitySuspended,
                               MembershipGranted, MembershipResumed, MembershipRevoked, MembershipSuspended)
from ..shared.constants import AUTHORIZATION_STORE, ORDERS
from ..tenancy.events import (MfaRequirementChanged, OwnerAdded, OwnerRemoved, SignUpPolicyChanged, TenantActivated, TenantCreated,
                              TenantDeleted, TenantReactivated, TenantSuspended)
from .policy import Knowledge, MemoryKeyValues, decide
from .relations import RelationEvent, parent_rules, relation_events, relation_rules

#: The knowledge, in the listener's store when it has one.
knowledge = Knowledge()
k = knowledge
_memory = MemoryKeyValues()


def use(store) -> None:
    """Points the knowledge at the store the SDK hands this listener's handlers."""
    k.kv = store if store is not None else _memory


def _now() -> int:
    return int(time.time() * 1000)


authorization_listener = group_domain.read_model("group.authorization", order=ORDERS.authorization, store=AUTHORIZATION_STORE)
L = authorization_listener


def _ikey(evt) -> str:
    return f"{evt.tenant}/{evt.aggregate_id}"


# --- tenancy ---------------------------------------------------------------------------------------------

@L.on(TenantCreated)
def _(d: TenantCreated, evt, ctx):
    use(ctx.store)
    k.tenants.set(evt.aggregate_id, {"status": d.status, "owners": [], "mfa_required": False, "suspension": None, "sso_enforced": None})


def _tenant(change):
    def handler(d, evt, ctx):
        use(ctx.store)
        k.tenants.update(evt.aggregate_id, lambda t: change(t, d))
    return handler


L.on(TenantActivated, _tenant(lambda t, d: t.update(status="active")))
L.on(TenantSuspended, _tenant(lambda t, d: t.update(status="suspended", suspension=d.reason)))
L.on(TenantReactivated, _tenant(lambda t, d: t.update(status="active", suspension=None)))
L.on(OwnerAdded, _tenant(lambda t, d: None if d.identity_id in t["owners"] else t["owners"].append(d.identity_id)))
L.on(OwnerRemoved, _tenant(lambda t, d: t.update(owners=[o for o in t["owners"] if o != d.identity_id])))
L.on(MfaRequirementChanged, _tenant(lambda t, d: t.update(mfa_required=d.required)))


@L.on(TenantDeleted)
def _(d, evt, ctx):
    use(ctx.store)
    k.tenants.update(evt.aggregate_id, lambda t: t.update(status="deleted", owners=[]))
    for table in (k.identities, k.groups, k.relations, k.parents):
        table.delete_prefix(f"{evt.aggregate_id}/")


@L.on(SignUpPolicyChanged)
def _(d: SignUpPolicyChanged, evt, ctx):
    use(ctx.store)
    k.sign_up = {"tenants": d.tenants, "accounts": d.accounts}


# --- account ----------------------------------------------------------------------------------------------

def _account(**values):
    def handler(d, evt, ctx):
        use(ctx.store)
        k.accounts.set(evt.aggregate_id, {**(k.accounts.get(evt.aggregate_id) or {}), **values})
    return handler


@L.on(AccountRegistered)
def _(d: AccountRegistered, evt, ctx):
    _account(status="active" if d.verified else "pending")(d, evt, ctx)


@L.on(EmailVerified)
def _(d, evt, ctx):
    use(ctx.store)
    k.accounts.update(evt.aggregate_id, lambda a: a.update(status="active") if a.get("status") == "pending" else None)


L.on(AccountSuspended, _account(status="suspended"))
L.on(AccountReactivated, _account(status="active"))
L.on(AccountDeleted, _account(status="deleted"))
L.on(TotpConfirmed, _account(mfa=True))
L.on(TotpDisabled, _account(mfa=False))


@L.on(SessionOpened)
def _(d: SessionOpened, evt, ctx):
    use(ctx.store)
    k.sessions.set(evt.aggregate_id, {"account_id": d.account_id, "factors": list(d.factors), "active": True, "method": d.method})


@L.on(SessionFactorAdded)
def _(d: SessionFactorAdded, evt, ctx):
    use(ctx.store)
    k.sessions.update(evt.aggregate_id, lambda s: s["factors"].append(d.factor))


@L.on(SessionEnded)
def _(d, evt, ctx):
    use(ctx.store)
    k.sessions.update(evt.aggregate_id, lambda s: s.update(active=False))


@L.on(SsoEnforcementChanged)
def _(d: SsoEnforcementChanged, evt, ctx):
    use(ctx.store)

    def change(t):
        if d.enforced:
            t["sso_enforced"] = evt.aggregate_id
        elif t.get("sso_enforced") == evt.aggregate_id:
            t["sso_enforced"] = None
    k.tenants.update(evt.tenant, change)


def _sso_off(d, evt, ctx):
    use(ctx.store)
    k.tenants.update(evt.tenant, lambda t: t.update(sso_enforced=None) if t.get("sso_enforced") == evt.aggregate_id else None)


L.on(SsoDisabled, _sso_off)
L.on(SsoDeleted, _sso_off)


# --- identity ---------------------------------------------------------------------------------------------

@L.on(IdentityCreated)
def _(d: IdentityCreated, evt, ctx):
    use(ctx.store)
    k.identities.set(_ikey(evt), {"account_id": d.account_id, "active": True, "memberships": []})


def _identity(change):
    def handler(d, evt, ctx):
        use(ctx.store)
        k.identities.update(_ikey(evt), lambda i: change(i, d))
    return handler


def _set_membership(status):
    def change(i, d):
        for m in i["memberships"]:
            if m["membership_id"] == d.membership_id:
                m["status"] = status
    return change


L.on(IdentitySuspended, _identity(lambda i, d: i.update(active=False)))
L.on(IdentityReactivated, _identity(lambda i, d: i.update(active=True)))
L.on(MembershipGranted, _identity(lambda i, d: i["memberships"].append(
    {"membership_id": d.membership_id, "group": d.group, "scope": d.scope, "expires_at": d.expires_at, "status": "active"})))
L.on(MembershipSuspended, _identity(_set_membership("suspended")))
L.on(MembershipResumed, _identity(_set_membership("active")))
L.on(MembershipRevoked, _identity(lambda i, d: i.update(memberships=[m for m in i["memberships"] if m["membership_id"] != d.membership_id])))


@L.on(IdentityDeleted)
def _(d, evt, ctx):
    use(ctx.store)
    k.identities.delete(_ikey(evt))


@L.on(ApiKeyCreated)
def _(d: ApiKeyCreated, evt, ctx):
    use(ctx.store)
    k.api_keys.set(d.key_id, {"tenant": evt.tenant, "identity_id": evt.aggregate_id, "expires_at": d.expires_at, "revoked": False})


@L.on(ApiKeyRevoked)
def _(d: ApiKeyRevoked, evt, ctx):
    use(ctx.store)
    k.api_keys.update(d.key_id, lambda key: key.update(revoked=True))


# --- group: a deleted group's memberships stop counting at once -----------------------------------------

@L.on(GroupCreated)
def _(d: GroupCreated, evt, ctx):
    use(ctx.store)
    k.groups.set(_ikey(evt), d.permissions)


@L.on(GroupPermissionsSet)
def _(d: GroupPermissionsSet, evt, ctx):
    use(ctx.store)
    k.groups.set(_ikey(evt), d.permissions)


@L.on(GroupDeleted)
def _(d, evt, ctx):
    use(ctx.store)
    k.groups.delete(_ikey(evt))


@L.middleware(["*"], order=ORDERS.authorization)
def _(check: Check, outcome, ctx):
    use(ctx.store)
    refusal = decide(k, check, _now())
    if refusal:
        raise Rejected(refusal.message, refusal.code)


class Explain(Query[dict]):
    """Why a request would be allowed or refused: the middleware's own steps. For oneself,
    with one's real session; for another identity of the tenant (needs `group.Explain`), sessions aside."""
    target = {"aggregate_type": "identity.identity", "field": "identity_id"}
    identity_id: str
    #: The request type, qualified: `shop.CancelOrder`.
    type: str
    #: The aggregate the request is about: a command's, or a query's target. Unqualified: of the type's domain.
    aggregate_type: str | None = None
    aggregate_id: str | None = None
    data: dict | None = None


@L.query(Explain)
def _(d: Explain, qry, ctx):
    from kernable import RequestContext

    use(ctx.store)
    domain, _, name = (d.type or "").partition(".")
    if not domain or not name:
        raise Rejected("A qualified type: domain.Type", "invalid_type")
    asker = ctx.request
    oneself = asker.sender_identity == d.identity_id and asker.sender_tenant == qry.tenant
    request = asker if oneself else RequestContext(sender_tenant=qry.tenant, sender_identity=d.identity_id)
    check = Check(kind="command" if d.aggregate_id else "query", id="", tenant=qry.tenant, domain=domain, type=name, data=d.data or {},
                  request=request)
    if d.aggregate_id and d.aggregate_type:
        check.aggregate_type, check.aggregate_id = d.aggregate_type, d.aggregate_id
        check.target = Target(d.aggregate_type if "." in d.aggregate_type else f"{domain}.{d.aggregate_type}", d.aggregate_id)
    steps: list[str] = []
    refusal = decide(k, check, _now(), trace=steps, assume_session=not oneself)
    return {"allowed": refusal is None, "code": refusal.code if refusal else None, "steps": steps}


_installed = False


def install_relations() -> None:
    """Teaches the listener the relations declared with `relation()` and `parent()`. `kit_domains()`
    calls it; declare your relations before that."""
    global _installed
    if _installed:
        return
    _installed = True
    for event in relation_events():
        def handler(data, evt, ctx, event=event):
            from kernable.messages import from_wire
            use(ctx.store)
            e = RelationEvent(from_wire(data), evt.aggregate_id, evt.tenant)
            aggregate = f"{evt.domain}.{evt.aggregate_type}"
            for rule in [r for r in relation_rules if r.event == event and r.type == aggregate]:
                ids = rule.ids(e)
                ids = [i for i in (ids if isinstance(ids, list) else [ids]) if isinstance(i, str)]
                k.relate(evt.tenant, {"type": rule.type, "id": evt.aggregate_id}, rule.name, ids, rule.op == "set")
            for rule in [p for p in parent_rules if p.event == event and p.type == aggregate]:
                p = rule.parent(e)
                if p and p.get("type") and p.get("id"):
                    k.parents.set(f"{evt.tenant}/{rule.type}/{evt.aggregate_id}", {"type": p["type"], "id": p["id"]})
        authorization_listener.on(event, handler)


def use_authorization_store(url: str) -> None:
    """Where the middleware keeps its knowledge; call before the app runs."""
    authorization_listener.store_url = url
