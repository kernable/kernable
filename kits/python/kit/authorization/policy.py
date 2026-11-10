"""What the authorization middleware knows (folded from events) and how it decides
(design/kits.md "The authorization middleware"). Pure: no kernel needed, so it is tested alone."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from kernable.messages import from_wire

from ..shared.constants import PLATFORM_TENANT
from .levels import ESCALATION, rule_of


@dataclass
class Refusal:
    code: str  # unauthenticated, forbidden, tenant_suspended, mfa_required, sso_required, sign_up_closed
    message: str


class KeyValues(Protocol):
    """Where the knowledge lives: the listener's store, or memory (unit tests)."""
    def get(self, ns: str, key: str, default: Any = None) -> Any: ...
    def set(self, ns: str, key: str, value: Any) -> None: ...
    def remove(self, ns: str, key: str) -> None: ...
    def all(self, ns: str) -> list[tuple[str, Any]]: ...


class MemoryKeyValues:
    """Key-values in memory, the same as a store holds them (as JSON)."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, str]] = {}

    def get(self, ns: str, key: str, default: Any = None) -> Any:
        value = self._data.get(ns, {}).get(key)
        return default if value is None else json.loads(value)

    def set(self, ns: str, key: str, value: Any) -> None:
        self._data.setdefault(ns, {})[key] = json.dumps(value)

    def remove(self, ns: str, key: str) -> None:
        self._data.get(ns, {}).pop(key, None)

    def all(self, ns: str) -> list[tuple[str, Any]]:
        return [(k, json.loads(v)) for k, v in sorted(self._data.get(ns, {}).items())]


class Table:
    """One namespace of the knowledge, read and written like a dict. Values are copies: write back with `set` or `update`."""

    def __init__(self, owner: "Knowledge", ns: str) -> None:
        self._owner = owner
        self._ns = ns

    def get(self, key: str) -> Any:
        return self._owner.kv.get(self._ns, key)

    def set(self, key: str, value: Any) -> None:
        self._owner.kv.set(self._ns, key, value)

    def delete(self, key: str) -> None:
        self._owner.kv.remove(self._ns, key)

    def update(self, key: str, change) -> None:
        """Changes the value under `key`, if there is one."""
        value = self.get(key)
        if value is not None:
            change(value)
            self.set(key, value)

    def delete_prefix(self, prefix: str) -> None:
        """Removes every key starting with `prefix` (a deleted tenant's)."""
        for key, _ in self._owner.kv.all(self._ns):
            if key.startswith(prefix):
                self._owner.kv.remove(self._ns, key)


class Knowledge:
    """What the middleware knows, folded from events (design/kits.md, "What it knows")."""

    def __init__(self, kv: KeyValues | None = None) -> None:
        self.kv: KeyValues = kv or MemoryKeyValues()
        self.tenants = Table(self, "tenants")
        #: `mfa`: the account turned a second factor on: every session needs it.
        self.accounts = Table(self, "accounts")
        self.sessions = Table(self, "sessions")
        #: `tenant/identityId`
        self.identities = Table(self, "identities")
        #: keyId
        self.api_keys = Table(self, "apiKeys")
        #: `tenant/groupId`
        self.groups = Table(self, "groups")
        #: `tenant/type/id` → relation → identity IDs
        self.relations = Table(self, "relations")
        #: `tenant/type/id` → the parent
        self.parents = Table(self, "parents")
        self._settings = Table(self, "settings")

    @property
    def sign_up(self) -> dict:
        return self._settings.get("signUp") or {"tenants": "off", "accounts": "off"}

    @sign_up.setter
    def sign_up(self, policy: dict) -> None:
        self._settings.set("signUp", policy)

    def identity(self, tenant: str, identity_id: str | None):
        return self.identities.get(f"{tenant}/{identity_id}") if identity_id else None

    def holds(self, tenant: str, target: dict, relation: str, identity_id: str) -> bool:
        rels = self.relations.get(f"{tenant}/{target['type']}/{target['id']}") or {}
        return identity_id in rels.get(relation, [])

    def relate(self, tenant: str, target: dict, relation: str, ids: list[str], on: bool) -> None:
        key = f"{tenant}/{target['type']}/{target['id']}"
        rels = self.relations.get(key) or {}
        current = set(rels.get(relation, []))
        current = current | set(ids) if on else current - set(ids)
        rels[relation] = sorted(current)
        self.relations.set(key, rels)


def covers(pattern: str, name: str) -> bool:
    """Whether `pattern` covers `name`: `*`, `domain.*` or the qualified type; `name` may be `domain.*` itself."""
    if pattern == "*" or pattern == name:
        return True
    return pattern.endswith(".*") and name != "*" and name.split(".")[0] == pattern[:-2]


def _refuse(code: str, message: str) -> Refusal:
    return Refusal(code, message)


def target_of(check) -> dict | None:
    """The aggregate the request is about: a command's, or the one a query declares (`target`)."""
    if check.target:
        return {"type": check.target.type, "id": check.target.id}
    if check.kind == "command" and check.aggregate_type and check.aggregate_id:
        return {"type": f"{check.domain}.{check.aggregate_type}", "id": check.aggregate_id}
    return None


def _is_self(k: Knowledge, check, r, target) -> bool:
    """Whether the target is the sender itself: its identity, account or session (built-in `self`)."""
    if not target:
        return False
    if target["type"] == "identity.identity":
        return target["id"] == r.sender_identity and check.tenant == r.sender_tenant
    if target["type"] == "account.account":
        return target["id"] == r.sender_account
    if target["type"] == "account.session":
        return target["id"] == r.sender_session or (k.sessions.get(target["id"]) or {}).get("account_id") == r.sender_account
    return False


def _authenticate(k: Knowledge, r, now: int) -> dict | Refusal:
    """Who sends the request: an active session of an active account, or a valid API key (step 3)."""
    sid = r.sender_session
    if not sid:
        return _refuse("unauthenticated", "Sign in first")
    session = k.sessions.get(sid)
    if session:
        if not session["active"]:
            return _refuse("unauthenticated", "The session has ended")
        if session["account_id"] != r.sender_account:
            return _refuse("unauthenticated", "The session is another account's")
        if (k.accounts.get(session["account_id"]) or {}).get("status") != "active":
            return _refuse("unauthenticated", "The account is not active")
        return {"account_id": session["account_id"], "factors": session["factors"], "method": session.get("method"), "api_key": False}
    key = k.api_keys.get(sid)
    if key:
        if key["revoked"] or (key["expires_at"] is not None and now > key["expires_at"]):
            return _refuse("unauthenticated", "The API key is revoked or expired")
        if key["identity_id"] != r.sender_identity or key["tenant"] != r.sender_tenant:
            return _refuse("unauthenticated", "The API key is another identity's")
        return {"account_id": None, "factors": ["api_key"], "method": None, "api_key": True}
    return _refuse("unauthenticated", "Unknown session")


def _permissions(k: Knowledge, tenant: str, identity_id: str, now: int) -> list[tuple[dict, dict]]:
    """The permissions an identity holds through its memberships, at `now`."""
    identity = k.identity(tenant, identity_id)
    if not identity or not identity["active"]:
        return []
    return [(p, m) for m in identity["memberships"]
            if m["status"] == "active" and (m["expires_at"] is None or now < m["expires_at"])
            for p in (k.groups.get(f"{tenant}/{m['group']}") or [])]


def _matches(k: Knowledge, tenant: str, identity_id: str, p: dict, m: dict, type_: str, target, check, r) -> bool:
    if not covers(p["pattern"], type_):
        return False
    scope = m.get("scope")
    if scope and not (target and scope["type"] == target["type"] and scope["id"] == target["id"]):
        parent = k.parents.get(f"{tenant}/{target['type']}/{target['id']}") if target else None
        if not (parent and scope["type"] == parent["type"] and scope["id"] == parent["id"]):
            return False
    when = p.get("when")
    if not when:
        return True
    if not target:
        return False  # a relation needs a target
    if when == "self":
        return _is_self(k, check, r, target)
    if k.holds(check.tenant, target, when, identity_id):
        return True
    if not p.get("via_parent"):
        return False
    parent = k.parents.get(f"{check.tenant}/{target['type']}/{target['id']}")
    return bool(parent) and k.holds(check.tenant, parent, when, identity_id)


def _concerned(k: Knowledge, type_: str, check, data: dict) -> list[str]:
    """The patterns concerned by a request that hands out permissions (no escalation)."""
    def of_group(g):
        return [p["pattern"] for p in (k.groups.get(f"{check.tenant}/{g}") or [])] if isinstance(g, str) else []
    if type_ == "identity.GrantMembership":
        return of_group(data.get("group"))
    if type_ in ("invitation.CreateInvitation", "invitation.CreateInvitationLink"):
        return [p for g in (data.get("groups") or []) for p in of_group((g or {}).get("group"))]
    return [str((p or {}).get("pattern")) for p in (data.get("permissions") or [])]


def decide(k: Knowledge, check, now: int, trace: list[str] | None = None, assume_session: bool = False) -> Refusal | None:
    """Decides on a request (design/kits.md, steps 1–9): None allows it. `now` is the middleware's
    clock (ms). With `trace`, each deciding step is noted (`group.Explain`); `assume_session` explains
    what another identity may do, its sessions aside."""
    type_ = f"{check.domain}.{check.type}"
    r = check.request
    rule = rule_of(type_)
    data = from_wire(check.data) if isinstance(check.data, dict) else {}

    def say(line: str) -> None:
        if trace is not None:
            trace.append(line)

    def allow(why: str) -> None:
        say(f"allowed: {why}")
        return None

    def no(code: str, message: str) -> Refusal:
        say(f"refused ({code}): {message}")
        return _refuse(code, message)

    # 1. Privileged keys (the kernel checked the grant): the kit's reactors and the bootstrap.
    if r.bypass_authorization:
        return allow("a privileged key (bypass_authorization)")
    if rule["level"] == "internal":
        return no("forbidden", f"{type_} is sent by the kit only")

    # 2. Public: the command carries its proof, the aggregate checks it.
    if rule["level"] == "public" and not (type_ == "account.RegisterAccount" and data.get("verified") is True):
        if type_ == "account.RegisterAccount" and k.sign_up["accounts"] != "open":
            return no("sign_up_closed", "Registration is by invitation only")
        return allow(f"{type_} is public; the command carries its proof")

    # 3. Who is asking.
    if assume_session:
        who = k.identity(r.sender_tenant, r.sender_identity) or {}
        sender = {"account_id": who.get("account_id"), "factors": ["password", "assumed"], "method": None, "api_key": not who.get("account_id")}
        say("sessions not checked (explaining for another identity)")
    else:
        sender = _authenticate(k, r, now)
    if isinstance(sender, Refusal):
        return no(sender.code, sender.message)
    if not assume_session:
        say(f"signed in with the API key {r.sender_session}" if sender["api_key"]
            else f"signed in as account {sender['account_id']} (session {r.sender_session}, {' + '.join(sender['factors'])})")

    # An account with a second factor on: nothing but signing out until the session proved it.
    account = k.accounts.get(sender["account_id"]) if sender["account_id"] else None
    two = len(sender["factors"]) >= 2 or "passkey" in sender["factors"]
    if account and account.get("mfa") and not two and type_ != "account.EndSession":
        return no("mfa_required", "Prove the second factor first")
    target = target_of(check)
    # Oneself: the account and its sessions at once; an identity only after the tenant's checks.
    self_ = bool(rule.get("self")) and _is_self(k, check, r, target)
    if self_ and target and target["type"] != "identity.identity":
        # Oneself in another tenant than the platform's (the API checks a person against a tenant
        # they join, before they hold an identity there): that tenant's checks too.
        if check.domain == "account" and check.tenant != PLATFORM_TENANT:
            t = k.tenants.get(check.tenant)
            if not t or t["status"] == "deleted":
                return no("forbidden", "No such tenant")
            if t["status"] != "active":
                return no("tenant_suspended", f"The tenant is {t['status']}")
            if t["mfa_required"] and not two:
                return no("mfa_required", "This tenant requires a second factor")
            sso = t.get("sso_enforced")
            if sso and not assume_session and sender["method"] != f"sso:{sso}":
                return no("sso_required", "Sign in through the tenant's single sign-on")
        return allow(f"oneself ({target['type']})")
    if type_ == "account.SuspendAccount" and data.get("reason") == "owner_requested" and _is_self(k, check, r, target):
        return allow("one's own account, for oneself")

    # 4. Authenticated: a session is enough.
    if rule["level"] == "authenticated":
        if type_ == "tenancy.CreateTenant":
            own = (data.get("owner") or {}).get("account_id") and data["owner"]["account_id"] == sender["account_id"]
            if own and k.sign_up["tenants"] != "off" and (data.get("status") or "active") == k.sign_up["tenants"]:
                return allow("self-service sign-up of a tenant")
        else:
            return allow(f"{type_} needs a session only")

    # 5. The tenant, and the sender's tenant.
    platform = r.sender_tenant == PLATFORM_TENANT
    if not r.sender_tenant or (not platform and r.sender_tenant != check.tenant):
        return no("forbidden", "Not a member of this tenant")
    tenant = k.tenants.get(check.tenant)
    if not platform:
        if not tenant or tenant["status"] == "deleted":
            return no("forbidden", "No such tenant")
        if tenant["status"] != "active" and type_ != "tenancy.ReactivateTenant":
            return no("tenant_suspended", f"The tenant is {tenant['status']}")

    # 6. The identity, and the tenant's MFA requirement.
    if r.on_behalf_of:
        return no("forbidden", "Acting on behalf of others is not available yet")
    identity_id = r.sender_identity
    identity = k.identity(r.sender_tenant, identity_id)
    if not identity_id or not identity or not identity["active"]:
        return no("forbidden", "No active identity in this tenant")
    if not sender["api_key"] and identity["account_id"] != sender["account_id"]:
        return no("forbidden", "The identity is another account's")
    home = k.tenants.get(r.sender_tenant)
    for t in (tenant, home):
        if t and t["mfa_required"] and not sender["api_key"] and not two:
            return no("mfa_required", "This tenant requires a second factor")

    # 7. Owners hold every permission in their tenant, except the platform's; platform owners hold all.
    owner = bool(home) and identity_id in home["owners"]
    sso = (tenant or {}).get("sso_enforced")
    if sso and not platform and not owner and not sender["api_key"] and not assume_session and sender["method"] != f"sso:{sso}":
        return no("sso_required", "Sign in through the tenant's single sign-on")
    if self_:
        return allow("oneself (identity.identity)")
    if rule["level"] == "platform" and not platform:
        own = owner and ((type_ == "tenancy.SuspendTenant" and data.get("reason") == "owner_requested")
                         or (type_ == "tenancy.ReactivateTenant" and (tenant or {}).get("suspension") == "owner_requested"))
        return allow("an owner, for their own suspension") if own else no("forbidden", f"{type_} is for the platform")
    if owner:
        return allow("an owner of the platform" if platform else "an owner of the tenant (every permission)")
    if type_ == "invitation.CreateInvitation" and data.get("owner") is True and not platform:
        return no("forbidden", "Owner invitations are the owners'")
    if not platform and (tenant or {}).get("status") != "active":
        return no("tenant_suspended", f"The tenant is {(tenant or {}).get('status')}")
    if rule["level"] == "owners":
        return no("forbidden", f"{type_} is for the tenant's owners")

    # 8. Memberships: pattern, scope, relation, expiry.
    held = _permissions(k, r.sender_tenant, identity_id, now)
    match = next(((p, m) for p, m in held if _matches(k, r.sender_tenant, identity_id, p, m, type_, target, check, r)), None)
    if not match:
        return no("forbidden", f"No permission for {type_}")

    # 9. No escalation: only permissions held without scope and relation count.
    if type_ in ESCALATION:
        plain = [p["pattern"] for p, m in held if not p.get("when") and not m.get("scope")]
        missing = next((x for x in _concerned(k, type_, check, data) if not any(covers(h, x) for h in plain)), None)
        if missing:
            return no("forbidden", f"Granting {missing} needs holding it")
    if trace is None:
        return None
    p, m = match
    until = ""
    if m["expires_at"] is not None:
        try:
            until = ", until " + datetime.fromtimestamp(m["expires_at"] / 1000, tz=timezone.utc).isoformat()
        except (OverflowError, ValueError, OSError):
            until = f", until {m['expires_at']}"
    return allow(f"membership {m['membership_id']} of group {m['group']}{' (platform)' if platform else ''}: {p['pattern']}"
                 + (f" when {p['when']}{' (via parent)' if p.get('via_parent') else ''}" if p.get("when") else "")
                 + (f", scoped to {m['scope']['type']} {m['scope']['id']}" if m.get("scope") else "") + until)
