"""The bootstrap: run once, with a key granted `privileged` for `bypass_authorization`. It
creates the platform tenant, the sign-up policy, and the API's service identity with its key."""

import secrets

from kernable import Kernable, Outcome, RequestContext

from .account.commands import RegisterAccount
from .group.commands import CreateGroup
from .identity.commands import CreateApiKey, CreateIdentity, CreateServiceIdentity, GrantMembership
from .identity.domain import api_key
from .shared.constants import PLATFORM_TENANT
from .tenancy.commands import AddOwner, ChangeSignUpPolicy, CreateTenant

#: What the API may ask for itself: lookups before anyone is signed in.
API_PERMISSIONS = [{"pattern": p} for p in [
    "account.FindAccountByEmail", "account.GetAccount", "account.FindAccountByLink", "account.RegisterAccount",
    "account.RequestVerification", "account.RequestPasswordReset", "account.NoteRegistrationAttempt",
    "account.SignInWithPasskey", "account.ProvePasskey", "account.SignInWithProvider", "account.LinkProvider",
    "account.VerifySsoDomain", "account.FindSsoByDomain", "account.GetSsoSecret",
    "account.SetCache", "account.GetCache", "account.DeleteCache",
    "identity.FindIdentityByAccount", "identity.FindApiKey",
    "tenancy.GetTenantBySlug", "tenancy.GetSignUpPolicy",
    "invitation.AcceptInvitation", "invitation.PeekInvitation",
]]


def _ok(outcome: Outcome) -> None:
    if not outcome.committed and outcome.code != "exists":
        raise RuntimeError(f"bootstrap: {outcome.code}: {outcome.message}")


def bootstrap(kernable: Kernable, sign_up: dict, platform_name: str = "Platform", admin: dict | None = None) -> dict:
    """Sets up the platform: idempotent, so it may run again. `sign_up`: `{"tenants": "off" | "pending"
    | "active", "accounts": "off" | "open"}`. `admin` (`{"email", "name", "password"}`): the first
    platform administrator, an owner of the platform tenant.

    Returns `{"api": {"identity_id", "key_id", "key"}, "admin": {"account_id", "identity_id"} | None}`:
    give the API's identity and key to `KitApi`, and keep the key secret."""
    context = RequestContext(bypass_authorization=True)
    tenant = PLATFORM_TENANT

    def submit(command, aggregate_id: str, id: str | None = None) -> None:
        _ok(kernable.submit(command, aggregate_id=aggregate_id, tenant=tenant, context=context,
                            id=id or f"bootstrap-{type(command).__name__}-{aggregate_id}"))

    submit(CreateTenant(slug="platform", name=platform_name, owner={"email": ""}), tenant)
    submit(ChangeSignUpPolicy(tenants=sign_up["tenants"], accounts=sign_up["accounts"]), tenant)
    identity_id = "api"
    submit(CreateServiceIdentity(display_name="API"), identity_id)
    submit(CreateGroup(name="api", description="What the API asks for itself", permissions=API_PERMISSIONS), "api")
    submit(GrantMembership(membership_id="api", group="api"), identity_id)
    key_id = secrets.token_hex(8)
    secret = secrets.token_urlsafe(32)
    submit(CreateApiKey(key_id=key_id, secret=secret, label="API"), identity_id, secrets.token_hex(8))
    first = None
    if admin:
        # Fixed IDs, so a second run finds them: the account (verified), its platform identity, owner.
        first = {"account_id": "platform-admin", "identity_id": "platform-admin"}
        submit(RegisterAccount(email=admin["email"], name=admin["name"], password=admin["password"], verified=True), first["account_id"])
        submit(CreateIdentity(account_id=first["account_id"], display_name=admin["name"]), first["identity_id"])
        submit(AddOwner(identity_id=first["identity_id"]), tenant, "bootstrap-admin-owner")
    return {"api": {"identity_id": identity_id, "key_id": key_id, "key": api_key(key_id, secret)}, "admin": first}
