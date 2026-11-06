"""The account aggregates: state, how each event changes it, unique keys and facts."""

from kernable import Aggregate

from ..shared.constants import LOCKOUT_AFTER, LOCKOUT_MAX_MINUTES, MINUTE
from .domain import AccountStatus, account_domain
from .events import (AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailChanged,
                     EmailChangeRequested, EmailVerified, PasskeyAdded, PasskeyRemoved, PasskeyUsed, PasswordChanged,
                     PasswordReset, PasswordResetRequested, ProviderLinked, ProviderUnlinked, RecoveryCodesGenerated,
                     SecondFactorFailed, SecondFactorProved, SessionEnded, SessionFactorAdded, SessionOpened, SessionRefreshed,
                     SignedIn, SignInFailed, SsoCreated, SsoDeleted, SsoDisabled, SsoDomainAdded, SsoDomainRemoved,
                     SsoDomainVerified, SsoEnabled, SsoEnforcementChanged, SsoUpdated, TotpConfirmed, TotpDisabled, TotpStarted,
                     VerificationRequested)


class Account(Aggregate, domain=account_domain, schema="py-1"):
    """A person's account, in the platform tenant. Its snapshots say `py-1`: a TypeScript
    worker's snapshots (other key names) are never mixed in."""
    id: str | None = None
    status: AccountStatus = "none"
    email: str | None = None
    email_verified: bool = False
    name: str | None = None
    password: dict | None = None
    failures: dict = {"count": 0, "last_at": 0}
    #: Open emailed tokens; the token itself is in the vault until it is used.
    open_tokens: list = []
    suspension: dict | None = None
    #: A new address waiting for its confirmation: it holds no key yet.
    pending_email: str | None = None
    totp: dict | None = None
    #: SHA-256 of the unused recovery codes.
    recovery_codes: list = []
    passkeys: list = []
    links: list = []


def locked_until(s: Account) -> int:
    """Until when the account is locked after failed sign-ins: computed, never stored."""
    count = s.failures["count"]
    if count < LOCKOUT_AFTER:
        return 0
    return s.failures["last_at"] + min(2 ** (count - LOCKOUT_AFTER), LOCKOUT_MAX_MINUTES) * MINUTE


def _open(s: Account, purpose: str, d) -> None:
    s.open_tokens = [t for t in s.open_tokens if t["purpose"] != purpose]
    s.open_tokens.append({"token_id": d.token_id, "purpose": purpose, "token": d.token, "expires_at": d.expires_at})


def _close(s: Account, token_id: str) -> None:
    s.open_tokens = [t for t in s.open_tokens if t["token_id"] != token_id]


@Account.apply(AccountRegistered)
def _(s: Account, d: AccountRegistered, evt):
    s.update(id=evt.aggregate_id, status="active" if d.verified else "pending", email=d.email, email_verified=d.verified,
             name=d.name, password={"hash": d.password_hash, "set_at": d.at} if d.password_hash else None)


@Account.apply(VerificationRequested)
def _(s: Account, d: VerificationRequested):
    _open(s, "verify", d)


@Account.apply(EmailVerified)
def _(s: Account, d: EmailVerified):
    s.email_verified = True
    if s.status == "pending":
        s.status = "active"
    _close(s, d.token_id)


@Account.apply(AccountSuspended)
def _(s: Account, d: AccountSuspended):
    s.status = "suspended"
    s.suspension = {"reason": d.reason, "at": d.at}


@Account.apply(AccountReactivated)
def _(s: Account):
    s.status = "active"
    s.suspension = None


@Account.apply(AccountDeleted)
def _(s: Account):
    s.update(status="deleted", password=None, open_tokens=[])


@Account.apply(PasswordChanged)
def _(s: Account, d: PasswordChanged):
    s.password = {"hash": d.hash, "set_at": d.at}


@Account.apply(PasswordResetRequested)
def _(s: Account, d: PasswordResetRequested):
    _open(s, "reset", d)


@Account.apply(PasswordReset)
def _(s: Account, d: PasswordReset):
    s.password = {"hash": d.hash, "set_at": d.at}
    s.failures = {"count": 0, "last_at": 0}
    _close(s, d.token_id)


@Account.apply(EmailChangeRequested)
def _(s: Account, d: EmailChangeRequested):
    s.pending_email = d.email
    _open(s, "email_change", d)


@Account.apply(EmailChanged)
def _(s: Account, d: EmailChanged):
    s.update(email=d.email, email_verified=True, pending_email=None)
    _close(s, d.token_id)


@Account.apply(TotpStarted)
def _(s: Account, d: TotpStarted):
    s.totp = {"secret": d.secret, "confirmed": False, "last_step": 0}


@Account.apply(TotpConfirmed)
def _(s: Account, d: TotpConfirmed):
    if s.totp:
        s.totp.update(confirmed=True, last_step=d.step)


@Account.apply(TotpDisabled)
def _(s: Account):
    s.totp = None
    s.recovery_codes = []


@Account.apply(RecoveryCodesGenerated)
def _(s: Account, d: RecoveryCodesGenerated):
    s.recovery_codes = list(d.hashes)


@Account.apply(SecondFactorProved)
def _(s: Account, d: SecondFactorProved):
    if d.step is not None and s.totp:
        s.totp["last_step"] = d.step
    if d.recovery_code_hash:
        s.recovery_codes = [h for h in s.recovery_codes if h != d.recovery_code_hash]
    s.failures = {"count": 0, "last_at": 0}


@Account.apply(SecondFactorFailed)
def _(s: Account, d: SecondFactorFailed):
    s.failures = {"count": s.failures["count"] + 1, "last_at": d.at}


@Account.apply(PasskeyAdded)
def _(s: Account, d: PasskeyAdded):
    s.passkeys.append({"credential_id": d.credential_id, "public_key": d.public_key, "counter": d.counter, "label": d.label})


@Account.apply(PasskeyRemoved)
def _(s: Account, d: PasskeyRemoved):
    s.passkeys = [p for p in s.passkeys if p["credential_id"] != d.credential_id]


@Account.apply(PasskeyUsed)
def _(s: Account, d: PasskeyUsed):
    for p in s.passkeys:
        if p["credential_id"] == d.credential_id:
            p["counter"] = d.counter


@Account.apply(ProviderLinked)
def _(s: Account, d: ProviderLinked):
    s.links.append({"provider": d.provider, "subject": d.subject})


@Account.apply(ProviderUnlinked)
def _(s: Account, d: ProviderUnlinked):
    s.links = [x for x in s.links if not (x["provider"] == d.provider and x["subject"] == d.subject)]


@Account.apply(SignedIn)
def _(s: Account):
    s.failures = {"count": 0, "last_at": 0}


@Account.apply(SignInFailed)
def _(s: Account, d: SignInFailed):
    if d.reason == "wrong_password":
        s.failures = {"count": s.failures["count"] + 1, "last_at": d.at}


# Each external login belongs to one account.
Account.unique("link", lambda s: None if s.status == "deleted" else [f"{x['provider']}:{x['subject']}" for x in s.links if x["subject"]],
                         personal=lambda s: s.id)
# Held from registration on, while not deleted: nobody registers an email twice.
Account.unique("email", lambda s: s.email if s.status not in ("none", "deleted") else None, personal=lambda s: s.id)
Account.fact("active", lambda s: s.status == "active")
Account.fact("mfa", lambda s: bool(s.totp and s.totp["confirmed"]))


# --- Sessions ---------------------------------------------------------------------------------------


class Session(Aggregate, domain=account_domain, schema="py-1"):
    """One sign-in."""
    status: str = "none"
    account_id: str | None = None
    method: str | None = None
    factors: list = []
    generation: int = 0
    refresh_token_hash: str | None = None
    opened_at: int = 0
    last_refresh_at: int = 0
    end_reason: str | None = None


@Session.apply(SessionOpened)
def _(s: Session, d: SessionOpened):
    s.update(status="active", account_id=d.account_id, method=d.method, factors=list(d.factors), generation=1,
             refresh_token_hash=d.refresh_token_hash, opened_at=d.at, last_refresh_at=d.at)


@Session.apply(SessionRefreshed)
def _(s: Session, d: SessionRefreshed):
    s.update(generation=d.generation, refresh_token_hash=d.refresh_token_hash, last_refresh_at=d.at)


@Session.apply(SessionFactorAdded)
def _(s: Session, d: SessionFactorAdded):
    s.factors.append(d.factor)


@Session.apply(SessionEnded)
def _(s: Session, d: SessionEnded):
    s.status = "ended"
    s.end_reason = d.reason


# --- account.sso: a tenant's single sign-on, under the tenant's ID ---------------------------


class Sso(Aggregate, domain=account_domain, schema="py-1"):
    """A tenant's single sign-on, under the tenant's ID."""
    status: str = "none"
    issuer: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    domains: list = []
    enforced: bool = False


@Sso.apply(SsoCreated)
def _(s: Sso, d: SsoCreated):
    s.update(status="active", issuer=d.issuer, client_id=d.client_id, client_secret=d.client_secret)


@Sso.apply(SsoUpdated)
def _(s: Sso, d: SsoUpdated):
    s.update(issuer=d.issuer, client_id=d.client_id, client_secret=d.client_secret)


@Sso.apply(SsoDomainAdded)
def _(s: Sso, d: SsoDomainAdded):
    s.domains.append({"domain": d.domain, "txt": d.txt, "verified": False})


@Sso.apply(SsoDomainVerified)
def _(s: Sso, d: SsoDomainVerified):
    for x in s.domains:
        if x["domain"] == d.domain:
            x["verified"] = True


@Sso.apply(SsoDomainRemoved)
def _(s: Sso, d: SsoDomainRemoved):
    s.domains = [x for x in s.domains if x["domain"] != d.domain]


@Sso.apply(SsoEnforcementChanged)
def _(s: Sso, d: SsoEnforcementChanged):
    s.enforced = d.enforced


@Sso.apply(SsoDisabled)
def _(s: Sso):
    s.status = "disabled"


@Sso.apply(SsoEnabled)
def _(s: Sso):
    s.status = "active"


@Sso.apply(SsoDeleted)
def _(s: Sso):
    s.update(status="deleted", client_secret=None, domains=[])


# A verified domain belongs to one tenant's SSO.
Sso.unique("domain", lambda s: None if s.status == "deleted" else [x["domain"] for x in s.domains if x["verified"]], scope="global")

#: The aggregate types, for workers and `requires`.
account_aggregate, session_aggregate, sso_aggregate = Account.__aggregate__, Session.__aggregate__, Sso.__aggregate__
