"""The account events: what happened to accounts, sessions and SSOs, the vocabulary other domains read."""

from typing import Literal

from kernable import Event

from .domain import AccountSuspensionReason, SessionEnd, account_domain, own


def _token(d, _id):
    return f"token:{d['token_id']}"


def _of_account(d, _id):
    return d["account_id"]


class AccountRegistered(Event):
    personal = [{"fields": ["email", "name", "password_hash"], "subject": own}]
    email: str | None
    name: str | None
    #: argon2id; None for an account without a password (passkeys, single sign-on).
    password_hash: str | None
    #: True when an accepted invitation proved the email: active at once.
    verified: bool
    at: int


class VerificationRequested(Event):
    """The token stays in the vault under its own subject until it is used."""
    personal = [{"fields": ["email"], "subject": own}, {"fields": ["token"], "subject": _token}]
    token_id: str
    token: str | None
    email: str | None
    expires_at: int


class EmailVerified(Event):
    """Erases the used token."""
    erases = _token
    token_id: str


class SignedIn(Event):
    personal = [{"fields": ["ip", "user_agent"], "subject": own}]
    session_id: str
    method: str
    factors: list[str]
    refresh_token_hash: str
    ip: str | None
    user_agent: str | None
    #: The account has a second factor on: the session must prove it before anything else.
    mfa: bool
    at: int


class SignInFailed(Event):
    """An event, not a rejection: audited, and it counts toward the lockout."""
    personal = [{"fields": ["ip"], "subject": own}]
    method: str
    reason: Literal["wrong_password", "locked", "not_verified", "not_active", "not_linked"]
    ip: str | None
    at: int


class SessionOpened(Event):
    personal = [{"fields": ["ip", "user_agent"], "subject": _of_account}]
    account_id: str
    method: str
    factors: list[str]
    refresh_token_hash: str
    ip: str | None
    user_agent: str | None
    at: int


class SessionRefreshed(Event):
    personal = [{"fields": ["ip"], "subject": _of_account}]
    generation: int
    refresh_token_hash: str
    ip: str | None
    account_id: str
    #: The session's factors, so the API issues the new access token without asking.
    factors: list[str]
    at: int


class SessionFactorAdded(Event):
    factor: str


class SessionEnded(Event):
    reason: SessionEnd


class AccountSuspended(Event):
    """Ends every session (a reactor); sign-in is refused."""
    reason: AccountSuspensionReason
    at: int


class AccountReactivated(Event):
    pass


class AccountDeleted(Event):
    """Erases the account's personal data, in its events and its sessions'."""
    erases = own


class PasswordChanged(Event):
    """Ends the account's other sessions (a reactor); `keep_session_id` is the one it was changed from."""
    personal = [{"fields": ["hash"], "subject": own}]
    hash: str | None
    keep_session_id: str | None
    at: int


class PasswordResetRequested(Event):
    personal = [{"fields": ["email"], "subject": own}, {"fields": ["token"], "subject": _token}]
    token_id: str
    token: str | None
    email: str | None
    expires_at: int


class PasswordReset(Event):
    """Erases the used token; ends every session (a reactor)."""
    personal = [{"fields": ["hash"], "subject": own}]
    erases = _token
    token_id: str
    hash: str | None
    at: int


class EmailChangeRequested(Event):
    """The new address gets a token; it holds nothing until confirmed."""
    personal = [{"fields": ["email"], "subject": own}, {"fields": ["token"], "subject": _token}]
    token_id: str
    token: str | None
    email: str | None
    expires_at: int


class EmailChanged(Event):
    personal = [{"fields": ["email"], "subject": own}]
    erases = _token
    token_id: str
    email: str | None


class RegistrationAttempted(Event):
    """Someone registered with this account's address: it gets a mail saying it exists."""
    personal = [{"fields": ["email"], "subject": own}]
    email: str | None
    at: int


class TotpStarted(Event):
    personal = [{"fields": ["secret"], "subject": own}]
    secret: str | None


class TotpConfirmed(Event):
    step: int
    at: int


class TotpDisabled(Event):
    pass


class RecoveryCodesGenerated(Event):
    hashes: list[str]


class SecondFactorProved(Event):
    session_id: str
    method: Literal["totp", "recovery", "passkey"]
    step: int | None
    recovery_code_hash: str | None


class SecondFactorFailed(Event):
    session_id: str
    at: int


class PasskeyAdded(Event):
    personal = [{"fields": ["credential_id", "label"], "subject": own}]
    credential_id: str | None
    public_key: str
    counter: int
    label: str | None
    at: int


class PasskeyRemoved(Event):
    personal = [{"fields": ["credential_id"], "subject": own}]
    credential_id: str | None


class PasskeyUsed(Event):
    personal = [{"fields": ["credential_id"], "subject": own}]
    credential_id: str | None
    counter: int


class ProviderLinked(Event):
    personal = [{"fields": ["subject", "email"], "subject": own}]
    provider: str
    subject: str | None
    email: str | None
    at: int


class ProviderUnlinked(Event):
    personal = [{"fields": ["subject"], "subject": own}]
    provider: str
    subject: str | None


def _of_sso(_d, sso_id):
    return f"sso:{sso_id}"


class SsoCreated(Event):
    personal = [{"fields": ["client_secret"], "subject": _of_sso}]
    issuer: str
    client_id: str
    client_secret: str | None


class SsoUpdated(Event):
    personal = [{"fields": ["client_secret"], "subject": _of_sso}]
    issuer: str
    client_id: str
    client_secret: str | None


class SsoDomainAdded(Event):
    domain: str
    txt: str


class SsoDomainVerified(Event):
    domain: str


class SsoDomainRemoved(Event):
    domain: str


class SsoEnforcementChanged(Event):
    enforced: bool


class SsoDisabled(Event):
    pass


class SsoEnabled(Event):
    pass


class SsoDeleted(Event):
    erases = _of_sso


# Declared on the domain, so a process that only reads them knows them too.
account_domain.event(*[c for c in list(globals().values()) if isinstance(c, type) and issubclass(c, Event) and c is not Event])
