"""The account commands: each class with its handler."""

import re

from kernable import Command, Rejected
from webauthn import verify_authentication_response, verify_registration_response
from webauthn.helpers import base64url_to_bytes, bytes_to_base64url

from ..shared.constants import (DAY, HOUR, MINUTE, PASSKEYS, PLATFORM_TENANT, RESET_MINUTES, SESSION_IDLE_DAYS,
                                SESSION_MAX_DAYS, VERIFICATION_HOURS, now_ms)
from ..shared.secrets import (trim_space, check_password, email_domain, equal_secrets, hash_password, normal_email, parse_token, random_secret, sha256,
                              totp_step, valid_email, valid_password)
from .aggregates import Account, Session, Sso, locked_until
from .domain import ACCOUNT_SUSPENSION_REASONS, AccountSuspensionReason, SessionEnd, own
from .events import (AccountDeleted, AccountReactivated, AccountRegistered, AccountSuspended, EmailChanged, EmailChangeRequested,
                     EmailVerified, PasskeyAdded, PasskeyRemoved, PasskeyUsed, PasswordChanged, PasswordReset, PasswordResetRequested,
                     ProviderLinked, ProviderUnlinked, RecoveryCodesGenerated, RegistrationAttempted, SecondFactorFailed,
                     SecondFactorProved, SessionEnded, SessionFactorAdded, SessionOpened, SessionRefreshed, SignedIn, SignInFailed,
                     SsoCreated, SsoDeleted, SsoDisabled, SsoDomainAdded, SsoDomainRemoved, SsoDomainVerified, SsoEnabled,
                     SsoEnforcementChanged, SsoUpdated, TotpConfirmed, TotpDisabled, TotpStarted, VerificationRequested)

# --- Registration and verification -------------------------------------------------------------------


class RegisterAccount(Command):
    vault = {"personal": [{"fields": ["email", "name"], "subject": own}], "secret": ["password"]}
    email: str
    name: str
    password: str | None = None
    #: Set by the API when an accepted invitation proved the email: active at once.
    verified: bool | None = None


def verification_requested(email: str, at: int) -> VerificationRequested:
    """A new verification token, kept in the vault under its own subject until used."""
    return VerificationRequested(token_id=random_secret(9), token=random_secret(), email=email, expires_at=at + VERIFICATION_HOURS * HOUR)


@Account.command(RegisterAccount)
def _(s: Account, d: RegisterAccount, cmd, ctx):
    at = now_ms(ctx)
    # A registration whose verification expired unverified is taken over by a new one of the same address.
    expired = s.status == "pending" and not any(t["purpose"] == "verify" and t["expires_at"] > at for t in s.open_tokens)
    if s.status != "none" and not expired:
        raise Rejected("The account exists", "exists")
    email = normal_email(d.email)
    if expired and email != s.email:
        raise Rejected("Only the same address takes over a registration", "exists")
    if not valid_email(email):
        raise Rejected("Not an email address", "invalid_email")
    if not isinstance(d.name, str) or not trim_space(d.name) or len(d.name) > 200:
        raise Rejected("A name is needed", "invalid_name")
    if d.password is not None and not valid_password(d.password):
        raise Rejected("A password has 10 to 1024 characters", "weak_password")
    password_hash = hash_password(d.password) if d.password is not None else None
    verified = d.verified is True
    registered = AccountRegistered(email=email, name=trim_space(d.name), password_hash=password_hash, verified=verified, at=at)
    return [registered] if verified else [registered, verification_requested(email, at)]


class RequestVerification(Command):
    pass


@Account.command(RequestVerification)
def _(s: Account, d, cmd, ctx):
    if s.status != "pending":
        raise Rejected("Only a pending account is verified", "invalid_state")
    return [verification_requested(s.email, now_ms(ctx))]


def _proven(s: Account, purpose: str, value, aggregate_id: str, now: int) -> str:
    parts = parse_token(value)
    open_ = next((t for t in s.open_tokens if t["purpose"] == purpose and parts and t["token_id"] == parts[1]), None)
    if not parts or parts[0] != aggregate_id or not open_ or not open_["token"] or not equal_secrets(open_["token"], parts[2]):
        raise Rejected("The link is not valid", "invalid_token")
    if now > open_["expires_at"]:
        raise Rejected("The link has expired", "expired_token")
    return open_["token_id"]


class VerifyEmail(Command):
    """Public: the token from the mail is the proof."""
    vault = {"secret": ["token"]}
    token: str


@Account.command(VerifyEmail)
def _(s: Account, d: VerifyEmail, cmd, ctx):
    return [EmailVerified(token_id=_proven(s, "verify", d.token, cmd.aggregate_id, now_ms(ctx)))]


# --- Signing in ----------------------------------------------------------------------------------------


def _refresh_ok(refresh_token, session_id) -> None:
    refresh = parse_token(refresh_token)
    if not refresh or refresh[0] != session_id or refresh[1] != "1":
        raise Rejected("A malformed refresh token", "invalid_token")


class SignIn(Command):
    """Public: the password is the proof. The IP and user agent stay with the events (personal)."""
    vault = {"secret": ["password", "refresh_token", "ip", "user_agent"]}
    session_id: str
    password: str
    #: `<sessionId>.1.<secret>`, generated by the API; the account keeps its hash only.
    refresh_token: str
    ip: str | None = None
    user_agent: str | None = None


@Account.command(SignIn)
def _(s: Account, d: SignIn, cmd, ctx):
    at = now_ms(ctx)

    def failed(reason):
        return [SignInFailed(method="password", reason=reason, ip=d.ip, at=at)]

    if s.status in ("none", "deleted"):
        raise Rejected("No such account", "not_found")
    _refresh_ok(d.refresh_token, d.session_id)
    if at < locked_until(s):
        return failed("locked")
    if not check_password((s.password or {}).get("hash"), d.password):
        return failed("wrong_password")
    if s.status == "pending":
        return failed("not_verified")
    if s.status != "active":
        return failed("not_active")
    return [SignedIn(session_id=d.session_id, method="password", factors=["password"], refresh_token_hash=sha256(d.refresh_token),
                     ip=d.ip, user_agent=d.user_agent, mfa=bool(s.totp and s.totp["confirmed"]), at=at)]


# --- Sessions --------------------------------------------------------------------------------------------


class OpenSession(Command):
    """Internal: the sessions reactor opens it after SignedIn."""
    vault = {"secret": ["ip", "user_agent"]}
    account_id: str
    method: str
    factors: list[str]
    refresh_token_hash: str
    ip: str | None
    user_agent: str | None
    at: int


@Session.command(OpenSession)
def _(s: Session, d: OpenSession):
    if s.status != "none":
        raise Rejected("The session exists", "exists")
    return [SessionOpened(account_id=d.account_id, method=d.method, factors=d.factors, refresh_token_hash=d.refresh_token_hash,
                          ip=d.ip, user_agent=d.user_agent, at=d.at)]


class RefreshSession(Command):
    """Public: the refresh token is the proof. It names its session and generation; one of the
    session's older tokens is a reuse and ends the session. Any other token is refused, so knowing
    a session's ID is not enough to end it."""
    vault = {"secret": ["refresh_token", "new_refresh_token", "ip"]}
    refresh_token: str
    #: `<sessionId>.<generation + 1>.<secret>`, generated by the API.
    new_refresh_token: str
    ip: str | None = None


@Session.command(RefreshSession)
def _(s: Session, d: RefreshSession, cmd, ctx):
    at = now_ms(ctx)
    parts = parse_token(d.refresh_token)
    nxt = parse_token(d.new_refresh_token)
    if s.status != "active" or not parts or parts[0] != cmd.aggregate_id or not re.fullmatch(r"[0-9]+", parts[1]):
        raise Rejected("Not a valid session", "invalid_token")
    hashed = sha256(d.refresh_token)
    if any(equal_secrets(h, hashed) for h in s.previous_token_hashes):
        return [SessionEnded(reason="reuse")]
    if int(parts[1]) != s.generation or not equal_secrets(hashed, s.refresh_token_hash or ""):
        raise Rejected("Not a valid session", "invalid_token")
    if at > s.last_refresh_at + SESSION_IDLE_DAYS * DAY:
        return [SessionEnded(reason="idle")]
    if at > s.opened_at + SESSION_MAX_DAYS * DAY:
        return [SessionEnded(reason="expired")]
    if not nxt or nxt[0] != cmd.aggregate_id or nxt[1] != str(s.generation + 1):
        raise Rejected("A malformed refresh token", "invalid_token")
    return [SessionRefreshed(generation=s.generation + 1, refresh_token_hash=sha256(d.new_refresh_token), ip=d.ip,
                             account_id=s.account_id, factors=list(s.factors), at=at)]


class AddSessionFactor(Command):
    """Internal: after a second factor was proved on the account."""
    factor: str
    #: The account that proved it: only its own session gains the factor.
    account_id: str


@Session.command(AddSessionFactor)
def _(s: Session, d: AddSessionFactor):
    if s.status != "active":
        raise Rejected("The session has ended", "invalid_state")
    if d.account_id != s.account_id:
        raise Rejected("Another account's session", "invalid_session")
    return [] if d.factor in s.factors else [SessionFactorAdded(factor=d.factor)]


class EndSession(Command):
    reason: SessionEnd | None = None


@Session.command(EndSession)
def _(s: Session, d: EndSession):
    if s.status != "active":
        raise Rejected("The session has ended", "invalid_state")
    return [SessionEnded(reason=d.reason or "signed_out")]


# --- The account's lifecycle -------------------------------------------------------------------------------


class SuspendAccount(Command):
    """The platform's; the person itself with `owner_requested`. Ends every session (a reactor)."""
    reason: AccountSuspensionReason


@Account.command(SuspendAccount)
def _(s: Account, d: SuspendAccount, cmd, ctx):
    if s.status not in ("pending", "active"):
        raise Rejected("Cannot suspend this account", "invalid_state")
    if d.reason not in ACCOUNT_SUSPENSION_REASONS:
        raise Rejected("Unknown reason", "invalid_reason")
    return [AccountSuspended(reason=d.reason, at=now_ms(ctx))]


class ReactivateAccount(Command):
    pass


@Account.command(ReactivateAccount)
def _(s: Account):
    if s.status != "suspended":
        raise Rejected("Only a suspended account is reactivated", "invalid_state")
    return [AccountReactivated()]


class DeleteAccount(Command):
    """Final, only from suspended: erases the account's personal data; its identities go too (a reactor)."""


@Account.command(DeleteAccount)
def _(s: Account):
    if s.status != "suspended":
        raise Rejected("Only a suspended account is deleted", "invalid_state")
    return [AccountDeleted()]


# --- Password and email ----------------------------------------------------------------------------------


class ChangePassword(Command):
    """The account's own. Needs the current password if it has one; ends the other sessions (a reactor)."""
    vault = {"secret": ["current", "new"]}
    current: str | None = None
    new: str
    #: The session the change comes from: it stays.
    keep_session_id: str | None = None


@Account.command(ChangePassword)
def _(s: Account, d: ChangePassword, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    if s.password and not check_password(s.password["hash"], d.current):
        raise Rejected("The current password is wrong", "wrong_password")
    if not valid_password(d.new):
        raise Rejected("A password has 10 to 1024 characters", "weak_password")
    return [PasswordChanged(hash=hash_password(d.new), keep_session_id=d.keep_session_id, at=now_ms(ctx))]


class RequestPasswordReset(Command):
    """The API's: asked for by email, without saying whether the account exists."""


@Account.command(RequestPasswordReset)
def _(s: Account, d, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    return [PasswordResetRequested(token_id=random_secret(9), token=random_secret(), email=s.email,
                                   expires_at=now_ms(ctx) + RESET_MINUTES * MINUTE)]


class ResetPassword(Command):
    """Public: the emailed token is the proof. Ends every session (a reactor)."""
    vault = {"secret": ["token", "new"]}
    token: str
    new: str


@Account.command(ResetPassword)
def _(s: Account, d: ResetPassword, cmd, ctx):
    token_id = _proven(s, "reset", d.token, cmd.aggregate_id, now_ms(ctx))
    if not valid_password(d.new):
        raise Rejected("A password has 10 to 1024 characters", "weak_password")
    return [PasswordReset(token_id=token_id, hash=hash_password(d.new), at=now_ms(ctx))]


class RequestEmailChange(Command):
    """The account's own: the new address gets a token and holds nothing until confirmed."""
    vault = {"personal": [{"fields": ["email"], "subject": own}]}
    email: str


@Account.command(RequestEmailChange)
def _(s: Account, d: RequestEmailChange, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    email = normal_email(d.email)
    if not valid_email(email):
        raise Rejected("Not an email address", "invalid_email")
    if email == s.email:
        raise Rejected("That is the address already", "unchanged")
    return [EmailChangeRequested(token_id=random_secret(9), token=random_secret(), email=email,
                                 expires_at=now_ms(ctx) + VERIFICATION_HOURS * HOUR)]


class ConfirmEmailChange(Command):
    """Public: the token sent to the new address is the proof. The kernel refuses a taken address (the key)."""
    vault = {"secret": ["token"]}
    token: str


@Account.command(ConfirmEmailChange)
def _(s: Account, d: ConfirmEmailChange, cmd, ctx):
    token_id = _proven(s, "email_change", d.token, cmd.aggregate_id, now_ms(ctx))
    return [EmailChanged(token_id=token_id, email=s.pending_email)]


class NoteRegistrationAttempt(Command):
    """The API's, when someone registers with this account's address: the account gets a mail."""


@Account.command(NoteRegistrationAttempt)
def _(s: Account, d, cmd, ctx):
    if s.status not in ("active", "suspended"):
        raise Rejected("No account to tell", "invalid_state")
    return [RegistrationAttempted(email=s.email, at=now_ms(ctx))]


# --- TOTP and recovery codes ---------------------------------------------------------------------------


class StartTotp(Command):
    """The account's own: the secret the API generated and showed (as a QR code). Its `secret` field
    is personal data (in the vault), declared by `personal`."""
    vault = {"personal": [{"fields": ["secret"], "subject": own}]}
    secret: str


@Account.command(StartTotp)
def _(s: Account, d: StartTotp):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    if s.totp and s.totp["confirmed"]:
        raise Rejected("TOTP is on already", "exists")
    if not isinstance(d.secret, str) or not re.fullmatch(r"[A-Z2-7]{16,64}", d.secret):
        raise Rejected("A base32 secret", "invalid_secret")
    return [TotpStarted(secret=d.secret)]


def _recovery_hashes(codes) -> list[str]:
    if not isinstance(codes, list) or len(codes) < 8 or not all(isinstance(c, str) and len(c) >= 10 for c in codes):
        raise Rejected("At least 8 recovery codes of 10 characters", "invalid_recovery_codes")
    return [sha256(c) for c in codes]


class ConfirmTotp(Command):
    """The first code turns TOTP on, with recovery codes."""
    vault = {"secret": ["code", "recovery_codes"]}
    code: str
    recovery_codes: list[str]


@Account.command(ConfirmTotp)
def _(s: Account, d: ConfirmTotp, cmd, ctx):
    if not s.totp or not s.totp["secret"] or s.totp["confirmed"]:
        raise Rejected("Start TOTP first", "invalid_state")
    now = now_ms(ctx)
    step = totp_step(s.totp["secret"], d.code, now)
    if step is None:
        raise Rejected("The code is not right", "wrong_code")
    return [TotpConfirmed(step=step, at=now), RecoveryCodesGenerated(hashes=_recovery_hashes(d.recovery_codes))]


def _second_factor(s: Account, code, now: int) -> dict | None:
    """A TOTP code (once) or an unused recovery code."""
    totp = s.totp
    if not totp or not totp["confirmed"] or not totp["secret"]:
        return None
    step = totp_step(totp["secret"], code, now)
    if step is not None:
        return {"method": "totp", "step": step, "recovery_code_hash": None} if step > totp["last_step"] else None
    digest = sha256(code) if isinstance(code, str) else ""
    return {"method": "recovery", "step": None, "recovery_code_hash": digest} if digest in s.recovery_codes else None


class DisableTotp(Command):
    vault = {"secret": ["code"]}
    code: str


@Account.command(DisableTotp)
def _(s: Account, d: DisableTotp, cmd, ctx):
    if not _second_factor(s, d.code, now_ms(ctx)):
        raise Rejected("The code is not right", "wrong_code")
    return [TotpDisabled()]


class RegenerateRecoveryCodes(Command):
    vault = {"secret": ["code", "recovery_codes"]}
    code: str
    recovery_codes: list[str]


@Account.command(RegenerateRecoveryCodes)
def _(s: Account, d: RegenerateRecoveryCodes, cmd, ctx):
    if not _second_factor(s, d.code, now_ms(ctx)):
        raise Rejected("The code is not right", "wrong_code")
    return [RecoveryCodesGenerated(hashes=_recovery_hashes(d.recovery_codes))]


class ProveSecondFactor(Command):
    """Public: the code is the proof. A failure is an event and counts toward the lockout."""
    vault = {"secret": ["code"]}
    session_id: str
    code: str


@Account.command(ProveSecondFactor)
def _(s: Account, d: ProveSecondFactor, cmd, ctx):
    now = now_ms(ctx)
    if s.status != "active" or not s.totp or not s.totp["confirmed"]:
        raise Rejected("No second factor to prove", "invalid_state")
    if now < locked_until(s):
        return [SecondFactorFailed(session_id=d.session_id, at=now)]
    proof = _second_factor(s, d.code, now)
    if not proof:
        return [SecondFactorFailed(session_id=d.session_id, at=now)]
    return [SecondFactorProved(session_id=d.session_id, **proof)]


# --- Passkeys ----------------------------------------------------------------------------------------


def _another_method(s: Account, passkey: str | None = None, link: dict | None = None) -> bool:
    """Whether the account keeps another way to sign in (never remove the last one)."""
    return (s.password is not None or any(p["credential_id"] != passkey for p in s.passkeys)
            or any(not (link and x["provider"] == link["provider"] and x["subject"] == link["subject"]) for x in s.links))


class AddPasskey(Command):
    """The account's own: a passkey registered against a challenge the API issued."""
    vault = {"personal": [{"fields": ["label"], "subject": own}]}
    raw = ["response"]
    challenge: str
    response: dict
    label: str | None = None


@Account.command(AddPasskey)
def _(s: Account, d: AddPasskey, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    try:
        v = verify_registration_response(credential=d.response, expected_challenge=base64url_to_bytes(d.challenge),
                                         expected_rp_id=PASSKEYS.rp_id, expected_origin=PASSKEYS.origins, require_user_verification=True)
    except Exception as err:  # the library raises its own errors for every flaw
        raise Rejected(f"Not a valid passkey: {err}", "invalid_passkey") from None
    credential_id = bytes_to_base64url(v.credential_id)
    if any(p["credential_id"] == credential_id for p in s.passkeys):
        raise Rejected("The passkey is added already", "exists")
    return [PasskeyAdded(credential_id=credential_id, public_key=bytes_to_base64url(v.credential_public_key), counter=v.sign_count,
                         label=d.label, at=now_ms(ctx))]


class RemovePasskey(Command):
    credential_id: str


@Account.command(RemovePasskey)
def _(s: Account, d: RemovePasskey):
    if not any(p["credential_id"] == d.credential_id for p in s.passkeys):
        raise Rejected("No such passkey", "not_found")
    if not _another_method(s, passkey=d.credential_id):
        raise Rejected("The last way to sign in stays", "last_method")
    return [PasskeyRemoved(credential_id=d.credential_id)]


def _asserted(s: Account, challenge: str, response: dict) -> dict | None:
    passkey = next((p for p in s.passkeys if p["credential_id"] == (response or {}).get("id")), None)
    if not passkey or not passkey["credential_id"]:
        return None
    try:
        v = verify_authentication_response(credential=response, expected_challenge=base64url_to_bytes(challenge), expected_rp_id=PASSKEYS.rp_id,
                                           expected_origin=PASSKEYS.origins, credential_public_key=base64url_to_bytes(passkey["public_key"]),
                                           credential_current_sign_count=passkey["counter"], require_user_verification=True)
    except Exception:
        return None
    return {"credential_id": passkey["credential_id"], "counter": v.new_sign_count}


class SignInWithPasskey(Command):
    """The API's (it issued the challenge): a passwordless sign-in, counting as two factors."""
    vault = {"secret": ["refresh_token", "ip", "user_agent"]}
    raw = ["response"]
    session_id: str
    challenge: str
    response: dict
    refresh_token: str
    ip: str | None = None
    user_agent: str | None = None


@Account.command(SignInWithPasskey)
def _(s: Account, d: SignInWithPasskey, cmd, ctx):
    at = now_ms(ctx)

    def failed(reason):
        return [SignInFailed(method="passkey", reason=reason, ip=d.ip, at=at)]

    if s.status in ("none", "deleted"):
        raise Rejected("No such account", "not_found")
    _refresh_ok(d.refresh_token, d.session_id)
    if at < locked_until(s):
        return failed("locked")
    ok = _asserted(s, d.challenge, d.response)
    if not ok:
        return failed("wrong_password")
    if s.status != "active":
        return failed("not_active")
    return [PasskeyUsed(**ok), SignedIn(session_id=d.session_id, method="passkey", factors=["passkey"], refresh_token_hash=sha256(d.refresh_token),
                                        ip=d.ip, user_agent=d.user_agent, mfa=False, at=at)]


class ProvePasskey(Command):
    """The API's: a passkey as the second factor after a password."""
    raw = ["response"]
    session_id: str
    challenge: str
    response: dict


@Account.command(ProvePasskey)
def _(s: Account, d: ProvePasskey, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    ok = _asserted(s, d.challenge, d.response)
    if not ok:
        return [SecondFactorFailed(session_id=d.session_id, at=now_ms(ctx))]
    return [PasskeyUsed(**ok), SecondFactorProved(session_id=d.session_id, method="passkey", step=None, recovery_code_hash=None)]


# --- OAuth/OIDC and SSO --------------------------------------------------------------------------------


def _provider(value) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"(sso:[\w-]+|[a-z][\w-]{1,40})", value):
        raise Rejected("A provider is a name, or sso:<id>", "invalid_provider")
    return value


class SignInWithProvider(Command):
    """The API's: it did the OAuth/OIDC exchange and checked the ID token. A linked login signs in;
    an unlinked one is linked first only for a tenant's SSO (`auto_link`, which the API sets), the
    same verified email, and an email in one of the domains the tenant verified for that SSO
    (`domains`, which the API read): else a tenant's provider could take over any account by
    answering with its email (`email_not_in_domain`). Otherwise `not_linked`."""
    vault = {"secret": ["refresh_token", "ip", "user_agent", "subject", "email"]}
    session_id: str
    provider: str
    subject: str
    email: str | None = None
    email_verified: bool | None = None
    auto_link: bool | None = None
    #: For an `sso:` provider: the domains its tenant verified.
    domains: list[str] | None = None
    refresh_token: str
    ip: str | None = None
    user_agent: str | None = None


@Account.command(SignInWithProvider)
def _(s: Account, d: SignInWithProvider, cmd, ctx):
    at = now_ms(ctx)
    p = _provider(d.provider)

    def failed(reason):
        return [SignInFailed(method=p, reason=reason, ip=d.ip, at=at)]

    if s.status in ("none", "deleted"):
        raise Rejected("No such account", "not_found")
    _refresh_ok(d.refresh_token, d.session_id)
    if not isinstance(d.subject, str) or not d.subject:
        raise Rejected("Which subject?", "invalid_subject")
    if s.status != "active":
        return failed("not_active")
    sso = p.startswith("sso:")
    linked = any(x["provider"] == p and x["subject"] == d.subject for x in s.links)
    auto = not linked and sso and d.auto_link is True and d.email_verified is True and normal_email(d.email) == s.email
    if auto and not (isinstance(d.domains, list) and any(isinstance(x, str) and x.lower() == email_domain(d.email) for x in d.domains)):
        raise Rejected("The email is not in a domain of this single sign-on", "email_not_in_domain")
    if not linked and not auto:
        return failed("not_linked")
    signed = SignedIn(session_id=d.session_id, method=p, factors=["sso" if sso else "provider"],
                      refresh_token_hash=sha256(d.refresh_token), ip=d.ip, user_agent=d.user_agent,
                      mfa=bool(s.totp and s.totp["confirmed"]), at=at)
    return [ProviderLinked(provider=p, subject=d.subject, email=normal_email(d.email), at=at), signed] if auto else [signed]


class LinkProvider(Command):
    """The account's own, through the API (it checked the provider's answer)."""
    vault = {"secret": ["subject", "email"]}
    provider: str
    subject: str
    email: str | None = None


@Account.command(LinkProvider)
def _(s: Account, d: LinkProvider, cmd, ctx):
    if s.status != "active":
        raise Rejected("The account is not active", "invalid_state")
    p = _provider(d.provider)
    if not isinstance(d.subject, str) or not d.subject:
        raise Rejected("Which subject?", "invalid_subject")
    if any(x["provider"] == p and x["subject"] == d.subject for x in s.links):
        raise Rejected("Linked already", "exists")
    return [ProviderLinked(provider=p, subject=d.subject, email=normal_email(d.email) if d.email else None, at=now_ms(ctx))]


class UnlinkProvider(Command):
    vault = {"secret": ["subject"]}
    provider: str
    subject: str


@Account.command(UnlinkProvider)
def _(s: Account, d: UnlinkProvider):
    if not any(x["provider"] == d.provider and x["subject"] == d.subject for x in s.links):
        raise Rejected("Not linked", "not_found")
    if not _another_method(s, link={"provider": d.provider, "subject": d.subject}):
        raise Rejected("The last way to sign in stays", "last_method")
    return [ProviderUnlinked(provider=d.provider, subject=d.subject)]


def _issuer(value) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"https?://\S+", value):
        raise Rejected("An issuer URL", "invalid_issuer")
    return value.rstrip("/")


def _sso_domain(value) -> str:
    d = trim_space(value).lower() if isinstance(value, str) else ""
    if not re.fullmatch(r"([a-z0-9](-?[a-z0-9])*\.)+[a-z]{2,}", d):
        raise Rejected("A domain name", "invalid_domain")
    return d


class CreateSso(Command):
    """The tenant's owners: an OIDC provider for the tenant's people."""
    vault = {"secret": ["client_secret"]}
    issuer: str
    client_id: str
    client_secret: str


@Sso.command(CreateSso)
def _(s: Sso, d: CreateSso):
    if s.status != "none":
        raise Rejected("The SSO exists", "exists")
    if not d.client_id or not d.client_secret:
        raise Rejected("A client ID and secret", "invalid_client")
    return [SsoCreated(issuer=_issuer(d.issuer), client_id=d.client_id, client_secret=d.client_secret)]


class UpdateSso(Command):
    vault = {"secret": ["client_secret"]}
    issuer: str
    client_id: str
    client_secret: str


@Sso.command(UpdateSso)
def _(s: Sso, d: UpdateSso):
    if s.status not in ("active", "disabled"):
        raise Rejected("No such SSO", "not_found")
    return [SsoUpdated(issuer=_issuer(d.issuer), client_id=d.client_id, client_secret=d.client_secret)]


class AddSsoDomain(Command):
    domain: str


@Sso.command(AddSsoDomain)
def _(s: Sso, d: AddSsoDomain):
    if s.status not in ("active", "disabled"):
        raise Rejected("No such SSO", "not_found")
    domain = _sso_domain(d.domain)
    if any(x["domain"] == domain for x in s.domains):
        raise Rejected("Added already", "exists")
    return [SsoDomainAdded(domain=domain, txt=f"kernable-sso={random_secret(18)}")]


class VerifySsoDomain(Command):
    """The API's: it found the TXT record at `_kernable-sso.<domain>`."""
    domain: str
    found: list[str]


@Sso.command(VerifySsoDomain)
def _(s: Sso, d: VerifySsoDomain):
    x = next((y for y in s.domains if y["domain"] == d.domain), None)
    if not x:
        raise Rejected("No such domain", "not_found")
    if x["verified"]:
        return []
    if not isinstance(d.found, list) or x["txt"] not in d.found:
        raise Rejected("The TXT record is not there", "not_verified")
    return [SsoDomainVerified(domain=x["domain"])]


class RemoveSsoDomain(Command):
    domain: str


@Sso.command(RemoveSsoDomain)
def _(s: Sso, d: RemoveSsoDomain):
    if not any(y["domain"] == d.domain for y in s.domains):
        raise Rejected("No such domain", "not_found")
    return [SsoDomainRemoved(domain=d.domain)]


class EnforceSso(Command):
    enforced: bool


@Sso.command(EnforceSso)
def _(s: Sso, d: EnforceSso):
    if s.status != "active":
        raise Rejected("Only an active SSO is enforced", "invalid_state")
    return [SsoEnforcementChanged(enforced=d.enforced is True)]


class DisableSso(Command):
    pass


@Sso.command(DisableSso)
def _(s: Sso):
    if s.status != "active":
        raise Rejected("Only an active SSO is disabled", "invalid_state")
    return [SsoEnforcementChanged(enforced=False), SsoDisabled()] if s.enforced else [SsoDisabled()]


class EnableSso(Command):
    pass


@Sso.command(EnableSso)
def _(s: Sso):
    if s.status != "disabled":
        raise Rejected("Only a disabled SSO is enabled", "invalid_state")
    return [SsoEnabled()]


class DeleteSso(Command):
    """Erases the client secret."""


@Sso.command(DeleteSso)
def _(s: Sso):
    if s.status in ("none", "deleted"):
        raise Rejected("No such SSO", "not_found")
    return [SsoDeleted()]
