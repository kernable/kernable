"""The account reactors: side effects and follow-up commands."""

from ..shared.constants import ORDERS, PLATFORM_TENANT, internal
from ..shared.mail import Mail, current_mailer
from ..shared.secrets import token
from .commands import AddSessionFactor, EndSession, OpenSession
from .domain import account_domain
from .events import (AccountDeleted, AccountSuspended, EmailChangeRequested, PasswordChanged, PasswordReset, PasswordResetRequested,
                     RegistrationAttempted, SecondFactorProved, SignedIn, VerificationRequested)
from .queries import ListSessions


def _mail(to, purpose, link, subject, text) -> None:
    if to:
        current_mailer().send(Mail(to=to, subject=subject, text=text, purpose=purpose, token=link))


mail_reactor = account_domain.reactor("account.mail", order=ORDERS.reactors)


@mail_reactor.on(VerificationRequested)
def _(d: VerificationRequested, evt):
    if d.token:  # erased already: nothing to send
        link = token(evt.aggregate_id, d.token_id, d.token)
        _mail(d.email, "verify", link, "Confirm your email address", f"Confirm your email address with this code: {link}")


@mail_reactor.on(PasswordResetRequested)
def _(d: PasswordResetRequested, evt):
    if d.token:
        link = token(evt.aggregate_id, d.token_id, d.token)
        _mail(d.email, "reset", link, "Reset your password", f"Set a new password with this code: {link}")


@mail_reactor.on(EmailChangeRequested)
def _(d: EmailChangeRequested, evt):
    if d.token:
        link = token(evt.aggregate_id, d.token_id, d.token)
        _mail(d.email, "email_change", link, "Confirm your new email address", f"Confirm this address with this code: {link}")


@mail_reactor.on(RegistrationAttempted)
def _(d: RegistrationAttempted):
    _mail(d.email, "account_exists", "", "You already have an account",
          "Someone registered with this address, which has an account already. Sign in, or reset your password.")


sessions_reactor = account_domain.reactor("account.sessions", order=ORDERS.reactors)


@sessions_reactor.on(SignedIn)
def _(d: SignedIn, evt, ctx):
    """Opens the session of every sign-in."""
    ctx.submit(OpenSession(account_id=evt.aggregate_id, method=d.method, factors=d.factors, refresh_token_hash=d.refresh_token_hash,
                           ip=d.ip, user_agent=d.user_agent, at=d.at),
               aggregate_id=d.session_id, id=f"open-session-{d.session_id}", tenant=PLATFORM_TENANT, context=internal(ctx))


@sessions_reactor.on(SecondFactorProved)
def _(d: SecondFactorProved, evt, ctx):
    ctx.submit(AddSessionFactor(factor=d.method, account_id=evt.aggregate_id), aggregate_id=d.session_id,
               id=f"factor-{d.session_id}-{evt.command_id}", tenant=PLATFORM_TENANT, context=internal(ctx))


ending_reactor = account_domain.reactor("account.ending", order=ORDERS.reactors)


def _end_all(account_id: str, reason: str, ctx, keep: str | None = None) -> None:
    from kernable import Rejected

    for s in ctx.query(ListSessions(account_id=account_id), tenant=PLATFORM_TENANT, context=internal()):
        if not s["active"] or s["id"] == keep:
            continue
        try:
            ctx.submit(EndSession(reason=reason), aggregate_id=s["id"], id=f"end-{s['id']}-{reason}", tenant=PLATFORM_TENANT, context=internal(ctx))
        except Rejected as err:
            if err.code != "invalid_state":  # ended meanwhile (signed out)
                raise


@ending_reactor.on(AccountSuspended)
def _(d, evt, ctx):
    _end_all(evt.aggregate_id, "account_suspended", ctx)


@ending_reactor.on(AccountDeleted)
def _(d, evt, ctx):
    _end_all(evt.aggregate_id, "account_deleted", ctx)


@ending_reactor.on(PasswordChanged)
def _(d: PasswordChanged, evt, ctx):
    """A new password signs out everywhere else."""
    _end_all(evt.aggregate_id, "password_changed", ctx, d.keep_session_id)


@ending_reactor.on(PasswordReset)
def _(d, evt, ctx):
    _end_all(evt.aggregate_id, "password_changed", ctx)
