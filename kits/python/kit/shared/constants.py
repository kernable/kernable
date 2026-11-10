"""Settings of the kit. The kit is yours: change them here (design/kits.md, "Constants")."""

from types import SimpleNamespace

#: The tenant the platform's own accounts, sessions, groups and administrators live in.
PLATFORM_TENANT = "platform"

#: Slugs nobody may take for a tenant.
RESERVED_SLUGS = ["platform", "www", "api", "admin", "app", "mail", "auth", "login", "static"]

#: How long a suspended tenant waits before it may be deleted.
DELETE_AFTER_DAYS = 0

#: Failed sign-ins before the account locks for a while: 1, 2, 4 … minutes, at most the maximum.
LOCKOUT_AFTER = 5
LOCKOUT_MAX_MINUTES = 15

VERIFICATION_HOURS = 48
RESET_MINUTES = 60
#: At most one mail per account in this many minutes for what anyone can ask for by email: a
#: password reset, the "you already have an account" mail. A request within it sends nothing.
MAIL_COOLDOWN_MINUTES = 5
ACCESS_TOKEN_MINUTES = 15
SESSION_IDLE_DAYS = 14
SESSION_MAX_DAYS = 90
INVITATION_DAYS = 7
#: How many of a session's older refresh tokens are recognised as a reuse (which ends the session).
REUSE_WINDOW = 32

#: Listener orders the kit takes: its keys need a grant of `reserved_order`. `cleanup`:
#: reactors that must see a deleted tenant's rows before the read models drop them.
ORDERS = SimpleNamespace(authorization=-30, cleanup=-25, read_models=-20, reactors=-10)

MINUTE = 60_000
HOUR = 60 * MINUTE
DAY = 24 * HOUR

def internal(ctx=None):
    """The context of the kit's own follow-up commands (the event's, followed): they pass the
    middleware as internal. Without a listener context, a bare internal one."""
    import dataclasses

    from kernable import RequestContext
    return dataclasses.replace(ctx.follow(), bypass_authorization=True) if ctx is not None else RequestContext(bypass_authorization=True)

#: The relying party passkeys are bound to (WebAuthn): your site's domain, name and origins.
PASSKEYS = SimpleNamespace(rp_id="localhost", rp_name="kernable", origins=["http://localhost:3000"])
CHALLENGE_SECONDS = 300

#: The authorization middleware's store; `use_authorization_store` changes it.
AUTHORIZATION_STORE = "sqlite:authorization.db"


def now_ms(ctx) -> int:
    """The command's time (`ctx.now()`) in milliseconds, as events carry it."""
    return int(ctx.now().timestamp() * 1000)
