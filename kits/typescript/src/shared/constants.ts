// Settings of the kit. The kit is yours: change them here (design/kits.md, "Constants").

/** The tenant the platform's own accounts, sessions, groups and administrators live in. */
export const PLATFORM_TENANT = "platform";

/** Slugs nobody may take for a tenant. */
export const RESERVED_SLUGS = ["platform", "www", "api", "admin", "app", "mail", "auth", "login", "static"];

/** How long a suspended tenant waits before it may be deleted. */
export const DELETE_AFTER_DAYS = 0;

/** Failed sign-ins before the account locks for a while: 1, 2, 4 … minutes, at most the maximum. */
export const LOCKOUT_AFTER = 5;
export const LOCKOUT_MAX_MINUTES = 15;

export const VERIFICATION_HOURS = 48;
export const RESET_MINUTES = 60;
/**
 * At most one mail per account in this many minutes for what anyone can ask for by email: a
 * password reset, the "you already have an account" mail. A request within it sends nothing.
 */
export const MAIL_COOLDOWN_MINUTES = 5;
export const ACCESS_TOKEN_MINUTES = 15;
export const SESSION_IDLE_DAYS = 14;
export const SESSION_MAX_DAYS = 90;
export const INVITATION_DAYS = 7;
/** How many of a session's older refresh tokens are recognised as a reuse (which ends the session). */
export const REUSE_WINDOW = 32;

/** Listener orders the kit takes: its keys need a grant of `reserved_order`. */
/** `cleanup`: reactors that must see a deleted tenant's rows before the read models drop them. */
export const ORDERS = { authorization: -30, cleanup: -25, readModels: -20, reactors: -10 } as const;

export const MINUTE = 60_000;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;

/** The context of the kit's own follow-up commands: they pass the middleware as internal. */
export const INTERNAL = { bypassAuthorization: true } as const;

/** The authorization middleware's store; `useAuthorizationStore` changes it. */
export const AUTHORIZATION_STORE = "sqlite:authorization.db";

/** The relying party passkeys are bound to (WebAuthn): your site's domain, name and origins; `usePasskeys` sets it. */
export const PASSKEYS = { rpId: "localhost", rpName: "kernable", origins: ["http://localhost:3000"] };
export const CHALLENGE_SECONDS = 300;
