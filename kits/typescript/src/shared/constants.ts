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
export const ACCESS_TOKEN_MINUTES = 15;
export const SESSION_IDLE_DAYS = 14;
export const SESSION_MAX_DAYS = 90;
export const INVITATION_DAYS = 7;

/** Listener orders the kit takes: its keys need a grant of `reserved_order`. */
/** `cleanup`: reactors that must see a deleted tenant's rows before the read models drop them. */
export const ORDERS = { authorization: -30, cleanup: -25, readModels: -20, reactors: -10 } as const;

export const MINUTE = 60_000;
export const HOUR = 60 * MINUTE;
export const DAY = 24 * HOUR;

/** The context of the kit's own follow-up commands: they pass the middleware as internal. */
export const INTERNAL = { bypassAuthorization: true } as const;
