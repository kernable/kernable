//! Settings of the kit. The kit is yours: change them here (design/kits.md, "Constants").

/// The tenant the platform's own accounts, sessions, groups and administrators live in.
pub const PLATFORM_TENANT: &str = "platform";

/// Slugs nobody may take for a tenant.
pub const RESERVED_SLUGS: &[&str] = &[
    "platform", "www", "api", "admin", "app", "mail", "auth", "login", "static",
];

/// How long a suspended tenant waits before it may be deleted.
pub const DELETE_AFTER_DAYS: i64 = 0;

/// Failed sign-ins before the account locks for a while: 1, 2, 4 … minutes, at most the maximum.
pub const LOCKOUT_AFTER: u32 = 5;
pub const LOCKOUT_MAX_MINUTES: i64 = 15;

pub const VERIFICATION_HOURS: i64 = 48;
pub const RESET_MINUTES: i64 = 60;
pub const ACCESS_TOKEN_MINUTES: i64 = 15;
pub const SESSION_IDLE_DAYS: i64 = 14;
pub const SESSION_MAX_DAYS: i64 = 90;
pub const INVITATION_DAYS: i64 = 7;

/// Listener orders the kit takes: its keys need a grant of `reserved_order`.
/// `CLEANUP`: reactors that must see a deleted tenant's rows before the read models drop them.
pub mod orders {
    pub const AUTHORIZATION: i32 = -30;
    pub const CLEANUP: i32 = -25;
    pub const READ_MODELS: i32 = -20;
    pub const REACTORS: i32 = -10;
}

pub const MINUTE: i64 = 60_000;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

/// The relying party passkeys are bound to (WebAuthn): your site's domain, name and origins.
pub mod passkeys {
    pub const RP_ID: &str = "localhost";
    pub const RP_NAME: &str = "kernable";
    pub const ORIGINS: &[&str] = &["http://localhost:3000"];
}
pub const CHALLENGE_SECONDS: i64 = 300;

/// The authorization middleware's store; `use_authorization_store` changes it.
pub const AUTHORIZATION_STORE: &str = "sqlite:authorization.db";

/// The snapshot format of the kit's aggregates in Rust: another language's kit keeps its own.
pub const SNAPSHOT_SCHEMA: &str = "rs-1";
