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
/// How many of a session's older refresh tokens are recognised as a reuse (which ends the session).
pub const REUSE_WINDOW: usize = 32;

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
/// [`use_passkeys`] sets it at runtime; the defaults suit local development.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelyingParty {
    /// Your site's domain: `example.com`.
    pub rp_id: String,
    /// The name authenticators show.
    pub rp_name: String,
    /// The origins your pages are served from: `https://example.com`.
    pub origins: Vec<String>,
}

impl Default for RelyingParty {
    fn default() -> RelyingParty {
        RelyingParty {
            rp_id: "localhost".into(),
            rp_name: "kernable".into(),
            origins: vec!["http://localhost:3000".into()],
        }
    }
}

static PASSKEYS: std::sync::RwLock<Option<RelyingParty>> = std::sync::RwLock::new(None);

/// Binds passkeys to your site: call it before the app runs and the API serves.
pub fn use_passkeys(rp: RelyingParty) {
    *PASSKEYS.write().unwrap() = Some(rp);
}

/// The relying party passkeys are bound to now.
pub fn passkeys() -> RelyingParty {
    PASSKEYS.read().unwrap().clone().unwrap_or_default()
}
pub const CHALLENGE_SECONDS: i64 = 300;

/// The authorization middleware's store; `use_authorization_store` changes it.
pub const AUTHORIZATION_STORE: &str = "sqlite:authorization.db";

/// The snapshot format of the kit's aggregates in Rust: another language's kit keeps its own.
pub const SNAPSHOT_SCHEMA: &str = "rs-1";
