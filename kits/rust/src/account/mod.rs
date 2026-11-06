//! account: one Account per person and a Session per sign-in, in the platform tenant, and a
//! tenant's single sign-on (design/kits.md "account"). Vault fields are declared on the
//! event and command types; `account()` registers the parts.

pub mod aggregates;
pub mod commands;
pub mod events;
pub mod queries;
pub mod reactors;
pub mod readmodels;

use kernable::{Aggregate, Domain, Listener, domain};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use aggregates::{Account, Session, Sso};
pub use commands::*;
pub use events::*;
pub use queries::*;
pub use readmodels::{AccountRow, SessionRow, SsoRow};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    #[default]
    None,
    Pending,
    Active,
    Suspended,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountSuspensionReason {
    OwnerRequested,
    Abuse,
    ByPlatform,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenPurpose {
    Verify,
    Reset,
    EmailChange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEnd {
    SignedOut,
    Revoked,
    AccountSuspended,
    PasswordChanged,
    AccountDeleted,
    Reuse,
    Idle,
    Expired,
}

impl SessionEnd {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionEnd::SignedOut => "signed_out",
            SessionEnd::Revoked => "revoked",
            SessionEnd::AccountSuspended => "account_suspended",
            SessionEnd::PasswordChanged => "password_changed",
            SessionEnd::AccountDeleted => "account_deleted",
            SessionEnd::Reuse => "reuse",
            SessionEnd::Idle => "idle",
            SessionEnd::Expired => "expired",
        }
    }
}

/// Why a sign-in failed (an event, not a rejection).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    WrongPassword,
    Locked,
    NotVerified,
    NotActive,
    NotLinked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondFactor {
    Totp,
    Recovery,
    Passkey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Suspension {
    pub reason: AccountSuspensionReason,
    pub at: i64,
}

/// The account itself as the subject of its personal data.
pub fn own(_d: &Value, aggregate_id: &str) -> String {
    aggregate_id.to_owned()
}

/// The account domain with its parts.
pub struct AccountDomain {
    pub domain: Domain,
    pub account: Aggregate<Account>,
    pub session: Aggregate<Session>,
    pub sso: Aggregate<Sso>,
    pub accounts: Listener,
    pub ssos: Listener,
}

/// The account domain, every part registered.
pub fn account() -> AccountDomain {
    let account_domain = domain("account");
    let account = aggregates::account(&account_domain);
    let session = aggregates::session(&account_domain);
    let sso = aggregates::sso(&account_domain);
    commands::handle(&account, &session, &sso);
    let (accounts, account_state) = readmodels::accounts(&account_domain);
    let (ssos, sso_state) = readmodels::ssos(&account_domain);
    queries::answer(&accounts, &account_state, &ssos, &sso_state);
    reactors::react(&account_domain);
    AccountDomain {
        domain: account_domain,
        account,
        session,
        sso,
        accounts,
        ssos,
    }
}
