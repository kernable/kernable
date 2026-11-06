//! The account events: what happened to accounts, sessions and SSOs, the vocabulary other
//! domains read.

use kernable::event;
use serde_json::Value;

use super::{AccountSuspensionReason, FailureReason, SecondFactor, SessionEnd, own};

fn token_subject(d: &Value, _id: &str) -> String {
    format!("token:{}", d["tokenId"].as_str().unwrap_or_default())
}

fn of_account(d: &Value, _id: &str) -> String {
    d["accountId"].as_str().unwrap_or_default().to_owned()
}

fn of_sso(_d: &Value, sso_id: &str) -> String {
    format!("sso:{sso_id}")
}

#[event("account", personal(fields = [email, name, password_hash], subject = own))]
pub struct AccountRegistered {
    pub email: Option<String>,
    pub name: Option<String>,
    /// argon2id; `None` for an account without a password (passkeys, single sign-on).
    pub password_hash: Option<String>,
    /// True when an accepted invitation proved the email: active at once.
    pub verified: bool,
    pub at: i64,
}

/// The token stays in the vault under its own subject until it is used.
#[event("account", personal(fields = [email], subject = own), personal(fields = [token], subject = token_subject))]
pub struct VerificationRequested {
    pub token_id: String,
    pub token: Option<String>,
    pub email: Option<String>,
    pub expires_at: i64,
}

/// Erases the used token.
#[event("account", erases = token_subject)]
pub struct EmailVerified {
    pub token_id: String,
}

#[event("account", personal(fields = [ip, user_agent], subject = own))]
pub struct SignedIn {
    pub session_id: String,
    pub method: String,
    pub factors: Vec<String>,
    pub refresh_token_hash: String,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
    /// The account has a second factor on: the session must prove it before anything else.
    pub mfa: bool,
    pub at: i64,
}

/// An event, not a rejection: audited, and it counts toward the lockout.
#[event("account", personal(fields = [ip], subject = own))]
pub struct SignInFailed {
    pub method: String,
    pub reason: FailureReason,
    pub ip: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [ip, user_agent], subject = of_account))]
pub struct SessionOpened {
    pub account_id: String,
    pub method: String,
    pub factors: Vec<String>,
    pub refresh_token_hash: String,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [ip], subject = of_account))]
pub struct SessionRefreshed {
    pub generation: u64,
    pub refresh_token_hash: String,
    pub ip: Option<String>,
    pub account_id: String,
    /// The session's factors, so the API issues the new access token without asking.
    pub factors: Vec<String>,
    pub at: i64,
}

#[event("account")]
pub struct SessionFactorAdded {
    pub factor: String,
}

#[event("account")]
pub struct SessionEnded {
    pub reason: SessionEnd,
}

/// Ends every session (a reactor); sign-in is refused.
#[event("account")]
pub struct AccountSuspended {
    pub reason: AccountSuspensionReason,
    pub at: i64,
}

#[event("account")]
pub struct AccountReactivated {}

/// Erases the account's personal data, in its events and its sessions'.
#[event("account", erases = own)]
pub struct AccountDeleted {}

/// Ends the account's other sessions (a reactor); `keep_session_id` is the one it was changed from.
#[event("account", personal(fields = [hash], subject = own))]
pub struct PasswordChanged {
    pub hash: Option<String>,
    pub keep_session_id: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [email], subject = own), personal(fields = [token], subject = token_subject))]
pub struct PasswordResetRequested {
    pub token_id: String,
    pub token: Option<String>,
    pub email: Option<String>,
    pub expires_at: i64,
}

/// Erases the used token; ends every session (a reactor).
#[event("account", personal(fields = [hash], subject = own), erases = token_subject)]
pub struct PasswordReset {
    pub token_id: String,
    pub hash: Option<String>,
    pub at: i64,
}

/// The new address gets a token; it holds nothing until confirmed.
#[event("account", personal(fields = [email], subject = own), personal(fields = [token], subject = token_subject))]
pub struct EmailChangeRequested {
    pub token_id: String,
    pub token: Option<String>,
    pub email: Option<String>,
    pub expires_at: i64,
}

#[event("account", personal(fields = [email], subject = own), erases = token_subject)]
pub struct EmailChanged {
    pub token_id: String,
    pub email: Option<String>,
}

/// Someone registered with this account's address: it gets a mail saying it exists.
#[event("account", personal(fields = [email], subject = own))]
pub struct RegistrationAttempted {
    pub email: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [secret], subject = own))]
pub struct TotpStarted {
    pub secret: Option<String>,
}

#[event("account")]
pub struct TotpConfirmed {
    pub step: i64,
    pub at: i64,
}

#[event("account")]
pub struct TotpDisabled {}

#[event("account")]
pub struct RecoveryCodesGenerated {
    pub hashes: Vec<String>,
}

#[event("account")]
pub struct SecondFactorProved {
    pub session_id: String,
    pub method: SecondFactor,
    pub step: Option<i64>,
    pub recovery_code_hash: Option<String>,
}

#[event("account")]
pub struct SecondFactorFailed {
    pub session_id: String,
    pub at: i64,
}

#[event("account", personal(fields = [credential_id, label], subject = own))]
pub struct PasskeyAdded {
    pub credential_id: Option<String>,
    pub public_key: String,
    pub counter: u64,
    pub label: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [credential_id], subject = own))]
pub struct PasskeyRemoved {
    pub credential_id: Option<String>,
}

#[event("account", personal(fields = [credential_id], subject = own))]
pub struct PasskeyUsed {
    pub credential_id: Option<String>,
    pub counter: u64,
}

#[event("account", personal(fields = [subject, email], subject = own))]
pub struct ProviderLinked {
    pub provider: String,
    pub subject: Option<String>,
    pub email: Option<String>,
    pub at: i64,
}

#[event("account", personal(fields = [subject], subject = own))]
pub struct ProviderUnlinked {
    pub provider: String,
    pub subject: Option<String>,
}

#[event("account", personal(fields = [client_secret], subject = of_sso))]
pub struct SsoCreated {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
}

#[event("account", personal(fields = [client_secret], subject = of_sso))]
pub struct SsoUpdated {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
}

#[event("account")]
pub struct SsoDomainAdded {
    pub domain: String,
    pub txt: String,
}

#[event("account")]
pub struct SsoDomainVerified {
    pub domain: String,
}

#[event("account")]
pub struct SsoDomainRemoved {
    pub domain: String,
}

#[event("account")]
pub struct SsoEnforcementChanged {
    pub enforced: bool,
}

#[event("account")]
pub struct SsoDisabled {}

#[event("account")]
pub struct SsoEnabled {}

#[event("account", erases = of_sso)]
pub struct SsoDeleted {}
