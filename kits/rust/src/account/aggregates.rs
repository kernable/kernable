//! The account aggregates: state, how each event changes it, unique keys and facts.

use kernable::{Aggregate, Domain, Unique};
use serde::{Deserialize, Serialize};

use super::{AccountStatus, FailureReason, Suspension, TokenPurpose, events::*};
use crate::shared::constants::{
    LOCKOUT_AFTER, LOCKOUT_MAX_MINUTES, MINUTE, REUSE_WINDOW, SNAPSHOT_SCHEMA,
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Password {
    pub hash: Option<String>,
    pub set_at: i64,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Failures {
    pub count: u32,
    pub last_at: i64,
}

/// An emailed token waiting to be used; the token itself is in the vault until then.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenToken {
    pub token_id: String,
    pub purpose: TokenPurpose,
    pub token: Option<String>,
    pub expires_at: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Totp {
    pub secret: Option<String>,
    pub confirmed: bool,
    pub last_step: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Passkey {
    pub credential_id: Option<String>,
    pub public_key: String,
    pub counter: u64,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub provider: String,
    pub subject: Option<String>,
}

/// A person's account, in the platform tenant.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub status: AccountStatus,
    pub email: Option<String>,
    pub email_verified: bool,
    pub name: Option<String>,
    pub password: Option<Password>,
    pub failures: Failures,
    pub open_tokens: Vec<OpenToken>,
    pub suspension: Option<Suspension>,
    /// A new address waiting for its confirmation: it holds no key yet.
    pub pending_email: Option<String>,
    pub totp: Option<Totp>,
    /// SHA-256 of the unused recovery codes.
    pub recovery_codes: Vec<String>,
    pub passkeys: Vec<Passkey>,
    pub links: Vec<Link>,
    /// When the last "you already have an account" mail went out (`RegistrationAttempted`); 0: never.
    #[serde(default)]
    pub attempt_noted_at: i64,
}

impl Account {
    /// Until when the account is locked after failed sign-ins: computed, never stored.
    pub fn locked_until(&self) -> i64 {
        let count = self.failures.count;
        if count < LOCKOUT_AFTER {
            return 0;
        }
        let minutes = 2i64
            .saturating_pow(count - LOCKOUT_AFTER)
            .min(LOCKOUT_MAX_MINUTES);
        self.failures.last_at + minutes * MINUTE
    }

    /// TOTP is on (confirmed): every session must prove a second factor.
    pub fn mfa(&self) -> bool {
        self.totp.as_ref().is_some_and(|t| t.confirmed)
    }

    fn open(
        &mut self,
        purpose: TokenPurpose,
        token_id: String,
        token: Option<String>,
        expires_at: i64,
    ) {
        self.open_tokens.retain(|t| t.purpose != purpose);
        self.open_tokens.push(OpenToken {
            token_id,
            purpose,
            token,
            expires_at,
        });
    }

    fn close(&mut self, token_id: &str) {
        self.open_tokens.retain(|t| t.token_id != token_id);
    }
}

/// Snapshot schema `rs-2`: the state gained `attempt_noted_at`; snapshots of `rs-1` lack it, so the
/// kernel drops them and the state is rebuilt from the events.
const ACCOUNT_SNAPSHOT_SCHEMA: &str = "rs-2";

pub fn account(domain: &Domain) -> Aggregate<Account> {
    let a = domain
        .aggregate("account", Account::default())
        .schema(ACCOUNT_SNAPSHOT_SCHEMA);
    a.apply(AccountRegistered, |s, d, evt, _| {
        s.id = evt.aggregate_id.clone();
        s.status = if d.verified {
            AccountStatus::Active
        } else {
            AccountStatus::Pending
        };
        s.email = d.email;
        s.email_verified = d.verified;
        s.name = d.name;
        s.password = d.password_hash.map(|hash| Password {
            hash: Some(hash),
            set_at: d.at,
        });
    });
    a.apply(VerificationRequested, |s, d, _, _| {
        s.open(TokenPurpose::Verify, d.token_id, d.token, d.expires_at)
    });
    a.apply(EmailVerified, |s, d, _, _| {
        s.email_verified = true;
        if s.status == AccountStatus::Pending {
            s.status = AccountStatus::Active;
        }
        s.close(&d.token_id);
    });
    a.apply(AccountSuspended, |s, d, _, _| {
        s.status = AccountStatus::Suspended;
        s.suspension = Some(Suspension {
            reason: d.reason,
            at: d.at,
        });
    });
    a.apply(AccountReactivated, |s, _, _, _| {
        s.status = AccountStatus::Active;
        s.suspension = None;
    });
    a.apply(AccountDeleted, |s, _, _, _| {
        s.status = AccountStatus::Deleted;
        s.password = None;
        s.open_tokens.clear();
    });
    a.apply(PasswordChanged, |s, d, _, _| {
        s.password = Some(Password {
            hash: d.hash,
            set_at: d.at,
        })
    });
    a.apply(PasswordResetRequested, |s, d, _, _| {
        s.open(TokenPurpose::Reset, d.token_id, d.token, d.expires_at)
    });
    a.apply(PasswordReset, |s, d, _, _| {
        s.password = Some(Password {
            hash: d.hash,
            set_at: d.at,
        });
        s.failures = Failures::default();
        s.close(&d.token_id);
    });
    a.apply(EmailChangeRequested, |s, d, _, _| {
        s.pending_email = d.email;
        s.open(TokenPurpose::EmailChange, d.token_id, d.token, d.expires_at);
    });
    a.apply(EmailChanged, |s, d, _, _| {
        s.email = d.email;
        s.email_verified = true;
        s.pending_email = None;
        s.close(&d.token_id);
    });
    a.apply(TotpStarted, |s, d, _, _| {
        s.totp = Some(Totp {
            secret: d.secret,
            confirmed: false,
            last_step: 0,
        })
    });
    a.apply(TotpConfirmed, |s, d, _, _| {
        if let Some(t) = s.totp.as_mut() {
            t.confirmed = true;
            t.last_step = d.step;
        }
    });
    a.apply(TotpDisabled, |s, _, _, _| {
        s.totp = None;
        s.recovery_codes.clear();
    });
    a.apply(RecoveryCodesGenerated, |s, d, _, _| {
        s.recovery_codes = d.hashes
    });
    a.apply(SecondFactorProved, |s, d, _, _| {
        if let (Some(step), Some(t)) = (d.step, s.totp.as_mut()) {
            t.last_step = step;
        }
        if let Some(hash) = d.recovery_code_hash {
            s.recovery_codes.retain(|h| *h != hash);
        }
        s.failures = Failures::default();
    });
    a.apply(SecondFactorFailed, |s, d, _, _| {
        s.failures = Failures {
            count: s.failures.count + 1,
            last_at: d.at,
        }
    });
    a.apply(PasskeyAdded, |s, d, _, _| {
        s.passkeys.push(Passkey {
            credential_id: d.credential_id,
            public_key: d.public_key,
            counter: d.counter,
            label: d.label,
        })
    });
    a.apply(PasskeyRemoved, |s, d, _, _| {
        s.passkeys.retain(|p| p.credential_id != d.credential_id)
    });
    a.apply(PasskeyUsed, |s, d, _, _| {
        for p in s
            .passkeys
            .iter_mut()
            .filter(|p| p.credential_id == d.credential_id)
        {
            p.counter = d.counter;
        }
    });
    a.apply(ProviderLinked, |s, d, _, _| {
        s.links.push(Link {
            provider: d.provider,
            subject: d.subject,
        })
    });
    a.apply(ProviderUnlinked, |s, d, _, _| {
        s.links
            .retain(|x| !(x.provider == d.provider && x.subject == d.subject))
    });
    a.apply(RegistrationAttempted, |s, d, _, _| {
        s.attempt_noted_at = d.at
    });
    // A sign-in that still owes its second factor is not complete: its failures stay, so wrong
    // codes after the right password add up to the lockout (SecondFactorProved clears them).
    a.apply(SignedIn, |s, d, _, _| {
        if !d.mfa {
            s.failures = Failures::default();
        }
    });
    a.apply(SignInFailed, |s, d, _, _| {
        if d.reason == FailureReason::WrongPassword {
            s.failures = Failures {
                count: s.failures.count + 1,
                last_at: d.at,
            };
        }
    });

    // Each external login belongs to one account.
    a.unique(
        "link",
        |s| {
            if s.status == AccountStatus::Deleted {
                return vec![];
            }
            s.links
                .iter()
                .filter_map(|x| {
                    x.subject
                        .as_ref()
                        .map(|sub| format!("{}:{sub}", x.provider))
                })
                .collect()
        },
        Unique::tenant().personal(|s: &Account| Some(s.id.clone())),
    );
    // Held from registration on, while not deleted: nobody registers an email twice.
    a.unique(
        "email",
        |s| {
            s.email
                .clone()
                .filter(|_| !matches!(s.status, AccountStatus::None | AccountStatus::Deleted))
        },
        Unique::tenant().personal(|s: &Account| Some(s.id.clone())),
    );
    a.fact("active", |s| s.status == AccountStatus::Active);
    a.fact("mfa", Account::mfa);
    a
}

// --- Sessions ----------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    #[default]
    None,
    Active,
    Ended,
}

/// The session's snapshot format: `rs-2` since the state gained `previous_token_hashes`; snapshots
/// of `rs-1` lack it, so the kernel drops them and the state is rebuilt from the events.
const SESSION_SNAPSHOT_SCHEMA: &str = "rs-2";

/// One sign-in.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    pub status: SessionStatus,
    pub account_id: String,
    pub method: String,
    pub factors: Vec<String>,
    pub generation: u64,
    pub refresh_token_hash: String,
    /// The hashes of the last refresh tokens before the current one: a reuse is one of them.
    pub previous_token_hashes: Vec<String>,
    pub opened_at: i64,
    pub last_refresh_at: i64,
    pub end_reason: Option<super::SessionEnd>,
}

pub fn session(domain: &Domain) -> Aggregate<Session> {
    let a = domain
        .aggregate("session", Session::default())
        .schema(SESSION_SNAPSHOT_SCHEMA);
    a.apply(SessionOpened, |s, d, _, _| {
        *s = Session {
            status: SessionStatus::Active,
            account_id: d.account_id,
            method: d.method,
            factors: d.factors,
            generation: 1,
            refresh_token_hash: d.refresh_token_hash,
            previous_token_hashes: Vec::new(),
            opened_at: d.at,
            last_refresh_at: d.at,
            end_reason: None,
        }
    });
    a.apply(SessionRefreshed, |s, d, _, _| {
        if !s.refresh_token_hash.is_empty() {
            let previous = std::mem::take(&mut s.refresh_token_hash);
            s.previous_token_hashes.push(previous);
            let extra = s.previous_token_hashes.len().saturating_sub(REUSE_WINDOW);
            s.previous_token_hashes.drain(..extra);
        }
        s.generation = d.generation;
        s.refresh_token_hash = d.refresh_token_hash;
        s.last_refresh_at = d.at;
    });
    a.apply(SessionFactorAdded, |s, d, _, _| s.factors.push(d.factor));
    a.apply(SessionEnded, |s, d, _, _| {
        s.status = SessionStatus::Ended;
        s.end_reason = Some(d.reason);
    });
    a
}

// --- account.sso: a tenant's single sign-on, under the tenant's ID ---------------------------

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SsoStatus {
    #[default]
    None,
    Active,
    Disabled,
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SsoDomain {
    pub domain: String,
    pub txt: String,
    pub verified: bool,
}

/// A tenant's single sign-on.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Sso {
    pub status: SsoStatus,
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub domains: Vec<SsoDomain>,
    pub enforced: bool,
}

pub fn sso(domain: &Domain) -> Aggregate<Sso> {
    let a = domain
        .aggregate("sso", Sso::default())
        .schema(SNAPSHOT_SCHEMA);
    a.apply(SsoCreated, |s, d, _, _| {
        s.status = SsoStatus::Active;
        s.issuer = d.issuer;
        s.client_id = d.client_id;
        s.client_secret = d.client_secret;
    });
    a.apply(SsoUpdated, |s, d, _, _| {
        s.issuer = d.issuer;
        s.client_id = d.client_id;
        s.client_secret = d.client_secret;
    });
    a.apply(SsoDomainAdded, |s, d, _, _| {
        s.domains.push(SsoDomain {
            domain: d.domain,
            txt: d.txt,
            verified: false,
        })
    });
    a.apply(SsoDomainVerified, |s, d, _, _| {
        for x in s.domains.iter_mut().filter(|x| x.domain == d.domain) {
            x.verified = true;
        }
    });
    a.apply(SsoDomainRemoved, |s, d, _, _| {
        s.domains.retain(|x| x.domain != d.domain)
    });
    a.apply(SsoEnforcementChanged, |s, d, _, _| s.enforced = d.enforced);
    a.apply(SsoDisabled, |s, _, _, _| s.status = SsoStatus::Disabled);
    a.apply(SsoEnabled, |s, _, _, _| s.status = SsoStatus::Active);
    a.apply(SsoDeleted, |s, _, _, _| {
        s.status = SsoStatus::Deleted;
        s.client_secret = None;
        s.domains.clear();
    });
    // A verified domain belongs to one tenant's SSO.
    a.unique(
        "domain",
        |s| {
            if s.status == SsoStatus::Deleted {
                return vec![];
            }
            s.domains
                .iter()
                .filter(|x| x.verified)
                .map(|x| x.domain.clone())
                .collect()
        },
        Unique::global(),
    );
    a
}
