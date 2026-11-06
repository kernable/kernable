//! The identity events: what happened to identities, the vocabulary other domains read.

use kernable::event;

use super::{IdentityKind, Scope, own};

#[event("identity", personal(fields = [display_name, title, email], subject = own))]
pub struct IdentityCreated {
    pub kind: IdentityKind,
    pub account_id: Option<String>,
    pub display_name: Option<String>,
    pub title: Option<String>,
    pub email: Option<String>,
}

#[event("identity", personal(fields = [display_name, title, email], subject = own))]
pub struct ProfileUpdated {
    pub display_name: Option<String>,
    pub title: Option<String>,
    pub email: Option<String>,
}

#[event("identity")]
pub struct MembershipGranted {
    pub membership_id: String,
    pub group: String,
    /// `None`: the whole tenant; else one aggregate (or what has it as parent).
    pub scope: Option<Scope>,
    pub expires_at: Option<i64>,
}

#[event("identity")]
pub struct MembershipSuspended {
    pub membership_id: String,
}

#[event("identity")]
pub struct MembershipResumed {
    pub membership_id: String,
}

#[event("identity")]
pub struct MembershipRevoked {
    pub membership_id: String,
}

#[event("identity")]
pub struct ApiKeyCreated {
    pub key_id: String,
    /// SHA-256 of the whole key: it has high entropy, so the hash needs no vault.
    pub hash: String,
    pub label: String,
    pub expires_at: Option<i64>,
}

#[event("identity")]
pub struct ApiKeyRevoked {
    pub key_id: String,
}

#[event("identity")]
pub struct IdentitySuspended {
    pub reason: String,
}

#[event("identity")]
pub struct IdentityReactivated {}

/// Erases the identity's personal data.
#[event("identity", erases = own)]
pub struct IdentityDeleted {}
