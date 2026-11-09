//! The invitation events: what happened to invitations, the vocabulary other domains read.

use kernable::event;
use serde_json::Value;

use super::InvitedGroup;

fn own(_d: &Value, invitation_id: &str) -> String {
    format!("invitation:{invitation_id}")
}

fn token_subject(d: &Value, _id: &str) -> String {
    format!("token:{}", d["tokenId"].as_str().unwrap_or_default())
}

fn previous_token(d: &Value, _id: &str) -> String {
    format!(
        "token:{}",
        d["previousTokenId"].as_str().unwrap_or_default()
    )
}

/// The email belongs to the invitation (its own subject); the token sits in the vault until used.
#[event("invitation", personal(fields = [email], subject = own), personal(fields = [token], subject = token_subject))]
pub struct InvitationCreated {
    pub email: Option<String>,
    pub groups: Vec<InvitedGroup>,
    /// An owner invitation: accepting makes the person an owner.
    pub owner: bool,
    pub token_id: String,
    pub token: Option<String>,
    pub expires_at: i64,
}

/// A new token; erases the old one.
#[event("invitation", personal(fields = [token], subject = token_subject), erases = previous_token)]
pub struct InvitationResent {
    pub previous_token_id: String,
    pub token_id: String,
    pub token: Option<String>,
    pub expires_at: i64,
}

#[event("invitation", erases = token_subject)]
pub struct InvitationRevoked {
    pub token_id: String,
}

#[event("invitation", erases = token_subject)]
pub struct InvitationDeclined {
    pub token_id: String,
}

/// The identity reactor creates the identity with these groups (and the owner) from it.
#[event("invitation", erases = token_subject)]
pub struct InvitationAccepted {
    pub token_id: String,
    pub account_id: String,
    pub new_account: bool,
    pub groups: Vec<InvitedGroup>,
    pub owner: bool,
}

#[event("invitation")]
pub struct InvitationMailSent {
    pub token_id: String,
}

/// Erases the invited email: when the tenant is deleted, or the person asks.
#[event("invitation", erases = own)]
pub struct InvitationErased {}

/// A reusable invitation: anyone signed in who has its token may join the tenant with these
/// groups, until it is revoked, expires or is used up. Only the token's hash is kept: the API shows
/// the token once.
#[event("invitation")]
pub struct InvitationLinkCreated {
    pub groups: Vec<InvitedGroup>,
    pub hash: String,
    pub label: String,
    pub expires_at: Option<i64>,
    pub max_uses: Option<i64>,
}

#[event("invitation")]
pub struct InvitationLinkRevoked {}

/// Someone joined through the link: the identity reactor creates their identity with these groups.
#[event("invitation")]
pub struct InvitationLinkUsed {
    pub account_id: String,
    pub groups: Vec<InvitedGroup>,
}
