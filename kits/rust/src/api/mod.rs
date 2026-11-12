//! What only your API does: sign-in, tokens, OIDC, passkey challenges, API keys. Async, for
//! tokio-based frameworks (axum, actix).

#[allow(clippy::module_inception)]
pub mod api;
pub mod tokens;

pub use api::{
    ApiError, ApiResult, KitApi, NewInvitationLink, OAuth2, Provider, SignedIn, oauth2_email,
};
pub use tokens::{
    Claims, SigningKeys, check_access_token, issue_access_token, new_signing_keys,
    signing_keys_from,
};
