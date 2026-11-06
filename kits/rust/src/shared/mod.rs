//! What the kit's domains share: settings, secrets, mail and passkeys.

pub mod constants;
pub mod mail;
pub mod passkeys;
pub mod secrets;

use kernable::{ListenerContext, RequestContext};

/// The context of the kit's own follow-up commands (the event's, followed): they pass the
/// middleware as internal. Without a listener context, a bare internal one.
pub fn internal(ctx: Option<&ListenerContext>) -> RequestContext {
    RequestContext {
        bypass_authorization: true,
        ..ctx.map(ListenerContext::follow).unwrap_or_default()
    }
}
