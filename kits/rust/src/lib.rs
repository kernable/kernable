//! The kernable kit: tenancy, accounts, identities, groups, invitations and
//! authorization as source code in your project, yours to change. See README.md.

/// Registers a read model's handler that changes its shared state under a write lock:
/// `on!(listener, state, TenantRenamed, |t, d, evt| ...)` gets the state, the event's data and
/// its metadata.
macro_rules! on {
    ($listener:expr, $state:expr, $event:expr, |$t:ident, $d:pat_param, $evt:pat_param| $body:expr) => {{
        let state = ::std::sync::Arc::clone(&$state);
        $listener.on($event, move |$d, evt, _ctx| {
            let state = ::std::sync::Arc::clone(&state);
            async move {
                let $evt = evt;
                let mut guard = state.write().unwrap();
                let $t = &mut *guard;
                $body;
                Ok(())
            }
        });
    }};
}

pub mod account;
pub mod group;
pub mod identity;
pub mod invitation;
pub mod shared;
pub mod tenancy;

use kernable::Domain;

/// Every domain of the kit, built anew, ready for an app: `kernable.app(App::new("kit").domains(kit_domains()))`.
pub fn kit_domains() -> Vec<Domain> {
    vec![
        tenancy::tenancy().domain,
        account::account().domain,
        group::group().domain,
        identity::identity().domain,
        invitation::invitation().domain,
    ]
}
