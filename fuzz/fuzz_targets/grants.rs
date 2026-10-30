//! Any bytes as a grant in `kernable-grant`: refused as malformed or turned into permissions,
//! never a crash; and a malformed one never grants anything.
#![no_main]

use std::collections::HashMap;

use kernable_core::{proto::Grant, signing};
use kernable_kernel::{Permissions, config, grants};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    let Ok(grant) = Grant::decode(data) else {
        return;
    };
    let _ = signing::grant_message(&grant);
    let malformed = grants::malformed(&grant);
    let held = HashMap::from([(grant.key_id.clone(), grant.clone())]);
    let permissions = Permissions::new(&held, &HashMap::new());
    for p in &grant.permissions {
        for t in &p.types {
            for rules in [&permissions.submit, &permissions.listen, &permissions.manage] {
                let _ = config::allowed(rules, &grant.key_id, t);
            }
            let _ = config::readable(&permissions.listen, &grant.key_id, std::slice::from_ref(t));
        }
    }
    // Revoked, the key gets nothing, whatever the grant says.
    let revoked = HashMap::from([(grant.key_id.clone(), 0)]);
    let none = Permissions::new(&held, &revoked);
    assert!(
        !config::allowed(&none.submit, &grant.key_id, "x.Y"),
        "a revoked key may submit ({malformed:?})"
    );
});
