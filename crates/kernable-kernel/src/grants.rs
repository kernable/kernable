//! What each key may do, from the grants that root keys signed (see `Grant` in kernable.proto).
//!
//! The kernel keeps the newest verified grant per key and derives [`Permissions`] from them:
//! one table per action, key ID -> type patterns, as checked with [`crate::config::allowed`].

use std::collections::HashMap;

use kernable_core::proto::Grant;

use crate::config;

/// The actions a grant names; `*` stands for all of them.
pub const ACTIONS: [&str; 13] = [
    "submit",
    "run",
    "subscribe",
    "listen",
    "query",
    "answer",
    "check",
    "privileged",
    "vault_read",
    "vault_erase",
    "data_read",
    "data_write",
    "cache",
];

/// Key ID -> the type patterns it may use for one action.
pub type Rules = HashMap<String, Vec<String>>;

/// What every granted key may do; revoked keys are left out.
#[derive(Debug, Default)]
pub struct Permissions {
    /// Commands a key may submit.
    pub submit: Rules,
    /// Commands a key may run as a worker; it writes events of their domains only.
    pub run: Rules,
    /// Events a key may read with a subscription.
    pub subscribe: Rules,
    /// Events a key may read as a listener (read model, reactor).
    pub listen: Rules,
    /// Queries a key may ask.
    pub query: Rules,
    /// Queries a key may answer as a read model.
    pub answer: Rules,
    /// Commands and queries a key may check as a middleware.
    pub check: Rules,
    /// Privileged request context switches a key may set (`bypass_authorization`).
    pub privileged: Rules,
    /// Event and command types whose vault values a listener or middleware key gets.
    pub vault_read: Rules,
    /// Erasing event types a worker key may write.
    pub vault_erase: Rules,
    /// Domains whose files a key may read (`domain.*`).
    pub data_read: Rules,
    /// Domains whose files a key may put and delete (`domain.*`).
    pub data_write: Rules,
    /// Domains whose cache a key may use (`domain.*`).
    pub cache: Rules,
    /// Old key ID -> new key ID: a listener registered with the old key may connect with the
    /// new one and keeps its position, order and middleware registration.
    pub successors: HashMap<String, String>,
}

impl Permissions {
    /// The permissions of `grants`, leaving out the keys in `revoked` and the grants of revoked
    /// root keys.
    pub fn new(grants: &HashMap<String, Grant>, revoked: &HashMap<String, i64>) -> Self {
        let mut permissions = Permissions::default();
        // A revoked root key takes the grants it signed with it.
        let root_revoked = |g: &Grant| {
            g.root_sig
                .as_ref()
                .is_some_and(|s| revoked.contains_key(&s.key_id))
        };
        for grant in grants
            .values()
            .filter(|g| !revoked.contains_key(&g.key_id) && !root_revoked(g))
        {
            for p in &grant.permissions {
                for action in ACTIONS
                    .iter()
                    .filter(|a| p.action == "*" || p.action == **a)
                {
                    permissions
                        .rules_mut(action)
                        .entry(grant.key_id.clone())
                        .or_default()
                        .extend(p.types.iter().cloned());
                }
            }
            if !grant.succeeds.is_empty() {
                permissions
                    .successors
                    .insert(grant.succeeds.clone(), grant.key_id.clone());
            }
        }
        permissions
    }

    fn rules_mut(&mut self, action: &str) -> &mut Rules {
        match action {
            "submit" => &mut self.submit,
            "run" => &mut self.run,
            "subscribe" => &mut self.subscribe,
            "listen" => &mut self.listen,
            "query" => &mut self.query,
            "answer" => &mut self.answer,
            "check" => &mut self.check,
            "privileged" => &mut self.privileged,
            "vault_read" => &mut self.vault_read,
            "vault_erase" => &mut self.vault_erase,
            "data_read" => &mut self.data_read,
            "data_write" => &mut self.data_write,
            "cache" => &mut self.cache,
            _ => unreachable!("not an action: {action}"),
        }
    }
}

/// Why a grant is malformed, if it is: unknown actions or invalid type patterns.
pub fn malformed(grant: &Grant) -> Option<String> {
    for p in &grant.permissions {
        if p.action != "*" && !ACTIONS.contains(&p.action.as_str()) {
            return Some(format!("unknown action {}", p.action));
        }
        let switch = p.action == "privileged";
        if let Some(t) = p
            .types
            .iter()
            .find(|t| !(config::valid_pattern(t) || switch))
        {
            return Some(format!("invalid type pattern {t}"));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use kernable_core::proto::Permission;

    use super::*;

    fn grant(key_id: &str, permissions: &[(&str, &[&str])], succeeds: &str) -> Grant {
        Grant {
            key_id: key_id.into(),
            permissions: permissions
                .iter()
                .map(|(action, types)| Permission {
                    action: (*action).into(),
                    types: types.iter().map(|t| (*t).into()).collect(),
                })
                .collect(),
            succeeds: succeeds.into(),
            issued_at: 0,
            root_sig: None,
        }
    }

    #[test]
    fn grants_become_rules_per_action() {
        let grants = HashMap::from([
            ("a".into(), grant("a", &[("*", &["shop.*"])], "")),
            (
                "b".into(),
                grant("b", &[("submit", &["shop.Add"]), ("listen", &["*"])], "old"),
            ),
            ("c".into(), grant("c", &[("*", &["*"])], "")),
        ]);
        let p = Permissions::new(&grants, &HashMap::from([("c".into(), 0)]));
        assert!(config::allowed(&p.run, "a", "shop.Add"));
        assert!(config::allowed(&p.vault_erase, "a", "shop.Erased"));
        assert!(config::allowed(&p.submit, "b", "shop.Add"));
        assert!(!config::allowed(&p.run, "b", "shop.Add"));
        assert!(config::allowed(&p.listen, "b", "other.Event"));
        assert!(!config::allowed(&p.submit, "c", "shop.Add"), "revoked");
        assert_eq!(p.successors.get("old").map(String::as_str), Some("b"));
    }

    #[test]
    fn a_revoked_root_takes_its_grants_with_it() {
        let mut signed = grant("a", &[("*", &["*"])], "");
        signed.root_sig = Some(kernable_core::proto::Signature {
            key_id: "root".into(),
            sig: vec![],
        });
        let grants = HashMap::from([("a".into(), signed)]);
        let p = Permissions::new(&grants, &HashMap::from([("root".into(), 0)]));
        assert!(!config::allowed(&p.submit, "a", "shop.Add"));
    }

    #[test]
    fn malformed_grants_are_named() {
        assert_eq!(malformed(&grant("a", &[("*", &["shop.*"])], "")), None);
        assert_eq!(
            malformed(&grant(
                "a",
                &[("privileged", &["bypass_authorization"])],
                ""
            )),
            None
        );
        assert!(malformed(&grant("a", &[("write", &["*"])], "")).is_some());
        assert!(malformed(&grant("a", &[("submit", &["shop"])], "")).is_some());
    }
}
