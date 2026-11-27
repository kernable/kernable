//! What each key may do, from the grants that root keys signed (see `Grant` in kernable.proto).
//!
//! The kernel keeps the newest verified grant per key and derives [`Permissions`] from them:
//! one table per action, key ID -> type patterns, as checked with [`crate::config::allowed`].
//!
//! A grant is signed by a root key, or by a delegate: a key whose own grant, signed by a root
//! key, has the action [`GRANT`] naming the actions it may grant. The delegated grant carries the
//! delegate's grant (`issuer`); one delegation only ([`chain`]).

use std::collections::HashMap;

use kernable_core::{proto::Grant, signing};

use crate::config;

/// The actions a grant names; `*` stands for all of them.
pub const ACTIONS: [&str; 21] = [
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
    "manage",
    "replicate",
    "read",
    "export",
    "backup",
    "reserved_order",
    "process",
    "copy",
];

/// The action of a delegate: its types are the actions it may grant others. Not one of
/// [`ACTIONS`], so the action `*` never includes it: a key delegates only what it names.
pub const GRANT: &str = "grant";

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
    /// Services whose membership a key may apply (service names, or `*`).
    pub manage: Rules,
    /// Keys a standby may follow the kernel with (`*`): it gets every record, vault value and
    /// file (design/replication.md).
    pub replicate: Rules,
    /// Keys a reader may follow the kernel with (`*`): it gets every record, vault value and
    /// file, as a standby does, but may never follow as a standby (design/replication.md).
    pub read: Rules,
    /// Keys that may export the whole kernel (`*`): every record, file and vault value, the
    /// vault values sealed (`kernable export`).
    pub export: Rules,
    /// Keys a backup follower may follow the kernel with (`*`): it gets every record and
    /// file, the vault values sealed (`kernable backup`).
    pub backup: Rules,
    /// Keys whose listeners and middlewares may take negative orders (`*`), which are
    /// reserved for kernable's own kits: they run before every app listener.
    pub reserved_order: Rules,
    /// Business processes a key may register and remove (qualified process names,
    /// `shop.fulfilment`, or `shop.*`).
    pub process: Rules,
    /// Copies at a relay whose copy key a key may be handed (`copy=<copy>`, the copy's name, or
    /// `*`): the pusher seals it to the key (design/instance-keys.md).
    pub copy: Rules,
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
        // A delegated grant counts while its delegate's grant (the newest held, or the one it
        // carries) still lets the delegate grant its actions and the root that signed that one
        // is not revoked; the delegate's own revocation is `root_revoked` above.
        let delegation_holds = |g: &Grant| {
            let Some(carried) = g.issuer.as_deref() else {
                return true;
            };
            let issuer = newest(carried, grants.get(&carried.key_id));
            !root_revoked(issuer) && covers(issuer, g)
        };
        for grant in grants
            .values()
            .filter(|g| !revoked.contains_key(&g.key_id) && !root_revoked(g) && delegation_holds(g))
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
            "manage" => &mut self.manage,
            "replicate" => &mut self.replicate,
            "read" => &mut self.read,
            "export" => &mut self.export,
            "backup" => &mut self.backup,
            "reserved_order" => &mut self.reserved_order,
            "process" => &mut self.process,
            "copy" => &mut self.copy,
            _ => unreachable!("not an action: {action}"),
        }
    }
}

/// The actions `grant` lets its key grant others (`grant=<action>,...`).
pub fn delegable(grant: &Grant) -> impl Iterator<Item = &str> {
    grant
        .permissions
        .iter()
        .filter(|p| p.action == GRANT)
        .flat_map(|p| p.types.iter().map(String::as_str))
}

/// Whether the delegate's grant `issuer` lets it grant `action`: named, never `*` or `grant`.
fn may_grant(issuer: &Grant, action: &str) -> bool {
    action != "*" && action != GRANT && delegable(issuer).any(|a| a == action)
}

/// Whether the delegate's grant `issuer` lets it grant every action of `grant`.
fn covers(issuer: &Grant, grant: &Grant) -> bool {
    grant
        .permissions
        .iter()
        .all(|p| may_grant(issuer, &p.action))
}

/// The newer of the grant a delegated grant carries and the one held for the delegate (if
/// it is the delegate's): a newer grant of the delegate replaces the carried one.
fn newest<'a>(carried: &'a Grant, held: Option<&'a Grant>) -> &'a Grant {
    match held {
        Some(h) if h.key_id == carried.key_id && h.issued_at > carried.issued_at => h,
        _ => carried,
    }
}

/// Checks how `grant` is signed: by one of `roots`, or by a delegate whose grant (the one it
/// carries, or `held`, the newest the kernel holds for the delegate, if newer) is signed by one
/// of `roots` and lets it grant every action of `grant`. One delegation only. Returns the
/// delegate's grant it was checked against (`None`: signed by a root). Revocations are the
/// caller's: of the root, of the delegate, and of the root of the delegate's grant.
pub fn chain(
    grant: &Grant,
    roots: &[String],
    held: Option<&Grant>,
) -> Result<Option<Grant>, String> {
    let by_root = |g: &Grant| {
        g.root_sig.as_ref().is_some_and(|s| {
            roots.contains(&s.key_id) && signing::verify(s, &signing::grant_message(g)).is_ok()
        })
    };
    let sig = grant.root_sig.as_ref().ok_or("grant is not signed")?;
    if roots.contains(&sig.key_id) {
        if grant.issuer.is_some() {
            return Err("a grant signed by a root key carries no issuer".into());
        }
        return match by_root(grant) {
            true => Ok(None),
            false => Err("invalid root signature".into()),
        };
    }
    let Some(carried) = grant.issuer.as_deref() else {
        return Err("not signed by a root key".into());
    };
    if carried.key_id != sig.key_id {
        return Err("the issuer's grant is not the signing key's".into());
    }
    if carried.issuer.is_some() {
        return Err("one delegation only: the issuer's grant must be signed by a root key".into());
    }
    if !by_root(carried) {
        return Err("the issuer's grant is not signed by a root key".into());
    }
    if let Some(reason) = malformed(carried) {
        return Err(format!("the issuer's grant: {reason}"));
    }
    if signing::verify(sig, &signing::grant_message(grant)).is_err() {
        return Err("invalid signature of the issuer".into());
    }
    if !grant.succeeds.is_empty() {
        return Err("a delegated grant cannot succeed another key".into());
    }
    let issuer = match held {
        // A newer grant of the delegate counts only if it is a root's as well.
        Some(h) if by_root(h) => newest(carried, Some(h)),
        _ => carried,
    };
    if let Some(p) = grant
        .permissions
        .iter()
        .find(|p| !may_grant(issuer, &p.action))
    {
        return Err(format!("the issuer may not grant {}", p.action));
    }
    Ok(Some(issuer.clone()))
}

/// Why a grant is malformed, if it is: unknown actions or invalid type patterns.
pub fn malformed(grant: &Grant) -> Option<String> {
    for p in &grant.permissions {
        if p.action == GRANT {
            // The actions it may grant, named one by one: never `*` or `grant` itself.
            if p.types.is_empty() {
                return Some("grant names no action".into());
            }
            if let Some(t) = p.types.iter().find(|t| !ACTIONS.contains(&t.as_str())) {
                return Some(format!("grant may name actions only, not {t}"));
            }
            continue;
        }
        if p.action != "*" && !ACTIONS.contains(&p.action.as_str()) {
            return Some(format!("unknown action {}", p.action));
        }
        let switch = p.action == "privileged";
        // `manage` also takes a service (a domain); "*" is a valid pattern already.
        let service = |t: &str| p.action == "manage" && config::valid_domain(t);
        if let Some(t) = p
            .types
            .iter()
            .find(|t| !(config::valid_pattern(t) || switch || service(t)))
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
            issuer: None,
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

    /// Permissions as the CLI writes them.
    fn perms(permissions: &[(&str, &[&str])]) -> Vec<Permission> {
        permissions
            .iter()
            .map(|(action, types)| Permission {
                action: (*action).into(),
                types: types.iter().map(|t| (*t).into()).collect(),
            })
            .collect()
    }

    fn key(seed: u8) -> ed25519_dalek::SigningKey {
        ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
    }

    fn id(key: &ed25519_dalek::SigningKey) -> String {
        signing::key_id(&key.verifying_key())
    }

    /// A root, a move key it lets grant `replicate` and `read` at `issued_at`, and the roots.
    fn delegate(
        issued_at: i64,
        actions: &[&str],
    ) -> (ed25519_dalek::SigningKey, Grant, Vec<String>) {
        let (root, mover) = (key(1), key(2));
        let issuer = signing::grant(
            &root,
            &id(&mover),
            perms(&[("grant", actions)]),
            "",
            issued_at,
        );
        (mover, issuer, vec![id(&root)])
    }

    #[test]
    fn a_delegate_grants_only_the_actions_its_grant_names() {
        let (mover, issuer, roots) = delegate(10, &["replicate", "read"]);
        let follower = id(&key(3));
        let read = signing::delegated_grant(
            &mover,
            issuer.clone(),
            &follower,
            perms(&[("read", &["*"])]),
            20,
        );
        assert_eq!(chain(&read, &roots, None).unwrap(), Some(issuer.clone()));
        let grants = HashMap::from([(follower.clone(), read.clone())]);
        let p = Permissions::new(&grants, &HashMap::new());
        assert!(config::allowed(&p.read, &follower, "*"));
        assert!(!config::allowed(&p.replicate, &follower, "*"));

        // Outside the list, `*`, `grant` itself, or a successor: refused.
        for refused in [
            perms(&[("submit", &["*"])]),
            perms(&[("read", &["*"]), ("manage", &["*"])]),
            perms(&[("*", &["*"])]),
            perms(&[("grant", &["read"])]),
        ] {
            let g = signing::delegated_grant(&mover, issuer.clone(), &follower, refused, 20);
            assert!(chain(&g, &roots, None).is_err(), "{g:?}");
            let grants = HashMap::from([(follower.clone(), g)]);
            let p = Permissions::new(&grants, &HashMap::new());
            assert!(!config::allowed(&p.submit, &follower, "*"));
            assert!(!config::allowed(&p.manage, &follower, "*"));
        }
        let mut succeeding = read.clone();
        succeeding.succeeds = id(&key(9));
        succeeding.root_sig = Some(signing::sign(&mover, &signing::grant_message(&succeeding)));
        assert!(chain(&succeeding, &roots, None).is_err());

        // Not signed by the delegate, or a root-signed grant carrying an issuer: refused.
        let mut forged = read.clone();
        forged.root_sig = Some(signing::sign(&key(4), &signing::grant_message(&forged)));
        assert!(chain(&forged, &roots, None).is_err());
        let mut odd = signing::grant(&key(1), &follower, perms(&[("read", &["*"])]), "", 5);
        odd.issuer = Some(Box::new(issuer.clone()));
        assert!(chain(&odd, &roots, None).is_err());
    }

    #[test]
    fn one_delegation_only() {
        let (mover, issuer, roots) = delegate(10, &["replicate", "read", "manage"]);
        // The move key's grant names `grant` as an action it could pass on: malformed.
        assert!(malformed(&grant("a", &[("grant", &["grant"])], "")).is_some());
        assert!(malformed(&grant("a", &[("grant", &["*"])], "")).is_some());
        assert!(malformed(&grant("a", &[("grant", &[])], "")).is_some());
        assert_eq!(
            malformed(&grant("a", &[("grant", &["read", "replicate"])], "")),
            None
        );
        // A key the move key granted cannot delegate further: its grant is not a root's.
        let second = key(5);
        let second_grant = signing::delegated_grant(
            &mover,
            issuer,
            &id(&second),
            perms(&[("manage", &["*"])]),
            20,
        );
        let third = signing::delegated_grant(
            &second,
            second_grant,
            &id(&key(6)),
            perms(&[("manage", &["*"])]),
            30,
        );
        let e = chain(&third, &roots, None).unwrap_err();
        assert!(e.contains("one delegation only"), "{e}");
        // The action `*` of a root grant does not include `grant`.
        let all = signing::grant(&key(1), &id(&key(7)), perms(&[("*", &["*"])]), "", 1);
        assert_eq!(delegable(&all).count(), 0);
        let by_all =
            signing::delegated_grant(&key(7), all, &id(&key(8)), perms(&[("read", &["*"])]), 2);
        assert!(chain(&by_all, &roots, None).is_err());
    }

    #[test]
    fn revoking_the_delegate_or_narrowing_its_grant_ends_what_it_granted() {
        let (mover, issuer, roots) = delegate(10, &["replicate", "read"]);
        let follower = id(&key(3));
        let read = signing::delegated_grant(
            &mover,
            issuer.clone(),
            &follower,
            perms(&[("read", &["*"])]),
            20,
        );
        let grants = HashMap::from([(follower.clone(), read.clone())]);
        // The move key revoked: its grants go.
        let p = Permissions::new(&grants, &HashMap::from([(id(&mover), 0)]));
        assert!(!config::allowed(&p.read, &follower, "*"));
        // The root of the move key's grant revoked: too.
        let p = Permissions::new(&grants, &HashMap::from([(roots[0].clone(), 0)]));
        assert!(!config::allowed(&p.read, &follower, "*"));
        // A newer grant of the move key without `read`: the follower's grant no longer counts,
        // and a new one is refused.
        let narrower = signing::grant(
            &key(1),
            &id(&mover),
            perms(&[("grant", &["replicate"])]),
            "",
            15,
        );
        let grants = HashMap::from([
            (follower.clone(), read.clone()),
            (id(&mover), narrower.clone()),
        ]);
        let p = Permissions::new(&grants, &HashMap::new());
        assert!(!config::allowed(&p.read, &follower, "*"));
        let e = chain(&read, &roots, Some(&narrower)).unwrap_err();
        assert!(e.contains("may not grant read"), "{e}");
        // An older one held changes nothing.
        let older = signing::grant(
            &key(1),
            &id(&mover),
            perms(&[("grant", &["replicate"])]),
            "",
            5,
        );
        assert!(chain(&read, &roots, Some(&older)).is_ok());
    }
}
