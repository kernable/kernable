//! The group aggregates: state, how each event changes it, unique keys and facts.

use kernable::{Aggregate, Domain, Unique};
use serde::{Deserialize, Serialize};

use super::{Permission, events::*};
use crate::shared::constants::SNAPSHOT_SCHEMA;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Group {
    pub active: bool,
    pub deleted: bool,
    pub name: String,
    pub description: String,
    pub permissions: Vec<Permission>,
}

pub fn group(domain: &Domain) -> Aggregate<Group> {
    let a = domain
        .aggregate("group", Group::default())
        .schema(SNAPSHOT_SCHEMA);
    a.apply(GroupCreated, |s, d, _, _| {
        *s = Group {
            active: true,
            deleted: false,
            name: d.name,
            description: d.description,
            permissions: d.permissions,
        }
    });
    a.apply(GroupUpdated, |s, d, _, _| {
        if let Some(name) = d.name {
            s.name = name;
        }
        if let Some(description) = d.description {
            s.description = description;
        }
    });
    a.apply(GroupPermissionsSet, |s, d, _, _| {
        s.permissions = d.permissions
    });
    a.apply(GroupDeleted, |s, _, _, _| {
        s.active = false;
        s.deleted = true;
    });
    a.unique(
        "name",
        |s| s.active.then(|| s.name.trim().to_lowercase()),
        Unique::tenant(),
    );
    a.fact("active", |s| s.active);
    a
}
