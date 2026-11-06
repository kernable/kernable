//! The group events: what happened to groups, the vocabulary other domains read.

use kernable::event;

use super::Permission;

#[event("group")]
pub struct GroupCreated {
    pub name: String,
    pub description: String,
    pub permissions: Vec<Permission>,
}

/// Only what changed.
#[event("group")]
pub struct GroupUpdated {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The whole list in one event: simpler to read in the audit.
#[event("group")]
pub struct GroupPermissionsSet {
    pub permissions: Vec<Permission>,
}

#[event("group")]
pub struct GroupDeleted {}
