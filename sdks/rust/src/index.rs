//! Unique keys, facts and requirements: what the kernel checks across aggregates at commit.

use std::sync::Arc;

use kernable_core::proto;
use sha2::{Digest, Sha256};

use crate::{Error, Kernable, Outcome, Submit, Wait};

/// The type of the command every aggregate that declares unique keys or facts runs without a
/// handler of its own: `_reindex_<aggregate_type>`, e.g. `reindex_type("customer")` is
/// `_reindex_customer` (command types are unique within a domain). It produces no events, so
/// the worker sends the keys and facts of the current state; [`Kernable::reindex`] submits it.
/// Command types starting with `_reindex_` are reserved for it.
pub fn reindex_type(aggregate_type: &str) -> String {
    format!("_reindex_{aggregate_type}")
}

/// Whose personal data a key's value is: from the state.
pub(crate) type KeySubject<S> = Arc<dyn Fn(&S) -> Option<String> + Send + Sync>;

/// How a unique key is unique, for [`crate::Aggregate::unique`]: within the command's tenant
/// ([`Unique::tenant`], also `Unique::default()`) or across tenants ([`Unique::global`]), and
/// optionally personal: `Unique::global().personal(|s: &Customer| Some(s.id.clone()))`.
pub struct Unique<S> {
    pub(crate) global: bool,
    pub(crate) personal: Option<KeySubject<S>>,
}

impl<S> Default for Unique<S> {
    fn default() -> Self {
        Unique::tenant()
    }
}

impl<S> Unique<S> {
    /// Unique within the command's tenant: another tenant may hold the same value.
    pub fn tenant() -> Self {
        Unique {
            global: false,
            personal: None,
        }
    }

    /// Unique across tenants. The key's identity has no tenant, so a command that is rejected
    /// with `unique_taken` learns that some tenant holds the value.
    pub fn global() -> Self {
        Unique {
            global: true,
            personal: None,
        }
    }

    /// Makes the value personal data of the subject `subject` returns from the state: the
    /// kernel keeps the key in its vault, and erasing the subject frees it. `None` or an empty
    /// subject: the aggregate holds no value of this key.
    #[must_use]
    pub fn personal(
        mut self,
        subject: impl Fn(&S) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.personal = Some(Arc::new(subject));
        self
    }
}

/// The unique keys and facts an aggregate holds in some state, as
/// [`crate::Aggregate::index`] computes them for a test, in the order they are declared.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Index {
    pub keys: Vec<UniqueKey>,
    /// The names of the facts that are true.
    pub facts: Vec<String>,
}

/// A value of a unique key: no other aggregate holds the same value of the same key (of the
/// same domain and aggregate type, in the same tenant unless global) at the same time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueKey {
    /// The key's name, as declared.
    pub name: String,
    /// The value. The kernel gets only its SHA-256, never the value.
    pub value: String,
    /// Unique across tenants.
    pub global: bool,
    /// The person the value belongs to, for a personal key.
    pub subject: Option<String>,
}

impl UniqueKey {
    pub(crate) fn to_wire(&self) -> proto::UniqueKey {
        proto::UniqueKey {
            name: self.name.clone(),
            global: self.global,
            value_hash: Sha256::digest(self.value.as_bytes()).to_vec(),
            subject: self.subject.clone().unwrap_or_default(),
        }
    }
}

/// A fact that another aggregate of the command's tenant must hold when a command commits,
/// returned by the function given to [`crate::Aggregate::requires`]:
/// `Requirement::new("shop", "vehicle", &data.vehicle_id, "open")`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Requirement {
    pub domain: String,
    /// Unqualified, e.g. `vehicle`.
    pub aggregate_type: String,
    pub aggregate_id: String,
    /// The name of a fact that aggregate declares.
    pub fact: String,
}

impl Requirement {
    /// The fact `fact` of the `domain.aggregate_type` aggregate with ID `aggregate_id`.
    pub fn new(domain: &str, aggregate_type: &str, aggregate_id: &str, fact: &str) -> Requirement {
        Requirement {
            domain: domain.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            fact: fact.to_owned(),
        }
    }

    pub(crate) fn to_wire(&self) -> proto::Requirement {
        proto::Requirement {
            domain: self.domain.clone(),
            aggregate_type: self.aggregate_type.clone(),
            aggregate_id: self.aggregate_id.clone(),
            fact: self.fact.clone(),
        }
    }
}

/// Aggregates to reindex with [`Kernable::reindex`], built with methods that each return the
/// changed request: `Reindex::new("shop", "customer", ["k1", "k2"]).tenant("t1")`.
#[derive(Clone, Debug)]
pub struct Reindex {
    domain: String,
    aggregate_type: String,
    aggregate_ids: Vec<String>,
    tenant: Option<String>,
}

impl Reindex {
    /// The `domain.aggregate_type` aggregates with these IDs.
    pub fn new(
        domain: &str,
        aggregate_type: &str,
        aggregate_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Reindex {
        Reindex {
            domain: domain.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_ids: aggregate_ids.into_iter().map(Into::into).collect(),
            tenant: None,
        }
    }

    /// The tenant they belong to. Default: the connection's ([`crate::Options::tenant`]).
    pub fn tenant(mut self, tenant: impl Into<String>) -> Reindex {
        self.tenant = Some(tenant.into());
        self
    }
}

impl Kernable {
    /// Stores the unique keys and facts of aggregates as their current state yields them:
    /// submits the built-in `_reindex_<aggregate_type>` command ([`reindex_type`]; no events)
    /// to each ID in turn and returns each ID with its outcome. Run it after declaring a key or fact on an aggregate that has events
    /// already: until an aggregate runs a command, it holds what its last command left. An ID
    /// without events holds nothing and commits. A rejection is an outcome, e.g.
    /// `unique_taken` if another aggregate holds one of its keys (a duplicate from before the
    /// key was declared). [`Error`] as for [`Kernable::submit`], for the first ID that fails;
    /// the ones before it are done. Needs the `submit` grant for
    /// `<domain>._reindex_<aggregate_type>`. Example:
    /// `kernable.reindex(Reindex::new("shop", "customer", ["k1", "k2"])).await?`.
    pub async fn reindex(&self, reindex: Reindex) -> Result<Vec<(String, Outcome)>, Error> {
        let mut outcomes = Vec::with_capacity(reindex.aggregate_ids.len());
        let command = reindex_type(&reindex.aggregate_type);
        for id in reindex.aggregate_ids {
            let mut submit = Submit::new(&reindex.domain, &reindex.aggregate_type, &id, &command)
                .wait(Wait::Committed);
            if let Some(tenant) = &reindex.tenant {
                submit = submit.tenant(tenant.clone());
            }
            let outcome = self.submit(submit).await?;
            outcomes.push((id, outcome));
        }
        Ok(outcomes)
    }
}
