//! Registered aggregates: which aggregate and command types a worker runs.
//!
//! A worker registers its aggregates when it connects (see `Register.aggregates`), and the
//! registration is stored. The kernel refuses commands for an aggregate or command type that no
//! worker registered and no membership lists, so a typo fails at once instead of waiting
//! forever, and is not ready while a registered or listed aggregate has no worker connected.
//! An aggregate its service's membership does not list is dynamic: not stored, not waited
//! for, and forgotten once its last worker leaves. `kernable workers remove` unregisters one
//! by hand. Replicas register the same aggregates: they are counted, not stored.

use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, MutexGuard, PoisonError},
};

use kernable_core::proto::{Membership, Register, WorkerRecord};
use tonic::Status;

use crate::{Kernel, Permissions, config, internal, now_ms};

struct Entry {
    record: WorkerRecord,
    /// Connected workers that run it.
    connected: usize,
    /// Its service's membership does not list it: not stored, nobody waits for it.
    dynamic: bool,
}

#[derive(Default)]
pub(crate) struct Registry {
    entries: Mutex<HashMap<String, Entry>>,
}

impl Registry {
    pub(crate) fn load(records: Vec<WorkerRecord>) -> Self {
        let entries = records
            .into_iter()
            .map(|record| {
                let entry = Entry {
                    record,
                    connected: 0,
                    dynamic: false,
                };
                (entry.record.aggregate_type.clone(), entry)
            })
            .collect();
        Registry {
            entries: Mutex::new(entries),
        }
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether a worker registered the qualified `aggregate_type` with the qualified
    /// `command_type`, or, while none did, a membership lists the aggregate.
    pub(crate) fn runs(&self, kernel: &Kernel, aggregate_type: &str, command_type: &str) -> bool {
        match self.entries().get(aggregate_type) {
            Some(e) => e.record.command_types.iter().any(|t| t == command_type),
            None => kernel.memberships.lists_worker(aggregate_type),
        }
    }

    /// Aggregates the kernel waits for without a connected worker: registered and not dynamic,
    /// or `listed` and never registered. Sorted.
    pub(crate) fn missing(&self, listed: &[String]) -> Vec<String> {
        let entries = self.entries();
        let mut missing: Vec<String> = entries
            .values()
            .filter(|e| !e.dynamic && e.connected == 0)
            .map(|e| e.record.aggregate_type.clone())
            .chain(listed.iter().filter(|a| !entries.contains_key(*a)).cloned())
            .collect();
        missing.sort();
        missing.dedup();
        missing
    }

    /// Applies a service's membership to its aggregates: listed ones are stored and waited
    /// for; others are dynamic, and forgotten if no worker runs them.
    pub(crate) fn apply(&self, kernel: &Kernel, m: &Membership) -> Result<(), Status> {
        let mut entries = self.entries();
        let mut changed = Vec::new();
        entries.retain(|_, e| {
            if e.record.service != m.service {
                return true;
            }
            let dynamic = !m.workers.contains(&e.record.aggregate_type);
            if dynamic && e.connected == 0 {
                changed.push(WorkerRecord {
                    removed: true,
                    ..e.record.clone()
                });
                return false;
            }
            if e.dynamic && !dynamic {
                changed.push(e.record.clone());
            }
            e.dynamic = dynamic;
            true
        });
        kernel.store.save_workers(&changed).map_err(internal)
    }

    /// Registers the aggregates of a worker that proved its key, and unregisters those its
    /// service no longer declares. Returns the qualified command types it runs.
    pub(crate) fn connect(
        &self,
        kernel: &Kernel,
        reg: &Register,
        permissions: &Permissions,
    ) -> Result<Vec<String>, Status> {
        let mut entries = self.entries();
        let now = now_ms();
        let mut registered = Vec::new();
        for a in &reg.aggregates {
            // Another key only as the registered key's successor (`succeeds` in its grant).
            if let Some(e) = entries.get(&a.aggregate_type)
                && e.record.key_id != reg.key_id
            {
                if permissions.successors.get(&e.record.key_id) != Some(&reg.key_id) {
                    return Err(Status::permission_denied(format!(
                        "{} is registered with another key",
                        a.aggregate_type
                    )));
                }
                tracing::info!(aggregate = %a.aggregate_type, key = %reg.key_id, "aggregate moves to its successor key");
            }
            let record = WorkerRecord {
                aggregate_type: a.aggregate_type.clone(),
                key_id: reg.key_id.clone(),
                command_types: a.command_types.clone(),
                service: reg.service.clone(),
                last_seen: now,
                removed: false,
            };
            let dynamic = !kernel
                .memberships
                .counts_worker(&reg.service, &a.aggregate_type);
            registered.push((record, dynamic));
        }
        let stored: Vec<WorkerRecord> = registered
            .iter()
            .filter(|(_, dynamic)| !dynamic)
            .map(|(record, _)| record.clone())
            .collect();
        kernel.store.save_workers(&stored).map_err(internal)?;
        for (record, dynamic) in registered {
            let connected = entries
                .get(&record.aggregate_type)
                .map_or(0, |e| e.connected);
            entries.insert(
                record.aggregate_type.clone(),
                Entry {
                    record,
                    connected: connected + 1,
                    dynamic,
                },
            );
        }
        Ok(reg
            .aggregates
            .iter()
            .flat_map(|a| a.command_types.clone())
            .collect())
    }

    /// A worker of `aggregates` went away; the last one of an aggregate updates its `last_seen`,
    /// or forgets it if it is dynamic.
    pub(crate) fn disconnect(&self, kernel: &Kernel, aggregates: &[String]) {
        let mut entries = self.entries();
        let mut changed = Vec::new();
        for a in aggregates {
            let Some(e) = entries.get_mut(a) else {
                continue;
            };
            e.connected = e.connected.saturating_sub(1);
            if e.connected > 0 {
                continue;
            }
            if e.dynamic {
                entries.remove(a);
            } else {
                e.record.last_seen = now_ms();
                changed.push(e.record.clone());
            }
        }
        if let Err(e) = kernel.store.save_workers(&changed) {
            tracing::error!(error = %e, "saving worker registrations failed");
        }
    }

    /// The aggregates with a connected worker, seen now: written on shutdown.
    pub(crate) fn seen_now(&self) -> Vec<WorkerRecord> {
        let now = now_ms();
        self.entries()
            .values_mut()
            .filter(|e| e.connected > 0 && !e.dynamic)
            .map(|e| {
                e.record.last_seen = now;
                e.record.clone()
            })
            .collect()
    }
}

/// Checks the aggregates a worker registers, before its key is proven.
pub(crate) fn validate(reg: &Register) -> Result<(), Status> {
    if reg.aggregates.is_empty() {
        return Err(Status::invalid_argument(
            "a worker runs at least one aggregate",
        ));
    }
    let mut seen = HashSet::new();
    for a in &reg.aggregates {
        let Some((domain, _)) = a
            .aggregate_type
            .split_once('.')
            .filter(|_| config::valid_qualified(&a.aggregate_type))
        else {
            return Err(Status::invalid_argument(
                "register qualified aggregate types: domain.AggregateType",
            ));
        };
        if !seen.insert(&a.aggregate_type) {
            return Err(Status::invalid_argument(format!(
                "{} is registered twice",
                a.aggregate_type
            )));
        }
        let own = |t: &String| {
            config::valid_qualified(t) && t.split_once('.').is_some_and(|(d, _)| d == domain)
        };
        if a.command_types.is_empty() || !a.command_types.iter().all(own) {
            return Err(Status::invalid_argument(format!(
                "register {}'s command types qualified with its domain: {domain}.CommandType",
                a.aggregate_type
            )));
        }
    }
    Ok(())
}
