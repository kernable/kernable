//! The command queue, in kernel memory: one FIFO per aggregate, at most one leased command per
//! aggregate, and ready aggregates handed out oldest first.
//!
//! The store keeps commands durable; after a restart the kernel rebuilds the queue from
//! `Store::pending`. Leases are not persisted, so every unfinished command is pending again.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable_core::proto::Command;

/// Tenant, domain, aggregate type, aggregate ID.
type StreamKey = (String, String, String, String);

fn stream_key(c: &Command) -> StreamKey {
    (
        c.tenant_id.clone(),
        c.domain.clone(),
        c.aggregate_type.clone(),
        c.aggregate_id.clone(),
    )
}

struct Held {
    key: StreamKey,
    expires_at: Instant,
}

#[derive(Default)]
struct Inner {
    /// Commands per aggregate in submission order; the head is next or running.
    aggregates: HashMap<StreamKey, VecDeque<Command>>,
    /// Aggregates whose head is free to lease, per qualified command type, in the order they
    /// became ready.
    ready: HashMap<String, BTreeMap<u64, StreamKey>>,
    leases: HashMap<String, Held>,
    /// IDs of all queued commands, so a repeated push is ignored.
    queued: HashSet<String>,
    next_seq: u64,
    next_lease: u64,
}

impl Inner {
    fn make_ready(&mut self, key: StreamKey) {
        let Some(head) = self.aggregates.get(&key).and_then(VecDeque::front) else {
            return;
        };
        let seq = self.next_seq;
        self.next_seq += 1;
        self.ready
            .entry(crate::config::qualified(&head.domain, &head.command_type))
            .or_default()
            .insert(seq, key);
    }

    fn expire(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .leases
            .iter()
            .filter(|(_, h)| h.expires_at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for lease_id in expired {
            let held = self.leases.remove(&lease_id).expect("just listed");
            self.make_ready(held.key);
        }
    }
}

#[derive(Default)]
pub(crate) struct Queue {
    inner: Mutex<Inner>,
}

impl Queue {
    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Number of queued commands, waiting or leased.
    pub(crate) fn len(&self) -> usize {
        self.inner().queued.len()
    }

    /// Adds a command behind the others of its aggregate. Returns false if it is queued already.
    pub(crate) fn push(&self, command: Command) -> bool {
        let mut inner = self.inner();
        if !inner.queued.insert(command.command_id.clone()) {
            return false;
        }
        let key = stream_key(&command);
        let commands = inner.aggregates.entry(key.clone()).or_default();
        commands.push_back(command);
        if commands.len() == 1 {
            inner.make_ready(key);
        }
        true
    }

    /// Leases the command that has waited longest among the free aggregates whose next
    /// command has one of the qualified `command_types`. Expired leases are given back first.
    pub(crate) fn lease(&self, command_types: &[&str], ttl: Duration) -> Option<(String, Command)> {
        let now = Instant::now();
        let mut inner = self.inner();
        inner.expire(now);
        let (command_type, seq) = command_types
            .iter()
            .filter_map(|t| Some((*t, *inner.ready.get(*t)?.first_key_value()?.0)))
            .min_by_key(|(_, seq)| *seq)?;
        let key = inner
            .ready
            .get_mut(command_type)
            .and_then(|r| r.remove(&seq))
            .expect("just found");
        let command = inner.aggregates[&key]
            .front()
            .expect("ready aggregates have a head")
            .clone();
        inner.next_lease += 1;
        let lease_id = format!("{}#{}", command.command_id, inner.next_lease);
        let held = Held {
            key,
            expires_at: now + ttl,
        };
        inner.leases.insert(lease_id.clone(), held);
        Some((lease_id, command))
    }

    /// Whether `lease_id` is still the live lease of its command.
    pub(crate) fn holds(&self, lease_id: &str) -> bool {
        self.inner().leases.contains_key(lease_id)
    }

    /// Removes the leased command, which is completed, and frees its aggregate for the next one.
    pub(crate) fn done(&self, lease_id: &str) {
        let mut inner = self.inner();
        let Some(held) = inner.leases.remove(lease_id) else {
            return;
        };
        let commands = inner.aggregates.get_mut(&held.key).expect("held aggregate");
        let command = commands.pop_front().expect("held command");
        if commands.is_empty() {
            inner.aggregates.remove(&held.key);
        }
        inner.queued.remove(&command.command_id);
        inner.make_ready(held.key);
    }

    /// Hands a leased command back, so it can be leased again right away.
    pub(crate) fn release(&self, lease_id: &str) {
        let mut inner = self.inner();
        if let Some(held) = inner.leases.remove(lease_id) {
            inner.make_ready(held.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(60);

    fn cmd(id: &str, command_type: &str, aggregate_id: &str) -> Command {
        Command {
            command_id: id.into(),
            domain: "d".into(),
            aggregate_id: aggregate_id.into(),
            command_type: command_type.into(),
            ..Default::default()
        }
    }

    fn lease(q: &Queue, types: &[&str]) -> Option<(String, String)> {
        q.lease(types, TTL).map(|(l, c)| (l, c.command_id))
    }

    #[test]
    fn one_command_per_aggregate_at_a_time_in_submission_order() {
        let q = Queue::default();
        q.push(cmd("c1", "PlaceOrder", "o1"));
        q.push(cmd("c2", "AddItem", "o1"));
        q.push(cmd("c3", "AddItem", "o2"));

        // A worker for AddItem may not overtake c1 on o1, but o2 is free.
        let (l3, id) = lease(&q, &["d.AddItem"]).unwrap();
        assert_eq!(id, "c3");
        let (l1, id) = lease(&q, &["d.PlaceOrder", "d.AddItem"]).unwrap();
        assert_eq!(id, "c1");
        assert!(lease(&q, &["d.PlaceOrder", "d.AddItem"]).is_none());

        q.done(&l1);
        let (_, id) = lease(&q, &["d.AddItem"]).unwrap();
        assert_eq!(id, "c2");
        q.done(&l3);
        assert!(lease(&q, &["d.AddItem"]).is_none());
    }

    #[test]
    fn oldest_ready_aggregate_goes_first_across_types() {
        let q = Queue::default();
        q.push(cmd("c1", "B", "x"));
        q.push(cmd("c2", "A", "y"));
        assert_eq!(lease(&q, &["d.A", "d.B"]).unwrap().1, "c1");
        assert_eq!(lease(&q, &["d.A", "d.B"]).unwrap().1, "c2");
        assert!(lease(&q, &[]).is_none());
    }

    #[test]
    fn released_and_expired_leases_are_given_out_again() {
        let q = Queue::default();
        q.push(cmd("c1", "A", "x"));
        let (first, _) = lease(&q, &["d.A"]).unwrap();
        q.release(&first);
        assert!(!q.holds(&first));

        let (_, id) = q.lease(&["d.A"], Duration::ZERO).unwrap();
        assert_eq!(id.command_id, "c1");
        let (again, id) = lease(&q, &["d.A"]).unwrap(); // the zero-TTL lease has expired
        assert_eq!(id, "c1");
        assert!(q.holds(&again));
    }

    #[test]
    fn repeated_pushes_are_ignored() {
        let q = Queue::default();
        assert!(q.push(cmd("c1", "A", "x")));
        assert!(!q.push(cmd("c1", "A", "x")));
        let (l, _) = lease(&q, &["d.A"]).unwrap();
        q.done(&l);
        assert!(lease(&q, &["d.A"]).is_none());
    }
}
