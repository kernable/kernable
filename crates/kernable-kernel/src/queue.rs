//! The command queue, in kernel memory: one FIFO per aggregate, at most one leased command per
//! aggregate, and ready aggregates handed out oldest first.
//!
//! Scheduled commands (`Command.deliver_at` after the command was received) wait in a timer list
//! beside the FIFOs, ordered by their delivery time, so they never hold up their aggregate's
//! other commands. The kernel takes them out once due ([`Queue::take_due`]), checks them again
//! and hands them to their aggregate's FIFO ([`Queue::promote`]), or completes them without
//! running them. They count towards the queue's limits like queued commands.
//!
//! The store keeps commands durable; after a restart the kernel rebuilds the queue from
//! `Store::pending`, which routes scheduled commands to the timer list again by the same rule
//! ([`scheduled`]). Leases are not persisted, so every unfinished command is pending again.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use kernable_core::proto::Command;
use prost::Message;

use crate::config::Limits;

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

/// Whether a command received at `received_at` (ms) is a scheduled one: due only later. A
/// command whose delivery time had passed when it arrived is an ordinary one.
pub(crate) fn scheduled(c: &Command, received_at: i64) -> bool {
    c.deliver_at > received_at
}

struct Held {
    key: StreamKey,
    expires_at: Instant,
}

#[derive(Default)]
struct Inner {
    /// Commands per aggregate in submission order, with when they were received; the head is
    /// next or running.
    aggregates: HashMap<StreamKey, VecDeque<(Command, i64)>>,
    /// Aggregates whose head is free to lease, per qualified command type, in the order they
    /// became ready.
    ready: HashMap<String, BTreeMap<u64, StreamKey>>,
    leases: HashMap<String, Held>,
    /// IDs of all queued commands, so a repeated push is ignored.
    queued: HashSet<String>,
    /// The encoded size of all queued commands.
    bytes: usize,
    next_seq: u64,
    next_lease: u64,
    /// Scheduled commands not yet due, with when they were received, by when to look at them
    /// next (their delivery time, or a later try) and then arrival.
    timers: BTreeMap<(i64, u64), (Command, i64)>,
    /// Where each command in `timers` is.
    timer_of: HashMap<String, (i64, u64)>,
    /// Scheduled commands taken out as due (or by a cancel) and being checked: neither waiting
    /// nor in their aggregate's FIFO yet.
    checking: HashMap<String, (Command, i64)>,
    /// Scheduled commands (in `timers` or `checking`) per aggregate.
    scheduled_per: HashMap<StreamKey, usize>,
}

impl Inner {
    fn make_ready(&mut self, key: StreamKey) {
        let Some((head, _)) = self.aggregates.get(&key).and_then(VecDeque::front) else {
            return;
        };
        let seq = self.next_seq;
        self.next_seq += 1;
        self.ready
            .entry(crate::config::qualified(&head.domain, &head.command_type))
            .or_default()
            .insert(seq, key);
    }

    /// Adds a scheduled command to the timer list, to be looked at `at`.
    fn add_timer(&mut self, at: i64, command: Command, received_at: i64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.timer_of.insert(command.command_id.clone(), (at, seq));
        self.timers.insert((at, seq), (command, received_at));
    }

    /// Takes a scheduled command out of the timer list.
    fn remove_timer(&mut self, command_id: &str) -> Option<(Command, i64)> {
        let at = self.timer_of.remove(command_id)?;
        self.timers.remove(&at)
    }

    /// One scheduled command less of its aggregate.
    fn unschedule(&mut self, key: &StreamKey) {
        if let Some(n) = self.scheduled_per.get_mut(key) {
            *n -= 1;
            if *n == 0 {
                self.scheduled_per.remove(key);
            }
        }
    }

    /// Appends a command to its aggregate's FIFO.
    fn append(&mut self, command: Command, received_at: i64) {
        let key = stream_key(&command);
        let commands = self.aggregates.entry(key.clone()).or_default();
        commands.push_back((command, received_at));
        if commands.len() == 1 {
            self.make_ready(key);
        }
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

    /// Number of queued commands, waiting or leased, scheduled ones included.
    pub(crate) fn len(&self) -> usize {
        self.inner().queued.len()
    }

    /// Number of scheduled commands not handed to their aggregate yet.
    pub(crate) fn scheduled_len(&self) -> usize {
        let inner = self.inner();
        inner.timer_of.len() + inner.checking.len()
    }

    /// Whether a new `command` fits the queue's limits; otherwise which limit it exceeds
    /// (the reason counted in `kernable_refused_total`). A command already queued always fits.
    pub(crate) fn room(&self, command: &Command, limits: &Limits) -> Result<(), &'static str> {
        let inner = self.inner();
        if inner.queued.contains(&command.command_id) {
            return Ok(());
        }
        if inner.queued.len() >= limits.queued_commands {
            return Err("queued_commands");
        }
        let key = stream_key(command);
        let behind = inner.aggregates.get(&key).map_or(0, VecDeque::len)
            + inner.scheduled_per.get(&key).copied().unwrap_or(0);
        if behind >= limits.queued_per_aggregate {
            return Err("queued_per_aggregate");
        }
        if inner.bytes + command.encoded_len() > limits.queued_bytes {
            return Err("queued_bytes");
        }
        Ok(())
    }

    /// Adds a command behind the others of its aggregate, or a [`scheduled`] one to the timer
    /// list. Returns false if it is queued already.
    pub(crate) fn push(&self, command: Command, received_at: i64) -> bool {
        let mut inner = self.inner();
        if !inner.queued.insert(command.command_id.clone()) {
            return false;
        }
        inner.bytes += command.encoded_len();
        if scheduled(&command, received_at) {
            *inner.scheduled_per.entry(stream_key(&command)).or_default() += 1;
            inner.add_timer(command.deliver_at, command, received_at);
        } else {
            inner.append(command, received_at);
        }
        true
    }

    /// When the first scheduled command in the timer list is to be looked at (ms since epoch).
    pub(crate) fn next_timer(&self) -> Option<i64> {
        self.inner().timers.keys().next().map(|(at, _)| *at)
    }

    /// Takes the scheduled commands due by `now` out of the timer list, in order, with when they
    /// were received. Each is then being checked: [`Queue::promote`] hands it to its aggregate,
    /// [`Queue::defer`] puts it back, [`Queue::forget`] drops it.
    pub(crate) fn take_due(&self, now: i64) -> Vec<(Command, i64)> {
        let mut inner = self.inner();
        let mut due = Vec::new();
        while let Some(entry) = inner.timers.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let (command, received_at) = entry.remove();
            inner.timer_of.remove(&command.command_id);
            inner
                .checking
                .insert(command.command_id.clone(), (command.clone(), received_at));
            due.push((command, received_at));
        }
        due
    }

    /// Takes the scheduled command `command_id` out of the timer list if it waits there (for a
    /// cancel), as [`Queue::take_due`] would; false if it does not (due already, or unknown).
    pub(crate) fn take_waiting(&self, command_id: &str) -> bool {
        let mut inner = self.inner();
        let Some(taken) = inner.remove_timer(command_id) else {
            return false;
        };
        inner.checking.insert(command_id.to_owned(), taken);
        true
    }

    /// Whether the scheduled command `command_id` waits in the timer list.
    pub(crate) fn waiting(&self, command_id: &str) -> bool {
        self.inner().timer_of.contains_key(command_id)
    }

    /// Puts a scheduled command being checked back into the timer list, to be looked at `at`.
    pub(crate) fn defer(&self, command_id: &str, at: i64) {
        let mut inner = self.inner();
        if let Some((command, received_at)) = inner.checking.remove(command_id) {
            inner.add_timer(at, command, received_at);
        }
    }

    /// Hands a scheduled command being checked to its aggregate's FIFO, behind the commands
    /// queued there now.
    pub(crate) fn promote(&self, command_id: &str) {
        let mut inner = self.inner();
        if let Some((command, received_at)) = inner.checking.remove(command_id) {
            inner.unschedule(&stream_key(&command));
            inner.append(command, received_at);
        }
    }

    /// Drops a scheduled command that is not handed to its aggregate (completed without
    /// running, or no longer this kernel's). Returns whether it was there.
    pub(crate) fn forget(&self, command_id: &str) -> bool {
        let mut inner = self.inner();
        let Some((command, _)) = inner
            .checking
            .remove(command_id)
            .or_else(|| inner.remove_timer(command_id))
        else {
            return false;
        };
        inner.unschedule(&stream_key(&command));
        inner.queued.remove(command_id);
        inner.bytes = inner.bytes.saturating_sub(command.encoded_len());
        true
    }

    /// Leases the command that has waited longest among the free aggregates whose next
    /// command has one of the qualified `command_types`, but none of the `busy` ones (commands
    /// the asking worker still runs: a lease that ran out under it goes to another worker first,
    /// and back to it once its own attempt is over). Expired leases are given back first.
    /// Returns the lease ID, the command and when it was received.
    pub(crate) fn lease(
        &self,
        command_types: &[&str],
        ttl: Duration,
        busy: &HashSet<String>,
    ) -> Option<(String, Command, i64)> {
        let now = Instant::now();
        let mut inner = self.inner();
        inner.expire(now);
        let inner_ref = &*inner;
        let (command_type, seq) = command_types
            .iter()
            .filter_map(|t| {
                let (seq, _) = inner_ref.ready.get(*t)?.iter().find(|(_, key)| {
                    inner_ref.aggregates[*key]
                        .front()
                        .is_some_and(|(c, _)| !busy.contains(&c.command_id))
                })?;
                Some((*t, *seq))
            })
            .min_by_key(|(_, seq)| *seq)?;
        let key = inner
            .ready
            .get_mut(command_type)
            .and_then(|r| r.remove(&seq))
            .expect("just found");
        let (command, received_at) = inner.aggregates[&key]
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
        Some((lease_id, command, received_at))
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
        let (command, _) = commands.pop_front().expect("held command");
        if commands.is_empty() {
            inner.aggregates.remove(&held.key);
        }
        inner.queued.remove(&command.command_id);
        inner.bytes = inner.bytes.saturating_sub(command.encoded_len());
        inner.make_ready(held.key);
    }

    /// Hands a leased command back, so it can be leased again right away.
    pub(crate) fn release(&self, lease_id: &str) {
        let mut inner = self.inner();
        if let Some(held) = inner.leases.remove(lease_id) {
            inner.make_ready(held.key);
        }
    }
    /// Hands a leased command back after `delay`; until then its aggregate stays held. For a
    /// result the store could not take: trying again at once would only fail again.
    pub(crate) fn release_after(&self, lease_id: &str, delay: Duration) {
        let mut inner = self.inner();
        if let Some(mut held) = inner.leases.remove(lease_id) {
            held.expires_at = Instant::now() + delay;
            inner.next_lease += 1;
            let hold = format!("held#{}", inner.next_lease);
            inner.leases.insert(hold, held);
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
        q.lease(types, TTL, &HashSet::new())
            .map(|(l, c, _)| (l, c.command_id))
    }

    #[test]
    fn one_command_per_aggregate_at_a_time_in_submission_order() {
        let q = Queue::default();
        q.push(cmd("c1", "PlaceOrder", "o1"), 0);
        q.push(cmd("c2", "AddItem", "o1"), 0);
        q.push(cmd("c3", "AddItem", "o2"), 0);

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
    fn a_command_released_after_a_delay_waits_for_it() {
        let q = Queue::default();
        q.push(cmd("c1", "A", "o1"), 0);
        let (l1, _) = lease(&q, &["d.A"]).unwrap();
        q.release_after(&l1, Duration::from_millis(50));
        assert!(!q.holds(&l1));
        assert!(lease(&q, &["d.A"]).is_none(), "not before the delay");
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(lease(&q, &["d.A"]).map(|(_, id)| id).as_deref(), Some("c1"));
    }

    #[test]
    fn oldest_ready_aggregate_goes_first_across_types() {
        let q = Queue::default();
        q.push(cmd("c1", "B", "x"), 0);
        q.push(cmd("c2", "A", "y"), 0);
        assert_eq!(lease(&q, &["d.A", "d.B"]).unwrap().1, "c1");
        assert_eq!(lease(&q, &["d.A", "d.B"]).unwrap().1, "c2");
        assert!(lease(&q, &[]).is_none());
    }

    #[test]
    fn released_and_expired_leases_are_given_out_again() {
        let q = Queue::default();
        q.push(cmd("c1", "A", "x"), 0);
        let (first, _) = lease(&q, &["d.A"]).unwrap();
        q.release(&first);
        assert!(!q.holds(&first));

        let (_, id, _) = q.lease(&["d.A"], Duration::ZERO, &HashSet::new()).unwrap();
        assert_eq!(id.command_id, "c1");
        let (again, id) = lease(&q, &["d.A"]).unwrap(); // the zero-TTL lease has expired
        assert_eq!(id, "c1");
        assert!(q.holds(&again));
    }

    #[test]
    fn a_lease_that_ran_out_goes_to_another_worker_than_the_one_still_running_it() {
        let q = Queue::default();
        q.push(cmd("c1", "A", "x"), 0);
        q.push(cmd("c2", "A", "y"), 0);
        let (_, first, _) = q.lease(&["d.A"], Duration::ZERO, &HashSet::new()).unwrap();
        assert_eq!(first.command_id, "c1");
        // Its lease ran out while the worker still runs it: it gets the next command, not c1.
        let running = HashSet::from(["c1".to_owned()]);
        let (_, next, _) = q.lease(&["d.A"], TTL, &running).unwrap();
        assert_eq!(next.command_id, "c2");
        assert!(
            q.lease(&["d.A"], TTL, &running).is_none(),
            "c1 waits for another"
        );
        // Another worker gets it; and so does the first once its own attempt is over.
        assert_eq!(lease(&q, &["d.A"]).unwrap().1, "c1");
    }

    #[test]
    fn room_counts_commands_per_aggregate_and_bytes() {
        let q = Queue::default();
        let limits = Limits {
            queued_commands: 3,
            queued_per_aggregate: 2,
            ..Limits::default()
        };
        q.push(cmd("c1", "A", "o1"), 0);
        q.push(cmd("c2", "A", "o1"), 0);
        assert_eq!(
            q.room(&cmd("c3", "A", "o1"), &limits),
            Err("queued_per_aggregate")
        );
        assert_eq!(
            q.room(&cmd("c1", "A", "o1"), &limits),
            Ok(()),
            "already queued"
        );
        assert_eq!(q.room(&cmd("c3", "A", "o2"), &limits), Ok(()));
        q.push(cmd("c3", "A", "o2"), 0);
        assert_eq!(
            q.room(&cmd("c4", "A", "o3"), &limits),
            Err("queued_commands")
        );

        let bytes = q.inner().bytes;
        let small = Limits {
            queued_bytes: bytes + 1,
            ..Limits::default()
        };
        assert_eq!(q.room(&cmd("c4", "A", "o3"), &small), Err("queued_bytes"));
        let (lease, _, _) = q
            .lease(&["d.A"], Duration::from_secs(9), &HashSet::new())
            .unwrap();
        q.done(&lease);
        assert!(
            q.inner().bytes < bytes,
            "a completed command frees its bytes"
        );
    }

    #[test]
    fn repeated_pushes_are_ignored() {
        let q = Queue::default();
        assert!(q.push(cmd("c1", "A", "x"), 0));
        assert!(!q.push(cmd("c1", "A", "x"), 0));
        let (l, _) = lease(&q, &["d.A"]).unwrap();
        q.done(&l);
        assert!(lease(&q, &["d.A"]).is_none());
    }

    fn at(id: &str, aggregate_id: &str, deliver_at: i64) -> Command {
        Command {
            deliver_at,
            ..cmd(id, "A", aggregate_id)
        }
    }

    #[test]
    fn a_scheduled_command_does_not_block_its_aggregate() {
        let q = Queue::default();
        assert!(q.push(at("later", "o1", 1_000), 10));
        q.push(cmd("now", "A", "o1"), 11);
        assert_eq!(q.len(), 2);
        assert_eq!(q.scheduled_len(), 1);
        let (l, id) = lease(&q, &["d.A"]).unwrap();
        assert_eq!(id, "now", "the scheduled command is not in the FIFO");
        q.done(&l);
        assert!(lease(&q, &["d.A"]).is_none());

        assert!(q.take_due(999).is_empty(), "not before its time");
        let due = q.take_due(1_000);
        assert_eq!(due.len(), 1);
        assert_eq!(q.scheduled_len(), 1, "counted while it is checked");
        assert!(!q.waiting("later"));
        q.promote("later");
        assert_eq!(q.scheduled_len(), 0);
        let (l, id) = lease(&q, &["d.A"]).unwrap();
        assert_eq!(id, "later");
        q.done(&l);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn a_due_command_queues_behind_its_aggregates_commands() {
        let q = Queue::default();
        q.push(at("s", "o1", 100), 0);
        q.push(cmd("c1", "A", "o1"), 0);
        let (l1, _) = lease(&q, &["d.A"]).unwrap();
        q.take_due(100);
        q.promote("s");
        assert!(lease(&q, &["d.A"]).is_none(), "o1 is busy with c1");
        q.done(&l1);
        assert_eq!(lease(&q, &["d.A"]).unwrap().1, "s");
    }

    #[test]
    fn due_commands_come_out_in_delivery_order() {
        let q = Queue::default();
        q.push(at("c3", "o3", 300), 0);
        q.push(at("c1", "o1", 100), 0);
        q.push(at("c2", "o2", 200), 0);
        q.push(at("c2b", "o2", 200), 0);
        assert_eq!(q.next_timer(), Some(100));
        let ids =
            |v: Vec<(Command, i64)>| v.into_iter().map(|(c, _)| c.command_id).collect::<Vec<_>>();
        assert_eq!(ids(q.take_due(250)), ["c1", "c2", "c2b"]);
        assert_eq!(q.next_timer(), Some(300));
        // One put back for a later try comes out again then.
        q.defer("c1", 400);
        assert_eq!(ids(q.take_due(350)), ["c3"]);
        assert_eq!(ids(q.take_due(400)), ["c1"]);
        assert_eq!(q.next_timer(), None);
    }

    #[test]
    fn a_rebuild_routes_by_when_a_command_was_received() {
        let q = Queue::default();
        // As `Store::pending` returns them after a restart: one scheduled and due long since,
        // one scheduled for later, and one whose time had passed when it arrived.
        q.push(at("overdue", "o1", 100), 50);
        q.push(at("later", "o1", 10_000), 50);
        q.push(at("ordinary", "o1", 100), 200);
        assert_eq!(q.scheduled_len(), 2);
        assert_eq!(lease(&q, &["d.A"]).unwrap().1, "ordinary");
        let due = q.take_due(5_000);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0.command_id, "overdue");
        assert_eq!(due[0].1, 50, "with when it was received");
        assert!(!q.push(at("later", "o1", 10_000), 50), "not twice");
    }

    #[test]
    fn a_cancelled_or_dropped_scheduled_command_frees_its_room() {
        let q = Queue::default();
        let limits = Limits {
            queued_commands: 10,
            queued_per_aggregate: 2,
            ..Limits::default()
        };
        q.push(at("s1", "o1", 100), 0);
        q.push(at("s2", "o1", 100), 0);
        assert_eq!(
            q.room(&cmd("c3", "A", "o1"), &limits),
            Err("queued_per_aggregate"),
            "scheduled commands count towards their aggregate's limit"
        );
        assert!(q.take_waiting("s1"));
        assert!(!q.take_waiting("s1"), "taken once");
        assert!(q.forget("s1"));
        assert!(q.forget("s2"));
        assert!(!q.forget("s2"));
        assert_eq!(q.len(), 0);
        assert_eq!(q.scheduled_len(), 0);
        assert_eq!(q.inner().bytes, 0);
        assert_eq!(q.room(&cmd("c3", "A", "o1"), &limits), Ok(()));
    }
}
