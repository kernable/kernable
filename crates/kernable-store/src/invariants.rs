//! The invariants of the kernel, as checks that every robustness test runs: crash tests,
//! property tests, fuzzing and soak runs (`design/invariants.md` has the catalogue).
//!
//! - [`Log`] holds a store's log and the commands it refers to; [`Log::check`] checks the
//!   store invariants (S1–S9) on it.
//! - [`Observed`] records what clients and listeners were told; [`Observed::check`] checks the
//!   observation invariants (O1–O4) against a [`Log`].
//!
//! A check never stops at the first violation: it returns all of them, each with the name of
//! its invariant, so a failing test says which guarantee broke and where.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use kernable_core::proto::{
    AggregateIndex, Batch, Command, CommandResult, Event, ListenerRecord, command_result::Outcome,
};

use crate::{Result, Store, StreamId};

/// A broken invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The invariant's name, as in `design/invariants.md`: `S3 stream versions`.
    pub invariant: &'static str,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.invariant, self.detail)
    }
}

pub const S1: &str = "S1 integrity";
pub const S2: &str = "S2 global positions";
pub const S3: &str = "S3 stream versions";
pub const S4: &str = "S4 atomic commands";
pub const S5: &str = "S5 events follow their command";
pub const S6: &str = "S6 one outcome";
pub const S7: &str = "S7 schemas only grow";
pub const S8: &str = "S8 listener positions";
pub const S9: &str = "S9 unique keys";
pub const O1: &str = "O1 stable outcomes";
pub const O2: &str = "O2 acknowledged means delivered";
pub const O3: &str = "O3 delivery in order";
pub const O4: &str = "O4 faithful delivery";

/// Everything the store invariants are checked on, read from a store or built by hand.
#[derive(Debug, Default, Clone)]
pub struct Log {
    /// Every event, in the order the store returns them.
    pub events: Vec<Event>,
    /// Every stored command, and every command named to [`Log::read_with`], by ID; `None` if
    /// the store does not know it.
    pub commands: BTreeMap<String, Option<Command>>,
    /// The stored result of every command in `commands`; `None` while it is pending.
    pub results: BTreeMap<String, Option<CommandResult>>,
    /// IDs of the commands the store holds as pending.
    pub pending: BTreeSet<String>,
    pub listeners: Vec<ListenerRecord>,
    /// The keys and facts every stream holds ([`Store::index`]).
    pub index: Vec<(StreamId, AggregateIndex)>,
    /// The store's own head position.
    pub head: u64,
    /// Problems `Store::verify` found (S1).
    pub integrity: Vec<String>,
}

impl Log {
    /// Reads everything the invariants need. `kernel_key_ids`: every key the kernel signed
    /// checkpoints and revocations with.
    pub fn read(store: &dyn Store, kernel_key_ids: &[String]) -> Result<Log> {
        Self::read_with(store, kernel_key_ids, &[])
    }

    /// Like [`Log::read`], also reading the commands `ids` (e.g. [`Observed::command_ids`]).
    pub fn read_with(store: &dyn Store, kernel_key_ids: &[String], ids: &[String]) -> Result<Log> {
        let mut log = Log {
            head: store.head_position()?,
            listeners: store.listeners()?,
            index: store.index()?,
            integrity: store.verify(kernel_key_ids)?.problems,
            ..Log::default()
        };
        store.each_event(&mut |e| log.events.push(e.clone()))?;
        for p in store.pending()? {
            log.pending.insert(p.command.command_id);
        }
        let ids: BTreeSet<String> = store
            .command_ids()?
            .into_iter()
            .chain(log.events.iter().map(|e| e.command_id.clone()))
            .chain(log.pending.iter().cloned())
            .chain(ids.iter().cloned())
            .collect();
        for id in ids {
            let command = store.command(&id)?.map(|p| p.command);
            log.commands.insert(id.clone(), command);
            log.results.insert(id.clone(), store.result(&id)?);
        }
        Ok(log)
    }

    /// Checks S1–S9.
    pub fn check(&self) -> Vec<Violation> {
        let mut v = Vec::new();
        for problem in &self.integrity {
            v.push(violation(S1, problem.clone()));
        }
        self.positions(&mut v);
        self.streams(&mut v);
        self.commands(&mut v);
        self.schemas(&mut v);
        for l in &self.listeners {
            if l.position > self.head {
                v.push(violation(
                    S8,
                    format!(
                        "listener {} is at {}, beyond the head {}",
                        l.name, l.position, self.head
                    ),
                ));
            }
        }
        self.keys(&mut v);
        v
    }

    /// The event at a global position, if the log has it.
    pub fn event(&self, position: u64) -> Option<&Event> {
        let i = usize::try_from(position.checked_sub(1)?).ok()?;
        self.events
            .get(i)
            .filter(|e| e.global_position == position)
            .or_else(|| self.events.iter().find(|e| e.global_position == position))
    }

    /// S2: the positions are exactly 1..=head, in order.
    fn positions(&self, v: &mut Vec<Violation>) {
        for (i, e) in self.events.iter().enumerate() {
            let expected = i as u64 + 1;
            if e.global_position != expected {
                v.push(violation(
                    S2,
                    format!("event number {expected} has position {}", e.global_position),
                ));
                return;
            }
        }
        let last = self.events.len() as u64;
        if self.head != last {
            v.push(violation(
                S2,
                format!("the head is {}, the last event is {last}", self.head),
            ));
        }
    }

    /// S3: every stream's versions are exactly 1..=n, in log order.
    fn streams(&self, v: &mut Vec<Violation>) {
        let mut versions: BTreeMap<(&str, &str, &str, &str), u64> = BTreeMap::new();
        for e in &self.events {
            let stream = (
                e.tenant_id.as_str(),
                e.domain.as_str(),
                e.aggregate_type.as_str(),
                e.aggregate_id.as_str(),
            );
            let version = versions.entry(stream).or_default();
            if e.version != *version + 1 {
                v.push(violation(
                    S3,
                    format!(
                        "event {} of {}/{}.{}/{} has version {}, expected {}",
                        e.global_position,
                        e.tenant_id,
                        e.domain,
                        e.aggregate_type,
                        e.aggregate_id,
                        e.version,
                        *version + 1
                    ),
                ));
            }
            *version = e.version;
        }
    }

    /// S4, S5 and S6: a command's events are one contiguous run on its own stream, carrying its
    /// context; it is stored as committed with exactly those events, and never pending with any.
    fn commands(&self, v: &mut Vec<Violation>) {
        let mut runs: BTreeMap<&str, Vec<&Event>> = BTreeMap::new();
        let mut previous: Option<&str> = None;
        for e in &self.events {
            let id = e.command_id.as_str();
            let run = runs.entry(id).or_default();
            if !run.is_empty() && previous != Some(id) {
                v.push(violation(
                    S4,
                    format!(
                        "command {id}: event {} is apart from its other events",
                        e.global_position
                    ),
                ));
            }
            run.push(e);
            previous = Some(id);
        }
        for (id, events) in &runs {
            match self.commands.get(*id) {
                Some(Some(cmd)) => {
                    for e in events {
                        let follows = e.tenant_id == cmd.tenant_id
                            && e.domain == cmd.domain
                            && e.aggregate_type == cmd.aggregate_type
                            && e.aggregate_id == cmd.aggregate_id
                            && e.context == cmd.context;
                        if !follows {
                            v.push(violation(
                                S5,
                                format!(
                                    "event {} differs from command {id} in tenant, domain, \
                                     aggregate or context",
                                    e.global_position
                                ),
                            ));
                        }
                    }
                }
                _ => v.push(violation(
                    S6,
                    format!("command {id} has events but is not stored"),
                )),
            }
            if self.pending.contains(*id) {
                v.push(violation(
                    S6,
                    format!("command {id} has events but is still pending"),
                ));
            }
            let positions: Vec<u64> = events.iter().map(|e| e.global_position).collect();
            match self.committed(id) {
                Some(stored) if stored == positions => {}
                Some(stored) => v.push(violation(
                    S6,
                    format!(
                        "command {id} is stored with events {stored:?}, the log has {positions:?}"
                    ),
                )),
                None => v.push(violation(
                    S6,
                    format!("command {id} has events {positions:?} but is not committed"),
                )),
            }
        }
        for (id, result) in &self.results {
            if let Some(stored) = self.committed(id)
                && !stored.is_empty()
                && !runs.contains_key(id.as_str())
            {
                v.push(violation(
                    S6,
                    format!("command {id} is committed with events {stored:?}, the log has none"),
                ));
            }
            if result.is_some() && self.pending.contains(id) {
                v.push(violation(
                    S6,
                    format!("command {id} is completed and pending"),
                ));
            }
        }
    }

    /// S7: per qualified event type, the payload version never goes down.
    fn schemas(&self, v: &mut Vec<Violation>) {
        let mut highest: BTreeMap<(&str, &str), u32> = BTreeMap::new();
        for e in &self.events {
            let schema = highest
                .entry((e.domain.as_str(), e.event_type.as_str()))
                .or_default();
            if e.schema < *schema {
                v.push(violation(
                    S7,
                    format!(
                        "event {} writes {}.{} at schema {} after schema {}",
                        e.global_position, e.domain, e.event_type, e.schema, *schema
                    ),
                ));
            }
            *schema = (*schema).max(e.schema);
        }
    }

    /// S9: no key ID is held by two streams, a key is held either by its ID or by a vault
    /// value, and only streams with events hold anything. Personal keys live in the vault, so
    /// their uniqueness is the kernel's to keep.
    fn keys(&self, v: &mut Vec<Violation>) {
        let written: BTreeSet<(&str, &str, &str, &str)> = self
            .events
            .iter()
            .map(|e| {
                (
                    e.tenant_id.as_str(),
                    e.domain.as_str(),
                    e.aggregate_type.as_str(),
                    e.aggregate_id.as_str(),
                )
            })
            .collect();
        let mut holders: BTreeMap<&[u8], &StreamId> = BTreeMap::new();
        for (stream, index) in &self.index {
            let name = format!(
                "{}/{}.{}/{}",
                stream.tenant_id, stream.domain, stream.aggregate_type, stream.aggregate_id
            );
            let key = (
                stream.tenant_id.as_str(),
                stream.domain.as_str(),
                stream.aggregate_type.as_str(),
                stream.aggregate_id.as_str(),
            );
            if !written.contains(&key) {
                v.push(violation(
                    S9,
                    format!("{name} holds keys or facts but has no events"),
                ));
            }
            for k in &index.keys {
                if k.id.is_empty() == k.vault_ref.is_empty() {
                    v.push(violation(
                        S9,
                        format!(
                            "{name}: key {} needs either an ID or a vault reference",
                            k.name
                        ),
                    ));
                }
                if k.id.is_empty() {
                    continue;
                }
                if let Some(other) = holders.insert(&k.id, stream)
                    && other != stream
                {
                    v.push(violation(
                        S9,
                        format!(
                            "{name}: key {} is held by {}/{}.{}/{} too",
                            k.name,
                            other.tenant_id,
                            other.domain,
                            other.aggregate_type,
                            other.aggregate_id
                        ),
                    ));
                }
            }
        }
    }

    /// The positions of a command's stored commit, if it is stored as committed.
    fn committed(&self, id: &str) -> Option<Vec<u64>> {
        match self.results.get(id)?.as_ref()?.outcome.as_ref()? {
            Outcome::Committed(c) => Some(c.events.iter().map(|e| e.global_position).collect()),
            _ => None,
        }
    }
}

/// What clients and listeners were told, recorded by a test harness as it happens.
#[derive(Debug, Default, Clone)]
pub struct Observed {
    /// Every outcome a client received, by command ID.
    outcomes: BTreeMap<String, Vec<CommandResult>>,
    /// Every connection of every listener, by name, in the order they were made.
    listeners: BTreeMap<String, Vec<Connection>>,
}

/// One connection of a listener, from its `Ready` to its end.
#[derive(Debug, Default, Clone)]
struct Connection {
    /// The event types it asked for (`*`, `domain.*`, `domain.Type`); empty: all.
    types: Vec<String>,
    /// `Ready.after_position`.
    after: u64,
    /// Every delivered event, in the order it came.
    delivered: Vec<Event>,
    /// The highest `up_to` of a batch it received.
    up_to: u64,
    /// The highest position it acknowledged.
    acked: u64,
}

impl Observed {
    /// A client received `result` for a command the kernel accepted. A refusal before the
    /// command is stored (by a middleware) is no outcome; do not record it.
    pub fn outcome(&mut self, result: &CommandResult) {
        self.outcomes
            .entry(result.command_id.clone())
            .or_default()
            .push(result.clone());
    }

    /// `listener` connected with these event types and got `Ready` with `after_position`.
    pub fn connected(&mut self, listener: &str, types: &[String], after_position: u64) {
        self.listeners
            .entry(listener.to_owned())
            .or_default()
            .push(Connection {
                types: types.to_vec(),
                after: after_position,
                ..Connection::default()
            });
    }

    /// `listener` received `batch` on its newest connection.
    pub fn delivered(&mut self, listener: &str, batch: &Batch) {
        let connection = self.connection(listener);
        connection.delivered.extend(batch.events.iter().cloned());
        connection.up_to = connection.up_to.max(batch.up_to);
    }

    /// `listener` acknowledged `position` on its newest connection.
    pub fn acked(&mut self, listener: &str, position: u64) {
        let connection = self.connection(listener);
        connection.acked = connection.acked.max(position);
    }

    /// The IDs of every command a client was told about.
    pub fn command_ids(&self) -> Vec<String> {
        self.outcomes.keys().cloned().collect()
    }

    /// Every position `listener` was delivered, on any connection.
    pub fn delivered_positions(&self, listener: &str) -> BTreeSet<u64> {
        self.listeners
            .get(listener)
            .into_iter()
            .flatten()
            .flat_map(|c| c.delivered.iter().map(|e| e.global_position))
            .collect()
    }

    fn connection(&mut self, listener: &str) -> &mut Connection {
        let connections = self.listeners.entry(listener.to_owned()).or_default();
        if connections.is_empty() {
            connections.push(Connection::default());
        }
        connections.last_mut().expect("not empty")
    }

    /// Checks O1–O4 against the log the store holds afterwards.
    pub fn check(&self, log: &Log) -> Vec<Violation> {
        let mut v = Vec::new();
        self.outcomes(log, &mut v);
        for (name, connections) in &self.listeners {
            let mut reached = None;
            for (n, c) in connections.iter().enumerate() {
                c.check(name, n + 1, log, &mut v);
                if let Some(reached) = reached
                    && c.after > reached
                {
                    v.push(violation(
                        O2,
                        format!(
                            "listener {name}, connection {}: resumed after {}, but acknowledged \
                             only up to {reached}",
                            n + 1,
                            c.after
                        ),
                    ));
                }
                reached = Some(reached.unwrap_or(0).max(c.acked).max(c.after));
            }
            if let Some(stored) = log.listeners.iter().find(|l| &l.name == name) {
                let reached = connections
                    .iter()
                    .map(|c| c.acked.max(c.after))
                    .max()
                    .unwrap_or(0);
                if stored.position > reached {
                    v.push(violation(
                        O2,
                        format!(
                            "listener {name} is stored at {}, but acknowledged only up to {reached}",
                            stored.position
                        ),
                    ));
                }
            }
        }
        v
    }

    /// O1: what a client was told about a command is what the store holds.
    fn outcomes(&self, log: &Log, v: &mut Vec<Violation>) {
        for (id, results) in &self.outcomes {
            let stored = log.results.get(id).cloned().flatten();
            let stored = stored.as_ref().and_then(|r| r.outcome.as_ref());
            for result in results {
                let told = result.outcome.as_ref();
                let same = match (told, stored) {
                    (Some(Outcome::Accepted(_)), _) => true,
                    (Some(Outcome::Committed(a)), Some(Outcome::Committed(b))) => {
                        same_events(&a.events, &b.events)
                    }
                    (Some(Outcome::Rejected(a)), Some(Outcome::Rejected(b))) => a == b,
                    _ => false,
                };
                if !same {
                    v.push(violation(
                        O1,
                        format!(
                            "command {id}: a client was told {}, the store holds {}",
                            describe(told),
                            describe(stored)
                        ),
                    ));
                }
            }
        }
    }
}

impl Connection {
    /// O2–O4 for one connection.
    fn check(&self, name: &str, n: usize, log: &Log, v: &mut Vec<Violation>) {
        let at = format!("listener {name}, connection {n}");
        let mut last = self.after;
        for e in &self.delivered {
            let pos = e.global_position;
            if pos <= last {
                v.push(violation(
                    O3,
                    format!("{at}: event {pos} came after {last}"),
                ));
            }
            last = last.max(pos);
            if !matches(&self.types, e) {
                v.push(violation(
                    O4,
                    format!(
                        "{at}: got event {pos} of {}.{}, which it did not ask for",
                        e.domain, e.event_type
                    ),
                ));
            }
            match log.event(pos) {
                Some(stored) if stored == e => {}
                Some(_) => v.push(violation(
                    O4,
                    format!("{at}: event {pos} differs from the stored one"),
                )),
                None => v.push(violation(
                    O4,
                    format!("{at}: got event {pos}, which the log does not have"),
                )),
            }
        }
        if self.acked > self.up_to {
            v.push(violation(
                O2,
                format!(
                    "{at}: acknowledged {} beyond the delivered {}",
                    self.acked, self.up_to
                ),
            ));
        }
        let delivered: BTreeSet<u64> = self.delivered.iter().map(|e| e.global_position).collect();
        let missing: Vec<u64> = log
            .events
            .iter()
            .filter(|e| e.global_position > self.after && e.global_position <= self.acked)
            .filter(|e| matches(&self.types, e))
            .map(|e| e.global_position)
            .filter(|pos| !delivered.contains(pos))
            .collect();
        if !missing.is_empty() {
            v.push(violation(
                O2,
                format!(
                    "{at}: acknowledged up to {} without getting events {missing:?}",
                    self.acked
                ),
            ));
        }
    }
}

/// Whether `e` is one of `types` (`*`, `domain.*`, `domain.Type`); empty: every type.
pub fn matches(types: &[String], e: &Event) -> bool {
    types.is_empty()
        || types.iter().any(|t| match t.split_once('.') {
            _ if t == "*" => true,
            Some((domain, "*")) => domain == e.domain,
            Some((domain, event_type)) => domain == e.domain && event_type == e.event_type,
            None => false,
        })
}

fn same_events(a: &[Event], b: &[Event]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(a, b)| a.global_position == b.global_position && a.hash == b.hash)
}

fn describe(outcome: Option<&Outcome>) -> String {
    match outcome {
        None => "nothing".into(),
        Some(Outcome::Accepted(_)) => "accepted".into(),
        Some(Outcome::Committed(c)) => format!(
            "committed {:?}",
            c.events
                .iter()
                .map(|e| e.global_position)
                .collect::<Vec<_>>()
        ),
        Some(Outcome::Rejected(r)) => format!("rejected ({}: {})", r.code, r.message),
    }
}

fn violation(invariant: &'static str, detail: String) -> Violation {
    Violation { invariant, detail }
}
