//! Durable, named, ordered event listeners (read models and reactors).
//!
//! Event N reaches a listener of order k only after every registered listener with a lower
//! order has acknowledged N; equal orders run in parallel. A registered listener counts even
//! while it is disconnected, so a stalled listener holds back later orders and settling
//! submits; unless its service's membership does not list it (see `Membership`) or it is live
//! (not registered at all, see `Hello.live`). A new listener starts at the newest event unless
//! it asks for an earlier position. Positions are kept in memory and written to the store
//! periodically: after a crash a listener may see a few events again (delivery is at least
//! once).
//!
//! Listeners also answer queries (read models register query types): the kernel sends a query
//! on the listener's stream, in order with the batches, so an answer reflects every event the
//! listener was sent before it.
//!
//! A listener may run as several instances under one name (`Hello.instance`): a
//! replicated group (read models on several nodes) delivers every event to every instance from
//! its own position and routes queries and checks to any instance that has reached what they
//! need; a single group (reactors) delivers to one instance at a time while the others wait to
//! take over. The group counts as one listener: its position is the highest any instance
//! acknowledged, and it has caught up once one instance has.
//!
//! A listener can also be a middleware, before or after requests (see `Hello.middlewares`).
//! Its registration is stored, so it keeps checking, and refusing while it is disconnected,
//! the types it covers until it reconnects without that middleware, its service's membership
//! no longer lists it as one, or it is removed. Checks are
//! sent in order with the batches, and only once the listener has caught up with the events
//! committed before it connected.
//!
//! A listener may give up on an event and park it (`Ack.parked`): it moves on, and the kernel
//! keeps the event's position with the reason until an operator delivers it again or dismisses
//! it (`ListenerService.Retry`). A retry goes out in a batch of its own (`Batch.retries`); the
//! event stays parked until the listener acknowledges that batch without parking it again.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kernable_core::{
    proto::{
        Ack, Batch, Challenge, Event, Hello, ListenerDelivery, ListenerMessage, ListenerRecord,
        Membership, MiddlewareCall, ParkedEvent, QueryAnswer, QueryCall, QueryRequest, Ready,
        Signature, Verdict, Waiting, When, listener_delivery::Msg as Out,
        listener_message::Msg as In,
    },
    signing,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::{Status, Streaming};

use prost::Message;

use crate::{BoxError, Kernel, SUBSCRIBE_BATCH, config, internal, memberships::Memberships};

/// Parked events per listener the UI shows.
const PARKED_SHOWN: usize = 20;

/// How often acknowledged positions are written to the store.
pub(crate) const FLUSH: Duration = Duration::from_millis(100);

/// Most events one listener may have parked; an ack beyond it is refused, so the listener
/// stops instead of parking everything.
pub(crate) const MAX_PARKED: usize = 10_000;

/// Most instances one listener may have connected at once.
pub(crate) const MAX_INSTANCES: usize = 64;

struct Entry {
    /// The registration; its position is the highest any instance acknowledged.
    record: ListenerRecord,
    /// The connected instances by instance ID (`""` for a listener without instances).
    instances: HashMap<String, Instance>,
    /// The group's instances take turns (`Hello.single`); also for a listener without instances.
    single: bool,
    /// Changed since the last flush.
    dirty: bool,
    /// Not registered: never stored, forgotten when it disconnects (`Hello.live`).
    live: bool,
    /// Its service's membership lists it, or none decides.
    listed: bool,
    /// Parked events sent to the connected listener again: (the batch's `up_to`, position).
    retrying: Vec<(u64, u64)>,
}

/// One connection of a listener.
struct Instance {
    /// What this instance acknowledged.
    position: u64,
    /// Qualified query types it answers.
    query_types: Vec<String>,
    /// Its stream, for queries and checks.
    out: Outbox,
    /// The newest event when it connected: it checks requests only once it has processed it.
    caught_up_at: u64,
    /// When it connected (ms since epoch).
    since: i64,
}

impl Instance {
    fn caught_up(&self) -> bool {
        self.position >= self.caught_up_at
    }
}

impl Entry {
    /// Whether others wait for it: for readiness, settling and the order of delivery.
    fn counts(&self) -> bool {
        !self.live && self.listed
    }

    fn connected(&self) -> bool {
        !self.instances.is_empty()
    }

    /// Whether its instances each get every event, so none may park one.
    fn replicated(&self) -> bool {
        !self.single
    }
}

/// A connection of a listener: its name and instance ID.
type Conn = (String, String);

/// What a listener sends back for a call.
pub(crate) enum Reply {
    Answer(QueryAnswer),
    Verdict(Verdict),
}

type Calls = HashMap<String, (Conn, oneshot::Sender<Reply>)>;

/// The listener whose handler sent a request (`listener` in submits and queries). It handles
/// one event at a time, so the request must not wait for it.
#[derive(Clone)]
pub(crate) struct Caller {
    pub(crate) name: String,
    order: i32,
    position: u64,
}

impl Caller {
    /// Whether the answer of the listener `e` to a query for `min_position` can come while
    /// the caller is busy: not the caller itself, and not one that needs the caller's progress.
    fn independent(&self, e: &Entry, min_position: u64) -> bool {
        e.record.name != self.name
            && (e.record.order <= self.order || min_position <= self.position)
    }
}

/// A listener as reported in metrics.
pub(crate) struct ListenerStatus {
    pub(crate) name: String,
    pub(crate) position: u64,
    pub(crate) connected: bool,
    pub(crate) parked: usize,
}

#[derive(Default)]
pub(crate) struct Registry {
    entries: Mutex<HashMap<String, Entry>>,
    /// Queries and checks sent to a listener and not answered yet:
    /// call ID -> (connection, waiting caller).
    calls: Mutex<Calls>,
    /// Spreads queries over listeners that answer the same type; also makes call IDs unique.
    turn: AtomicU64,
    /// Wakes order gates and settling submits when a position or the set of listeners changes.
    pub(crate) progress: Notify,
}

impl Registry {
    pub(crate) fn load(records: Vec<ListenerRecord>, memberships: &Memberships) -> Self {
        let entries = records
            .into_iter()
            .map(|record| {
                let entry = Entry {
                    listed: memberships.counts_listener(&record.service, &record.name),
                    record,
                    instances: HashMap::new(),
                    single: true,
                    dirty: false,
                    live: false,
                    retrying: Vec::new(),
                };
                (entry.record.name.clone(), entry)
            })
            .collect();
        Registry {
            entries: Mutex::new(entries),
            calls: Mutex::default(),
            turn: AtomicU64::new(0),
            progress: Notify::new(),
        }
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The highest position a listener of `order` may receive: the lowest acknowledged
    /// position among listeners with a lower order, or `u64::MAX` if there are none.
    fn gate(&self, order: i32) -> u64 {
        self.entries()
            .values()
            .filter(|e| e.counts() && e.record.order < order)
            .map(|e| e.record.position)
            .min()
            .unwrap_or(u64::MAX)
    }

    /// The parked events of every listener that has some, by listener name.
    pub(crate) fn parked(&self) -> Vec<(String, Vec<ParkedEvent>)> {
        let mut all: Vec<(String, Vec<ParkedEvent>)> = self
            .entries()
            .values()
            .filter(|e| !e.record.parked.is_empty())
            .map(|e| (e.record.name.clone(), e.record.parked.clone()))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }

    pub(crate) fn snapshot(&self) -> Vec<ListenerStatus> {
        let mut all: Vec<ListenerStatus> = self
            .entries()
            .values()
            .map(|e| ListenerStatus {
                name: e.record.name.clone(),
                position: e.record.position,
                connected: e.connected(),
                parked: e.record.parked.len(),
            })
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// Listeners that count and have no connected instance that caught up with the events
    /// committed before it connected, and `listed` ones that never registered, by name.
    pub(crate) fn behind(&self, listed: &[String]) -> Vec<String> {
        let entries = self.entries();
        let mut behind: Vec<String> = entries
            .values()
            .filter(|e| e.counts() && !e.instances.values().any(Instance::caught_up))
            .map(|e| e.record.name.clone())
            .chain(listed.iter().filter(|n| !entries.contains_key(*n)).cloned())
            .collect();
        behind.sort();
        behind.dedup();
        behind
    }

    /// Every listener, registered or live, for the UI.
    pub(crate) fn view(&self) -> Vec<serde_json::Value> {
        let mut all: Vec<serde_json::Value> = self
            .entries()
            .values()
            .map(|e| {
                let middlewares: Vec<serde_json::Value> = e
                    .record
                    .middlewares
                    .iter()
                    .map(|m| {
                        let when = if m.when() == When::After {
                            "after"
                        } else {
                            "before"
                        };
                        serde_json::json!({ "when": when, "order": m.order, "types": m.types })
                    })
                    .collect();
                let mut instances: Vec<serde_json::Value> = e
                    .instances
                    .iter()
                    .filter(|(id, _)| !id.is_empty())
                    .map(|(id, i)| {
                        serde_json::json!({
                            "id": id,
                            "position": i.position,
                            "since": i.since,
                            "caughtUpAt": i.caught_up_at,
                        })
                    })
                    .collect();
                instances.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
                let first = e.instances.values().map(|i| (i.since, i.caught_up_at)).min();
                serde_json::json!({
                    "name": e.record.name,
                    "service": e.record.service,
                    "key": e.record.key_id,
                    "order": e.record.order,
                    "events": e.record.event_types,
                    "queries": e.instances.values().next().map(|i| i.query_types.clone()).unwrap_or_default(),
                    "middlewares": middlewares,
                    "position": e.record.position,
                    "connected": e.connected(),
                    "since": first.map_or(0, |f| f.0),
                    "caughtUpAt": first.map_or(0, |f| f.1),
                    "instances": instances,
                    "single": e.single,
                    "live": e.live,
                    "counts": e.counts(),
                    "lastSeen": e.record.last_seen,
                    "parked": e.record.parked.len(),
                    // The first ones, enough to see what goes wrong; the CLI lists all.
                    "parkedEvents": e.record.parked.iter().take(PARKED_SHOWN).map(|p| serde_json::json!({
                        "position": p.position,
                        "type": p.event_type,
                        "code": p.code,
                        "message": p.message,
                        "attempts": p.attempts,
                        "parkedAt": p.parked_at,
                        "retry": p.retry,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();
        all.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        all
    }

    /// Applies a service's membership to its listeners: listed ones count, others not, and a
    /// middleware it no longer lists as one stops checking. Returns the changed records.
    pub(crate) fn apply(&self, m: &Membership) -> Vec<ListenerRecord> {
        let mut changed = Vec::new();
        for e in self.entries().values_mut() {
            if e.live || e.record.service != m.service {
                continue;
            }
            e.listed = crate::memberships::listed_listener(m, &e.record.name);
            if !e.record.middlewares.is_empty() && !m.middlewares.contains(&e.record.name) {
                tracing::warn!(listener = %e.record.name, service = %m.service, "middleware removed: its service no longer lists it as one");
                e.record.middlewares.clear();
                changed.push(e.record.clone());
            }
        }
        changed
    }

    /// The registered listener `name`, as the sender of a request.
    pub(crate) fn caller(&self, name: &str) -> Option<Caller> {
        let entries = self.entries();
        let e = entries.get(name)?;
        Some(Caller {
            name: name.to_owned(),
            order: e.record.order,
            position: e.record.position,
        })
    }

    /// Whether every registered listener has acknowledged `position`; for a request of a
    /// `caller`, every listener with a lower order than the caller.
    pub(crate) fn settled(&self, position: u64, caller: Option<&Caller>) -> bool {
        self.entries()
            .values()
            .filter(|e| e.counts() && caller.is_none_or(|c| e.record.order < c.order))
            .all(|e| e.record.position >= position)
    }

    #[cfg(test)]
    fn position(&self, name: &str) -> u64 {
        self.entries().get(name).map_or(0, |e| e.record.position)
    }

    /// The listener's ack of `ack.position`, with the events it parked (their types looked up
    /// already, in `types`). Retries in batches up to the position are done: those not parked
    /// again leave the parked set. Refuses parked events outside what the ack covers.
    fn acked(&self, conn: &Conn, ack: &Ack, types: Vec<String>) -> Result<(), Status> {
        let name = conn.0.as_str();
        let mut entries = self.entries();
        let Some(e) = entries.get_mut(name) else {
            return Ok(());
        };
        if !ack.parked.is_empty() && e.live {
            return Err(Status::invalid_argument(
                "a live listener cannot park events",
            ));
        }
        if !ack.parked.is_empty() && e.replicated() {
            return Err(Status::invalid_argument(
                "an instance of a replicated listener cannot park events: every instance gets \
                 every event",
            ));
        }
        for (i, p) in ack.parked.iter().enumerate() {
            let covered = p.position <= ack.position
                && (p.position > e.record.position
                    || e.retrying.iter().any(|&(_, r)| r == p.position));
            if !covered || ack.parked[..i].iter().any(|o| o.position == p.position) {
                return Err(Status::invalid_argument(format!(
                    "parked event {} is not in the acknowledged batches",
                    p.position
                )));
            }
        }
        let added = ack
            .parked
            .iter()
            .filter(|p| {
                e.record
                    .parked
                    .binary_search_by_key(&p.position, |q| q.position)
                    .is_err()
            })
            .count();
        if e.record.parked.len() + added > MAX_PARKED {
            return Err(Status::resource_exhausted(format!(
                "listener {name} would have more than {MAX_PARKED} parked events: retry or \
                 dismiss them (`kernable listeners parked`)"
            )));
        }
        let done: Vec<u64> = e
            .retrying
            .iter()
            .filter(|&&(up_to, _)| up_to <= ack.position)
            .map(|&(_, position)| position)
            .collect();
        let parked_again = |position: u64| ack.parked.iter().any(|p| p.position == position);
        let before = e.record.parked.len();
        e.record
            .parked
            .retain(|p| !done.contains(&p.position) || parked_again(p.position));
        e.retrying.retain(|&(up_to, _)| up_to > ack.position);
        let mut changed = e.record.parked.len() != before;
        let now = crate::now_ms();
        for (p, event_type) in ack.parked.iter().zip(types) {
            let parked = ParkedEvent {
                position: p.position,
                event_type,
                code: truncated(&p.code, 128),
                message: truncated(&p.message, 1024),
                attempts: p.attempts,
                parked_at: now,
                retry: false,
            };
            match e
                .record
                .parked
                .binary_search_by_key(&p.position, |q| q.position)
            {
                Ok(i) => e.record.parked[i] = parked,
                Err(i) => e.record.parked.insert(i, parked),
            }
            changed = true;
        }
        if changed {
            e.record.parked_changed = true;
            e.dirty = true;
        }
        drop(entries);
        self.ack(conn, ack.position);
        Ok(())
    }

    /// Parked events of `name` to deliver again now: asked for, of `types` its key may still
    /// read, and not on their way already.
    fn due(&self, name: &str, types: Option<&[String]>) -> Vec<u64> {
        let entries = self.entries();
        let Some(e) = entries.get(name) else {
            return Vec::new();
        };
        e.record
            .parked
            .iter()
            .filter(|p| p.retry && !e.retrying.iter().any(|&(_, r)| r == p.position))
            .filter(|p| {
                types.is_none_or(|t| {
                    t.iter()
                        .any(|pattern| config::covers(pattern, &p.event_type))
                })
            })
            .map(|p| p.position)
            .collect()
    }

    /// Notes that the parked events at `positions` went out in a batch up to `up_to`.
    fn retrying(&self, name: &str, up_to: u64, positions: &[u64]) {
        if let Some(e) = self.entries().get_mut(name) {
            e.retrying.extend(positions.iter().map(|&p| (up_to, p)));
        }
    }

    /// Marks parked events of the registered listener `name` for delivery (or removes them,
    /// `dismiss`); all of them if `positions` is empty. Returns the positions it applied to, or
    /// None if there is no such listener. The change is stored with the next flush.
    pub(crate) fn retry(&self, name: &str, positions: &[u64], dismiss: bool) -> Option<Vec<u64>> {
        let mut entries = self.entries();
        let e = entries.get_mut(name).filter(|e| !e.live)?;
        let chosen = |p: &ParkedEvent| positions.is_empty() || positions.contains(&p.position);
        let applied: Vec<u64> = e
            .record
            .parked
            .iter()
            .filter(|p| chosen(p))
            .map(|p| p.position)
            .collect();
        if dismiss {
            e.record.parked.retain(|p| !chosen(p));
            // A retry on its way is no longer parked: its ack changes nothing.
            e.retrying.retain(|(_, r)| !applied.contains(r));
        } else {
            for p in e.record.parked.iter_mut().filter(|p| chosen(p)) {
                p.retry = true;
            }
        }
        if !applied.is_empty() {
            e.record.parked_changed = true;
            e.dirty = true;
        }
        drop(entries);
        self.progress.notify_waiters();
        Some(applied)
    }

    /// The service of the registered listener `name`, for authorizing a retry.
    pub(crate) fn service(&self, name: &str) -> Option<String> {
        self.entries()
            .get(name)
            .filter(|e| !e.live)
            .map(|e| e.record.service.clone())
    }

    /// What the connection `conn` acknowledged.
    fn instance_position(&self, conn: &Conn) -> u64 {
        self.entries()
            .get(&conn.0)
            .and_then(|e| e.instances.get(&conn.1))
            .map_or(0, |i| i.position)
    }

    fn ack(&self, conn: &Conn, position: u64) {
        if let Some(e) = self.entries().get_mut(&conn.0) {
            if let Some(i) = e.instances.get_mut(&conn.1) {
                i.position = i.position.max(position);
            }
            if position > e.record.position {
                e.record.position = position;
                e.dirty = true;
            }
        }
        self.progress.notify_waiters();
    }

    fn disconnect(&self, conn: &Conn) {
        let (name, instance) = conn;
        let mut entries = self.entries();
        if let Some(e) = entries.get_mut(name) {
            e.instances.remove(instance);
            // Retries belong to the instance that got them; a listener that parks has one.
            e.retrying.clear();
            if !e.connected() {
                if e.live {
                    entries.remove(name);
                } else {
                    e.record.last_seen = crate::now_ms();
                    e.dirty = true;
                }
            }
        }
        drop(entries);
        // Dropping the senders tells the waiting callers that no answer will come.
        self.calls().retain(|_, (c, _)| c != conn);
    }

    fn calls(&self) -> MutexGuard<'_, Calls> {
        self.calls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether any connected listener answers `query_type` (qualified) for `caller`.
    pub(crate) fn answers(
        &self,
        query_type: &str,
        query: &QueryRequest,
        caller: Option<&Caller>,
    ) -> bool {
        self.entries().values().any(|e| {
            e.instances
                .values()
                .any(|i| i.query_types.iter().any(|t| t == query_type))
                && caller.is_none_or(|c| c.independent(e, query.min_position))
        })
    }

    /// Whether a connected listener that answers `query_type` has processed the query's
    /// `min_position` (a wait-only query).
    pub(crate) fn caught_up(
        &self,
        query_type: &str,
        query: &QueryRequest,
        caller: Option<&Caller>,
    ) -> bool {
        self.entries().values().any(|e| {
            e.instances.values().any(|i| {
                i.position >= query.min_position && i.query_types.iter().any(|t| t == query_type)
            }) && caller.is_none_or(|c| c.independent(e, query.min_position))
        })
    }

    /// Picks a connected listener that answers `query_type` and has processed the query's
    /// `min_position`, and registers the call. Returns `None` if there is none (yet).
    pub(crate) fn dispatch(
        &self,
        query_type: &str,
        query: &QueryRequest,
        caller: Option<&Caller>,
    ) -> Option<Dispatched> {
        let entries = self.entries();
        let mut ready: Vec<(Conn, &Outbox)> = entries
            .iter()
            .filter(|(_, e)| caller.is_none_or(|c| c.independent(e, query.min_position)))
            .flat_map(|(name, e)| {
                e.instances
                    .iter()
                    .filter(|(_, i)| i.position >= query.min_position)
                    .filter(|(_, i)| i.query_types.iter().any(|t| t == query_type))
                    .map(move |(id, i)| ((name.clone(), id.clone()), &i.out))
            })
            .collect();
        if ready.is_empty() {
            return None;
        }
        ready.sort_by(|a, b| a.0.cmp(&b.0));
        let turn = self.turn.fetch_add(1, Ordering::Relaxed) as usize;
        let (conn, out) = ready.swap_remove(turn % ready.len());
        let call_id = format!("{}#{turn}", query.query_id);
        let (tx, rx) = oneshot::channel();
        self.calls().insert(call_id.clone(), (conn, tx));
        let call = QueryCall {
            call_id,
            query: Some(query.clone()),
        };
        Some(Dispatched {
            out: out.clone(),
            msg: Out::Query(call),
            reply: rx,
        })
    }

    /// The middlewares that check `name` (a qualified command or query type) at `when`, in
    /// chain order: by order, then by listener name. Includes disconnected ones.
    pub(crate) fn chain(&self, name: &str, when: When) -> Vec<String> {
        let mut chain: Vec<(i32, String)> = self
            .entries()
            .values()
            .filter_map(|e| {
                let m = e.record.middlewares.iter().find(|m| m.when() == when)?;
                m.types
                    .iter()
                    .any(|p| config::covers(p, name))
                    .then(|| (m.order, e.record.name.clone()))
            })
            .collect();
        chain.sort();
        chain.into_iter().map(|(_, name)| name).collect()
    }

    /// The key of the registered listener `name`.
    pub(crate) fn key(&self, name: &str) -> Option<String> {
        self.entries().get(name).map(|e| e.record.key_id.clone())
    }

    /// Registers `call` (its ID is set here) for the middleware `listener`. `Ok(None)` while
    /// the middleware is still catching up; an error if it is not connected.
    pub(crate) fn check(
        &self,
        listener: &str,
        id: &str,
        mut call: MiddlewareCall,
    ) -> Result<Option<Dispatched>, Status> {
        let entries = self.entries();
        let Some(e) = entries.get(listener).filter(|e| e.connected()) else {
            return Err(Status::unavailable(format!(
                "middleware {listener} is not connected"
            )));
        };
        let mut ready: Vec<(&String, &Instance)> =
            e.instances.iter().filter(|(_, i)| i.caught_up()).collect();
        if ready.is_empty() {
            return Ok(None);
        }
        ready.sort_by(|a, b| a.0.cmp(b.0));
        let turn = self.turn.fetch_add(1, Ordering::Relaxed);
        let (instance, chosen) = ready[turn as usize % ready.len()];
        let out = chosen.out.clone();
        let conn = (listener.to_owned(), instance.clone());
        call.call_id = format!("{id}#{turn}");
        let (tx, rx) = oneshot::channel();
        self.calls().insert(call.call_id.clone(), (conn, tx));
        Ok(Some(Dispatched {
            out,
            msg: Out::Check(call),
            reply: rx,
        }))
    }

    fn reply(&self, conn: &Conn, call_id: &str, reply: Reply) {
        let mut calls = self.calls();
        if calls.get(call_id).is_some_and(|(c, _)| c == conn)
            && let Some((_, tx)) = calls.remove(call_id)
        {
            let _ = tx.send(reply);
        }
    }

    /// Records changed since the last call; connected ones are seen now.
    pub(crate) fn take_dirty(&self) -> Vec<ListenerRecord> {
        let now = crate::now_ms();
        self.entries()
            .values_mut()
            .filter(|e| e.dirty && !e.live)
            .map(|e| {
                e.dirty = false;
                if e.connected() {
                    e.record.last_seen = now;
                }
                // Parked events only when they changed: a position flush leaves them stored.
                let parked = std::mem::take(&mut e.record.parked);
                let mut record = e.record.clone();
                if record.parked_changed {
                    record.parked = parked.clone();
                }
                e.record.parked = parked;
                e.record.parked_changed = false;
                record
            })
            .collect()
    }

    /// Marks these listeners changed again, after storing them failed.
    fn dirty_again(&self, records: &[ListenerRecord]) {
        let mut entries = self.entries();
        for r in records {
            if let Some(e) = entries.get_mut(&r.name) {
                e.dirty = true;
                e.record.parked_changed |= r.parked_changed;
            }
        }
    }

    /// Marks every connected listener changed, so the next flush stores it as seen now.
    pub(crate) fn seen_now(&self) {
        for e in self.entries().values_mut().filter(|e| e.connected()) {
            e.dirty = true;
        }
    }
}

/// Writes changed listener positions to the store.
pub(crate) fn flush(kernel: &Kernel) -> Result<(), BoxError> {
    let dirty = kernel.listeners.take_dirty();
    if !dirty.is_empty() {
        if let Err(e) = kernel.store.save_listeners(&dirty) {
            // Tried again at the next flush.
            kernel.listeners.dirty_again(&dirty);
            return Err(e.into());
        }
        crate::crash::point("listeners.flushed");
    }
    Ok(())
}

pub(crate) type Outbox = mpsc::Sender<Result<ListenerDelivery, Status>>;

async fn next(inbound: &mut Streaming<ListenerMessage>) -> Result<Option<In>, Status> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

async fn send(out: &Outbox, msg: Out) -> Result<(), Status> {
    out.send(Ok(ListenerDelivery { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("listener disconnected"))
}

/// Marks the listener's instance disconnected when its stream ends, however it ends.
struct DisconnectOnDrop(Arc<Kernel>, Conn);

impl Drop for DisconnectOnDrop {
    fn drop(&mut self) {
        tracing::info!(listener = %self.1.0, instance = %self.1.1, "listener disconnected");
        self.0.listeners.disconnect(&self.1);
        self.0.listeners.progress.notify_waiters();
    }
}

pub(crate) async fn run(
    kernel: Arc<Kernel>,
    metadata: &tonic::metadata::MetadataMap,
    inbound: &mut Streaming<ListenerMessage>,
    out: &Outbox,
) -> Result<(), Status> {
    // Handshake: Hello -> Challenge -> ChallengeResponse -> Ready.
    let Some(In::Hello(hello)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected Hello"));
    };
    crate::protocol(hello.protocol)?;
    if !config::valid_name(&hello.name) {
        return Err(Status::invalid_argument(
            "a listener name has 1 to 128 letters, digits, `-`, `_` and `.`",
        ));
    }
    if hello.order < 0 {
        return Err(Status::invalid_argument(
            "negative orders are reserved for kernable",
        ));
    }
    kernel.present(metadata, &hello.key_id)?;
    let permissions = kernel.permissions();
    let types = config::readable(&permissions.listen, &hello.key_id, &hello.event_types)
        .map_err(Status::permission_denied)?;
    if !hello.query_types.iter().all(|t| config::valid_qualified(t)) {
        return Err(Status::invalid_argument(
            "answer qualified query types: domain.QueryType",
        ));
    }
    if !hello
        .query_types
        .iter()
        .all(|t| config::allowed(&permissions.answer, &hello.key_id, t))
    {
        return Err(Status::permission_denied(
            "key may not answer these query types",
        ));
    }
    if !hello.instance.is_empty() && !config::valid_name(&hello.instance) {
        return Err(Status::invalid_argument(
            "an instance ID has 1 to 128 letters, digits, `-`, `_` and `.`",
        ));
    }
    if hello.single && hello.instance.is_empty() {
        return Err(Status::invalid_argument(
            "single is for a listener's instances: name the instance",
        ));
    }
    if hello.single && hello.live {
        return Err(Status::invalid_argument(
            "a live listener cannot be single: every process gets its own events",
        ));
    }
    if hello.live && !hello.middlewares.is_empty() {
        return Err(Status::invalid_argument(
            "a live listener cannot be a middleware",
        ));
    }
    if !hello.middlewares.is_empty() && !kernel.memberships.may_check(&hello.service, &hello.name) {
        return Err(Status::permission_denied(format!(
            "{} is not a middleware of {}: list it in the service's membership",
            hello.name, hello.service
        )));
    }
    for (i, m) in hello.middlewares.iter().enumerate() {
        if hello.middlewares[..i].iter().any(|o| o.when == m.when) {
            return Err(Status::invalid_argument(
                "a listener has at most one middleware before and one after",
            ));
        }
        if When::try_from(m.when).is_err() {
            return Err(Status::invalid_argument(
                "a middleware runs before or after",
            ));
        }
        if m.order < 0 {
            return Err(Status::invalid_argument(
                "negative middleware orders are reserved for kernable",
            ));
        }
        if m.types.is_empty() || !m.types.iter().all(|t| config::valid_pattern(t)) {
            return Err(Status::invalid_argument(
                "a middleware checks `*`, `domain.*` or qualified types",
            ));
        }
        if !m
            .types
            .iter()
            .all(|t| config::allowed(&permissions.check, &hello.key_id, t))
        {
            return Err(Status::permission_denied("key may not check these types"));
        }
    }

    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    send(
        out,
        Out::Challenge(Challenge {
            nonce: nonce.to_vec(),
        }),
    )
    .await?;
    let Some(In::ChallengeResponse(resp)) = kernel.handshake(next(inbound)).await? else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: hello.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;

    let conn: Conn = (hello.name.clone(), hello.instance.clone());
    // A listener without instances is a single group of one.
    let single = hello.single || hello.instance.is_empty();
    let mut waiting = false;
    let (start, head) = loop {
        let progressed = kernel.listeners.progress.notified();
        let head = kernel.store.head_position().map_err(internal)?;
        let step = 'decide: {
            let mut entries = kernel.listeners.entries();
            let previous = entries.get(&hello.name);
            if let Some(e) = previous.filter(|e| e.connected()) {
                if hello.instance.is_empty() || e.instances.contains_key("") {
                    return Err(Status::already_exists("listener is already connected"));
                }
                if e.instances.contains_key(&hello.instance) {
                    return Err(Status::already_exists(format!(
                        "instance {} of listener {} is already connected",
                        hello.instance, hello.name
                    )));
                }
                if e.single != single {
                    return Err(Status::failed_precondition(format!(
                        "the instances of listener {} disagree on single: connected ones are {}",
                        hello.name,
                        if e.single { "single" } else { "replicated" }
                    )));
                }
                if single {
                    // Another instance is active: wait for it to leave, then take over.
                    break 'decide Err(e.instances.keys().next().cloned().unwrap_or_default());
                }
            }
            if hello.live && previous.is_some_and(|e| !e.live) {
                return Err(Status::already_exists(
                    "a registered listener has this name: remove it, or use another name",
                ));
            }
            if !hello.live && previous.is_some_and(|e| e.live) {
                return Err(Status::already_exists(
                    "a live listener has this name: stop it, or use another name",
                ));
            }
            // A new name counts against the key's limit; a registered one always reconnects.
            let max = kernel.config().limits.listeners_per_key;
            if previous.is_none()
                && entries
                    .values()
                    .filter(|e| e.record.key_id == hello.key_id)
                    .count()
                    >= max
            {
                kernel.metrics.limited("listeners_per_key");
                return Err(Status::resource_exhausted(format!(
                    "the key has {max} listeners (--max-listeners-per-key): remove unused ones with \
                 `kernable listeners remove`"
                )));
            }
            if previous.is_some_and(|e| e.instances.len() >= MAX_INSTANCES) {
                return Err(Status::resource_exhausted(format!(
                    "listener {} has {MAX_INSTANCES} instances connected",
                    hello.name
                )));
            }
            // Another key only as the registered key's successor (`succeeds` in its grant).
            if let Some(e) = previous.filter(|e| e.record.key_id != hello.key_id) {
                if permissions.successors.get(&e.record.key_id) != Some(&hello.key_id) {
                    return Err(Status::permission_denied(
                        "listener is registered with another key",
                    ));
                }
                tracing::info!(listener = %hello.name, key = %hello.key_id, "listener moves to its successor key");
            }
            // A new listener starts at the newest event: it never reacts to what happened before
            // it existed, unless it asks to (a read model rebuilding from 0). An instance taking
            // over a single group goes on after the group's position.
            let position = match (hello.live, hello.from_position) {
                (true, None) => head,
                (true, Some(from)) => from,
                (false, Some(from)) if !waiting => from,
                (false, _) => previous.map_or(head, |e| e.record.position),
            };
            // The group's position is the highest any instance acknowledged; an instance that
            // rebuilds while others run does not take it back.
            let group = match previous.filter(|e| e.connected()) {
                Some(e) => e.record.position.max(position),
                None => position,
            };
            let record = ListenerRecord {
                name: hello.name.clone(),
                key_id: hello.key_id.clone(),
                order: hello.order,
                event_types: hello.event_types.clone(),
                position: group,
                removed: false,
                middlewares: hello.middlewares.clone(),
                service: hello.service.clone(),
                last_seen: crate::now_ms(),
                parked: previous
                    .map(|e| e.record.parked.clone())
                    .unwrap_or_default(),
                parked_changed: false,
            };
            // Registration is stored right away; positions follow with the periodic flush.
            if !hello.live {
                kernel
                    .store
                    .save_listeners(std::slice::from_ref(&record))
                    .map_err(internal)?;
            }
            let instance = Instance {
                position,
                query_types: hello.query_types.clone(),
                out: out.clone(),
                caught_up_at: head,
                since: crate::now_ms(),
            };
            let listed = kernel
                .memberships
                .counts_listener(&hello.service, &hello.name);
            let mut instances = entries
                .remove(&hello.name)
                .map(|e| e.instances)
                .unwrap_or_default();
            instances.insert(hello.instance.clone(), instance);
            entries.insert(
                hello.name.clone(),
                Entry {
                    record,
                    instances,
                    single,
                    dirty: false,
                    live: hello.live,
                    listed,
                    retrying: Vec::new(),
                },
            );
            Ok(position)
        };
        match step {
            Ok(position) => break (position, head),
            Err(active) => {
                if !waiting {
                    waiting = true;
                    tracing::info!(listener = %hello.name, instance = %hello.instance, %active, "listener instance waits");
                    send(out, Out::Waiting(Waiting { active })).await?;
                }
                tokio::select! {
                    _ = progressed => {}
                    _ = out.closed() => return Ok(()),
                }
            }
        }
    };
    let _disconnect = DisconnectOnDrop(Arc::clone(&kernel), conn.clone());
    tracing::info!(listener = %hello.name, instance = %hello.instance, order = hello.order, after = start, "listener connected");
    kernel.listeners.progress.notify_waiters();
    send(
        out,
        Out::Ready(Ready {
            after_position: start,
            head_position: head,
        }),
    )
    .await?;

    let sent = Arc::new(AtomicU64::new(start));
    // A listener of every type gets what the key's grant allows now, also after it changed.
    let filter = Arc::new(Mutex::new(types));
    let deliver = tokio::spawn(deliver(
        Arc::clone(&kernel),
        conn.clone(),
        hello.key_id.clone(),
        hello.order,
        Arc::clone(&filter),
        Arc::clone(&sent),
        out.clone(),
    ));
    let _abort = crate::AbortOnDrop(deliver);

    let mut changes = kernel.changes();
    loop {
        let message = tokio::select! {
            message = next(inbound) => message?,
            Ok(()) = changes.changed() => {
                // Permissions changed: end the stream if the key may no longer do what it
                // registered for.
                let permissions = kernel.permissions();
                if !still_allowed(&permissions, &hello) {
                    return Err(Status::permission_denied("listener key is no longer allowed or was revoked"));
                }
                if let Ok(types) = config::readable(&permissions.listen, &hello.key_id, &hello.event_types) {
                    *filter.lock().unwrap_or_else(PoisonError::into_inner) = types;
                }
                continue;
            }
        };
        match message {
            None => return Ok(()),
            Some(In::Ack(ack)) if ack.position <= sent.load(Ordering::SeqCst) => {
                let types = ack
                    .parked
                    .iter()
                    .map(|p| event_type_at(&kernel, p.position))
                    .collect::<Result<_, _>>()?;
                kernel.listeners.acked(&conn, &ack, types)?;
                crate::crash::point("listeners.acked");
            }
            Some(In::Ack(_)) => {
                return Err(Status::invalid_argument("ack beyond the delivered events"));
            }
            Some(In::Answer(a)) => {
                let call_id = a.call_id.clone();
                kernel.listeners.reply(&conn, &call_id, Reply::Answer(a));
            }
            Some(In::Verdict(v)) => {
                let call_id = v.call_id.clone();
                kernel.listeners.reply(&conn, &call_id, Reply::Verdict(v));
            }
            Some(_) => return Err(Status::invalid_argument("unexpected message")),
        }
    }
}

/// A query or check on its way to a listener: send `msg` on `out`, then wait for `reply`.
pub(crate) struct Dispatched {
    pub(crate) out: Outbox,
    pub(crate) msg: Out,
    pub(crate) reply: oneshot::Receiver<Reply>,
}

impl Dispatched {
    /// Sends the call and waits for the listener's reply; an error if it goes away first.
    pub(crate) async fn call(self) -> Result<Reply, Status> {
        send(&self.out, self.msg).await?;
        self.reply
            .await
            .map_err(|_| Status::unavailable("the listener went away; try again"))
    }
}

/// Sends batches of stored events after `sent`, never beyond the listener's order gate.
async fn deliver(
    kernel: Arc<Kernel>,
    conn: Conn,
    key_id: String,
    order: i32,
    filter: Arc<Mutex<Option<Vec<String>>>>,
    sent: Arc<AtomicU64>,
    out: Outbox,
) {
    let name = conn.0.as_str();
    loop {
        let progressed = kernel.listeners.progress.notified();
        let appended = kernel.appended.notified();
        let after = sent.load(Ordering::SeqCst);
        let types = filter
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let due = kernel.listeners.due(name, types.as_deref());
        if !due.is_empty() {
            let retries = match retries(&kernel, &key_id, &due, after) {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = out.send(Err(e)).await;
                    return;
                }
            };
            let positions: Vec<u64> = retries.retries.iter().map(|e| e.global_position).collect();
            kernel.listeners.retrying(name, after, &positions);
            if send(&out, Out::Batch(retries)).await.is_err() {
                return;
            }
            continue;
        }
        // Never beyond the order gate, nor beyond what is on the disk.
        let gate = kernel
            .listeners
            .gate(order)
            .min(kernel.durable.load(Ordering::SeqCst));
        let batch = if gate > after {
            match kernel.store.read_all(after, SUBSCRIBE_BATCH) {
                Ok(batch) => batch,
                Err(e) => {
                    let _ = out.send(Err(internal(e))).await;
                    return;
                }
            }
        } else {
            Vec::new()
        };
        let mut events = Vec::new();
        let mut up_to = after;
        for event in batch.into_iter().take_while(|e| e.global_position <= gate) {
            up_to = event.global_position;
            if crate::wanted(types.as_deref(), &event) {
                events.push(event);
            }
        }
        if up_to == after {
            tokio::select! {
                _ = progressed => {}
                _ = appended => {}
                _ = out.closed() => return,
            }
            continue;
        }
        if events.is_empty() && kernel.listeners.instance_position(&conn) == after {
            // Nothing to process and nothing outstanding: skip ahead without a round trip.
            sent.store(up_to, Ordering::SeqCst);
            kernel.listeners.ack(&conn, up_to);
            continue;
        }
        let batch = fitted(&kernel, &key_id, events, up_to);
        sent.store(batch.up_to, Ordering::SeqCst);
        if send(&out, Out::Batch(batch)).await.is_err() {
            return;
        }
    }
}

/// A batch of `events` up to `up_to` that fits `--max-message-bytes`: halved, ending at its
/// last event, until it does. A single event always goes; it came from a result that fit.
fn fitted(kernel: &Kernel, key_id: &str, mut events: Vec<Event>, up_to: u64) -> Batch {
    let max = kernel.config().limits.message_bytes;
    let mut batch = batch_of(kernel, key_id, events.clone(), up_to);
    while events.len() > 1 && batch.encoded_len() + crate::ENVELOPE_BYTES > max {
        events.truncate(events.len() / 2);
        let up_to = events.last().map_or(up_to, |e| e.global_position);
        batch = batch_of(kernel, key_id, events.clone(), up_to);
    }
    batch
}

/// A batch with the vault values of its events that the listener's key may read.
fn batch_of(kernel: &Kernel, key_id: &str, events: Vec<Event>, up_to: u64) -> Batch {
    let refs: Vec<String> = events
        .iter()
        .filter(|e| kernel.reads_vault(key_id, &config::qualified(&e.domain, &e.event_type)))
        .flat_map(|e| e.vault_refs.clone())
        .collect();
    Batch {
        vault: kernel.values_of(&refs),
        events,
        up_to,
        retries: Vec::new(),
    }
}

/// A batch of the parked events at `due` to deliver again, as many as fit `--max-message-bytes`
/// (at least one), with `up_to` the last position delivered.
fn retries(kernel: &Kernel, key_id: &str, due: &[u64], up_to: u64) -> Result<Batch, Status> {
    let mut events = Vec::new();
    for &position in due.iter().take(SUBSCRIBE_BATCH as usize) {
        events.extend(
            kernel
                .store
                .read_all(position - 1, 1)
                .map_err(internal)?
                .into_iter()
                .filter(|e| e.global_position == position),
        );
    }
    let max = kernel.config().limits.message_bytes;
    loop {
        let mut batch = batch_of(kernel, key_id, events.clone(), up_to);
        batch.retries = std::mem::take(&mut batch.events);
        if events.len() <= 1 || batch.encoded_len() + crate::ENVELOPE_BYTES <= max {
            return Ok(batch);
        }
        events.truncate(events.len() / 2);
    }
}

/// The qualified type of the event at `position`, which a listener parks; an error if there is
/// none.
fn event_type_at(kernel: &Kernel, position: u64) -> Result<String, Status> {
    let event = kernel
        .store
        .read_all(position.saturating_sub(1), 1)
        .map_err(internal)?
        .into_iter()
        .find(|e| e.global_position == position && position > 0)
        .ok_or_else(|| Status::invalid_argument(format!("no event at position {position}")))?;
    Ok(config::qualified(&event.domain, &event.event_type))
}

/// `s`, cut to at most `max` bytes at a character boundary.
fn truncated(s: &str, max: usize) -> String {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}

/// Whether the listener's key may still read, answer and check what it registered for.
fn still_allowed(permissions: &crate::Permissions, hello: &Hello) -> bool {
    let key = &hello.key_id;
    config::readable(&permissions.listen, key, &hello.event_types).is_ok()
        && hello
            .query_types
            .iter()
            .all(|t| config::allowed(&permissions.answer, key, t))
        && hello.middlewares.iter().all(|m| {
            m.types
                .iter()
                .all(|t| config::allowed(&permissions.check, key, t))
        })
}

#[cfg(test)]
mod tests {
    use kernable_core::proto::Parked;

    use super::*;

    fn parked(position: u64, retry: bool) -> ParkedEvent {
        ParkedEvent {
            position,
            event_type: "orders.ItemAdded".into(),
            code: "not_found".into(),
            retry,
            ..Default::default()
        }
    }

    fn registry(position: u64, events: Vec<ParkedEvent>) -> Registry {
        let record = ListenerRecord {
            name: "mailer".into(),
            position,
            parked: events,
            ..Default::default()
        };
        Registry::load(vec![record], &Memberships::load(Vec::new()))
    }

    fn ack(position: u64, parking: &[u64]) -> Ack {
        let parked = parking
            .iter()
            .map(|&position| Parked {
                position,
                code: "not_found".into(),
                ..Default::default()
            })
            .collect();
        Ack { position, parked }
    }

    fn mailer() -> Conn {
        ("mailer".into(), String::new())
    }

    fn stored(r: &Registry) -> Vec<(u64, bool)> {
        let entries = r.entries();
        let e = &entries["mailer"];
        e.record
            .parked
            .iter()
            .map(|p| (p.position, p.retry))
            .collect()
    }

    #[test]
    fn an_ack_that_would_park_beyond_the_cap_is_refused_and_changes_nothing() {
        let full = (1..=MAX_PARKED as u64).map(|p| parked(p, false)).collect();
        let r = registry(MAX_PARKED as u64, full);
        let n = MAX_PARKED as u64;
        let refused = r.acked(
            &mailer(),
            &ack(n + 1, &[n + 1]),
            vec!["orders.ItemAdded".into()],
        );
        assert_eq!(refused.unwrap_err().code(), tonic::Code::ResourceExhausted);
        assert_eq!(r.position("mailer"), n, "the position stays");
        assert_eq!(stored(&r).len(), MAX_PARKED);
        // Without parking, it moves on.
        r.acked(&mailer(), &ack(n + 1, &[]), Vec::new()).unwrap();
        assert_eq!(r.position("mailer"), n + 1);
    }

    #[test]
    fn a_retry_dismissed_on_its_way_stays_gone_when_it_is_acknowledged() {
        let r = registry(5, vec![parked(2, true), parked(4, true)]);
        // Both went out in a batch up to 5; then 2 is dismissed.
        r.retrying("mailer", 5, &[2, 4]);
        assert_eq!(r.retry("mailer", &[2], true), Some(vec![2]));
        assert_eq!(stored(&r), [(4, true)]);
        // The listener parks 4 again; 2 is no longer its to park.
        let late = r.acked(&mailer(), &ack(5, &[2]), vec!["orders.ItemAdded".into()]);
        assert_eq!(late.unwrap_err().code(), tonic::Code::InvalidArgument);
        r.acked(&mailer(), &ack(5, &[4]), vec!["orders.ItemAdded".into()])
            .unwrap();
        assert_eq!(
            stored(&r),
            [(4, false)],
            "parked again, no longer asked for"
        );
        // Handled on the next retry: gone.
        r.retry("mailer", &[], false);
        r.retrying("mailer", 5, &[4]);
        r.acked(&mailer(), &ack(5, &[]), Vec::new()).unwrap();
        assert!(stored(&r).is_empty());
    }

    /// A registry with `reports`, a replicated read model, connected as the given instances:
    /// (instance, acknowledged position, newest event when it connected).
    fn group(
        instances: &[(&str, u64, u64)],
    ) -> (
        Registry,
        Vec<mpsc::Receiver<Result<ListenerDelivery, Status>>>,
    ) {
        let record = ListenerRecord {
            name: "reports".into(),
            position: instances.iter().map(|i| i.1).max().unwrap_or(0),
            ..Default::default()
        };
        let r = Registry::load(vec![record], &Memberships::load(Vec::new()));
        let mut receivers = Vec::new();
        {
            let mut entries = r.entries();
            let e = entries.get_mut("reports").unwrap();
            e.single = false;
            for &(id, position, caught_up_at) in instances {
                let (tx, rx) = mpsc::channel(4);
                receivers.push(rx);
                e.instances.insert(
                    id.into(),
                    Instance {
                        position,
                        query_types: vec!["shop.GetReport".into()],
                        out: tx,
                        caught_up_at,
                        since: 1,
                    },
                );
            }
        }
        (r, receivers)
    }

    fn query(min_position: u64) -> QueryRequest {
        QueryRequest {
            query_id: "q".into(),
            min_position,
            ..Default::default()
        }
    }

    fn conn(instance: &str) -> Conn {
        ("reports".into(), instance.into())
    }

    #[test]
    fn a_group_is_through_an_event_once_one_instance_acknowledged_it() {
        let (r, _rx) = group(&[("node-a", 10, 0), ("node-b", 4, 0)]);
        assert!(r.settled(10, None), "node-a has it");
        assert!(!r.settled(11, None));
        r.ack(&conn("node-b"), 11);
        assert!(r.settled(11, None), "now node-b is ahead");
        assert_eq!(
            r.instance_position(&conn("node-a")),
            10,
            "each keeps its own"
        );
        assert_eq!(r.gate(1), 11, "a later order may get what one instance has");
    }

    #[test]
    fn a_dead_or_rebuilding_instance_holds_nothing_back() {
        let (r, _rx) = group(&[("node-a", 10, 10), ("node-b", 0, 10)]);
        assert!(r.behind(&[]).is_empty(), "node-a caught up");
        r.disconnect(&conn("node-a"));
        assert_eq!(r.behind(&[]), ["reports"], "node-b is still rebuilding");
        assert!(
            r.settled(10, None),
            "the group keeps what node-a acknowledged"
        );
        r.ack(&conn("node-b"), 10);
        assert!(r.behind(&[]).is_empty());
    }

    #[test]
    fn queries_go_only_to_an_instance_that_reached_their_position() {
        let (r, _rx) = group(&[("node-a", 10, 0), ("node-b", 4, 0)]);
        for _ in 0..4 {
            let d = r.dispatch("shop.GetReport", &query(8), None).unwrap();
            let calls = r.calls();
            let (c, _) = calls.values().last().unwrap();
            assert_eq!(c, &conn("node-a"));
            drop(calls);
            drop(d);
            r.calls().clear();
        }
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..4 {
            let _d = r.dispatch("shop.GetReport", &query(2), None).unwrap();
            seen.extend(r.calls().values().map(|(c, _)| c.1.clone()));
            r.calls().clear();
        }
        assert_eq!(seen.len(), 2, "both share queries they can answer");
        assert!(r.dispatch("shop.GetReport", &query(11), None).is_none());
        r.disconnect(&conn("node-a"));
        assert!(r.caught_up("shop.GetReport", &query(4), None));
        assert!(!r.caught_up("shop.GetReport", &query(8), None));
    }

    #[test]
    fn an_instance_of_a_replicated_listener_cannot_park() {
        let (r, _rx) = group(&[("node-a", 4, 0)]);
        let refused = r.acked(
            &conn("node-a"),
            &ack(5, &[5]),
            vec!["shop.ItemAdded".into()],
        );
        assert_eq!(refused.unwrap_err().code(), tonic::Code::InvalidArgument);
        r.acked(&conn("node-a"), &ack(5, &[]), Vec::new()).unwrap();
        assert_eq!(r.position("reports"), 5);
    }

    #[test]
    fn a_disconnected_instance_drops_only_its_own_calls() {
        let (r, _rx) = group(&[("node-a", 10, 0), ("node-b", 10, 0)]);
        let _a = r.dispatch("shop.GetReport", &query(0), None).unwrap();
        let _b = r.dispatch("shop.GetReport", &query(0), None).unwrap();
        assert_eq!(r.calls().len(), 2);
        r.disconnect(&conn("node-a"));
        let calls = r.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls.values().next().unwrap().0, conn("node-b"));
    }
}
