//! Durable, named, ordered event listeners (read models and reactors).
//!
//! Event N reaches a listener of order k only after every registered listener with a lower
//! order has acknowledged N; equal orders run in parallel. A registered listener counts even
//! while it is disconnected, so a stalled listener holds back later orders and settling
//! submits. Positions are kept in memory and written to the store periodically: after a
//! crash a listener may see a few events again (delivery is at least once).
//!
//! Listeners also answer queries (read models register query types): the kernel sends a query
//! on the listener's stream, in order with the batches, so an answer reflects every event the
//! listener was sent before it.
//!
//! A listener can also be a middleware (see `Hello.middleware`). Its registration is stored,
//! so it keeps checking, and refusing while it is disconnected, the types it covers until it
//! reconnects without a middleware or is removed. Checks are sent in order with the batches,
//! and only once the listener has caught up with the events committed before it connected.

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
        Batch, Challenge, ListenerDelivery, ListenerMessage, ListenerRecord, MiddlewareCall,
        QueryAnswer, QueryCall, QueryRequest, Ready, Signature, Verdict,
        listener_delivery::Msg as Out, listener_message::Msg as In, middleware_call,
    },
    signing,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::{Status, Streaming};

use crate::{BoxError, Kernel, SUBSCRIBE_BATCH, config, internal};

/// How often acknowledged positions are written to the store.
pub(crate) const FLUSH: Duration = Duration::from_millis(100);

struct Entry {
    record: ListenerRecord,
    connected: bool,
    /// Changed since the last flush.
    dirty: bool,
    /// Qualified query types the connected listener answers.
    query_types: Vec<String>,
    /// The connected listener's stream, for queries and checks.
    out: Option<Outbox>,
    /// The newest event when the listener connected: a middleware checks requests only
    /// once it has processed it.
    caught_up_at: u64,
}

/// What a listener sends back for a call.
pub(crate) enum Reply {
    Answer(QueryAnswer),
    Verdict(Verdict),
}

type Calls = HashMap<String, (String, oneshot::Sender<Reply>)>;

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
}

#[derive(Default)]
pub(crate) struct Registry {
    entries: Mutex<HashMap<String, Entry>>,
    /// Queries and checks sent to a listener and not answered yet:
    /// call ID -> (listener, waiting caller).
    calls: Mutex<Calls>,
    /// Spreads queries over listeners that answer the same type; also makes call IDs unique.
    turn: AtomicU64,
    /// Wakes order gates and settling submits when a position or the set of listeners changes.
    pub(crate) progress: Notify,
}

impl Registry {
    pub(crate) fn load(records: Vec<ListenerRecord>) -> Self {
        let entries = records
            .into_iter()
            .map(|record| {
                let entry = Entry {
                    record,
                    connected: false,
                    dirty: false,
                    query_types: Vec::new(),
                    out: None,
                    caught_up_at: 0,
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
            .filter(|e| e.record.order < order)
            .map(|e| e.record.position)
            .min()
            .unwrap_or(u64::MAX)
    }

    pub(crate) fn snapshot(&self) -> Vec<ListenerStatus> {
        let mut all: Vec<ListenerStatus> = self
            .entries()
            .values()
            .map(|e| ListenerStatus {
                name: e.record.name.clone(),
                position: e.record.position,
                connected: e.connected,
            })
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// Registered listeners that are not connected or have not caught up with the events
    /// committed before they connected, by name.
    pub(crate) fn behind(&self) -> Vec<String> {
        let mut behind: Vec<String> = self
            .entries()
            .values()
            .filter(|e| !e.connected || e.record.position < e.caught_up_at)
            .map(|e| e.record.name.clone())
            .collect();
        behind.sort();
        behind
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
            .filter(|e| caller.is_none_or(|c| e.record.order < c.order))
            .all(|e| e.record.position >= position)
    }

    fn position(&self, name: &str) -> u64 {
        self.entries().get(name).map_or(0, |e| e.record.position)
    }

    fn ack(&self, name: &str, position: u64) {
        if let Some(e) = self.entries().get_mut(name)
            && position > e.record.position
        {
            e.record.position = position;
            e.dirty = true;
        }
        self.progress.notify_waiters();
    }

    fn disconnect(&self, name: &str) {
        if let Some(e) = self.entries().get_mut(name) {
            e.connected = false;
            e.out = None;
            e.query_types.clear();
        }
        // Dropping the senders tells the waiting callers that no answer will come.
        self.calls().retain(|_, (listener, _)| listener != name);
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
            e.out.is_some()
                && e.query_types.iter().any(|t| t == query_type)
                && caller.is_none_or(|c| c.independent(e, query.min_position))
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
        let mut ready: Vec<(&String, &Outbox)> = entries
            .iter()
            .filter(|(_, e)| e.record.position >= query.min_position)
            .filter(|(_, e)| e.query_types.iter().any(|t| t == query_type))
            .filter(|(_, e)| caller.is_none_or(|c| c.independent(e, query.min_position)))
            .filter_map(|(name, e)| Some((name, e.out.as_ref()?)))
            .collect();
        if ready.is_empty() {
            return None;
        }
        ready.sort_by(|a, b| a.0.cmp(b.0));
        let turn = self.turn.fetch_add(1, Ordering::Relaxed) as usize;
        let (name, out) = ready[turn % ready.len()];
        let call_id = format!("{}#{turn}", query.query_id);
        let (tx, rx) = oneshot::channel();
        self.calls().insert(call_id.clone(), (name.clone(), tx));
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

    /// The middlewares that check `name` (a qualified command or query type), in chain order:
    /// by order, then by listener name. Includes disconnected ones.
    pub(crate) fn chain(&self, name: &str) -> Vec<String> {
        let mut chain: Vec<(i32, String)> = self
            .entries()
            .values()
            .filter_map(|e| {
                let m = e.record.middleware.as_ref()?;
                m.types
                    .iter()
                    .any(|p| config::covers(p, name))
                    .then(|| (m.order, e.record.name.clone()))
            })
            .collect();
        chain.sort();
        chain.into_iter().map(|(_, name)| name).collect()
    }

    /// Registers a check of `request` by the middleware `listener`. `Ok(None)` while the
    /// middleware is still catching up; an error if it is not connected.
    pub(crate) fn check(
        &self,
        listener: &str,
        id: &str,
        request: middleware_call::Request,
    ) -> Result<Option<Dispatched>, Status> {
        let entries = self.entries();
        let Some(out) = entries.get(listener).and_then(|e| {
            let caught_up = e.record.position >= e.caught_up_at;
            e.out.as_ref().map(|out| caught_up.then_some(out))
        }) else {
            return Err(Status::unavailable(format!(
                "middleware {listener} is not connected"
            )));
        };
        let Some(out) = out else {
            return Ok(None);
        };
        let turn = self.turn.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("{id}#{turn}");
        let (tx, rx) = oneshot::channel();
        self.calls()
            .insert(call_id.clone(), (listener.to_owned(), tx));
        let call = MiddlewareCall {
            call_id,
            request: Some(request),
        };
        Ok(Some(Dispatched {
            out: out.clone(),
            msg: Out::Check(call),
            reply: rx,
        }))
    }

    fn reply(&self, listener: &str, call_id: &str, reply: Reply) {
        let mut calls = self.calls();
        if calls.get(call_id).is_some_and(|(l, _)| l == listener)
            && let Some((_, tx)) = calls.remove(call_id)
        {
            let _ = tx.send(reply);
        }
    }

    /// Records changed since the last call.
    pub(crate) fn take_dirty(&self) -> Vec<ListenerRecord> {
        self.entries()
            .values_mut()
            .filter(|e| e.dirty)
            .map(|e| {
                e.dirty = false;
                e.record.clone()
            })
            .collect()
    }
}

/// Writes changed listener positions to the store.
pub(crate) fn flush(kernel: &Kernel) -> Result<(), BoxError> {
    let dirty = kernel.listeners.take_dirty();
    if !dirty.is_empty() {
        kernel.store.save_listeners(&dirty)?;
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

/// Marks the listener disconnected when its stream ends, however it ends.
struct DisconnectOnDrop(Arc<Kernel>, String);

impl Drop for DisconnectOnDrop {
    fn drop(&mut self) {
        tracing::info!(listener = %self.1, "listener disconnected");
        self.0.listeners.disconnect(&self.1);
        self.0.listeners.progress.notify_waiters();
    }
}

pub(crate) async fn run(
    kernel: Arc<Kernel>,
    inbound: &mut Streaming<ListenerMessage>,
    out: &Outbox,
) -> Result<(), Status> {
    // Handshake: Hello -> Challenge -> ChallengeResponse -> Ready.
    let Some(In::Hello(hello)) = next(inbound).await? else {
        return Err(Status::invalid_argument("expected Hello"));
    };
    crate::protocol(hello.protocol)?;
    if hello.name.is_empty() {
        return Err(Status::invalid_argument("listener name is empty"));
    }
    if hello.order < 0 {
        return Err(Status::invalid_argument(
            "negative orders are reserved for kernable",
        ));
    }
    let types = config::readable(&kernel.config.listeners, &hello.key_id, &hello.event_types)
        .map_err(Status::permission_denied)?;
    if !hello.query_types.iter().all(|t| config::valid_qualified(t)) {
        return Err(Status::invalid_argument(
            "answer qualified query types: domain.QueryType",
        ));
    }
    if !hello
        .query_types
        .iter()
        .all(|t| config::allowed(&kernel.config.query_handlers, &hello.key_id, t))
    {
        return Err(Status::permission_denied(
            "key may not answer these query types",
        ));
    }
    if let Some(m) = &hello.middleware {
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
            .all(|t| config::allowed(&kernel.config.middlewares, &hello.key_id, t))
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
    let Some(In::ChallengeResponse(resp)) = next(inbound).await? else {
        return Err(Status::invalid_argument("expected ChallengeResponse"));
    };
    let proof = Signature {
        key_id: hello.key_id.clone(),
        sig: resp.sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))?;

    let head = kernel.store.head_position().map_err(internal)?;
    let start = {
        let mut entries = kernel.listeners.entries();
        let previous = entries.get(&hello.name);
        if previous.is_some_and(|e| e.connected) {
            return Err(Status::already_exists("listener is already connected"));
        }
        if previous.is_some_and(|e| e.record.key_id != hello.key_id) {
            return Err(Status::permission_denied(
                "listener is registered with another key",
            ));
        }
        let position = hello
            .from_position
            .unwrap_or_else(|| previous.map_or(0, |e| e.record.position));
        let record = ListenerRecord {
            name: hello.name.clone(),
            key_id: hello.key_id,
            order: hello.order,
            event_types: hello.event_types,
            position,
            removed: false,
            middleware: hello.middleware,
        };
        // Registration is stored right away; positions follow with the periodic flush.
        kernel
            .store
            .save_listeners(std::slice::from_ref(&record))
            .map_err(internal)?;
        entries.insert(
            hello.name.clone(),
            Entry {
                record,
                connected: true,
                dirty: false,
                query_types: hello.query_types.clone(),
                out: Some(out.clone()),
                caught_up_at: head,
            },
        );
        position
    };
    let _disconnect = DisconnectOnDrop(Arc::clone(&kernel), hello.name.clone());
    tracing::info!(listener = %hello.name, order = hello.order, after = start, "listener connected");
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
    let deliver = tokio::spawn(deliver(
        Arc::clone(&kernel),
        hello.name.clone(),
        hello.order,
        types,
        Arc::clone(&sent),
        out.clone(),
    ));
    let _abort = crate::AbortOnDrop(deliver);

    loop {
        match next(inbound).await? {
            None => return Ok(()),
            Some(In::Ack(ack)) if ack.position <= sent.load(Ordering::SeqCst) => {
                kernel.listeners.ack(&hello.name, ack.position);
            }
            Some(In::Ack(_)) => {
                return Err(Status::invalid_argument("ack beyond the delivered events"));
            }
            Some(In::Answer(a)) => {
                let call_id = a.call_id.clone();
                kernel
                    .listeners
                    .reply(&hello.name, &call_id, Reply::Answer(a));
            }
            Some(In::Verdict(v)) => {
                let call_id = v.call_id.clone();
                kernel
                    .listeners
                    .reply(&hello.name, &call_id, Reply::Verdict(v));
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
    name: String,
    order: i32,
    types: Option<Vec<String>>,
    sent: Arc<AtomicU64>,
    out: Outbox,
) {
    loop {
        let progressed = kernel.listeners.progress.notified();
        let appended = kernel.appended.notified();
        let after = sent.load(Ordering::SeqCst);
        let gate = kernel.listeners.gate(order);
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
        sent.store(up_to, Ordering::SeqCst);
        if events.is_empty() && kernel.listeners.position(&name) == after {
            // Nothing to process and nothing outstanding: skip ahead without a round trip.
            kernel.listeners.ack(&name, up_to);
        } else if send(&out, Out::Batch(Batch { events, up_to }))
            .await
            .is_err()
        {
            return;
        }
    }
}
