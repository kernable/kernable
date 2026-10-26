//! Durable, named, ordered event listeners (read models and reactors).
//!
//! Event N reaches a listener of order k only after every registered listener with a lower
//! order has acknowledged N; equal orders run in parallel. A registered listener counts even
//! while it is disconnected, so a stalled listener holds back later orders and settling
//! submits. Positions are kept in memory and written to the store periodically: after a
//! crash a listener may see a few events again (delivery is at least once).

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use kernable_core::{
    proto::{
        Batch, Challenge, ListenerDelivery, ListenerMessage, ListenerRecord, Ready, Signature,
        listener_delivery::Msg as Out, listener_message::Msg as In,
    },
    signing,
};
use tokio::sync::{Notify, mpsc};
use tonic::{Status, Streaming};

use crate::{BoxError, Kernel, SUBSCRIBE_BATCH, config, internal};

/// How often acknowledged positions are written to the store.
pub(crate) const FLUSH: Duration = Duration::from_millis(100);

struct Entry {
    record: ListenerRecord,
    connected: bool,
    /// Changed since the last flush.
    dirty: bool,
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
                };
                (entry.record.name.clone(), entry)
            })
            .collect();
        Registry {
            entries: Mutex::new(entries),
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

    /// Whether every registered listener has acknowledged `position`.
    pub(crate) fn settled(&self, position: u64) -> bool {
        self.entries()
            .values()
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

type Outbox = mpsc::Sender<Result<ListenerDelivery, Status>>;

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
    if hello.name.is_empty() {
        return Err(Status::invalid_argument("listener name is empty"));
    }
    if hello.order < 0 {
        return Err(Status::invalid_argument(
            "negative orders are reserved for kernable",
        ));
    }
    let readable = kernel
        .config
        .listeners
        .get(&hello.key_id)
        .ok_or_else(|| Status::permission_denied("key may not listen"))?;
    let types: Option<HashSet<String>> = if hello.event_types.is_empty() {
        (!readable.iter().any(|t| t == "*")).then(|| readable.iter().cloned().collect())
    } else if hello
        .event_types
        .iter()
        .all(|t| config::allowed(&kernel.config.listeners, &hello.key_id, t))
    {
        Some(hello.event_types.iter().cloned().collect())
    } else {
        return Err(Status::permission_denied(
            "key may not read these event types",
        ));
    };

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
            Some(_) => return Err(Status::invalid_argument("unexpected message")),
        }
    }
}

/// Sends batches of stored events after `sent`, never beyond the listener's order gate.
async fn deliver(
    kernel: Arc<Kernel>,
    name: String,
    order: i32,
    types: Option<HashSet<String>>,
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
            if types.as_ref().is_none_or(|t| t.contains(&event.event_type)) {
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
