//! Workers: run the commands of aggregates.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use kernable_core::{
    PROTOCOL,
    proto::{
        self, ChallengeResponse, EventSchema, Register, Rejection, Snapshot, WorkerAggregate,
        WorkerMessage, kernel_message::Msg as Out, worker_gateway_client::WorkerGatewayClient,
        worker_message::Msg as In,
    },
    signing,
};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Aggregate, BoxError, Client, Rejected, State,
    domain::{Failure, Runnable},
    log,
};

struct WorkerInner {
    client: Client,
    /// The service it belongs to (`Options::service`); empty if none.
    service: String,
    /// Qualified command type -> the aggregate that runs it.
    by_command: Mutex<HashMap<String, Arc<dyn Runnable>>>,
    settings: Mutex<(u32, Duration)>,
    stopped: watch::Sender<bool>,
    ready: watch::Sender<bool>,
    /// Whether the first connect gets its own line; an app prints it in its overview instead.
    announce: AtomicBool,
    /// Sessions the kernel accepted so far.
    connects: AtomicU64,
}

/// Runs the commands of its aggregates: the kernel sends each command with the aggregate's
/// events, the worker decides and sends the new events back, signed. Cloning gives another
/// handle to the same worker. Made with [`crate::Kernable::worker`]; the configuring methods
/// return it, so they chain.
///
/// `run().await` processes commands until `stop()`. To do something once it is connected,
/// spawn it and wait: `let worker = kernable.worker(&cart_aggregate); tokio::spawn({ let worker
/// = worker.clone(); async move { worker.run().await } }); worker.ready().await;`.
#[derive(Clone)]
pub struct Worker {
    inner: Arc<WorkerInner>,
}

impl Worker {
    pub(crate) fn new(client: Client, service: String) -> Worker {
        Worker {
            inner: Arc::new(WorkerInner {
                client,
                service,
                by_command: Mutex::default(),
                settings: Mutex::new((16, Duration::from_secs(1))),
                stopped: watch::Sender::new(false),
                ready: watch::Sender::new(false),
                announce: AtomicBool::new(true),
                connects: AtomicU64::new(0),
            }),
        }
    }

    /// Runs the commands of `aggregate`, too.
    pub fn aggregate<S: State>(&self, aggregate: &Aggregate<S>) -> Worker {
        self.runnable(Arc::new(aggregate.clone()))
    }

    /// Runs the commands of `runnable`, too: an aggregate of any state type.
    pub(crate) fn runnable(&self, runnable: Arc<dyn Runnable>) -> Worker {
        let mut by_command = self.inner.by_command.lock().unwrap();
        for t in runnable.command_types() {
            by_command.insert(
                format!("{}.{t}", runnable.domain().name()),
                Arc::clone(&runnable),
            );
        }
        drop(by_command);
        self.clone()
    }

    /// Commands executed in parallel (default 16).
    pub fn concurrency(&self, commands: u32) -> Worker {
        self.inner.settings.lock().unwrap().0 = commands;
        self.clone()
    }

    /// Delay before reconnecting after the session ends (default 1 s).
    pub fn reconnect(&self, delay: Duration) -> Worker {
        self.inner.settings.lock().unwrap().1 = delay;
        self.clone()
    }

    /// Whether the first connect gets its own line (default); an app prints it in its overview.
    pub(crate) fn announce(&self, announce: bool) {
        self.inner.announce.store(announce, Ordering::SeqCst);
    }

    /// Returns once the kernel accepted the worker; waits forever if it never connects.
    pub async fn ready(&self) {
        let _ = self.inner.ready.subscribe().wait_for(|r| *r).await;
    }

    /// Ends `run`, also from another task.
    pub fn stop(&self) {
        self.inner.stopped.send_replace(true);
    }

    /// Processes commands until `stop()`, reconnecting when the session drops. A handler's
    /// error or panic rejects that command (`handler_error`); it does not end `run`.
    pub async fn run(&self) {
        let mut stopped = self.inner.stopped.subscribe();
        while !*stopped.borrow() {
            let session = self.session();
            let result = tokio::select! {
                result = session => result,
                _ = stopped.wait_for(|s| *s) => return,
            };
            let delay = self.inner.settings.lock().unwrap().1;
            let error = log::reason(result.err().as_ref());
            self.inner
                .client
                .log
                .warn("worker", || log::connection_lost(&error, delay));
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = stopped.wait_for(|s| *s) => return,
            }
        }
    }

    async fn session(&self) -> Result<(), BoxError> {
        let client = &self.inner.client;
        let (concurrency, _) = *self.inner.settings.lock().unwrap();
        let by_command = self.inner.by_command.lock().unwrap().clone();
        let (tx, rx) = mpsc::channel(1024);
        let msg = |m: In| WorkerMessage { msg: Some(m) };
        tx.send(msg(In::Register(Register {
            worker_id: format!("rust-{}", std::process::id()),
            key_id: client.key_id.clone(),
            aggregates: aggregates(&by_command),
            max_concurrency: concurrency,
            event_schemas: event_schemas(&by_command),
            protocol: PROTOCOL,
            service: self.inner.service.clone(),
        })))
        .await?;
        let mut gateway = WorkerGatewayClient::new(client.channel.clone());
        let mut inbound = gateway
            .session(client.request(ReceiverStream::new(rx)))
            .await?
            .into_inner();
        // A command this worker cannot read ends the session: it reconnects and is refused
        // until it is updated.
        let (fail, mut failed) = mpsc::channel::<String>(1);
        loop {
            let m = tokio::select! {
                m = inbound.message() => m?,
                Some(reason) = failed.recv() => return Err(reason.into()),
            };
            let Some(m) = m else {
                return Ok(());
            };
            match m.msg {
                Some(Out::Challenge(c)) => {
                    let sig = client.sign(&signing::challenge_message(&c.nonce)).sig;
                    tx.send(msg(In::ChallengeResponse(ChallengeResponse { sig })))
                        .await?;
                }
                Some(Out::Registered(_)) => {
                    if self.inner.connects.fetch_add(1, Ordering::SeqCst) > 0 {
                        client.log.info("worker", || "connected again".into());
                    } else if self.inner.announce.load(Ordering::SeqCst) {
                        let command_types: Vec<String> = by_command.keys().cloned().collect();
                        client.log.info("worker", || {
                            log::worker_connected(&client.log.peer, &command_types)
                        });
                    }
                    self.inner.ready.send_replace(true);
                }
                Some(Out::Execute(x)) => {
                    let command = x.command.clone().unwrap_or_default();
                    let aggregate = by_command
                        .get(&format!("{}.{}", command.domain, command.command_type))
                        .filter(|a| a.name() == command.aggregate_type)
                        .cloned();
                    let (client, tx, fail) = (client.clone(), tx.clone(), fail.clone());
                    tokio::spawn(async move {
                        let outcome = match aggregate {
                            Some(aggregate) => tokio::task::spawn_blocking(move || {
                                let outcome = aggregate.execute(&x);
                                (x, outcome)
                            })
                            .await
                            .expect("execute does not panic"),
                            None => {
                                let reason = Rejected::new(format!(
                                    "no handler for {} on {}.{}",
                                    command.command_type, command.domain, command.aggregate_type
                                ))
                                .code("unknown_command");
                                (x, Err(Failure::Rejected(reason)))
                            }
                        };
                        let reply = match reply(&client, outcome) {
                            Ok(reply) => reply,
                            Err(reason) => {
                                client.log.error("worker", || reason.clone());
                                let _ = fail.send(reason).await;
                                return;
                            }
                        };
                        client.log.debug("worker", || {
                            let outcome = match &reply {
                                In::Result(r) => Ok(r.events.len()),
                                In::Rejection(r) => {
                                    Err(r.reason.as_ref().map_or("", |r| r.code.as_str()))
                                }
                                _ => Err(""),
                            };
                            log::command_done(
                                &format!("{}.{}", command.domain, command.command_type),
                                &command.aggregate_type,
                                &command.aggregate_id,
                                outcome,
                            )
                        });
                        let _ = tx.send(msg(reply)).await;
                    });
                }
                None => {}
            }
        }
    }
}

/// The signed result or rejection for an executed command, or why the worker cannot read it.
fn reply(
    client: &Client,
    (x, outcome): (proto::Execute, Result<crate::domain::Executed, Failure>),
) -> Result<In, String> {
    let cmd = x.command.unwrap_or_default();
    let reason = match outcome {
        Ok(done) => {
            let snapshot = done.snapshot.map(|(version, schema, state)| {
                let worker_sig =
                    client.sign(&signing::snapshot_message(&cmd, version, &schema, &state));
                Snapshot {
                    version,
                    schema,
                    state,
                    worker_sig: Some(worker_sig),
                }
            });
            let worker_sig = client.sign(&signing::result_message(
                &cmd,
                x.expected_version,
                &done.events,
            ));
            return Ok(In::Result(proto::Result {
                lease_id: x.lease_id,
                events: done.events,
                worker_sig: Some(worker_sig),
                snapshot,
                vault: done.vault,
            }));
        }
        Err(Failure::Schema(reason)) => return Err(reason),
        Err(Failure::Rejected(r)) => r,
        Err(Failure::Error(message)) => Rejected::new(message).code("handler_error"),
    };
    let reason = reason.to_wire();
    let worker_sig = client.sign(&signing::rejection_message(&cmd.command_id, &reason));
    Ok(In::Rejection(Rejection {
        lease_id: x.lease_id,
        reason: Some(reason),
        worker_sig: Some(worker_sig),
    }))
}

/// An aggregate's qualified type: `shop.Cart`.
fn qualified(aggregate: &dyn Runnable) -> String {
    format!("{}.{}", aggregate.domain().name(), aggregate.name())
}

/// The worker's aggregates with their qualified command types, as it registers them.
fn aggregates(by_command: &HashMap<String, Arc<dyn Runnable>>) -> Vec<WorkerAggregate> {
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (command_type, aggregate) in by_command {
        grouped
            .entry(qualified(aggregate.as_ref()))
            .or_default()
            .push(command_type.clone());
    }
    grouped
        .into_iter()
        .map(|(aggregate_type, mut command_types)| {
            command_types.sort();
            WorkerAggregate {
                aggregate_type,
                command_types,
            }
        })
        .collect()
}

/// The payload versions the worker writes, for every event type its aggregates know: the
/// kernel refuses the worker if the store already holds a newer one.
fn event_schemas(by_command: &HashMap<String, Arc<dyn Runnable>>) -> Vec<EventSchema> {
    let mut schemas = HashMap::new();
    for aggregate in by_command.values() {
        let domain = aggregate.domain();
        for t in aggregate.event_types() {
            schemas.insert(format!("{}.{t}", domain.name()), domain.schema_of(&t));
        }
    }
    schemas
        .into_iter()
        .map(|(event_type, schema)| EventSchema { event_type, schema })
        .collect()
}
