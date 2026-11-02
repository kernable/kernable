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

    /// Delay before reconnecting after the session ends (default 1 s); twice as long after each
    /// further failed attempt, at most 30 s.
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
        let mut attempt = 0;
        while !*stopped.borrow() {
            let connects = self.inner.connects.load(Ordering::SeqCst);
            let session = self.session();
            let result = tokio::select! {
                result = session => result,
                _ = stopped.wait_for(|s| *s) => return,
            };
            attempt = if self.inner.connects.load(Ordering::SeqCst) > connects {
                1
            } else {
                attempt + 1
            };
            let base = self.inner.settings.lock().unwrap().1;
            let log = &self.inner.client.log;
            let delay = log::reconnecting(log, "worker", result.err().as_ref(), base, attempt);
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
        // Executes whose history is still coming, by lease.
        let mut partial: HashMap<String, proto::Execute> = HashMap::new();
        loop {
            let m = tokio::select! {
                m = inbound.message() => m?,
                Some(reason) = failed.recv() => return Err(reason.into()),
            };
            let Some(m) = m else {
                return Ok(());
            };
            let x = match m.msg {
                Some(Out::Challenge(c)) => {
                    let sig = client.sign(&signing::challenge_message(&c.nonce)).sig;
                    tx.send(msg(In::ChallengeResponse(ChallengeResponse { sig })))
                        .await?;
                    continue;
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
                    continue;
                }
                Some(Out::Execute(x)) => x,
                // Protocol 3: a long history comes in parts.
                Some(Out::ExecuteMore(rest)) => {
                    let Some(mut x) = partial.remove(&rest.lease_id) else {
                        continue;
                    };
                    x.history.extend(rest.history);
                    x.vault.extend(rest.vault);
                    x.more = rest.more;
                    x
                }
                None => continue,
            };
            if x.more {
                partial.insert(x.lease_id.clone(), x);
                continue;
            }
            let command = x.command.clone().unwrap_or_default();
            let aggregate = by_command
                .get(&format!("{}.{}", command.domain, command.command_type))
                .filter(|a| a.name() == command.aggregate_type)
                .cloned();
            let (client, tx, fail) = (client.clone(), tx.clone(), fail.clone());
            tokio::spawn(async move {
                let outcome = match aggregate {
                    Some(aggregate) => {
                        let execute = x.clone();
                        let running =
                            tokio::task::spawn_blocking(move || aggregate.execute(&execute));
                        // A handler cannot be stopped from outside; one that runs past
                        // its lease at least shows.
                        let lease =
                            u64::try_from(x.deadline - crate::context::now_ms()).unwrap_or(0);
                        let stream = format!(
                            "{}.{} {}/{}",
                            command.domain,
                            command.command_type,
                            command.aggregate_type,
                            command.aggregate_id
                        );
                        let line = || {
                            format!(
                                "{stream} still running after its lease ended: the kernel \
                                     gives it to another worker; is the handler stuck?"
                            )
                        };
                        let finished = log::watched(
                            &client.log,
                            "worker",
                            Duration::from_millis(lease),
                            line,
                            running,
                        );
                        match finished.await {
                            Ok(outcome) => (x, outcome),
                            // A panic outside the handlers' own guard (an eraser, say)
                            // still answers the command.
                            Err(e) => {
                                let text = match e.try_into_panic() {
                                    Ok(panic) => crate::domain::panic_text(panic.as_ref()),
                                    Err(e) => e.to_string(),
                                };
                                (x, Err(Failure::Error(text)))
                            }
                        }
                    }
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
                        In::Rejection(r) => Err(r.reason.as_ref().map_or("", |r| r.code.as_str())),
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
            let index_sig = index_sig(
                &cmd,
                x.expected_version,
                (&done.keys, &done.facts, &done.requires),
                |m| client.sign(m),
            );
            return Ok(In::Result(proto::Result {
                lease_id: x.lease_id,
                events: done.events,
                worker_sig: Some(worker_sig),
                snapshot,
                vault: done.vault,
                keys: done.keys,
                facts: done.facts,
                requires: done.requires,
                index_sig,
            }));
        }
        Err(Failure::Schema(reason)) => return Err(reason),
        Err(Failure::Rejected(r)) => r,
        Err(Failure::Error(message)) => {
            // An unexpected error's text may hold personal data or internals, and a rejection is
            // kept for good: it says only that the handler failed; the log has the rest.
            client.log.error("worker", || {
                format!(
                    "{}.{} {}/{} failed: {message}",
                    cmd.domain, cmd.command_type, cmd.aggregate_type, cmd.aggregate_id
                )
            });
            Rejected::new("handler failed").code("handler_error")
        }
    };
    let reason = reason.to_wire();
    let worker_sig = client.sign(&signing::rejection_message(&cmd.command_id, &reason));
    Ok(In::Rejection(Rejection {
        lease_id: x.lease_id,
        reason: Some(reason),
        worker_sig: Some(worker_sig),
    }))
}

/// The signature over the unique keys, facts and requirements of a result for `cmd` at
/// `expected_version`; none if it has none of them.
fn index_sig(
    cmd: &proto::Command,
    expected_version: u64,
    (keys, facts, requires): (&[proto::UniqueKey], &[String], &[proto::Requirement]),
    sign: impl FnOnce(&[u8]) -> proto::Signature,
) -> Option<proto::Signature> {
    if keys.is_empty() && facts.is_empty() && requires.is_empty() {
        return None;
    }
    Some(sign(&signing::index_message(
        cmd,
        expected_version,
        keys,
        facts,
        requires,
    )))
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

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};

    use super::*;
    use crate::{NewEvent, Requirement, Unique, domain, domain::Runnable};

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct Order {
        number: Option<String>,
        email: Option<String>,
        customer: String,
        open: bool,
        paid: bool,
    }

    /// The index of a result is signed as the shared test vector `result_index` says: value
    /// hashes, scopes and subjects of the keys in declaration order, the true facts, and the
    /// requirements from the command's data.
    #[test]
    fn the_index_of_a_result_is_signed_as_the_test_vectors_say() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../proto/test-vectors/signing.json"
        );
        let vectors: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let vector = &vectors["result_index"];
        let input = &vector["input"]["command"];
        fn text(v: &Value) -> String {
            v.as_str().unwrap().to_owned()
        }

        let orders = domain("orders");
        let order = orders.aggregate("Order", Order::default());
        order.apply("OrderPlaced", |s, d: Value, _evt, _ctx| {
            *s = Order {
                number: Some(text(&d["number"])),
                email: Some(text(&d["email"])),
                customer: text(&d["customerId"]),
                open: true,
                paid: true,
            }
        });
        order.command("PlaceOrder", |_, d: Value, _cmd, _ctx| {
            let placed = json!({ "number": "A-1001", "email": "max@firma.de", "customerId": d["customerId"] });
            Ok(vec![NewEvent::new("OrderPlaced", placed)])
        });
        order
            .unique("number", |s| s.number.clone(), Unique::tenant())
            .unique(
                "email",
                |s| s.email.clone(),
                Unique::global().personal(|s: &Order| Some(s.customer.clone())),
            )
            .fact("open", |s| s.open)
            .fact("closed", |s| !s.open)
            .fact("paid", |s| s.paid)
            .requires("PlaceOrder", |_, d: Value| {
                [
                    Requirement::new(
                        "identity",
                        "Customer",
                        d["customerId"].as_str().unwrap(),
                        "active",
                    ),
                    Requirement::by_key("funds", "Vehicle", "pmdxId", "VEH-lyra", "woundUp")
                        .absent(),
                ]
            });

        let cmd = proto::Command {
            command_id: text(&input["command_id"]),
            tenant_id: text(&input["tenant_id"]),
            domain: text(&input["domain"]),
            aggregate_type: text(&input["aggregate_type"]),
            aggregate_id: text(&input["aggregate_id"]),
            command_type: text(&input["command_type"]),
            payload: hex::decode(text(&input["payload_hex"])).unwrap(),
            ..Default::default()
        };
        let expected_version = vector["input"]["expected_version"].as_u64().unwrap();
        let execute = proto::Execute {
            command: Some(cmd.clone()),
            expected_version,
            ..Default::default()
        };
        let done = order
            .execute(&execute)
            .unwrap_or_else(|_| panic!("executes"));
        let key = signing::signing_key_from_hex(&text(&vectors["key"]["seed"])).unwrap();
        let mut signed = Vec::new();
        let sig = index_sig(
            &cmd,
            expected_version,
            (&done.keys, &done.facts, &done.requires),
            |m| {
                signed = m.to_vec();
                signing::sign(&key, m)
            },
        )
        .unwrap();
        assert_eq!(hex::encode(&signed), text(&vector["message"]));
        assert_eq!(hex::encode(&sig.sig), text(&vector["signature"]));

        // Without keys, facts or requirements, a result has no index signature.
        let none = index_sig(&cmd, expected_version, (&[], &[], &[]), |_| unreachable!());
        assert!(none.is_none());
    }
}
