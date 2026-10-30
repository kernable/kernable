//! A model-based test: a seeded random sequence of submits, resubmits, concurrent bursts,
//! grant changes, a revocation, memberships and data writes, legitimate and hostile, against
//! one kernel with two workers and two listeners of different orders. A small model predicts
//! every answer. Checked along the way and at the end:
//!
//! - X1: nothing runs without a valid signature by a key whose newest grant allows it and that
//!   is not revoked; a refused command is never stored;
//! - X7: a membership changes only by a newer apply;
//! - X8: the unsigned `listener` field never widens anything;
//! - F2: a replayed data write is refused;
//! - O5: a settled submit was processed by every listener it waited for;
//! - O6: an event reaches order 1 only after order 0 acknowledged it;
//! - O7: one execution per aggregate at a time;
//! - S1–S8 and O1–O4 over the whole run.
//!
//! `KERNABLE_MODEL_SEEDS=<n>` runs n seeds (default 3), `KERNABLE_MODEL_STEPS=<n>` steps each
//! (default 150).

mod common;

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use common::*;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, DataRequest, Grant, Hello, ListenerMessage,
        Membership, SubmitRequest, Wait, command_result::Outcome,
        command_service_client::CommandServiceClient, data_service_client::DataServiceClient,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_store::invariants::{Log, Observed};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status};

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(default)
}

/// xorshift: the same seed runs the same sequence of steps.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

// ---- listeners ----

/// What the two listeners saw, with a global sequence number per delivery and acknowledgement.
#[derive(Default)]
struct Seen {
    observed: Observed,
    /// Per listener: the highest position it acknowledged.
    acked: HashMap<String, u64>,
    /// Per listener: (position up to which it acknowledged, sequence number).
    acks: HashMap<String, Vec<(u64, u64)>>,
    /// Per listener: (event position, sequence number) of each delivery.
    deliveries: HashMap<String, Vec<(u64, u64)>>,
}

struct Listeners {
    seen: Mutex<Seen>,
    seq: AtomicU64,
}

impl Listeners {
    fn tick(&self) -> u64 {
        self.seq.fetch_add(1, SeqCst)
    }
}

/// Listener `name` of `order`, connected for the whole run; it acknowledges after a short pause.
fn listener(addr: &str, name: &'static str, order: i32, all: Arc<Listeners>) {
    let addr = addr.to_owned();
    tokio::spawn(async move {
        let (tx, rx) = mpsc::channel(16);
        let send = |m: In| ListenerMessage { msg: Some(m) };
        let hello = Hello {
            protocol: kernable_core::PROTOCOL,
            name: name.into(),
            key_id: id(&key(5)),
            order,
            from_position: Some(0),
            ..Default::default()
        };
        tx.send(send(In::Hello(hello))).await.unwrap();
        let channel = transport::connect(&addr).await.unwrap();
        let mut inbound = ListenerServiceClient::new(channel)
            .listen(with_grant(ReceiverStream::new(rx), &key(5)))
            .await
            .unwrap()
            .into_inner();
        while let Ok(Some(msg)) = inbound.message().await {
            let reply = match msg.msg.unwrap() {
                Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                    sig: signing::sign(&key(5), &signing::challenge_message(&c.nonce)).sig,
                }),
                Out::Ready(r) => {
                    let mut seen = all.seen.lock().unwrap();
                    seen.observed.connected(name, &[], r.after_position);
                    continue;
                }
                Out::Batch(b) => {
                    {
                        let mut seen = all.seen.lock().unwrap();
                        seen.observed.delivered(name, &b);
                        for e in &b.events {
                            let tick = all.tick();
                            seen.deliveries
                                .entry(name.into())
                                .or_default()
                                .push((e.global_position, tick));
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(1 + b.up_to % 3)).await;
                    // Recorded before it is sent: the kernel cannot know of it earlier.
                    let mut seen = all.seen.lock().unwrap();
                    seen.observed.acked(name, b.up_to);
                    let tick = all.tick();
                    seen.acks
                        .entry(name.into())
                        .or_default()
                        .push((b.up_to, tick));
                    let acked = seen.acked.entry(name.into()).or_default();
                    *acked = (*acked).max(b.up_to);
                    In::Ack(Ack { position: b.up_to })
                }
                _ => continue,
            };
            if tx.send(send(reply)).await.is_err() {
                break;
            }
        }
    });
}

// ---- requests ----

/// Who signs a command, and what the model expects of it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Signer {
    /// Key 1: may submit everything.
    Client,
    /// Key 8 presenting its newest grant: may submit while that grant allows it.
    Shifting,
    /// Key 8 presenting an older grant: decided by the newest grant all the same.
    ShiftingOld,
    /// Key 9: may submit until it is revoked.
    Doomed,
    /// Key 4: may only subscribe.
    Subscriber,
    /// Signed by key 10, claiming to be key 1.
    Forger,
}

const SIGNERS: [Signer; 6] = [
    Signer::Client,
    Signer::Shifting,
    Signer::ShiftingOld,
    Signer::Doomed,
    Signer::Subscriber,
    Signer::Forger,
];

/// What came of a command: its committed versions, or its rejection code.
#[derive(Clone, Debug, PartialEq)]
enum Expected {
    Committed(Vec<u64>),
    Rejected(String),
}

fn expected(result: &CommandResult) -> Expected {
    match &result.outcome {
        Some(Outcome::Committed(_)) => Expected::Committed(committed_versions(result)),
        Some(Outcome::Rejected(r)) => Expected::Rejected(r.code.clone()),
        other => panic!("no outcome: {other:?}"),
    }
}

async fn submit_signed(
    addr: &str,
    cmd: Command,
    wait: Wait,
    listener: &str,
    grant: Option<&Grant>,
) -> Result<CommandResult, Status> {
    let request = SubmitRequest {
        command: Some(cmd),
        wait: wait.into(),
        listener: listener.into(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    let request = match grant {
        Some(grant) => presenting(request, grant),
        None => tonic::Request::new(request),
    };
    let channel = transport::connect(addr).await.unwrap();
    CommandServiceClient::new(channel)
        .submit(request)
        .await
        .map(|r| r.into_inner())
}

fn data(op: &str, value: &[u8]) -> DataRequest {
    let mut r = DataRequest {
        tenant_id: "t1".into(),
        domain: "orders".into(),
        key: "terms".into(),
        data: value.to_vec(),
        content_type: if op == "put" {
            "text/plain".into()
        } else {
            String::new()
        },
        issued_at: issued_now(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    r.client_sig = Some(signing::sign(&key(7), &signing::data_message(op, &r)));
    r
}

// ---- the model ----

#[derive(Default)]
struct Model {
    /// Each order's version; 0: it does not exist.
    versions: HashMap<String, u64>,
    /// Commands the kernel ran, with their outcome; the client's may be sent again.
    outcomes: HashMap<String, (Command, Expected)>,
    /// Commands it refused: never stored.
    refused: Vec<String>,
    /// Key 8's grants in issue order, and whether the newest lets it submit.
    shifting: Vec<Grant>,
    shifting_submits: bool,
    doomed: Option<Grant>,
    revoked: bool,
    /// Every membership of `shop` applied, oldest first; the last one holds.
    memberships: Vec<Membership>,
    /// The file as the model has it, and every write sent, for replays.
    file: Option<Vec<u8>>,
    writes: Vec<(String, DataRequest)>,
    next: u64,
}

impl Model {
    fn may_submit(&self, signer: Signer) -> Result<(), Code> {
        match signer {
            Signer::Client => Ok(()),
            Signer::Shifting | Signer::ShiftingOld if self.shifting_submits => Ok(()),
            Signer::Doomed if !self.revoked => Ok(()),
            Signer::Forger => Err(Code::Unauthenticated),
            _ => Err(Code::PermissionDenied),
        }
    }
}

struct Run {
    env: Env,
    rng: Rng,
    model: Model,
    listeners: Arc<Listeners>,
    seed: u64,
}

impl Run {
    async fn submit(&mut self) {
        let signer = *self.rng.pick(&SIGNERS);
        let order = format!("o{}", self.rng.below(5));
        let command_type = *self.rng.pick(&["PlaceOrder", "AddItem"]);
        self.model.next += 1;
        let command_id = format!("m{}", self.model.next);
        let cmd = command(&command_id, command_type, &order);
        let (cmd, grant) = match signer {
            Signer::Client => (signed(cmd, &key(1)), default_grant(&key(1))),
            Signer::Shifting => (signed(cmd, &key(8)), self.model.shifting.last().cloned()),
            Signer::ShiftingOld => {
                let old = self.rng.below(self.model.shifting.len() as u64) as usize;
                (signed(cmd, &key(8)), Some(self.model.shifting[old].clone()))
            }
            Signer::Doomed => (signed(cmd, &key(9)), self.model.doomed.clone()),
            Signer::Subscriber => (signed(cmd, &key(4)), default_grant(&key(4))),
            Signer::Forger => {
                let mut cmd = signed(cmd, &key(10));
                cmd.client_sig.as_mut().unwrap().key_id = id(&key(1));
                (cmd, None)
            }
        };
        // X8: the unsigned listener field, mostly empty.
        let caller = match self.rng.below(20) {
            0 | 1 => "audit-1",
            2 => "ghost",
            3 => "bad name!",
            _ => "",
        };
        let wait = if self.rng.below(4) == 0 {
            Wait::Committed
        } else {
            Wait::Settled
        };
        let answer = submit_signed(&self.env.addr, cmd.clone(), wait, caller, grant.as_ref()).await;

        let seed = self.seed;
        let allowed = self.model.may_submit(signer);
        let expected_code = match allowed {
            Err(code) => Some(code),
            Ok(()) if matches!(caller, "ghost" | "bad name!") => Some(Code::InvalidArgument),
            Ok(()) => None,
        };
        if let Some(code) = expected_code {
            let status = answer.expect_err(&format!("seed {seed}: {command_id} by {signer:?} ran"));
            assert_eq!(
                status.code(),
                code,
                "seed {seed}: {command_id} by {signer:?}"
            );
            self.model.refused.push(command_id);
            return;
        }
        let result =
            answer.unwrap_or_else(|e| panic!("seed {seed}: {command_id} by {signer:?}: {e}"));
        self.listeners
            .seen
            .lock()
            .unwrap()
            .observed
            .outcome(&result);
        let version = self.model.versions.get(&order).copied().unwrap_or(0);
        let predicted = match (command_type, version > 0) {
            ("PlaceOrder", false) | ("AddItem", true) => Expected::Committed(vec![version + 1]),
            _ => Expected::Rejected("invalid_state".into()),
        };
        let got = expected(&result);
        assert_eq!(got, predicted, "seed {seed}: {command_id} on {order}");
        if let Expected::Committed(versions) = &got {
            self.model.versions.insert(order.clone(), versions[0]);
        }
        if wait == Wait::Settled {
            self.check_settled(&result, caller);
        }
        if signer == Signer::Client {
            self.model.outcomes.insert(command_id, (cmd, got));
        }
    }

    /// O5: a settled submit was acknowledged by every listener it waited for: all of them, or
    /// for a request from a listener's handler, those of a lower order.
    fn check_settled(&self, result: &CommandResult, caller: &str) {
        let Some(Outcome::Committed(c)) = &result.outcome else {
            return;
        };
        if !c.settled {
            return;
        }
        let last = c.events.last().map_or(0, |e| e.global_position);
        let seen = self.listeners.seen.lock().unwrap();
        let waited_for: &[&str] = match caller {
            "audit-1" => &["audit-0"],
            _ => &["audit-0", "audit-1"],
        };
        for name in waited_for {
            let acked = seen.acked.get(*name).copied().unwrap_or(0);
            assert!(
                acked >= last,
                "seed {}: O5: settled at {last}, but {name} acknowledged only {acked}",
                self.seed
            );
        }
    }

    /// Commands sent again: the stored outcome, never a second run.
    async fn resubmit(&mut self) {
        let mut ids: Vec<&String> = self.model.outcomes.keys().collect();
        if ids.is_empty() {
            return;
        }
        ids.sort();
        let id = (*self.rng.pick(&ids)).clone();
        let (cmd, outcome) = self.model.outcomes[&id].clone();
        let result = submit_signed(
            &self.env.addr,
            cmd,
            Wait::Committed,
            "",
            default_grant(&key(1)).as_ref(),
        )
        .await
        .unwrap();
        assert_eq!(expected(&result), outcome, "seed {}: {id} again", self.seed);
    }

    /// Many commands at once, on few orders: two workers compete for them (O7).
    async fn burst(&mut self) {
        let mut sent = Vec::new();
        for _ in 0..8 {
            self.model.next += 1;
            let id = format!("m{}", self.model.next);
            let order = format!("o{}", self.rng.below(3));
            let command_type = *self.rng.pick(&["PlaceOrder", "AddItem"]);
            let cmd = signed(command(&id, command_type, &order), &key(1));
            let addr = self.env.addr.clone();
            sent.push(tokio::spawn(async move {
                submit_with(&addr, cmd, Wait::Committed).await
            }));
        }
        for answer in sent {
            let result = answer.await.unwrap().unwrap();
            self.listeners
                .seen
                .lock()
                .unwrap()
                .observed
                .outcome(&result);
        }
        // The order they ran in decides the outcomes: take the orders' state from the store.
        let read = kernable_store::open_read_only(&self.env.store).unwrap();
        for i in 0..5 {
            let order = format!("o{i}");
            let events = read.read_stream("t1", "orders", "Order", &order).unwrap();
            self.model.versions.insert(order, events.len() as u64);
        }
    }

    /// Key 8 gets a newer grant, which lets it submit or not.
    fn regrant(&mut self) {
        let submits = self.rng.below(2) == 0;
        let permissions: &[(&str, &[&str])] = if submits {
            &[("submit", &["*"])]
        } else {
            &[("query", &["*"])]
        };
        let newer = grant(&key(8), permissions);
        self.env.kernel.grant(newer.clone()).unwrap();
        self.model.shifting.push(newer);
        self.model.shifting_submits = submits;
    }

    fn revoke(&mut self) {
        if !self.model.revoked {
            self.env.kernel.revoke(&id(&key(9)), issued_now()).unwrap();
            self.model.revoked = true;
        }
    }

    /// X7: a newer membership replaces the applied one; an older one, sent again, never does.
    async fn membership(&mut self) {
        let replay = !self.model.memberships.is_empty() && self.rng.below(2) == 0;
        let seed = self.seed;
        if replay {
            let older = self.rng.pick(&self.model.memberships).clone();
            let newest = self.model.memberships.last().unwrap();
            let answer = apply_issued(&self.env.addr, older.clone(), &key(1)).await;
            if older == *newest {
                answer.unwrap();
            } else {
                assert_eq!(
                    answer.unwrap_err().code(),
                    Code::FailedPrecondition,
                    "seed {seed}"
                );
            }
        } else {
            let n = self.model.memberships.len();
            let mut m = membership("shop", &[], &[&format!("m{n}")], &[]);
            m.issued_at = issued_now();
            apply_issued(&self.env.addr, m.clone(), &key(1))
                .await
                .unwrap();
            self.model.memberships.push(m);
        }
        let read = kernable_store::open_read_only(&self.env.store).unwrap();
        let stored = read
            .memberships()
            .unwrap()
            .into_iter()
            .filter_map(|r| r.membership)
            .find(|m| m.service == "shop");
        assert_eq!(
            stored.as_ref(),
            self.model.memberships.last(),
            "seed {seed}"
        );
    }

    /// F2: data writes, new or replayed, and a read that must match the model.
    async fn data(&mut self) {
        let seed = self.seed;
        let mut files = DataServiceClient::new(transport::connect(&self.env.addr).await.unwrap());
        match self.rng.below(3) {
            0 if !self.model.writes.is_empty() => {
                let (op, request) = self.rng.pick(&self.model.writes).clone();
                let answer = if op == "put" {
                    files.put_data(request).await
                } else {
                    files.delete_data(request).await
                };
                assert_eq!(
                    answer.unwrap_err().code(),
                    Code::AlreadyExists,
                    "seed {seed}"
                );
            }
            1 => {
                let request = data("delete", b"");
                files.delete_data(request.clone()).await.unwrap();
                self.model.file = None;
                self.model.writes.push(("delete".into(), request));
            }
            _ => {
                self.model.next += 1;
                let value = format!("v{}", self.model.next).into_bytes();
                let request = data("put", &value);
                files.put_data(request.clone()).await.unwrap();
                self.model.file = Some(value);
                self.model.writes.push(("put".into(), request));
            }
        }
        let read = files.get_data(data("get", b"")).await.unwrap().into_inner();
        let file = read.found.then_some(read.data);
        assert_eq!(file, self.model.file, "seed {seed}: the file");
    }
}

async fn run(seed: u64, steps: u64) {
    let env = start(&format!("model-{seed}")).await;
    let held = Arc::new(Held::default());
    for _ in 0..2 {
        start_worker(
            env.addr.clone(),
            TestWorker {
                behavior: Behavior::Slow(Duration::from_millis(2)),
                held: Some(Arc::clone(&held)),
                ..TestWorker::honest()
            },
        )
        .await;
    }
    let listeners = Arc::new(Listeners {
        seen: Mutex::default(),
        seq: AtomicU64::new(0),
    });
    listener(&env.addr, "audit-0", 0, Arc::clone(&listeners));
    listener(&env.addr, "audit-1", 1, Arc::clone(&listeners));
    let started = Instant::now();
    while kernable_store::open_read_only(&env.store)
        .unwrap()
        .listeners()
        .unwrap()
        .len()
        < 2
    {
        assert!(started.elapsed() < Duration::from_secs(5), "listeners");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let shop: &[&str] = &["orders.*"];
    env.kernel
        .grant(grant(&key(7), &[("data_read", shop), ("data_write", shop)]))
        .unwrap();
    let doomed = grant(&key(9), &[("submit", &["*"])]);
    env.kernel.grant(doomed.clone()).unwrap();
    let mut run = Run {
        env,
        rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).max(1)),
        model: Model {
            doomed: Some(doomed),
            ..Model::default()
        },
        listeners,
        seed,
    };
    run.regrant();

    for _ in 0..steps {
        match run.rng.below(100) {
            0..55 => run.submit().await,
            55..63 => run.burst().await,
            63..70 => run.resubmit().await,
            70..80 => run.regrant(),
            80..82 => run.revoke(),
            82..90 => run.membership().await,
            _ => run.data().await,
        }
    }

    // The listeners catch up.
    let head = kernable_store::open_read_only(&run.env.store)
        .unwrap()
        .head_position()
        .unwrap();
    let started = Instant::now();
    loop {
        let done = {
            let seen = run.listeners.seen.lock().unwrap();
            ["audit-0", "audit-1"]
                .iter()
                .all(|n| seen.acked.get(*n).copied().unwrap_or(0) >= head)
        };
        if done {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "seed {seed}: listeners behind"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let seen = run.listeners.seen.lock().unwrap();
    // O6: order 1 got each event only after order 0 acknowledged it.
    let acks = seen.acks.get("audit-0").cloned().unwrap_or_default();
    for (position, delivered) in seen.deliveries.get("audit-1").cloned().unwrap_or_default() {
        let acked = acks
            .iter()
            .find(|(up_to, _)| *up_to >= position)
            .map(|(_, tick)| *tick);
        assert!(
            acked.is_some_and(|tick| tick < delivered),
            "seed {seed}: O6: event {position} reached audit-1 before audit-0 acknowledged it"
        );
    }
    // O7.
    assert_eq!(
        held.overlaps.load(SeqCst),
        0,
        "seed {seed}: O7: two executions at once"
    );
    // X1: what was refused was never stored.
    let read = kernable_store::open_read_only(&run.env.store).unwrap();
    for id in &run.model.refused {
        assert!(
            read.command(id).unwrap().is_none() && read.result(id).unwrap().is_none(),
            "seed {seed}: X1: refused {id} is stored"
        );
    }
    // S1–S8, O1–O4.
    let log = Log::read(read.as_ref(), &[id(&key(3))]).unwrap();
    let mut violations = log.check();
    violations.extend(seen.observed.check(&log));
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn random_sequences_keep_every_invariant() {
    let seeds = env_number("KERNABLE_MODEL_SEEDS", 3);
    let steps = env_number("KERNABLE_MODEL_STEPS", 150);
    for seed in 1..=seeds {
        run(seed, steps).await;
    }
}
