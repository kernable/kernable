//! Hostile participants, as faults among the others: they hold valid keys or sit on the network,
//! and try what an attacker or a broken process would. None of them may change what the log,
//! the read models or the other participants hold:
//!
//! - `replay`: a captured command sent again, unchanged (the stored outcome comes back), with
//!   another payload under its old signature or re-signed under its old ID (still only the
//!   stored outcome), signed by a key nobody granted, or moved to another tenant without a new
//!   signature (refused, never stored). H1 checks every answer at the next round.
//! - `rogue-worker`: a worker with the apps' key registers `bank_x`'s accounts and answers its
//!   commands with a bad signature, with a signature over another version, for a lease it never
//!   got, or not at all. Its events say `by: rogue`; none may reach the log (H2), and every
//!   command it held runs on an honest worker.
//! - `liar-listener`: a listener that now and then acknowledges a position it never received.
//!   The kernel must not move it past what it got: it resumes no later than its last true
//!   acknowledgement (H3), and the delivery checks (O1–O4) see what it received.
//! - `net-corrupt` (in the proxy): bytes flipped on their way to the kernel; signatures and the
//!   protocol refuse them, and every invariant holds.
//!
//! H4: no event of a command ID the attacker made up (`evil-…`) and none in its tenant.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        self, Ack, ChallengeResponse, Command, CommandResult, Hello, ListenerMessage, NewEvent,
        Register, Signature, SubmitRequest, Wait, WorkerAggregate, WorkerMessage,
        command_result::Outcome, command_service_client::CommandServiceClient,
        kernel_message::Msg as FromKernel, listener_delivery::Msg as Out,
        listener_message::Msg as In, listener_service_client::ListenerServiceClient,
        worker_gateway_client::WorkerGatewayClient, worker_message::Msg as ToKernel,
    },
    signing, transport,
};
use kernable_store::invariants::Observed;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{Rng, load::Identity, say};

/// The tenant the attacker moves commands to; nothing may be stored there (H4).
pub const EVIL_TENANT: &str = "evil";

/// What a replayed command was answered: the events it committed (positions) or its rejection.
#[derive(Clone, Debug, PartialEq)]
pub enum Answer {
    Committed(Vec<u64>),
    Rejected(String),
}

pub fn answer_of(result: &CommandResult) -> Option<Answer> {
    match &result.outcome {
        Some(Outcome::Committed(c)) => Some(Answer::Committed(
            c.events.iter().map(|e| e.global_position).collect(),
        )),
        Some(Outcome::Rejected(r)) => Some(Answer::Rejected(r.code.clone())),
        _ => None,
    }
}

#[derive(Clone, Default)]
pub struct Hostile {
    /// What the attacker saw that must not be, found at once.
    pub found: Arc<Mutex<Vec<String>>>,
    /// Replays to compare with the stored outcome at the next round: what, the ID, the answer.
    pub replays: Arc<Mutex<Vec<(String, String, Answer)>>>,
    /// Counts per attack and how the kernel answered, for the report.
    pub stats: Arc<Mutex<BTreeMap<String, u64>>>,
    /// Recent commands of the load, to replay.
    pub recent: Arc<Mutex<VecDeque<Command>>>,
    /// The liar acknowledges a position it never got at its next batch.
    lie: Arc<AtomicBool>,
}

impl Hostile {
    fn count(&self, what: &str) {
        *self
            .stats
            .lock()
            .unwrap()
            .entry(what.to_owned())
            .or_default() += 1;
    }

    fn found(&self, what: String) {
        say(&format!("hostile: {what}"));
        self.found.lock().unwrap().push(what);
    }

    /// Remembers a command the load sent, for a later replay.
    pub fn sent(&self, cmd: &Command) {
        let mut recent = self.recent.lock().unwrap();
        if recent.len() >= 500 {
            recent.pop_front();
        }
        recent.push_back(cmd.clone());
    }

    /// One replay attack on a command the load sent.
    pub fn replay(&self, addr: String, me: Identity, seed: u64) {
        let picked = {
            let recent = self.recent.lock().unwrap();
            if recent.is_empty() {
                None
            } else {
                let mut rng = Rng::new(seed);
                recent.get(rng.below(recent.len() as u64) as usize).cloned()
            }
        };
        let Some(original) = picked else { return };
        let this = self.clone();
        tokio::spawn(async move {
            let mut rng = Rng::new(seed ^ 0x5eed);
            let mut cmd = original.clone();
            let variant = match rng.below(5) {
                0 => "unchanged",
                1 => {
                    // Another payload under the old signature.
                    cmd.payload = br#"{"amount":99999}"#.to_vec();
                    "payload-unsigned"
                }
                2 => {
                    // Another payload, signed again by the same key, under the old ID.
                    cmd.payload = br#"{"amount":99999}"#.to_vec();
                    cmd.client_sig = Some(signing::sign(&me.key, &signing::command_message(&cmd)));
                    "payload-resigned"
                }
                3 => {
                    // A key nobody granted, a new ID.
                    let stranger = SigningKey::from_bytes(&rand_bytes(&mut rng));
                    cmd.command_id = format!("evil-{}", rng.step());
                    cmd.client_sig =
                        Some(signing::sign(&stranger, &signing::command_message(&cmd)));
                    "stranger-key"
                }
                _ => {
                    // Another tenant under the old signature.
                    cmd.command_id = format!("evil-{}", rng.step());
                    cmd.tenant_id = EVIL_TENANT.into();
                    "tenant-unsigned"
                }
            };
            let first = addr.split(',').next().unwrap_or_default().to_owned();
            let Some(result) = submit_once(&first, &me, cmd.clone()).await else {
                this.count(&format!("replay {variant}: no answer"));
                return;
            };
            match result {
                Err(status) => {
                    this.count(&format!("replay {variant}: refused {:?}", status.code()));
                    if matches!(variant, "unchanged" | "payload-resigned") {
                        // A replay by the key that sent it is answered, not refused. A cut
                        // connection (the faults) is no answer, as in the load.
                        if !matches!(
                            status.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::Unknown
                                | tonic::Code::Cancelled
                        ) {
                            this.found(format!(
                                "H1 replay {variant} of {} was refused: {status}",
                                cmd.command_id
                            ));
                        }
                    }
                }
                Ok(result) => {
                    let answer = answer_of(&result);
                    this.count(&format!(
                        "replay {variant}: {}",
                        match &answer {
                            Some(Answer::Committed(_)) => "committed".to_owned(),
                            Some(Answer::Rejected(code)) => format!("rejected {code}"),
                            None => "no outcome".to_owned(),
                        }
                    ));
                    match variant {
                        // Refused or rejected; never committed.
                        "stranger-key" | "tenant-unsigned" => {
                            if let Some(Answer::Committed(_)) = answer {
                                this.found(format!(
                                    "H1 replay {variant}: {} committed",
                                    cmd.command_id
                                ));
                            }
                        }
                        // The stored outcome of the original, compared at the next round.
                        _ => {
                            if let Some(answer) = answer {
                                this.replays.lock().unwrap().push((
                                    variant.into(),
                                    cmd.command_id.clone(),
                                    answer,
                                ));
                            }
                        }
                    }
                }
            }
        });
    }

    /// A rogue worker for `bank_x`'s accounts, for `how_long`: it never answers a command
    /// correctly.
    pub fn rogue(&self, addr: String, me: Identity, seed: u64, how_long: Duration) {
        let this = self.clone();
        tokio::spawn(async move {
            let first = addr.split(',').next().unwrap_or_default().to_owned();
            match tokio::time::timeout(how_long, rogue_session(&first, &me, seed, &this)).await {
                Ok(Err(status)) => {
                    this.count(&format!("rogue ended by the kernel: {:?}", status.code()))
                }
                Ok(Ok(())) => this.count("rogue ended: stream closed"),
                Err(_) => this.count("rogue left"),
            }
        });
    }

    /// The liar lies at its next batch.
    pub fn lie(&self) {
        self.lie.store(true, Ordering::SeqCst);
    }

    /// The liar listener: like the audit listener, but now and then acknowledging a position
    /// it never received.
    pub fn liar(&self, addr: String, me: Identity, observed: Arc<Mutex<Observed>>) {
        let this = self.clone();
        tokio::spawn(async move {
            let addresses: Vec<String> = addr.split(',').map(str::to_owned).collect();
            let truth = Arc::new(AtomicU64::new(0));
            for turn in 0..usize::MAX {
                let _ = liar_once(
                    &addresses[turn % addresses.len()],
                    &me,
                    &observed,
                    &this,
                    &truth,
                )
                .await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }

    /// H1–H3 for the round: what was found at once, and every replay against the stored outcome
    /// of its command. `stored`: the stored results by command ID.
    pub fn take_replays(&self) -> Vec<(String, String, Answer)> {
        std::mem::take(&mut *self.replays.lock().unwrap())
    }

    pub fn take_found(&self) -> Vec<String> {
        std::mem::take(&mut *self.found.lock().unwrap())
    }
}

fn rand_bytes(rng: &mut Rng) -> [u8; 32] {
    let mut out = [0u8; 32];
    for chunk in out.chunks_mut(8) {
        chunk.copy_from_slice(&rng.step().to_le_bytes());
    }
    out
}

/// One submit, no retries: `None` if the kernel could not be reached.
async fn submit_once(
    addr: &str,
    me: &Identity,
    cmd: Command,
) -> Option<Result<CommandResult, tonic::Status>> {
    let channel = transport::connect_pinging(addr, Some(Duration::from_secs(2)))
        .await
        .ok()?;
    let request = SubmitRequest {
        command: Some(cmd),
        wait: Wait::Committed.into(),
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    let mut client = CommandServiceClient::new(channel);
    let call = client.submit(me.request(request));
    match tokio::time::timeout(Duration::from_secs(40), call).await {
        Ok(Ok(r)) => Some(Ok(r.into_inner())),
        Ok(Err(status)) => Some(Err(status)),
        Err(_) => None,
    }
}

const COMMANDS: [&str; 7] = [
    "Deposit", "Withdraw", "Open", "Close", "Number", "Flag", "Note",
];

async fn rogue_session(
    addr: &str,
    me: &Identity,
    seed: u64,
    this: &Hostile,
) -> Result<(), tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: ToKernel| WorkerMessage { msg: Some(m) };
    let key_id = signing::key_id(&me.key.verifying_key());
    let register = Register {
        worker_id: format!("rogue-{seed}"),
        key_id: key_id.clone(),
        aggregates: vec![WorkerAggregate {
            aggregate_type: "bank_x.account".into(),
            command_types: COMMANDS.iter().map(|c| format!("bank_x.{c}")).collect(),
        }],
        max_concurrency: 4,
        protocol: kernable_core::PROTOCOL,
        ..Default::default()
    };
    let _ = tx.send(send(ToKernel::Register(register))).await;
    let channel = transport::connect_pinging(addr, Some(Duration::from_secs(2)))
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut inbound = WorkerGatewayClient::new(channel)
        .session(me.request(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    let mut rng = Rng::new(seed);
    let rogue_events = vec![NewEvent {
        event_type: "Deposited".into(),
        payload: br#"{"amount":1,"by":"rogue"}"#.to_vec(),
        schema: 1,
        ..Default::default()
    }];
    while let Some(msg) = inbound.message().await?.and_then(|m| m.msg) {
        let reply = match msg {
            FromKernel::Challenge(c) => ToKernel::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&me.key, &signing::challenge_message(&c.nonce)).sig,
            }),
            FromKernel::Registered(_) => {
                this.count("rogue registered");
                continue;
            }
            FromKernel::Execute(x) => {
                let cmd = x.command.clone().unwrap_or_default();
                let (how, result) = match rng.below(4) {
                    0 => {
                        let mut sig = vec![0u8; 64];
                        for b in sig.iter_mut() {
                            *b = rng.step() as u8;
                        }
                        (
                            "bad signature",
                            proto::Result {
                                lease_id: x.lease_id.clone(),
                                events: rogue_events.clone(),
                                worker_sig: Some(Signature {
                                    key_id: key_id.clone(),
                                    sig,
                                }),
                                ..Default::default()
                            },
                        )
                    }
                    1 => (
                        "signed over another version",
                        proto::Result {
                            lease_id: x.lease_id.clone(),
                            events: rogue_events.clone(),
                            worker_sig: Some(signing::sign(
                                &me.key,
                                &signing::result_message(
                                    &cmd,
                                    x.expected_version + 1,
                                    &rogue_events,
                                ),
                            )),
                            ..Default::default()
                        },
                    ),
                    2 => (
                        "a lease it never got",
                        proto::Result {
                            lease_id: format!("{}-not-mine", x.lease_id),
                            events: rogue_events.clone(),
                            worker_sig: Some(signing::sign(
                                &me.key,
                                &signing::result_message(&cmd, x.expected_version, &rogue_events),
                            )),
                            ..Default::default()
                        },
                    ),
                    _ => {
                        this.count("rogue: no answer");
                        continue;
                    }
                };
                this.count(&format!("rogue: {how}"));
                ToKernel::Result(result)
            }
            _ => continue,
        };
        if tx.send(send(reply)).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn liar_once(
    addr: &str,
    me: &Identity,
    observed: &Mutex<Observed>,
    this: &Hostile,
    truth: &AtomicU64,
) -> Result<(), tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: "liar".into(),
        key_id: signing::key_id(&me.key.verifying_key()),
        order: 0,
        ..Default::default()
    };
    let _ = tx.send(send(In::Hello(hello))).await;
    let channel = transport::connect_pinging(addr, Some(Duration::from_secs(2)))
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut inbound = ListenerServiceClient::new(channel)
        .listen(me.request(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    while let Some(msg) = inbound.message().await?.and_then(|m| m.msg) {
        let reply = match msg {
            Out::Challenge(c) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&me.key, &signing::challenge_message(&c.nonce)).sig,
            }),
            Out::Ready(r) => {
                // H3: never resumed past what it truly acknowledged.
                let true_ack = truth.load(Ordering::SeqCst);
                if r.after_position > true_ack && true_ack > 0 {
                    this.found(format!(
                        "H3 liar resumed after {}, beyond its last true acknowledgement {true_ack}",
                        r.after_position
                    ));
                }
                observed
                    .lock()
                    .unwrap()
                    .connected("liar", &[], r.after_position);
                continue;
            }
            Out::Batch(b) => {
                let mut seen = observed.lock().unwrap();
                seen.delivered("liar", &b);
                if this.lie.swap(false, Ordering::SeqCst) {
                    this.count("liar: acknowledged 1000 past its batch");
                    In::Ack(Ack {
                        position: b.up_to + 1000,
                        ..Default::default()
                    })
                } else {
                    seen.acked("liar", b.up_to);
                    truth.store(b.up_to, Ordering::SeqCst);
                    In::Ack(Ack {
                        position: b.up_to,
                        ..Default::default()
                    })
                }
            }
            _ => continue,
        };
        if tx.send(send(reply)).await.is_err() {
            break;
        }
    }
    Ok(())
}
