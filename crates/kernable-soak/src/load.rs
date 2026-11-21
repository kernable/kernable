//! The load: clients that keep submitting deposits, withdrawals (some sent `to` another account,
//! which must be open), opens, closes and new account numbers (a unique key; drawn from a small
//! pool, so they often collide) to the four apps, each command until it has an answer (a
//! resubmit with the same ID returns the stored outcome), and an audit listener on the raw
//! protocol. Both record what they were told in an [`Observed`]; the clients also record every
//! `unique_taken` and `requirement_missing` rejection as a [`Refusal`].

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, ChallengeResponse, Command, CommandResult, Hello, ListenerMessage, SubmitRequest,
        Wait, command_result::Outcome, command_service_client::CommandServiceClient,
        listener_delivery::Msg as Out, listener_message::Msg as In,
        listener_service_client::ListenerServiceClient,
    },
    signing, transport,
};
use kernable_store::invariants::Observed;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::Rng;

pub const DOMAINS: [&str; 4] = ["bank_ts", "bank_py", "bank_rs", "bank_go"];
pub const ACCOUNTS: u64 = 30;
/// Account numbers are drawn from `n0` to `n{NUMBERS - 1}`.
pub const NUMBERS: u64 = 40;
pub const TENANT: &str = "default";
/// After this long without an answer, a command is given up on (and counted).
const GIVE_UP: Duration = Duration::from_secs(300);

/// Who the soak's clients and audit listener are: the app key and its grant.
#[derive(Clone)]
pub struct Identity {
    pub key: Arc<SigningKey>,
    pub grant: String,
}

impl Identity {
    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if let Ok(value) = self.grant.parse() {
            request.metadata_mut().insert("kernable-grant", value);
        }
        request
    }
}

#[derive(Default)]
pub struct Stats {
    pub submitted: AtomicU64,
    pub committed: AtomicU64,
    pub rejected: Mutex<BTreeMap<String, u64>>,
    pub resubmits: AtomicU64,
    pub errors: Mutex<BTreeMap<String, u64>>,
    pub unanswered: Mutex<Vec<String>>,
    pub in_flight: AtomicU64,
}

/// A command rejected with `unique_taken` or `requirement_missing`, and when the kernel decided:
/// at a log position from `after` (the client's last committed event) to before `before` (the
/// first event of its next committed command; `None`: none yet).
#[derive(Clone)]
pub struct Refusal {
    pub code: String,
    pub command: Command,
    pub after: u64,
    pub before: Option<u64>,
}

/// The shared state of the load: whether new commands may start, and what was observed.
#[derive(Clone)]
pub struct Load {
    pub running: Arc<AtomicBool>,
    pub observed: Arc<Mutex<Observed>>,
    pub refusals: Arc<Mutex<Vec<Refusal>>>,
    pub stats: Arc<Stats>,
    pub run: String,
}

impl Load {
    pub fn new(run: &str) -> Load {
        Load {
            running: Arc::new(AtomicBool::new(true)),
            observed: Arc::default(),
            refusals: Arc::default(),
            stats: Arc::default(),
            run: run.into(),
        }
    }

    /// A client submitting one command after another while the load runs, at most one per
    /// `every` (zero: as fast as answers come).
    pub fn client(&self, n: u32, addr: String, me: Identity, seed: u64, every: Duration) {
        let load = self.clone();
        tokio::spawn(async move {
            let mut rng = Rng::new(seed);
            let mut channel = None;
            // The newest event of this client's commands, and its refusals still without
            // `before`.
            let (mut last, mut open) = (0, Vec::<usize>::new());
            for i in 0..u64::MAX {
                while !load.running.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let started = Instant::now();
                load.stats.in_flight.fetch_add(1, Ordering::SeqCst);
                let cmd = command(&load.run, n, i, &mut rng, &me.key);
                let result = load
                    .submit_until_answered(&addr, &me, cmd.clone(), &mut channel)
                    .await;
                match result.and_then(|r| r.outcome) {
                    Some(Outcome::Committed(c)) if !c.events.is_empty() => {
                        let positions = c.events.iter().map(|e| e.global_position);
                        let (first, newest) = (positions.clone().min(), positions.max());
                        let mut refusals = load.refusals.lock().unwrap();
                        for at in open.drain(..) {
                            refusals[at].before = first;
                        }
                        last = last.max(newest.unwrap_or(0));
                    }
                    Some(Outcome::Rejected(r))
                        if r.code == "unique_taken" || r.code == "requirement_missing" =>
                    {
                        let mut refusals = load.refusals.lock().unwrap();
                        open.push(refusals.len());
                        refusals.push(Refusal {
                            code: r.code,
                            command: cmd,
                            after: last,
                            before: None,
                        });
                    }
                    _ => {}
                }
                load.stats.in_flight.fetch_sub(1, Ordering::SeqCst);
                tokio::time::sleep(every.saturating_sub(started.elapsed())).await;
            }
        });
    }

    async fn submit_until_answered(
        &self,
        addr: &str,
        me: &Identity,
        cmd: Command,
        channel: &mut Option<tonic::transport::Channel>,
    ) -> Option<CommandResult> {
        let stats = &self.stats;
        stats.submitted.fetch_add(1, Ordering::SeqCst);
        let started = Instant::now();
        let mut pause = Duration::from_millis(100);
        // A pair's addresses, comma-separated: after a failure, the next one.
        let addresses: Vec<&str> = addr.split(',').collect();
        let mut turn = 0;
        for attempt in 0.. {
            if attempt > 0 {
                stats.resubmits.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_secs(2));
            }
            if started.elapsed() > GIVE_UP {
                stats
                    .unanswered
                    .lock()
                    .unwrap()
                    .push(cmd.command_id.clone());
                return None;
            }
            if channel.is_none() {
                turn += 1;
                // Pinged: a connection the network dropped ends instead of holding the call.
                *channel = transport::connect_pinging(
                    addresses[turn % addresses.len()],
                    Some(Duration::from_secs(2)),
                )
                .await
                .ok();
            }
            let Some(ch) = channel.clone() else { continue };
            let request = SubmitRequest {
                command: Some(cmd.clone()),
                wait: Wait::Committed.into(),
                protocol: kernable_core::PROTOCOL,
                ..Default::default()
            };
            let mut client = CommandServiceClient::new(ch);
            let call = client.submit(me.request(request));
            let result = match tokio::time::timeout(Duration::from_secs(40), call).await {
                Ok(Ok(result)) => result.into_inner(),
                Ok(Err(status)) => {
                    let code = format!("{:?}", status.code());
                    *stats.errors.lock().unwrap().entry(code).or_default() += 1;
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Cancelled
                    ) {
                        *channel = None;
                    }
                    continue;
                }
                Err(_) => {
                    *stats
                        .errors
                        .lock()
                        .unwrap()
                        .entry("ClientTimeout".into())
                        .or_default() += 1;
                    *channel = None;
                    continue;
                }
            };
            self.answered(&result);
            return Some(result);
        }
        None
    }

    fn answered(&self, result: &CommandResult) {
        self.observed.lock().unwrap().outcome(result);
        match &result.outcome {
            Some(Outcome::Committed(_)) => {
                self.stats.committed.fetch_add(1, Ordering::SeqCst);
            }
            Some(Outcome::Rejected(r)) => {
                *self
                    .stats
                    .rejected
                    .lock()
                    .unwrap()
                    .entry(r.code.clone())
                    .or_default() += 1;
            }
            _ => {
                *self
                    .stats
                    .rejected
                    .lock()
                    .unwrap()
                    .entry("(no outcome)".into())
                    .or_default() += 1;
            }
        }
    }

    /// The audit listener: acknowledges every batch and records connections, batches and
    /// acknowledgements, resuming at its stored position whenever the connection goes.
    pub fn audit(&self, addr: String, me: Identity) {
        let observed = Arc::clone(&self.observed);
        tokio::spawn(async move {
            // A pair's addresses in turn: the standby refuses, the primary takes it.
            let addresses: Vec<String> = addr.split(',').map(str::to_owned).collect();
            for turn in 0..usize::MAX {
                let _ = listen_once(&addresses[turn % addresses.len()], &me, &observed).await;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }
}

/// The `i`th command of client `n`: a deposit (45%) of 1 to 100, a withdrawal (25%) of 1 to 120,
/// half of them sent `to` another account, a new number (12%: one of `NUMBERS`, or none one time
/// in ten), an open (9%) or a close (9%).
fn command(run: &str, n: u32, i: u64, rng: &mut Rng, key: &SigningKey) -> Command {
    let domain = DOMAINS[rng.below(DOMAINS.len() as u64) as usize];
    let a = rng.below(ACCOUNTS);
    let account = format!("a{a}");
    let (command_type, payload) = match rng.below(100) {
        0..45 => ("Deposit", format!(r#"{{"amount":{}}}"#, 1 + rng.below(100))),
        45..70 => {
            let amount = 1 + rng.below(120);
            if rng.below(2) == 0 {
                ("Withdraw", format!(r#"{{"amount":{amount}}}"#))
            } else {
                let to = (a + 1 + rng.below(ACCOUNTS - 1)) % ACCOUNTS;
                ("Withdraw", format!(r#"{{"amount":{amount},"to":"a{to}"}}"#))
            }
        }
        70..82 => match rng.below(10) {
            0 => ("Number", r#"{"number":null}"#.to_owned()),
            _ => (
                "Number",
                format!(r#"{{"number":"n{}"}}"#, rng.below(NUMBERS)),
            ),
        },
        82..91 => ("Open", "{}".to_owned()),
        _ => ("Close", "{}".to_owned()),
    };
    let mut cmd = Command {
        command_id: format!("{run}-{n}-{i}"),
        tenant_id: TENANT.into(),
        domain: domain.into(),
        aggregate_type: "account".into(),
        aggregate_id: account,
        command_type: command_type.into(),
        payload: payload.into_bytes(),
        ..Default::default()
    };
    cmd.client_sig = Some(signing::sign(key, &signing::command_message(&cmd)));
    cmd
}

async fn listen_once(
    addr: &str,
    me: &Identity,
    observed: &Mutex<Observed>,
) -> Result<(), tonic::Status> {
    let (tx, rx) = mpsc::channel(16);
    let send = |m: In| ListenerMessage { msg: Some(m) };
    let hello = Hello {
        protocol: kernable_core::PROTOCOL,
        name: "audit".into(),
        key_id: signing::key_id(&me.key.verifying_key()),
        order: 0,
        ..Default::default()
    };
    let _ = tx.send(send(In::Hello(hello))).await;
    // Pinged, as the SDKs' listeners are: a connection the network drops without a word ends
    // within seconds and the audit listener connects again. Unpinged it hung on one for good,
    // and the flaggers, ordered after it, with it (a soak's B2 violations).
    let channel = transport::connect_pinging(addr, Some(std::time::Duration::from_secs(2)))
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
                observed
                    .lock()
                    .unwrap()
                    .connected("audit", &[], r.after_position);
                continue;
            }
            Out::Batch(b) => {
                let mut seen = observed.lock().unwrap();
                seen.delivered("audit", &b);
                seen.acked("audit", b.up_to);
                In::Ack(Ack {
                    position: b.up_to,
                    ..Default::default()
                })
            }
            _ => continue,
        };
        if tx.send(send(reply)).await.is_err() {
            break;
        }
    }
    Ok(())
}
