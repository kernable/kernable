//! Baselines, a Rust worker and an open/closed-loop load generator for the PoC benchmark
//! (POC_PLAN.md §9). `bench/run.sh` runs the whole matrix.

use std::{
    process::ExitCode,
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ChallengeResponse, Command, NewEvent, Register, Result as WorkResult, WorkerMessage,
        command_result::Outcome, command_service_client::CommandServiceClient,
        kernel_message::Msg as Out, worker_gateway_client::WorkerGatewayClient,
        worker_message::Msg as In,
    },
    signing, transport,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

const USAGE: &str = "usage:
  kernable-bench crypto
  kernable-bench store <store-url> [count]
  kernable-bench worker <addr> <worker.key> [concurrency]
  kernable-bench load <addr> <client.key> (rate=<n> | concurrency=<n>) [seconds=10] [aggregates=10000] [payload=1024]
  kernable-bench history <addr> <client.key> <events>";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match args.as_slice() {
        ["crypto"] => {
            crypto();
            Ok(())
        }
        ["store", url, rest @ ..] => store(
            url,
            rest.first()
                .map_or(Ok(5_000), |n| n.parse())
                .unwrap_or(5_000),
        ),
        ["worker", addr, key, rest @ ..] => {
            worker(
                addr,
                key,
                rest.first().and_then(|n| n.parse().ok()).unwrap_or(64),
            )
            .await
        }
        ["load", addr, key, opts @ ..] => load(addr, key, Opts::parse(opts)).await,
        ["history", addr, key, n] => history(addr, key, n.parse().unwrap_or(100)).await,
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

// ---- reporting ----

#[derive(Default)]
struct Tally {
    latencies_us: Vec<u64>,
    rejected: usize,
    errors: usize,
}

impl Tally {
    fn record(&mut self, latency: Duration, outcome: &Result<Outcome, tonic::Status>) {
        self.latencies_us.push(latency.as_micros() as u64);
        match outcome {
            Ok(Outcome::Committed(_)) => {}
            Ok(Outcome::Rejected(_)) => self.rejected += 1,
            Err(_) => self.errors += 1,
        }
    }

    fn report(mut self, name: &str, elapsed: Duration) {
        let n = self.latencies_us.len();
        if n == 0 {
            println!("{name}: no samples");
            return;
        }
        self.latencies_us.sort_unstable();
        let p = |q: f64| self.latencies_us[((n as f64 * q) as usize).min(n - 1)] as f64 / 1000.0;
        println!(
            "{name}: n={n} throughput={:.0}/s p50={:.2}ms p95={:.2}ms p99={:.2}ms p99.9={:.2}ms max={:.2}ms rejected={} errors={}",
            n as f64 / elapsed.as_secs_f64(),
            p(0.5),
            p(0.95),
            p(0.99),
            p(0.999),
            p(1.0),
            self.rejected,
            self.errors,
        );
    }
}

// ---- baselines ----

/// Cost of the signatures one command needs: client and worker sign, kernel verifies both.
fn crypto() {
    const N: u32 = 10_000;
    let key = SigningKey::from_bytes(&[1; 32]);
    let msg = vec![b'x'; 1024];
    let started = Instant::now();
    let mut sig = signing::sign(&key, &msg);
    for _ in 1..N {
        sig = signing::sign(&key, &msg);
    }
    let sign = started.elapsed() / N;
    let started = Instant::now();
    for _ in 0..N {
        signing::verify(&sig, &msg).unwrap();
    }
    let verify = started.elapsed() / N;
    println!(
        "crypto: sign={:.1}us verify={:.1}us per-command(2 sign + 2 verify)={:.1}us",
        sign.as_secs_f64() * 1e6,
        verify.as_secs_f64() * 1e6,
        (2 * sign + 2 * verify).as_secs_f64() * 1e6
    );
}

/// Lower bound: the whole command path in-process, without network or a worker process.
fn store(url: &str, count: usize) -> Result<(), BoxError> {
    let store = kernable_store::open(url)?;
    let (client, worker) = (
        SigningKey::from_bytes(&[1; 32]),
        SigningKey::from_bytes(&[2; 32]),
    );
    let payload = pad(1024);
    let run = run_id();
    let mut tally = Tally::default();
    let started = Instant::now();
    for i in 0..count {
        let t = Instant::now();
        let cmd = signed(&client, command(&run, i, 10_000, &payload));
        signing::verify(
            cmd.client_sig.as_ref().unwrap(),
            &signing::command_message(&cmd),
        )?;
        store.enqueue(&cmd, 0)?;
        let lease = store
            .lease(&["Write"], 0, 10_000)?
            .ok_or("nothing to lease")?;
        let history = store.read_stream(&cmd.tenant_id, &cmd.aggregate_type, &cmd.aggregate_id)?;
        let version = history.last().map_or(0, |e| e.version);
        let events = [NewEvent {
            event_type: "Written".into(),
            payload: lease.command.payload.clone(),
        }];
        let sig = signing::sign(
            &worker,
            &signing::result_message(&lease.command, version, &events),
        );
        signing::verify(
            &sig,
            &signing::result_message(&lease.command, version, &events),
        )?;
        let stored = store.commit(&lease.lease_id, version, &events, &sig, 0)?;
        let outcome = Outcome::Committed(kernable_core::proto::Committed { events: stored });
        tally.record(t.elapsed(), &Ok(outcome));
    }
    let backend = kernable_store::backend(url).0;
    tally.report(
        &format!("store {backend} (in-process, signed)"),
        started.elapsed(),
    );
    Ok(())
}

// ---- worker ----

/// A Rust worker for the `Bench` aggregate: echoes the command payload as one event.
async fn worker(addr: &str, key: &str, concurrency: u32) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let (tx, rx) = mpsc::channel(1024);
    let msg = |m: In| WorkerMessage { msg: Some(m) };
    tx.send(msg(In::Register(Register {
        worker_id: format!("rust-bench-{}", std::process::id()),
        key_id: signing::key_id(&key.verifying_key()),
        command_types: vec!["Write".into()],
        max_concurrency: concurrency,
    })))
    .await?;
    let mut client = WorkerGatewayClient::new(transport::connect(addr).await?);
    let mut inbound = client.session(ReceiverStream::new(rx)).await?.into_inner();
    while let Some(m) = inbound.message().await? {
        let reply = match m.msg {
            Some(Out::Challenge(c)) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
            }),
            Some(Out::Execute(x)) => {
                let cmd = x.command.unwrap_or_default();
                let events = vec![NewEvent {
                    event_type: "Written".into(),
                    payload: cmd.payload.clone(),
                }];
                let sig = signing::sign(
                    &key,
                    &signing::result_message(&cmd, x.expected_version, &events),
                );
                In::Result(WorkResult {
                    lease_id: x.lease_id,
                    events,
                    worker_sig: Some(sig),
                })
            }
            _ => continue,
        };
        tx.send(msg(reply)).await?;
    }
    Ok(())
}

// ---- load ----

struct Opts {
    rate: Option<f64>,
    concurrency: Option<usize>,
    seconds: f64,
    aggregates: usize,
    payload: usize,
}

impl Opts {
    fn parse(args: &[&str]) -> Self {
        let get = |name: &str| {
            args.iter()
                .find_map(|a| a.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
        };
        Opts {
            rate: get("rate").and_then(|v| v.parse().ok()),
            concurrency: get("concurrency").and_then(|v| v.parse().ok()),
            seconds: get("seconds").and_then(|v| v.parse().ok()).unwrap_or(10.0),
            aggregates: get("aggregates")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            payload: get("payload").and_then(|v| v.parse().ok()).unwrap_or(1024),
        }
    }
}

async fn channels(addr: &str, n: usize) -> Result<Vec<Channel>, BoxError> {
    let mut all = Vec::with_capacity(n);
    for _ in 0..n {
        all.push(transport::connect(addr).await?);
    }
    Ok(all)
}

async fn submit(channel: Channel, cmd: Command) -> Result<Outcome, tonic::Status> {
    let result = CommandServiceClient::new(channel).submit(cmd).await?;
    Ok(result
        .into_inner()
        .outcome
        .unwrap_or(Outcome::Rejected(Default::default())))
}

async fn load(addr: &str, key: &str, opts: Opts) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let pool = channels(addr, 4).await?;
    let payload = pad(opts.payload);
    let run = run_id();
    let duration = Duration::from_secs_f64(opts.seconds);
    let mut tally = Tally::default();
    let started = Instant::now();

    if let Some(rate) = opts.rate {
        // Open loop: commands are due at fixed times; latency counts from that time,
        // so a slow kernel cannot hide its queueing (no coordinated omission).
        let total = (rate * opts.seconds) as usize;
        let interval = Duration::from_secs_f64(1.0 / rate);
        let (tx, mut rx) = mpsc::unbounded_channel();
        for i in 0..total {
            let due = started + interval * i as u32;
            tokio::time::sleep_until(due.into()).await;
            let cmd = signed(&key, command(&run, i, opts.aggregates, &payload));
            let (channel, tx) = (pool[i % pool.len()].clone(), tx.clone());
            tokio::spawn(async move {
                let outcome = submit(channel, cmd).await;
                let _ = tx.send((due.elapsed(), outcome));
            });
        }
        drop(tx);
        while let Some((latency, outcome)) = rx.recv().await {
            tally.record(latency, &outcome);
        }
        tally.report(&format!("load rate={rate}/s"), started.elapsed());
    } else {
        // Closed loop: `concurrency` clients submit back to back; measures max throughput.
        let concurrency = opts.concurrency.unwrap_or(64);
        let mut tasks = Vec::new();
        for c in 0..concurrency {
            let (key, run, payload, channel) = (
                key.clone(),
                run.clone(),
                payload.clone(),
                pool[c % pool.len()].clone(),
            );
            let aggregates = opts.aggregates;
            tasks.push(tokio::spawn(async move {
                let mut local = Tally::default();
                let mut i = c;
                while started.elapsed() < duration {
                    let cmd = signed(&key, command(&run, i, aggregates, &payload));
                    let t = Instant::now();
                    let outcome = submit(channel.clone(), cmd).await;
                    local.record(t.elapsed(), &outcome);
                    i += concurrency;
                }
                local
            }));
        }
        for t in tasks {
            let local = t.await?;
            tally.latencies_us.extend(local.latencies_us);
            tally.rejected += local.rejected;
            tally.errors += local.errors;
        }
        tally.report(
            &format!("load concurrency={concurrency}"),
            started.elapsed(),
        );
    }
    Ok(())
}

/// Latency of commands on an aggregate that already has `events` events in its history.
async fn history(addr: &str, key: &str, events: usize) -> Result<(), BoxError> {
    const SAMPLES: usize = 200;
    let key = signing::read_signing_key(key)?;
    let channel = transport::connect(addr).await?;
    let payload = pad(1024);
    let run = run_id();
    let one = |i: usize| {
        let mut cmd = command(&run, i, 1, &payload);
        cmd.aggregate_id = format!("history-{run}");
        signed(&key, cmd)
    };
    for i in 0..events {
        submit(channel.clone(), one(i)).await?;
    }
    let mut tally = Tally::default();
    let started = Instant::now();
    for i in events..events + SAMPLES {
        let t = Instant::now();
        let outcome = submit(channel.clone(), one(i)).await;
        tally.record(t.elapsed(), &outcome);
    }
    tally.report(&format!("history events={events}"), started.elapsed());
    Ok(())
}

// ---- helpers ----

fn command(run: &str, i: usize, aggregates: usize, payload: &[u8]) -> Command {
    Command {
        command_id: format!("{run}-{i}"),
        tenant_id: "bench".into(),
        aggregate_type: "Bench".into(),
        aggregate_id: format!("a{}", i % aggregates.max(1)),
        command_type: "Write".into(),
        payload: payload.to_vec(),
        client_sig: None,
    }
}

fn signed(key: &SigningKey, mut cmd: Command) -> Command {
    cmd.client_sig = Some(signing::sign(key, &signing::command_message(&cmd)));
    cmd
}

/// A JSON payload of exactly `size` bytes (at least 10).
fn pad(size: usize) -> Vec<u8> {
    format!(r#"{{"pad":"{}"}}"#, "x".repeat(size.saturating_sub(10))).into_bytes()
}

fn run_id() -> String {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("OS randomness unavailable");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
