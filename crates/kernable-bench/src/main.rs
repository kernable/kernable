//! Baselines, a Rust worker, a no-op listener and an open/closed-loop load generator.
//! `bench/run.sh` runs the whole matrix.

use std::{
    process::ExitCode,
    sync::OnceLock,
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        Ack, AggregateIndex, ChallengeResponse, Command, Hello, ListenerMessage, NewEvent,
        QueryAnswer, QueryRequest, Register, Result as WorkResult, StoredKey, SubmitRequest,
        SubscribeRequest, UniqueKey, Wait, WorkerAggregate, WorkerMessage, command_result::Outcome,
        command_service_client::CommandServiceClient, event_service_client::EventServiceClient,
        kernel_message::Msg as Out, listener_delivery::Msg as Delivery,
        listener_message::Msg as Listen, listener_service_client::ListenerServiceClient,
        query_answer, query_result::Outcome as Answer, query_service_client::QueryServiceClient,
        worker_gateway_client::WorkerGatewayClient, worker_message::Msg as In,
    },
    signing, transport,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

/// The grant next to the key file (`<name>.grant`), sent with every call.
static GRANT: OnceLock<Option<String>> = OnceLock::new();

/// `message` as a request that carries the key's grant, if there is one.
fn granted<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(grant) = GRANT.get().and_then(Option::as_ref) {
        let value = grant.parse().expect("a grant is hex");
        request.metadata_mut().insert("kernable-grant", value);
    }
    request
}

const USAGE: &str = "usage:
  kernable-bench crypto
  kernable-bench store <store-url> [count]
  kernable-bench fill <store-url> <client.key> <worker.key> <from> <to> [payload]   writes commands from..to of the filled store
  kernable-bench worker <addr> <worker.key> [concurrency] [keys]   keys: every aggregate holds a unique key and a fact
  kernable-bench listener <addr> <listener.key> <name> <order>   acknowledges every event at once
  kernable-bench reader <addr> <listener.key> <name>   a live read model that answers bench.Get at once
  kernable-bench load <addr> <client.key> (rate=<n> | concurrency=<n>) [seconds=10] [aggregates=10000] [payload=1024] [wait=settled|committed]
      [target=spread|new|existing|repeat|query] [filled=<n>]
  kernable-bench subscribe <addr> <client.key> <after> <count>
  kernable-bench history <addr> <client.key> <events>";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    if let [_, _, key, ..] = args.as_slice() {
        let grant = format!("{}.grant", key.strip_suffix(".key").unwrap_or(key));
        let _ = GRANT.set(
            std::fs::read_to_string(grant)
                .ok()
                .map(|g| g.trim().to_owned()),
        );
    }
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
        ["fill", url, client, worker, from, to, rest @ ..] => fill(
            url,
            client,
            worker,
            from.parse().unwrap_or(0),
            to.parse().unwrap_or(0),
            rest.first()
                .and_then(|n| n.parse().ok())
                .unwrap_or(FILL_PAYLOAD),
        ),
        ["worker", addr, key, rest @ ..] => {
            worker(
                addr,
                key,
                rest.first().and_then(|n| n.parse().ok()).unwrap_or(64),
                rest.get(1) == Some(&"keys"),
            )
            .await
        }
        ["reader", addr, key, name] => reader(addr, key, name).await,
        ["subscribe", addr, key, after, count] => {
            subscribe(
                addr,
                key,
                after.parse().unwrap_or(0),
                count.parse().unwrap_or(10_000),
            )
            .await
        }
        ["listener", addr, key, name, order] => {
            listener(addr, key, name, order.parse().unwrap_or(0)).await
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
            Ok(Outcome::Committed(_) | Outcome::Accepted(_)) => {}
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
        let history = store.read_stream(
            &cmd.tenant_id,
            &cmd.domain,
            &cmd.aggregate_type,
            &cmd.aggregate_id,
        )?;
        let version = history.last().map_or(0, |e| e.version);
        let events = [NewEvent {
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: "Written".into(),
            payload: cmd.payload.clone(),
            schema: 1,
            origin_partition: String::new(),
            origin_position: 0,
        }];
        let sig = signing::sign(&worker, &signing::result_message(&cmd, version, &events));
        signing::verify(&sig, &signing::result_message(&cmd, version, &events))?;
        let stored = store.commit(&cmd.command_id, version, &events, &sig, None, 0)?;
        let outcome = Outcome::Committed(kernable_core::proto::Committed {
            events: stored,
            settled: false,
        });
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

/// A Rust worker for the `Bench` aggregate: echoes the command payload as one event. With
/// `keys`, every aggregate holds the unique key `Name` (its ID) and the fact `exists`, as the
/// filled store's aggregates do, so each command goes through the index.
async fn worker(addr: &str, key: &str, concurrency: u32, keys: bool) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let (tx, rx) = mpsc::channel(1024);
    let msg = |m: In| WorkerMessage { msg: Some(m) };
    tx.send(msg(In::Register(Register {
        protocol: kernable_core::PROTOCOL,
        worker_id: format!("rust-bench-{}", std::process::id()),
        key_id: signing::key_id(&key.verifying_key()),
        aggregates: vec![WorkerAggregate {
            aggregate_type: "bench.Bench".into(),
            command_types: vec!["bench.Write".into()],
        }],
        max_concurrency: concurrency,
        ..Default::default()
    })))
    .await?;
    let mut client = WorkerGatewayClient::new(transport::connect(addr).await?);
    let mut inbound = client
        .session(granted(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    while let Some(m) = inbound.message().await? {
        let reply = match m.msg {
            Some(Out::Challenge(c)) => In::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
            }),
            Some(Out::Execute(x)) => {
                let cmd = x.command.unwrap_or_default();
                let events = vec![NewEvent {
                    erases: String::new(),
                    vault_refs: Vec::new(),
                    event_type: "Written".into(),
                    payload: cmd.payload.clone(),
                    schema: 1,
                    origin_partition: String::new(),
                    origin_position: 0,
                }];
                let sig = signing::sign(
                    &key,
                    &signing::result_message(&cmd, x.expected_version, &events),
                );
                let (held, facts) = if keys {
                    (vec![name_key(&cmd.aggregate_id)], vec!["exists".to_owned()])
                } else {
                    (Vec::new(), Vec::new())
                };
                let index_sig = keys.then(|| {
                    signing::sign(
                        &key,
                        &signing::index_message(&cmd, x.expected_version, &held, &facts, &[]),
                    )
                });
                In::Result(WorkResult {
                    vault: Vec::new(),
                    lease_id: x.lease_id,
                    events,
                    worker_sig: Some(sig),
                    snapshot: None,
                    keys: held,
                    facts,
                    requires: Vec::new(),
                    index_sig,
                })
            }
            _ => continue,
        };
        tx.send(msg(reply)).await?;
    }
    Ok(())
}

/// A listener that does no work: it acknowledges each batch as soon as it arrives, so it
/// measures what ordering and settling cost by themselves.
async fn listener(addr: &str, key: &str, name: &str, order: i32) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let (tx, rx) = mpsc::channel(1024);
    let msg = |m: Listen| ListenerMessage { msg: Some(m) };
    tx.send(msg(Listen::Hello(Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: signing::key_id(&key.verifying_key()),
        order,
        event_types: Vec::new(),
        from_position: None,
        ..Default::default()
    })))
    .await?;
    let mut client = ListenerServiceClient::new(transport::connect(addr).await?);
    let mut inbound = client
        .listen(granted(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    while let Some(m) = inbound.message().await? {
        let reply = match m.msg {
            Some(Delivery::Challenge(c)) => Listen::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
            }),
            Some(Delivery::Batch(b)) => Listen::Ack(Ack {
                position: b.up_to,
                ..Default::default()
            }),
            _ => continue,
        };
        tx.send(msg(reply)).await?;
    }
    Ok(())
}

/// A live read model: acknowledges every batch at once and answers each `bench.Get` with its
/// payload, so a query measures the kernel's routing alone.
async fn reader(addr: &str, key: &str, name: &str) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let (tx, rx) = mpsc::channel(1024);
    let msg = |m: Listen| ListenerMessage { msg: Some(m) };
    tx.send(msg(Listen::Hello(Hello {
        protocol: kernable_core::PROTOCOL,
        name: name.into(),
        key_id: signing::key_id(&key.verifying_key()),
        query_types: vec!["bench.Get".into()],
        live: true,
        ..Default::default()
    })))
    .await?;
    let mut client = ListenerServiceClient::new(transport::connect(addr).await?);
    let mut inbound = client
        .listen(granted(ReceiverStream::new(rx)))
        .await?
        .into_inner();
    while let Some(m) = inbound.message().await? {
        let reply = match m.msg {
            Some(Delivery::Challenge(c)) => Listen::ChallengeResponse(ChallengeResponse {
                sig: signing::sign(&key, &signing::challenge_message(&c.nonce)).sig,
            }),
            Some(Delivery::Batch(b)) => Listen::Ack(Ack {
                position: b.up_to,
                ..Default::default()
            }),
            Some(Delivery::Query(q)) => Listen::Answer(QueryAnswer {
                call_id: q.call_id,
                outcome: Some(query_answer::Outcome::Response(
                    q.query.map(|q| q.payload).unwrap_or_default(),
                )),
            }),
            _ => continue,
        };
        tx.send(msg(reply)).await?;
    }
    Ok(())
}

// ---- filled stores ----

/// Events per aggregate in a filled store: command `i` writes version `i % 10` of `f{i / 10}`,
/// so the histories stay this long however large the store grows.
const PER_AGGREGATE: u64 = 10;
const FILL_PAYLOAD: usize = 256;

/// Command `i` of a filled store, signed: the same bytes every time, so `target=repeat`
/// submits exactly what `fill` stored.
fn fill_command(client: &SigningKey, i: u64, payload: &[u8]) -> Command {
    signed(
        client,
        Command {
            vault_refs: Vec::new(),
            command_id: format!("fill-{i}"),
            tenant_id: "bench".into(),
            domain: "bench".into(),
            aggregate_type: "Bench".into(),
            aggregate_id: format!("f{}", i / PER_AGGREGATE),
            command_type: "Write".into(),
            payload: payload.to_vec(),
            client_sig: None,
            context: None,
        },
    )
}

/// The unique key every bench aggregate holds with `worker ... keys`: `Name`, its ID.
fn name_key(aggregate_id: &str) -> UniqueKey {
    UniqueKey {
        name: "Name".into(),
        global: false,
        value_hash: Sha256::digest(aggregate_id.as_bytes()).to_vec(),
        subject: String::new(),
    }
}

/// Writes commands `from..to` of a filled store straight into it, as the kernel would have
/// committed them (signed by the client and the worker, with the aggregate's key and fact),
/// without the kernel. Signing runs on every core; writes hand the data to the OS and sync at
/// the end.
fn fill(
    url: &str,
    client: &str,
    worker: &str,
    from: u64,
    to: u64,
    payload: usize,
) -> Result<(), BoxError> {
    const CHUNK: u64 = 20_000;
    let store = kernable_store::open_with(url, kernable_store::Durability::Normal)?;
    let client = signing::read_signing_key(client)?;
    let worker = signing::read_signing_key(worker)?;
    let payload = pad(payload);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()) as u64;
    let one = |i: u64| {
        let cmd = fill_command(&client, i, &payload);
        let version = i % PER_AGGREGATE;
        let events = vec![NewEvent {
            erases: String::new(),
            vault_refs: Vec::new(),
            event_type: "Written".into(),
            payload: payload.clone(),
            schema: 1,
            origin_partition: String::new(),
            origin_position: 0,
        }];
        let sig = signing::sign(&worker, &signing::result_message(&cmd, version, &events));
        // The kernel stores an index with the aggregate's first commit only; later ones
        // hold the same and leave it alone.
        let index = (version == 0).then(|| {
            let key = name_key(&cmd.aggregate_id);
            AggregateIndex {
                keys: vec![StoredKey {
                    name: key.name.clone(),
                    global: false,
                    id: signing::unique_key_id(&cmd, &key).to_vec(),
                    vault_ref: String::new(),
                }],
                facts: vec!["exists".into()],
            }
        });
        (cmd, version, events, sig, index)
    };
    let started = Instant::now();
    let mut i = from;
    while i < to {
        let end = (i + CHUNK).min(to);
        let per = (end - i).div_ceil(threads);
        let chunk: Vec<_> = std::thread::scope(|s| {
            let parts: Vec<_> = (0..threads)
                .map(|t| {
                    let (lo, one) = (i + t * per, &one);
                    s.spawn(move || (lo..(lo + per).min(end)).map(one).collect::<Vec<_>>())
                })
                .collect();
            parts.into_iter().flat_map(|p| p.join().unwrap()).collect()
        });
        let now = now_ms();
        for (cmd, version, events, sig, index) in &chunk {
            store.enqueue(cmd, now)?;
            store.commit(&cmd.command_id, *version, events, sig, index.as_ref(), now)?;
        }
        i = end;
        if i.is_multiple_of(1_000_000) || i == to {
            let rate = (i - from) as f64 / started.elapsed().as_secs_f64();
            eprintln!("fill: {i} of {to} ({rate:.0}/s)");
        }
    }
    store.sync()?;
    println!(
        "fill {}..{}: {:.1}s",
        from,
        to,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Time to the first event and to `count` events of a subscription after position `after`.
async fn subscribe(addr: &str, key: &str, after: u64, count: usize) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let types = vec!["bench.*".to_owned()];
    let issued_at = now_ms();
    let request = SubscribeRequest {
        after_position: after,
        event_types: types.clone(),
        issued_at,
        client_sig: Some(signing::sign(
            &key,
            &signing::subscribe_message(after, &types, issued_at),
        )),
        protocol: kernable_core::PROTOCOL,
    };
    let started = Instant::now();
    let mut stream = EventServiceClient::new(transport::connect(addr).await?)
        .subscribe(granted(request))
        .await?
        .into_inner();
    let (mut first, mut n) = (None, 0);
    while n < count {
        match stream.message().await? {
            Some(_) => {
                first.get_or_insert_with(|| started.elapsed());
                n += 1;
            }
            None => break,
        }
    }
    let elapsed = started.elapsed();
    println!(
        "subscribe after={after}: first={:.2}ms {n} events in {:.1}ms ({:.0}/s)",
        first.unwrap_or_default().as_secs_f64() * 1e3,
        elapsed.as_secs_f64() * 1e3,
        n as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

// ---- load ----

struct Opts {
    rate: Option<f64>,
    concurrency: Option<usize>,
    seconds: f64,
    aggregates: usize,
    payload: usize,
    wait: Wait,
    target: Target,
    /// Commands in the filled store (`fill ... 0 <filled>`), for `existing` and `repeat`.
    filled: u64,
}

/// What a load sends.
#[derive(Clone, Copy, PartialEq)]
enum Target {
    /// Commands spread over `aggregates` aggregates of this run.
    Spread,
    /// Each command creates an aggregate.
    New,
    /// Commands on random aggregates of the filled store.
    Existing,
    /// Random commands of the filled store again: the kernel returns their stored results.
    Repeat,
    /// `bench.Get` queries, answered by `reader`.
    Query,
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
            wait: match get("wait") {
                Some("committed") => Wait::Committed,
                _ => Wait::Settled,
            },
            target: match get("target") {
                Some("new") => Target::New,
                Some("existing") => Target::Existing,
                Some("repeat") => Target::Repeat,
                Some("query") => Target::Query,
                _ => Target::Spread,
            },
            filled: get("filled").and_then(|v| v.parse().ok()).unwrap_or(0),
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

async fn submit(channel: Channel, cmd: Command, wait: Wait) -> Result<Outcome, tonic::Status> {
    let request = SubmitRequest {
        vault: Vec::new(),
        listener: String::new(),
        protocol: kernable_core::PROTOCOL,
        command: Some(cmd),
        wait: wait.into(),
    };
    let result = CommandServiceClient::new(channel)
        .submit(granted(request))
        .await?;
    Ok(result
        .into_inner()
        .outcome
        .unwrap_or(Outcome::Rejected(Default::default())))
}

/// Builds the requests of a load.
struct Requests {
    key: SigningKey,
    run: String,
    seed: u64,
    payload: Vec<u8>,
    fill_payload: Vec<u8>,
    aggregates: usize,
    filled: u64,
    target: Target,
}

enum Request {
    Submit(Command),
    Query(QueryRequest),
}

impl Requests {
    fn new(key: SigningKey, opts: &Opts) -> Self {
        let run = run_id();
        Requests {
            key,
            seed: u64::from_str_radix(&run, 16).unwrap_or(1),
            run,
            payload: pad(opts.payload),
            fill_payload: pad(FILL_PAYLOAD),
            aggregates: opts.aggregates,
            filled: opts.filled,
            target: opts.target,
        }
    }

    /// Request `i`, signed.
    fn get(&self, i: usize) -> Request {
        let pick = |n: u64| mix(self.seed ^ i as u64) % n.max(1);
        let mut cmd = command(&self.run, i, self.aggregates, &self.payload);
        match self.target {
            Target::Spread => {}
            Target::New => cmd.aggregate_id = format!("n{}-{i}", self.run),
            Target::Existing => {
                cmd.aggregate_id = format!("f{}", pick(self.filled / PER_AGGREGATE));
            }
            Target::Repeat => {
                return Request::Submit(fill_command(
                    &self.key,
                    pick(self.filled),
                    &self.fill_payload,
                ));
            }
            Target::Query => {
                let mut q = QueryRequest {
                    query_id: format!("{}-{i}", self.run),
                    tenant_id: tenant(),
                    domain: "bench".into(),
                    query_type: "Get".into(),
                    payload: self.payload.clone(),
                    issued_at: now_ms(),
                    protocol: kernable_core::PROTOCOL,
                    ..Default::default()
                };
                q.client_sig = Some(signing::sign(&self.key, &signing::query_message(&q)));
                return Request::Query(q);
            }
        }
        Request::Submit(signed(&self.key, cmd))
    }
}

async fn send(channel: Channel, request: Request, wait: Wait) -> Result<Outcome, tonic::Status> {
    match request {
        Request::Submit(cmd) => submit(channel, cmd, wait).await,
        Request::Query(q) => {
            let result = QueryServiceClient::new(channel)
                .query(granted(q))
                .await?
                .into_inner();
            Ok(match result.outcome {
                Some(Answer::Response(_)) => Outcome::Accepted(Default::default()),
                Some(Answer::Rejected(r)) => Outcome::Rejected(r),
                None => Outcome::Rejected(Default::default()),
            })
        }
    }
}

async fn load(addr: &str, key: &str, opts: Opts) -> Result<(), BoxError> {
    let key = signing::read_signing_key(key)?;
    let pool = channels(addr, 4).await?;
    let requests = std::sync::Arc::new(Requests::new(key, &opts));
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
            let request = requests.get(i);
            let (channel, tx) = (pool[i % pool.len()].clone(), tx.clone());
            tokio::spawn(async move {
                let outcome = send(channel, request, opts.wait).await;
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
            let (requests, channel, wait) =
                (requests.clone(), pool[c % pool.len()].clone(), opts.wait);
            tasks.push(tokio::spawn(async move {
                let mut local = Tally::default();
                let mut i = c;
                while started.elapsed() < duration {
                    let request = requests.get(i);
                    let t = Instant::now();
                    let outcome = send(channel.clone(), request, wait).await;
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
        submit(channel.clone(), one(i), Wait::Settled).await?;
    }
    let mut tally = Tally::default();
    let started = Instant::now();
    for i in events..events + SAMPLES {
        let t = Instant::now();
        let outcome = submit(channel.clone(), one(i), Wait::Settled).await;
        tally.record(t.elapsed(), &outcome);
    }
    tally.report(&format!("history events={events}"), started.elapsed());
    Ok(())
}

// ---- helpers ----

/// The tenant of the load's commands and queries: `KERNABLE_BENCH_TENANT`, else `bench` (with
/// partitions, each load generator writes its partition's tenant).
fn tenant() -> String {
    static TENANT: OnceLock<String> = OnceLock::new();
    TENANT
        .get_or_init(|| std::env::var("KERNABLE_BENCH_TENANT").unwrap_or_else(|_| "bench".into()))
        .clone()
}

fn command(run: &str, i: usize, aggregates: usize, payload: &[u8]) -> Command {
    Command {
        vault_refs: Vec::new(),
        command_id: format!("{run}-{i}"),
        tenant_id: tenant(),
        domain: "bench".into(),
        aggregate_type: "Bench".into(),
        aggregate_id: format!("a{}", i % aggregates.max(1)),
        command_type: "Write".into(),
        payload: payload.to_vec(),
        client_sig: None,
        context: None,
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// splitmix64: a well-spread number from `x`.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn run_id() -> String {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("OS randomness unavailable");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
