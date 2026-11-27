//! The steps that do more than send a command: each does the real thing (kills the kernel,
//! changes the database file, revokes a key, …) and returns what happened as beats the Stage
//! narrates one after the other: a caption, the nodes it lights, packets on wires, badges on
//! nodes, lines to show (a command and its output), and whether to read the store again.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::{Duration, Instant},
};

use kernable::{Kernable, Options, Outcome, Query, Submit};
use serde_json::{Value, json};

use super::{Run, bank, lock, scenes::Act, start_kernel, ui_json};
use crate::BoxError;

/// A beat that says `caption` and lights `light`.
pub(super) fn beat(caption: &str, light: &[&str]) -> Value {
    json!({ "caption": caption, "light": light, "packets": [], "badges": [], "lines": [] })
}

/// A beat with packets (`wire`, `label`, `kind`) sent together and badges (`node`, `text`,
/// `kind`) on nodes; `refresh` reads the store and the read model again after it.
fn beat_with(
    caption: &str,
    light: &[&str],
    packets: &[(&str, &str, &str)],
    badges: &[(&str, &str, &str)],
    lines: &[String],
    refresh: bool,
) -> Value {
    json!({
        "caption": caption,
        "light": light,
        "packets": packets.iter().map(|(wire, label, kind)| json!({ "wire": wire, "label": label, "kind": kind })).collect::<Vec<_>>(),
        "badges": badges.iter().map(|(node, text, kind)| json!({ "node": node, "text": text, "kind": kind })).collect::<Vec<_>>(),
        "lines": lines,
        "refresh": refresh,
    })
}

/// A line of the Stage's command list.
fn listed(label: &str, outcome: &str, detail: &str) -> Value {
    json!({ "label": label, "outcome": outcome, "detail": detail })
}

pub(super) async fn run(run: &Arc<Run>, act: Act, prefix: &str) -> Result<Value, BoxError> {
    match act {
        Act::Burst { accounts, n } => burst(run, accounts, n, prefix).await,
        Act::Crash { accounts, n } => crash(run, accounts, n, prefix).await,
        Act::Verify => verify(run).await,
        Act::Tamper => tamper(run).await,
        Act::Break => {
            run.switches.payments_broken.store(true, SeqCst);
            Ok(json!({
                "beats": [beat_with(
                    "The payments reactor's handler fails from now on, like a service that is down. Nothing else changes.",
                    &["reactor"], &[], &[("reactor", "down", "bad")], &[], false,
                )],
                "commands": [],
            }))
        }
        Act::FixAndRetry { to } => fix_and_retry(run, to).await,
        Act::Peek => peek(run).await,
        Act::NewKey => new_key(run, prefix).await,
        Act::Revoke => revoke(run, prefix).await,
        Act::FailClosed => fail_closed(run, prefix).await,
        Act::Rebuild => rebuild(run).await,
    }
}

fn deposit(id: &str, command_id: String, amount: i64) -> Submit {
    Submit::new("bank", "account", id, "Deposit")
        .id(command_id)
        .data(json!({ "amount": amount }))
}

/// `n` with the verb that agrees with it: `1 was …`, `2 were …`.
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms >= 100.0 {
        format!("{ms:.0} ms")
    } else {
        format!("{ms:.1} ms")
    }
}

/// Runs `kernable <args>` in the run's directory; returns the command line, as typed, and its
/// output lines.
async fn cli(run: &Run, args: &[&str]) -> Result<(i32, Vec<String>), BoxError> {
    let exe = std::env::current_exe()?;
    let (dir, owned): (_, Vec<String>) = (
        run.dir.clone(),
        args.iter().map(|a| (*a).to_owned()).collect(),
    );
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(exe)
            .args(owned)
            .current_dir(dir)
            .output()
    })
    .await??;
    let mut lines = vec![format!("$ kernable {}", args.join(" "))];
    for text in [&out.stdout, &out.stderr] {
        lines.extend(
            String::from_utf8_lossy(text)
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(str::to_owned),
        );
    }
    Ok((out.status.code().unwrap_or(1), lines))
}

/// The events of aggregate `id`, as the kernel's UI shows them.
async fn history(run: &Run, aggregate: &str, id: &str) -> Result<Vec<Value>, BoxError> {
    let path = format!("/api/history?tenant=default&domain=bank&type={aggregate}&id={id}");
    let history = ui_json(run, &path).await?;
    Ok(history["events"].as_array().cloned().unwrap_or_default())
}

async fn burst(
    run: &Arc<Run>,
    accounts: &[&str],
    n: usize,
    prefix: &str,
) -> Result<Value, BoxError> {
    for id in accounts {
        let open = Submit::new("bank", "account", id, "OpenAccount")
            .id(format!("{prefix}-open-{id}"))
            .data(json!({ "owner": id }));
        run.client.submit(open).await?;
    }
    let started = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..n {
        let client = run.client.clone();
        let id = accounts[i % accounts.len()].to_owned();
        let command_id = format!("{prefix}-{i}");
        tasks.push(tokio::spawn(async move {
            let sent = Instant::now();
            let outcome = client.submit(deposit(&id, command_id, 10)).await;
            (id, sent.elapsed(), outcome)
        }));
    }
    let mut waited = Vec::new();
    let mut versions: HashMap<String, Vec<u64>> = HashMap::new();
    let mut committed = 0;
    for task in tasks {
        let (id, took, outcome) = task.await?;
        waited.push(took);
        if let Ok(Outcome::Committed(events)) = outcome {
            committed += 1;
            versions
                .entry(id)
                .or_default()
                .extend(events.iter().map(|e| e.version));
        }
    }
    let took = started.elapsed();
    waited.sort();
    let p50 = waited[waited.len() / 2];
    let max = waited[waited.len() - 1];
    // Every account's versions: 2 (after the opening) up to its last, each once.
    let gapless = versions.values_mut().all(|v| {
        v.sort();
        v.iter().copied().eq(2..2 + v.len() as u64)
    });
    let one = accounts.len() == 1;
    let first = {
        let mut kept = lock(&run.kept);
        let first = kept.burst_ms;
        kept.burst_ms.get_or_insert(took.as_secs_f64() * 1000.0);
        first
    };
    let target = if one {
        format!("into {}", accounts[0])
    } else {
        format!("spread over {} accounts", accounts.len())
    };
    let label = format!("Deposit 10 ×{n}");
    let mut beats = vec![
        beat_with(
            &format!("{n} clients send Deposit 10 at the same moment, {target}."),
            &["client"],
            &[
                ("client-kernel", &label, ""),
                ("client-kernel", &label, ""),
                ("client-kernel", &label, ""),
            ],
            &[],
            &[],
            false,
        ),
        if one {
            beat_with(
                &format!(
                    "The kernel queues them for *{}* and leases them to the worker one at a time, each on the state the one before left. No version conflict can happen, so nothing is retried.",
                    accounts[0]
                ),
                &["kernel", "worker"],
                &[
                    ("kernel-worker", "one at a time", ""),
                    ("worker-kernel", "Deposited", "event"),
                ],
                &[("kernel", "queue per aggregate", "")],
                &[],
                false,
            )
        } else {
            beat_with(
                &format!(
                    "Each account has its own queue: the worker runs up to {} commands at once, still one per account.",
                    accounts.len()
                ),
                &["kernel", "worker"],
                &[
                    ("kernel-worker", "s1", ""),
                    ("kernel-worker", "s2", ""),
                    ("kernel-worker", "s3", ""),
                ],
                &[("kernel", "a queue per aggregate", "")],
                &[],
                false,
            )
        },
    ];
    let versions_said = if one {
        format!(
            "versions 2 to {} {}",
            n + 1,
            if gapless {
                "without a gap"
            } else {
                "with a gap!"
            }
        )
    } else if gapless {
        "every account's versions without a gap".to_owned()
    } else {
        "a gap in the versions!".to_owned()
    };
    let speed = match (one, first) {
        (false, Some(first)) => format!(
            " One account took {first:.0} ms for the same 200: {:.1}× as long.",
            first / (took.as_secs_f64() * 1000.0)
        ),
        _ => String::new(),
    };
    beats.push(beat_with(
        &format!(
            "Done in *{}*: {committed} of {n} committed, {versions_said}. A client waited {} (median), at most {}.{speed}",
            ms(took),
            ms(p50),
            ms(max)
        ),
        &["store", "model"],
        &[("kernel-store", "+200 events", "event"), ("store-model", "Deposited ×200", "event")],
        &[("kernel", "", "")],
        &[],
        true,
    ));
    let outcome = if committed == n {
        "committed"
    } else {
        "failed"
    };
    let headline = match (one, first) {
        (false, Some(first)) => format!(
            "{n} deposits · {} · {:.1}× faster on {} accounts",
            ms(took),
            first / (took.as_secs_f64() * 1000.0),
            accounts.len()
        ),
        _ => format!("{n} deposits · {} · 0 conflicts", ms(took)),
    };
    Ok(json!({
        "beats": beats,
        "headline": headline,
        "commands": [listed(&format!("Deposit 10 ×{n} {target}"), outcome, &format!("{committed} committed in {}", ms(took)))],
    }))
}

async fn crash(
    run: &Arc<Run>,
    accounts: &[&str],
    n: usize,
    prefix: &str,
) -> Result<Value, BoxError> {
    let done = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    // How many have committed, every few milliseconds, for the Stage's chart.
    let sampling = Arc::new(AtomicUsize::new(1));
    let sampler = {
        let (done, sampling) = (Arc::clone(&done), Arc::clone(&sampling));
        tokio::spawn(async move {
            let mut series = Vec::new();
            while sampling.load(SeqCst) == 1 {
                series.push(json!([
                    started.elapsed().as_millis() as u64,
                    done.load(SeqCst)
                ]));
                tokio::time::sleep(Duration::from_millis(4)).await;
            }
            series.push(json!([
                started.elapsed().as_millis() as u64,
                done.load(SeqCst)
            ]));
            series
        })
    };
    let ids: Vec<String> = accounts.iter().map(|s| (*s).to_owned()).collect();
    let mut tasks = Vec::new();
    for i in 0..n {
        let (client, done) = (run.client.clone(), Arc::clone(&done));
        let id = ids[i % ids.len()].clone();
        let command_id = format!("{prefix}-{i}");
        tasks.push(tokio::spawn(async move {
            match client.submit(deposit(&id, command_id.clone(), 1)).await {
                Ok(Outcome::Committed(_)) => {
                    done.fetch_add(1, SeqCst);
                    None
                }
                _ => Some((id, command_id)),
            }
        }));
    }
    // Kill it a third of the way in.
    while done.load(SeqCst) < n / 3 && started.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let ran_before: HashMap<String, u32> = run
        .stage
        .runs(prefix)
        .into_iter()
        .collect::<HashMap<_, _>>();
    lock(&run.kernel).kill();
    let acknowledged = done.load(SeqCst);
    let killed_ms = started.elapsed().as_millis() as u64;
    let restarted = Instant::now();
    let kernel = start_kernel(&run.dir, &run.grpc, &run.ui)
        .await
        .map_err(super::NotStarted::into_error)?;
    *lock(&run.kernel) = kernel;
    let restart = restarted.elapsed();
    let listening_ms = started.elapsed().as_millis() as u64;
    let mut failed = Vec::new();
    for task in tasks {
        if let Some(f) = task.await? {
            failed.push(f);
        }
    }
    // Every client whose submit failed sends it again with the same ID, until it gets an answer.
    let mut resent = Vec::new();
    for (id, command_id) in failed.clone() {
        let (client, done) = (run.client.clone(), Arc::clone(&done));
        resent.push(tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match client.submit(deposit(&id, command_id.clone(), 1)).await {
                    Ok(outcome) => {
                        if outcome.is_committed() {
                            done.fetch_add(1, SeqCst);
                        }
                        return outcome.is_committed();
                    }
                    Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(_) => return false,
                }
            }
        }));
    }
    let mut resent_ok = 0;
    for task in resent {
        if task.await? {
            resent_ok += 1;
        }
    }
    sampling.store(0, SeqCst);
    let series = sampler.await?;
    let ran_after: HashMap<String, u32> = run.stage.runs(prefix).into_iter().collect();
    let (mut stored_already, mut ran_now, mut ran_again) = (0, 0, 0);
    for (_, command_id) in &failed {
        let before = ran_before.get(command_id).copied().unwrap_or(0);
        let after = ran_after.get(command_id).copied().unwrap_or(0);
        if before == 0 {
            ran_now += 1;
        } else if after > before {
            ran_again += 1;
        } else {
            stored_already += 1;
        }
    }
    // What the store holds now.
    let mut deposits = 0;
    let mut command_ids = std::collections::HashSet::new();
    let mut balances = Vec::new();
    for id in &ids {
        let events = history(run, "account", id).await?;
        let mine: Vec<&Value> = events
            .iter()
            .filter(|e| {
                e["commandId"]
                    .as_str()
                    .is_some_and(|c| c.starts_with(prefix))
            })
            .collect();
        deposits += mine.len();
        command_ids.extend(
            mine.iter()
                .filter_map(|e| e["commandId"].as_str().map(str::to_owned)),
        );
        balances.push(format!("{id} {}", mine.len()));
    }
    let lost = n.saturating_sub(command_ids.len());
    let twice = deposits - command_ids.len();
    let verdict = if lost == 0 && twice == 0 {
        "Nothing lost, nothing twice."
    } else {
        "Something is wrong: see the counts."
    };
    let beats = vec![
        beat_with(
            &format!("{n} deposits of 1 are on their way to {}.", ids.join(", ")),
            &["client", "kernel", "worker"],
            &[
                ("client-kernel", "Deposit ×300", ""),
                ("kernel-worker", "Deposit", ""),
                ("worker-kernel", "Deposited", "event"),
            ],
            &[],
            &[],
            false,
        ),
        beat_with(
            &format!(
                "After *{acknowledged}* answered: *kill -9*. The kernel is gone at once: no shutdown, no flush. The other {} were queued, running in the worker, or committed without their answer.",
                n - acknowledged
            ),
            &["kernel"],
            &[("kernel-client", "connection lost", "bad")],
            &[("kernel", "killed", "bad")],
            &[],
            false,
        ),
        beat_with(
            &format!(
                "The kernel starts again on the same store, listening after {}. The worker and the read model reconnect by themselves.",
                ms(restart)
            ),
            &["kernel", "store"],
            &[("kernel-worker", "reconnected", "ok")],
            &[("kernel", "restarted", "ok")],
            &[],
            false,
        ),
        beat_with(
            &format!(
                "Each client whose submit failed sends it again, with the *same command ID*: {} commands. {}: the kernel answers with the stored result. {} for the first time. {}: their result was never stored, so they ran again.",
                failed.len(),
                count(stored_already, "was stored already", "were stored already"),
                count(ran_now, "runs", "run"),
                count(
                    ran_again,
                    "had reached the worker before the kill",
                    "had reached the worker before the kill"
                ),
            ),
            &["client", "kernel", "worker"],
            &[
                ("client-kernel", "same IDs again", ""),
                ("kernel-client", "answers", "ok"),
            ],
            &[("kernel", "", "")],
            &[],
            false,
        ),
        beat_with(
            &format!(
                "In the store: *{deposits}* deposits from {} command IDs, for {n} commands sent. {verdict} Per account: {}.",
                command_ids.len(),
                balances.join(", ")
            ),
            &["store", "model"],
            &[("store-model", "caught up", "event")],
            &[],
            &[],
            true,
        ),
    ];
    let outcome = if lost == 0 && twice == 0 && resent_ok == failed.len() {
        "committed"
    } else {
        "failed"
    };
    // The chart, drawn as far as each beat has told.
    let end = series
        .last()
        .and_then(|p| p[0].as_u64())
        .unwrap_or(listening_ms);
    let mut beats = beats;
    for (beat, until) in beats
        .iter_mut()
        .zip([killed_ms, killed_ms, listening_ms, end, end])
    {
        beat["chart"] = json!({ "until": until });
        beat["focus"] = json!("flow");
    }
    Ok(json!({
        "beats": beats,
        "headline": format!("{} of {n} · {lost} lost · {twice} twice", command_ids.len()),
        "chart": {
            "series": series,
            "n": n,
            "killedMs": killed_ms,
            "listeningMs": listening_ms,
        },
        "commands": [listed(
            &format!("Deposit 1 ×{n}, kill -9 after {acknowledged}"),
            outcome,
            &format!("{} sent again, {lost} lost, {twice} twice", failed.len()),
        )],
    }))
}

async fn verify(run: &Run) -> Result<Value, BoxError> {
    let (code, lines) = cli(run, &["verify", "sqlite:kernable.db", "instance.pub"]).await?;
    // Exit 2: intact, but something was signed at or after its key's revocation.
    let ok = code == 0 || code == 2;
    // `PROBLEM event 2: …` names a changed event.
    let bad: Vec<u64> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("PROBLEM event "))
        .filter_map(|l| l.split(':').next()?.trim().parse().ok())
        .collect();
    let said = if ok {
        "Every hash matches and every signature checks: the store is *intact*."
    } else {
        "verify names what was changed: the event's hash no longer matches its content, and the worker's signature over it no longer checks. The store is *tampered*."
    };
    let mut reads = beat_with(
        "kernable verify reads the store, recomputes every event's hash from its content and the hash before, and checks the signatures of workers, clients and the kernel's checkpoints.",
        &["store"],
        &[],
        &[],
        &[],
        false,
    );
    reads["chain"] = json!({ "scan": true, "bad": bad });
    let mut verdict = beat_with(
        said,
        &["store"],
        &[],
        &[(
            "store",
            if ok { "intact" } else { "tampered" },
            if ok { "ok" } else { "bad" },
        )],
        &lines,
        false,
    );
    verdict["chain"] = json!({ "bad": bad, "done": true });
    // Only a found change is the scene's headline; an intact store confirms the one before.
    let headline = (!ok).then(|| {
        let which: Vec<String> = bad.iter().map(|p| format!("#{p}")).collect();
        format!("tampered: event {} found", which.join(", "))
    });
    Ok(json!({
        "beats": [reads, verdict],
        "headline": headline,
        "commands": [],
    }))
}

async fn tamper(run: &Run) -> Result<Value, BoxError> {
    let path = run.dir.join("kernable.db");
    let (position, before, after) = tokio::task::spawn_blocking(move || {
        let conn = rusqlite::Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        // Ada's first deposit.
        let (position, before): (u64, String) = conn.query_row(
            "SELECT global_position, payload FROM events
             WHERE event_type = 'Deposited' AND aggregate_id = 'a1' ORDER BY global_position LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let after = r#"{"amount":1000000}"#.to_owned();
        conn.execute(
            "UPDATE events SET payload = ?1 WHERE global_position = ?2",
            rusqlite::params![after, position],
        )?;
        Ok::<_, BoxError>((position, before, after))
    })
    .await??;
    let sql = format!("UPDATE events SET payload = '{after}' WHERE global_position = {position};");
    let mut changes = beat_with(
        &format!(
            "Someone with access to the database file changes event *#{position}*: {before} becomes {after}."
        ),
        &["store"],
        &[],
        &[("store", "changed", "bad")],
        &["$ sqlite3 kernable.db".to_owned(), sql],
        true,
    );
    changes["chain"] = json!({ "changed": position });
    let mut accepted = beat_with(
        "SQLite accepts it, and the event store now shows the new amount. Nothing else notices: the read model still has the balance it computed when the event was new.",
        &["store", "model"],
        &[],
        &[],
        &[],
        false,
    );
    accepted["chain"] = json!({ "changed": position });
    Ok(json!({
        "beats": [changes, accepted],
        "headline": format!("event #{position}: 100 became 1,000,000"),
        "commands": [],
    }))
}

async fn fix_and_retry(run: &Run, to: &str) -> Result<Value, BoxError> {
    run.switches.payments_broken.store(false, SeqCst);
    let state = ui_json(run, "/api/state").await?;
    let parked = state["listeners"]
        .as_array()
        .and_then(|l| l.iter().find(|l| l["name"] == "payments"))
        .and_then(|l| l["parkedEvents"].as_array().cloned())
        .unwrap_or_default();
    let positions: Vec<u64> = parked
        .iter()
        .filter_map(|p| p["position"].as_u64())
        .collect();
    let (_, lines) = cli(
        run,
        &[
            "listeners",
            "retry",
            &run.grpc,
            "app.key",
            "payments",
            "--all",
        ],
    )
    .await?;
    // Wait until the credits are in the store.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut credited = Vec::new();
    while Instant::now() < deadline {
        let events = history(run, "account", to).await?;
        credited = positions
            .iter()
            .filter(|p| {
                let id = format!("credit-{p}");
                events.iter().any(|e| e["commandId"] == id.as_str())
            })
            .copied()
            .collect();
        if credited.len() == positions.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let which = positions
        .iter()
        .map(|p| format!("#{p}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(json!({
        "headline": "credited exactly once, after the retry",
        "beats": [
            beat_with("The payments service is back.", &["reactor"], &[], &[("reactor", "fixed", "ok")], &[], false),
            beat_with(
                &format!("An operator retries what the reactor parked: event {which}."),
                &["kernel", "reactor"],
                &[("store-reactor", "Paid", "event")],
                &[("reactor", "", "")],
                &lines,
                false,
            ),
            beat_with(
                &format!(
                    "The reactor credits {to} with the fixed ID credit-{}: had it run before, the kernel would have answered with the stored result. {} of {} credits are in the store.",
                    positions.first().copied().unwrap_or_default(),
                    credited.len(),
                    positions.len()
                ),
                &["reactor", "kernel", "store", "model"],
                &[("reactor-kernel", &format!("Credit {to}"), ""), ("kernel-store", "Credited", "event"), ("store-model", "Credited", "event")],
                &[],
                &[],
                true,
            ),
        ],
        "commands": [listed(&format!("Credit {to} (by the reactor, retried)"), if credited.len() == positions.len() { "committed" } else { "failed" }, &format!("parked {which}"))],
    }))
}

/// Bob, then Ada, ask for Ada's account.
async fn peek(run: &Run) -> Result<Value, BoxError> {
    let ask = |who: &str| {
        Query::new("bank", "GetAccount")
            .context(kernable::RequestContext {
                sender_identity: who.to_owned(),
                ..Default::default()
            })
            .data(json!({ "id": "a1" }))
    };
    let said = |answer: Result<Value, kernable::Error>| match answer {
        Ok(row) => format!("answered: balance {}", row["balance"]),
        Err(e) => format!("refused: {e}"),
    };
    let bob = said(run.client.query(ask("bob")).await);
    let ada = said(run.client.query(ask("ada")).await);
    let bob_refused = bob.starts_with("refused");
    Ok(json!({
        "beats": [
            beat_with(
                &format!("Bob asks for Ada's account a1. Before the kernel routes the query, the middleware checks it: {bob}."),
                &["client", "kernel", "middleware"],
                &[("client-kernel", "GetAccount a1", ""), ("kernel-middleware", "check", ""), ("middleware-kernel", "forbidden", "bad"), ("kernel-client", "refused", "bad")],
                &[("middleware", "bob: no", "bad")],
                &[],
                false,
            ),
            beat_with(
                &format!("Ada asks for her own account: {ada}. The read model answers only what the middleware lets through."),
                &["client", "kernel", "middleware", "model"],
                &[("client-kernel", "GetAccount a1", ""), ("kernel-middleware", "check", ""), ("middleware-kernel", "pass", "ok"), ("kernel-model", "query", ""), ("model-kernel", "answer", "ok"), ("kernel-client", "answer", "ok")],
                &[("middleware", "ada: yes", "ok")],
                &[],
                false,
            ),
        ],
        "headline": "customers see their own accounts only",
        "commands": [
            listed("GetAccount a1 (by bob)", if bob_refused { "rejected" } else { "failed" }, &bob),
            listed("GetAccount a1 (by ada)", if ada.starts_with("answered") { "committed" } else { "failed" }, &ada),
        ],
    }))
}

async fn new_key(run: &Run, prefix: &str) -> Result<Value, BoxError> {
    let (_, mut lines) = cli(run, &["keys", "new", "mobile"]).await?;
    let (_, granted) = cli(
        run,
        &[
            "grant",
            "root.key",
            "mobile.pub",
            "submit=bank.Deposit",
            "query=bank.*",
        ],
    )
    .await?;
    lines.extend(granted);
    let mobile = Kernable::new(
        Options::new(run.dir.join("mobile.key")).url(format!("http://{}", run.grpc)),
    )?;
    let outcome = mobile
        .submit(deposit("a1", format!("{prefix}-mobile"), 10))
        .await;
    let ok = matches!(outcome, Ok(Outcome::Committed(_)));
    lock(&run.kept).mobile = Some(mobile);
    // A revocation counts from a whole second on: one in this second would make the deposit
    // above look signed after it.
    let into_second = crate::now_ms().rem_euclid(1000) as u64;
    tokio::time::sleep(Duration::from_millis(1000 - into_second)).await;
    Ok(json!({
        "headline": "a key per client, granted by the root key",
        "beats": [
            beat_with(
                "A new key for the mobile app, and a grant for it, signed by the root key: it may deposit and query in bank, nothing else.",
                &["client"], &[], &[("client", "mobile key", "")], &lines, false,
            ),
            beat_with(
                &format!("The mobile app deposits 10 into a1, signed with its key: {}.", if ok { "committed" } else { "refused" }),
                &["client", "kernel", "worker", "store", "model"],
                &[("client-kernel", "Deposit 10", ""), ("kernel-store", "Deposited", "event"), ("store-model", "Deposited", "event")],
                &[],
                &[],
                true,
            ),
        ],
        "commands": [listed("Deposit 10 → a1 (mobile key)", if ok { "committed" } else { "failed" }, "")],
    }))
}

async fn revoke(run: &Run, prefix: &str) -> Result<Value, BoxError> {
    let since = crate::config::utc(crate::now_ms());
    let (_, lines) = cli(
        run,
        &["revoke", &run.grpc, "root.key", "mobile.pub", &since],
    )
    .await?;
    let mobile = lock(&run.kept).mobile.clone().ok_or("no mobile key yet")?;
    let refused = match mobile
        .submit(deposit("a1", format!("{prefix}-mobile"), 10))
        .await
    {
        Ok(Outcome::Committed(_)) => "committed: the revocation did not hold!".to_owned(),
        Ok(Outcome::Rejected(r)) => format!("rejected, {}", r.code),
        Err(e) => format!("refused: {e}"),
    };
    let app = run
        .client
        .submit(deposit("a1", format!("{prefix}-app"), 10))
        .await;
    let app_ok = matches!(app, Ok(Outcome::Committed(_)));
    Ok(json!({
        "headline": "revoked: refused at once",
        "beats": [
            beat_with(
                "The mobile key leaked. The root key signs its revocation; the kernel stores it, signed by itself, and ends the key's connections.",
                &["kernel"], &[], &[("client", "revoked", "bad")], &lines, false,
            ),
            beat_with(
                &format!("The mobile app tries to deposit again: {refused}."),
                &["client", "kernel"],
                &[("client-kernel", "Deposit 10", "bad"), ("kernel-client", "refused", "bad")],
                &[],
                &[],
                false,
            ),
            beat_with(
                &format!("The bank's own key goes on: its deposit is {}. After a restart the revocation still holds: it is in the store.", if app_ok { "committed" } else { "refused" }),
                &["client", "kernel", "store", "model"],
                &[("client-kernel", "Deposit 10", ""), ("kernel-store", "Deposited", "event"), ("store-model", "Deposited", "event")],
                &[("client", "", "")],
                &[],
                true,
            ),
        ],
        "commands": [
            listed("Deposit 10 → a1 (mobile key, revoked)", "rejected", &refused),
            listed("Deposit 10 → a1 (app key)", if app_ok { "committed" } else { "failed" }, ""),
        ],
    }))
}

async fn fail_closed(run: &Run, prefix: &str) -> Result<Value, BoxError> {
    let rbac = run
        .app
        .listeners()
        .iter()
        .find(|l| l.listener().name() == "rbac")
        .cloned()
        .ok_or("no rbac middleware")?;
    rbac.stop();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let teller = kernable::RequestContext {
        sender_identity: "teller".into(),
        ..Default::default()
    };
    let withdraw = |n: &str| {
        Submit::new("bank", "account", "a1", "Withdraw")
            .id(format!("{prefix}-{n}"))
            .context(teller.clone())
            .data(json!({ "amount": 10 }))
    };
    let away = match run.client.submit(withdraw("away")).await {
        Ok(Outcome::Committed(_)) => "committed: the middleware was bypassed!".to_owned(),
        Ok(Outcome::Rejected(r)) => format!("rejected, {}: {}", r.code, r.message),
        Err(e) => format!("refused: {e}"),
    };
    // The same middleware again, on a new connection of this process.
    let back = run.client.listener(rbac.listener());
    {
        let back = back.clone();
        tokio::spawn(async move { back.run().await });
    }
    let _ = tokio::time::timeout(Duration::from_secs(15), back.ready()).await;
    lock(&run.kept).listeners.push(back);
    let again = run.client.submit(withdraw("back")).await;
    let again_ok = matches!(again, Ok(Outcome::Committed(_)));
    Ok(json!({
        "headline": "refused while away, decided again when back",
        "beats": [
            beat_with(
                "The rbac middleware stops, like a crashed authorization service.",
                &["middleware"], &[], &[("middleware", "away", "bad")], &[], false,
            ),
            beat_with(
                &format!("A teller withdraws 10: {away}. The kernel lets nothing through that the middleware would have checked: it fails closed."),
                &["client", "kernel"],
                &[("client-kernel", "Withdraw 10", ""), ("kernel-client", "refused", "bad")],
                &[],
                &[],
                false,
            ),
            beat_with(
                &format!("The middleware is back and decides again: the teller's withdrawal is {}.", if again_ok { "committed" } else { "still refused" }),
                &["middleware", "kernel", "worker", "store", "model"],
                &[("kernel-middleware", "check", ""), ("middleware-kernel", "pass", "ok"), ("kernel-store", "Withdrawn", "event"), ("store-model", "Withdrawn", "event")],
                &[("middleware", "back", "ok")],
                &[],
                true,
            ),
        ],
        "commands": [
            listed("Withdraw 10 → a1 (teller, middleware away)", "rejected", &away),
            listed("Withdraw 10 → a1 (teller, middleware back)", if again_ok { "committed" } else { "failed" }, ""),
        ],
    }))
}

async fn rebuild(run: &Run) -> Result<Value, BoxError> {
    let state = ui_json(run, "/api/state").await?;
    let head = state["kernel"]["head"].as_u64().unwrap_or(0);
    let started = Instant::now();
    let (rows, paid_in) = bank::rebuild(&run.client, head).await?;
    let took = started.elapsed();
    let live = run.rows.view("accounts");
    let rebuilt = json!(rows);
    let equal = rebuilt == live;
    let said = if equal {
        "its rows equal the live read model's, value for value"
    } else {
        "its rows differ from the live read model's"
    };
    let paid: Vec<String> = paid_in.iter().map(|(id, n)| format!("{id} {n}")).collect();
    Ok(json!({
        "headline": format!("{head} events replayed: the same rows"),
        "beats": [
            beat_with(
                "A new read model starts empty and subscribes from position 0: it gets the whole history, in order.",
                &["store", "model"], &[("store-model", "#1 …", "event")], &[("model", "new", "")], &[], false,
            ),
            beat_with(
                &format!("It replayed *{head}* events in {}: {said}.", ms(took)),
                &["model"], &[("store-model", &format!("… #{head}"), "event")], &[("model", if equal { "equal" } else { "differs" }, if equal { "ok" } else { "bad" })],
                &[format!("rebuilt: {rebuilt}"), format!("live:    {live}")],
                false,
            ),
            beat_with(
                &format!("A question the live read model never asked, answered from the same old events: paid in, in all: {}.", paid.join(", ")),
                &["model"], &[], &[], &[], false,
            ),
        ],
        "commands": [],
    }))
}
