//! A check round, while the load and the faults pause: wait until every listener has caught
//! up, then check every invariant of the store (S1–S9) and of what clients and the audit
//! listener were told (O1–O4), and the apps' own rules against the log. Each round reads only
//! the log after the round before ([`Checked`] carries what the rules need of it), so a run of
//! days checks in bounded memory:
//!
//! - B1: no account is ever below zero;
//! - B2: every withdrawal of 50 or more is flagged exactly once, and nothing else is;
//! - B3: each app's read model holds the balance and flags the log says;
//! - B4: no two accounts of an app hold the same number at once, replaying the log, and the
//!   kernel's index holds exactly the number and the fact `open` each account's last event
//!   leaves it with;
//! - B5: no withdrawal sent `to` an account committed while that account was closed;
//! - B6: every `unique_taken` the load saw was for a number another account held, and every
//!   `requirement_missing` for an account that was closed, at some position between the
//!   client's commands before and after it (and no other command saw either);
//! - B7: the flaggers park only what they must: every event a flagger parked is a withdrawal of
//!   50 or more whose `Flag` was rejected, with that rejection's code (kills, restarts and a
//!   failing network never park), and a withdrawal that was never flagged was parked, so
//!   nothing is lost silently.
//! - B8: payloads arrive as they were sent: every `Noted` event holds the note its command carried
//!   (integers exactly, floats as the same number, strings to the byte), and every app's read model
//!   answers the note the log holds.
//!
//! H1–H4: what the hostile participants tried changed nothing (see `hostile.rs`).
//!
//! `bank_x` runs on workers in Rust and Go at once; each round says which language decided its
//! deposits, so a run shows both took turns.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use kernable::{Kernable, Query};
use kernable_core::{
    proto::{Command, UniqueKey, command_result::Outcome},
    signing,
};
use kernable_store::invariants::{Carry, Log};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::load::{ACCOUNTS, DOMAINS, Load, Refusal, TENANT, note_payload};

pub struct Report {
    pub head: u64,
    pub violations: Vec<String>,
    pub took: Duration,
    /// What the round saw besides violations, for the log.
    pub info: Vec<String>,
}

/// Read from a fresh read-only open: the head, every listener's
/// name and position, and how many commands are pending.
type Positions = (u64, Vec<(String, u64)>, usize);

fn positions(store: &str) -> Result<Positions, String> {
    let read = kernable_store::open_read_only(store).map_err(|e| e.to_string())?;
    let head = read.head_position().map_err(|e| e.to_string())?;
    let listeners = read
        .listeners()
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|l| !l.removed)
        .map(|l| (l.name, l.position))
        .collect();
    let pending = read.pending().map_err(|e| e.to_string())?.len();
    Ok((head, listeners, pending))
}

/// Reads the log after `after` from `store` as it is now, with its kernels' keys and the
/// commands `ids`, off the runtime: the load, the audit listener and the client of the check
/// round go on meanwhile.
async fn read_log(
    store: &str,
    kernel_key_ids: &[String],
    ids: &[String],
    after: u64,
) -> Result<Log, String> {
    let (store, keys, ids) = (store.to_owned(), kernel_key_ids.to_vec(), ids.to_vec());
    tokio::task::spawn_blocking(move || {
        kernable_store::open_read_only(&store)
            .and_then(|read| Log::read_since(read.as_ref(), &keys, &ids, after))
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}

/// The stored results of commands `ids`, by ID (`None`: not stored, or pending).
pub async fn results_of(
    store: &str,
    ids: Vec<String>,
) -> Result<BTreeMap<String, Option<kernable_core::proto::CommandResult>>, String> {
    let store = store.to_owned();
    tokio::task::spawn_blocking(move || {
        let read = kernable_store::open_read_only(&store).map_err(|e| e.to_string())?;
        let mut out = BTreeMap::new();
        for id in ids {
            let r = read.result(&id).map_err(|e| e.to_string())?;
            out.insert(id, r);
        }
        Ok(out)
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Waits until nothing is pending and every listener is at the head, twice in a row.
async fn quiesce(store: &str, limit: Duration) -> Result<u64, String> {
    let started = Instant::now();
    let mut last = None;
    loop {
        let at = store.to_owned();
        let state = tokio::task::spawn_blocking(move || positions(&at))
            .await
            .map_err(|e| e.to_string())
            .and_then(|state| state);
        if let Ok((head, listeners, 0)) = &state
            && listeners.iter().all(|(_, p)| p == head)
        {
            if last == Some(*head) {
                return Ok(*head);
            }
            last = Some(*head);
        } else {
            last = None;
        }
        if started.elapsed() > limit {
            return Err(format!(
                "Q quiescence: not quiet after {limit:?}: {state:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}

#[derive(Deserialize, Debug, PartialEq, Default)]
struct Row {
    balance: i64,
    flags: i64,
}
type Key = (&'static str, String);

/// What the rules carry from one round to the next: the store invariants' state, and each
/// app's accounts as the log left them.
#[derive(Default)]
pub struct Checked {
    carry: Carry,
    /// Balance and flags per account (B1, B3).
    accounts: BTreeMap<Key, (i64, i64)>,
    /// Withdrawals of 50 or more, by position, ever (B2, B7).
    big: BTreeMap<u64, Key>,
    /// Flags per withdrawal position, ever (B2).
    flagged: BTreeMap<u64, Vec<Key>>,
    /// Big withdrawals checked already (flagged, or justly parked).
    settled: BTreeSet<u64>,
    /// Parked positions checked already (B7).
    parked_seen: BTreeSet<u64>,
    /// Number and open per account; per number, who held it; per account, open or not (B4–B6).
    numbers: BTreeMap<Key, (Option<String>, bool)>,
    holders: BTreeMap<Key, History<Option<String>>>,
    opens: BTreeMap<Key, History<bool>>,
    /// Refusals checked already (B6): the load's list only grows.
    refusals_checked: usize,
    /// The last note per account, as the log holds it (B8).
    notes: BTreeMap<Key, Value>,
    /// Who decided `bank_x`'s deposits: language, count.
    by_x: BTreeMap<String, u64>,
    /// Notes whose integers beyond ±2^53 came back rounded where that is known, per domain.
    rounded: BTreeMap<String, u64>,
}

/// Where integers beyond ±2^53 are known to come back rounded to a float (JavaScript's numbers,
/// Go's generic JSON type): `bank_x` runs on Go workers and answers from a Go read model.
/// There, such a rounding counts as known, not as a violation; everything else stays exact.
const ROUNDS_BIG_INTEGERS: [&str; 3] = ["bank_ts", "bank_go", "bank_x"];

/// Whether `actual` differs from `expected` only by integers beyond ±2^53 that became the
/// nearest float.
fn only_rounded(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Number(e), Value::Number(a)) => e.as_f64() == a.as_f64(),
        (Value::Array(e), Value::Array(a)) => {
            e.len() == a.len() && e.iter().zip(a).all(|(e, a)| only_rounded(e, a))
        }
        (Value::Object(e), Value::Object(a)) => {
            e.len() == a.len()
                && e.iter()
                    .all(|(k, v)| a.get(k).is_some_and(|w| only_rounded(v, w)))
        }
        _ => expected == actual,
    }
}

/// Whether `actual` is the JSON value `expected`: integers exactly (an integer must stay one),
/// other numbers as the same float, strings and keys to the byte.
pub fn same_json(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Number(e), Value::Number(a)) => {
            if e.is_i64() || e.is_u64() {
                let int = |n: &serde_json::Number| {
                    n.as_i64()
                        .map(i128::from)
                        .or_else(|| n.as_u64().map(i128::from))
                };
                int(e).is_some() && int(e) == int(a)
            } else {
                e.as_f64() == a.as_f64()
            }
        }
        (Value::Array(e), Value::Array(a)) => {
            e.len() == a.len() && e.iter().zip(a).all(|(e, a)| same_json(e, a))
        }
        (Value::Object(e), Value::Object(a)) => {
            e.len() == a.len()
                && e.iter()
                    .all(|(k, v)| a.get(k).is_some_and(|w| same_json(v, w)))
        }
        _ => expected == actual,
    }
}

pub async fn round(
    store: &str,
    kernel_key_ids: &[String],
    standby: Option<&str>,
    load: &Load,
    client: &Kernable,
    checked: &mut Checked,
) -> Report {
    let started = Instant::now();
    let mut violations = Vec::new();
    let head = match quiesce(store, Duration::from_secs(300)).await {
        Ok(head) => head,
        Err(e) => {
            violations.push(e);
            0
        }
    };
    let after = checked.carry.head;
    let mut ids = load.observed.lock().unwrap().command_ids();
    ids.extend(load.stats.unanswered.lock().unwrap().iter().cloned());
    let log = match read_log(store, kernel_key_ids, &ids, after).await {
        Ok(log) => log,
        Err(e) => {
            violations.push(format!("reading the store: {e}"));
            return Report {
                head,
                violations,
                took: started.elapsed(),
                info: Vec::new(),
            };
        }
    };
    violations.extend(
        log.check_carried(&mut checked.carry)
            .iter()
            .map(ToString::to_string),
    );
    {
        let mut observed = load.observed.lock().unwrap();
        violations.extend(observed.check(&log).iter().map(ToString::to_string));
        // Checked: what comes after is checked next round.
        observed.forget_outcomes();
    }
    // A pair: the standby holds exactly what the primary holds, once it has caught up (P2).
    if let Some(standby) = standby {
        let waited = Instant::now();
        loop {
            match read_log(standby, kernel_key_ids, &[], after).await {
                Ok(s) if s.events == log.events => break,
                Ok(s) if waited.elapsed() > Duration::from_secs(120) => {
                    violations.push(format!(
                        "P2 standby: {} events after {after}, the primary {} ({} differ)",
                        s.events.len(),
                        log.events.len(),
                        s.events
                            .iter()
                            .zip(&log.events)
                            .filter(|(a, b)| a != b)
                            .count()
                    ));
                    break;
                }
                Err(e) if waited.elapsed() > Duration::from_secs(120) => {
                    violations.push(format!("P2 standby: reading its store: {e}"));
                    break;
                }
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }
    bank_rules(&log, checked, &mut violations);
    hostile_rules(store, &log, load, &mut violations).await;
    parking_rules(store, &log, checked, &mut violations).await;
    let refusals: Vec<Refusal> = {
        let all = load.refusals.lock().unwrap();
        all[checked.refusals_checked.min(all.len())..].to_vec()
    };
    checked.refusals_checked += refusals.len();
    index_rules(&log, checked, &refusals, &mut violations);

    // B3: every account of every app, as its read model answers.
    for domain in DOMAINS {
        for a in 0..ACCOUNTS {
            let id = format!("a{a}");
            let want = checked
                .accounts
                .get(&(domain, id.clone()))
                .map_or(Row::default(), |&(balance, flags)| Row { balance, flags });
            let query = Query::new(domain, "GetAccount")
                .data(json!({ "id": id }))
                .min_position(head);
            match tokio::time::timeout(Duration::from_secs(60), client.query::<Row>(query)).await {
                Ok(Ok(got)) if got == want => {}
                Ok(Ok(got)) => violations.push(format!(
                    "B3 read model: {domain} {id} is {got:?}, the log says {want:?}"
                )),
                Ok(Err(e)) => violations.push(format!("B3 read model: {domain} {id}: {e}")),
                Err(_) => {
                    violations.push(format!("B3 read model: {domain} {id}: no answer in 60 s"))
                }
            }
        }
    }
    // B8: every app's read model answers the note the log holds.
    for ((domain, id), want) in &checked.notes {
        let query = Query::new(domain, "GetNote")
            .data(json!({ "id": id }))
            .min_position(head);
        match tokio::time::timeout(Duration::from_secs(60), client.query::<Value>(query)).await {
            Ok(Ok(got)) if same_json(want, &got) => {}
            Ok(Ok(got)) if ROUNDS_BIG_INTEGERS.contains(domain) && only_rounded(want, &got) => {}
            Ok(Ok(got)) => violations.push(format!(
                "B8 read model note: {domain} {id} answers {}, the log holds {}",
                short(&got),
                short(want)
            )),
            Ok(Err(e)) => violations.push(format!("B8 read model note: {domain} {id}: {e}")),
            Err(_) => violations.push(format!(
                "B8 read model note: {domain} {id}: no answer in 60 s"
            )),
        }
    }
    let info = vec![
        format!("bank_x deposits decided by {:?}", checked.by_x),
        format!(
            "notes with integers beyond 2^53 rounded (known: JavaScript, Go's generic JSON): {:?}",
            checked.rounded
        ),
        format!("hostile: {:?}", load.hostile.stats.lock().unwrap()),
    ];
    Report {
        head,
        violations,
        took: started.elapsed(),
        info,
    }
}

/// A value for a message: its JSON, at most 200 characters.
fn short(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 200 {
        format!("{}…", s.chars().take(200).collect::<String>())
    } else {
        s
    }
}

/// H1–H4: no rogue event and no event of the attacker's in the log; every replay answered with
/// the stored outcome of its command; what the hostile participants saw at once.
async fn hostile_rules(store: &str, log: &Log, load: &Load, violations: &mut Vec<String>) {
    for e in &log.events {
        if String::from_utf8_lossy(&e.payload).contains(r#""by":"rogue""#) {
            violations.push(format!(
                "H2 rogue worker: its event at {} ({} {} {}) is in the log",
                e.global_position, e.domain, e.aggregate_id, e.event_type
            ));
        }
        if e.command_id.starts_with("evil-") || e.tenant_id == crate::hostile::EVIL_TENANT {
            violations.push(format!(
                "H4 attacker: event at {} of command {} in tenant {}",
                e.global_position, e.command_id, e.tenant_id
            ));
        }
    }
    violations.extend(load.hostile.take_found());
    let replays = load.hostile.take_replays();
    if replays.is_empty() {
        return;
    }
    let ids = replays.iter().map(|(_, id, _)| id.clone()).collect();
    match results_of(store, ids).await {
        Ok(stored) => {
            for (variant, id, answer) in replays {
                let want = stored
                    .get(&id)
                    .and_then(|r| r.as_ref())
                    .and_then(crate::hostile::answer_of);
                if want.as_ref() != Some(&answer) {
                    violations.push(format!(
                        "H1 replay {variant} of {id} was answered {answer:?}, its command's stored outcome is {want:?}"
                    ));
                }
            }
        }
        Err(e) => violations.push(format!("H1: reading the stored outcomes: {e}")),
    }
}

/// B1 and B2 over the new events, from the accounts and flags the rounds before left.
fn bank_rules(log: &Log, checked: &mut Checked, violations: &mut Vec<String>) {
    for e in &log.events {
        let Some(domain) = DOMAINS.iter().find(|d| **d == e.domain) else {
            continue;
        };
        let data: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
        let key = (*domain, e.aggregate_id.clone());
        let account = checked.accounts.entry(key.clone()).or_default();
        let amount = data["amount"].as_i64().unwrap_or(0);
        match e.event_type.as_str() {
            "Deposited" => {
                account.0 += amount;
                if *domain == "bank_x" {
                    let by = data["by"].as_str().unwrap_or("?").to_owned();
                    *checked.by_x.entry(by).or_default() += 1;
                }
            }
            "Withdrawn" => {
                account.0 -= amount;
                if account.0 < 0 {
                    violations.push(format!(
                        "B1 never below zero: {domain} {} at {} after event {}",
                        e.aggregate_id, account.0, e.global_position
                    ));
                }
                if amount >= 50 {
                    checked.big.insert(e.global_position, key);
                }
            }
            "Flagged" => {
                account.1 += 1;
                let at = data["position"].as_u64().unwrap_or(0);
                checked.flagged.entry(at).or_default().push(key);
            }
            "Noted" => {
                // B8: the note as its command sent it.
                let sent = data["k"]
                    .as_u64()
                    .and_then(|k| serde_json::from_str::<Value>(&note_payload(k)).ok());
                match sent {
                    Some(sent) if same_json(&sent, &data) => {}
                    Some(sent)
                        if ROUNDS_BIG_INTEGERS.contains(domain) && only_rounded(&sent, &data) =>
                    {
                        *checked.rounded.entry((*domain).to_owned()).or_default() += 1;
                    }
                    Some(sent) => violations.push(format!(
                        "B8 payload unchanged: {domain} {} at {} holds {}, its command sent {}",
                        e.aggregate_id,
                        e.global_position,
                        short(&data),
                        short(&sent)
                    )),
                    None => violations.push(format!(
                        "B8 payload unchanged: {domain} {} at {} holds {} (no note k)",
                        e.aggregate_id,
                        e.global_position,
                        short(&data)
                    )),
                }
                checked.notes.insert(key, data);
            }
            "Opened" | "Closed" | "Numbered" => {}
            other => violations.push(format!("unexpected event {other} at {}", e.global_position)),
        }
    }
    // Flags: each for a big withdrawal, once.
    for (position, flags) in &checked.flagged {
        match checked.big.get(position) {
            Some(key) if flags.len() == 1 && &flags[0] == key => {}
            Some(key) => violations.push(format!(
                "B2 flagged once: the withdrawal at {position} ({key:?}) was flagged as {flags:?}"
            )),
            None => violations.push(format!(
                "B2 flagged once: {flags:?} flag position {position}, no big withdrawal"
            )),
        }
    }
    // Reported once: kept as one entry so the next rounds see it again only if it changes.
    checked
        .flagged
        .retain(|p, f| checked.big.contains_key(p) && f.len() == 1);
}

/// B7: what the flaggers parked, against the withdrawals and their `Flag` commands; and every
/// big withdrawal not flagged by now (everything is quiet) was parked: nothing lost silently.
async fn parking_rules(
    store: &str,
    log: &Log,
    checked: &mut Checked,
    violations: &mut Vec<String>,
) {
    let mut parked: BTreeMap<u64, (String, String, String)> = BTreeMap::new();
    for l in log
        .listeners
        .iter()
        .filter(|l| l.name.starts_with("flagger-"))
    {
        for p in &l.parked {
            parked.insert(
                p.position,
                (l.name.clone(), p.code.clone(), p.message.clone()),
            );
        }
    }
    let open: Vec<u64> = checked
        .big
        .keys()
        .filter(|p| !checked.settled.contains(p) && !checked.flagged.contains_key(p))
        .copied()
        .collect();
    let fresh: Vec<u64> = parked
        .keys()
        .filter(|p| !checked.parked_seen.contains(p))
        .copied()
        .collect();
    let ids: Vec<String> = open
        .iter()
        .chain(&fresh)
        .map(|p| format!("flag-{p}"))
        .collect();
    let results = match results_of(store, ids).await {
        Ok(r) => r,
        Err(e) => {
            violations.push(format!("B7: reading the Flag commands: {e}"));
            return;
        }
    };
    let describe = |position: u64| -> String {
        let id = format!("flag-{position}");
        match results.get(&id) {
            None | Some(None) => format!("{id} was never stored, or is pending"),
            Some(Some(result)) => match &result.outcome {
                Some(Outcome::Rejected(r)) => format!("{id} rejected ({}: {})", r.code, r.message),
                Some(Outcome::Committed(_)) => format!("{id} committed, without a Flagged event"),
                other => format!("{id}: {other:?}"),
            },
        }
    };
    for position in &fresh {
        let (listener, code, message) = &parked[position];
        let rejected = match results.get(&format!("flag-{position}")) {
            Some(Some(result)) => match &result.outcome {
                Some(Outcome::Rejected(r)) => Some(r.code.clone()),
                _ => None,
            },
            _ => None,
        };
        if !checked.big.contains_key(position)
            || checked.flagged.contains_key(position)
            || rejected.as_deref() != Some(code.as_str())
        {
            violations.push(format!(
                "B7 justified parking: {listener} parked the event at {position} ({code}: \
                 {message}); its Flag command: {}",
                describe(*position)
            ));
        }
        checked.parked_seen.insert(*position);
    }
    for position in open {
        if !parked.contains_key(&position) {
            violations.push(format!(
                "B7 nothing lost silently: the withdrawal at {position} ({:?}) was neither \
                 flagged nor parked; its Flag command: {}",
                checked.big[&position],
                describe(position)
            ));
        }
        checked.settled.insert(position);
    }
    // Flagged ones are settled too: a later flag of them shows in `flagged` again.
    let flagged: Vec<u64> = checked.flagged.keys().copied().collect();
    checked.settled.extend(flagged);
}

/// What held a number or whether an account was open, from a log position on: in order.
type History<T> = Vec<(u64, T)>;

/// Whether `holds` is true of the value at some position from `after` to before `before`.
fn ever<T>(
    history: &[(u64, T)],
    after: u64,
    before: u64,
    holds: impl Fn(Option<&T>) -> bool,
) -> bool {
    let at = history
        .iter()
        .rev()
        .find(|(p, _)| *p <= after)
        .map(|(_, v)| v);
    holds(at)
        || history
            .iter()
            .any(|(p, v)| *p > after && *p < before && holds(Some(v)))
}

/// B4, B5 and B6 over the new events, the kernel's index and the refusals new since the round
/// before.
fn index_rules(
    log: &Log,
    checked: &mut Checked,
    refusals: &[Refusal],
    violations: &mut Vec<String>,
) {
    for e in &log.events {
        let Some(domain) = DOMAINS.iter().find(|d| **d == e.domain) else {
            continue;
        };
        let data: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
        let (id, at) = (e.aggregate_id.clone(), e.global_position);
        let account = checked
            .numbers
            .entry((domain, id.clone()))
            .or_default()
            .clone();
        match e.event_type.as_str() {
            "Numbered" => {
                let number = data["number"].as_str().map(str::to_owned);
                if let Some(n) = &number
                    && let Some((_, Some(other))) = checked
                        .holders
                        .get(&(*domain, n.clone()))
                        .and_then(|h| h.last())
                    && *other != id
                {
                    violations.push(format!(
                        "B4 one holder: {domain} {id} took number {n} at {at}, held by {other}"
                    ));
                }
                if account.0 != number {
                    if let Some(old) = &account.0 {
                        checked
                            .holders
                            .entry((domain, old.clone()))
                            .or_default()
                            .push((at, None));
                    }
                    if let Some(n) = &number {
                        checked
                            .holders
                            .entry((domain, n.clone()))
                            .or_default()
                            .push((at, Some(id.clone())));
                    }
                }
                checked
                    .numbers
                    .get_mut(&(*domain, id.clone()))
                    .expect("entered")
                    .0 = number;
            }
            "Opened" | "Closed" => {
                let open = e.event_type == "Opened";
                checked
                    .numbers
                    .get_mut(&(*domain, id.clone()))
                    .expect("entered")
                    .1 = open;
                checked
                    .opens
                    .entry((domain, id.clone()))
                    .or_default()
                    .push((at, open));
            }
            "Withdrawn" => {
                if let Some(to) = data["to"].as_str()
                    && !checked
                        .numbers
                        .get(&(*domain, to.to_owned()))
                        .is_some_and(|a| a.1)
                {
                    violations.push(format!(
                        "B5 requirement: the withdrawal at {at} from {domain} {id} was sent to \
                         {to}, which was closed"
                    ));
                }
            }
            _ => {}
        }
    }

    // B4: the index holds what the log leaves every account with.
    type Held = (Vec<(String, Vec<u8>)>, Vec<String>);
    let mut want: BTreeMap<(String, String), Held> = BTreeMap::new();
    for ((domain, id), (number, open)) in &checked.numbers {
        let keys = number.as_ref().map_or(Vec::new(), |n| {
            let stream = Command {
                tenant_id: TENANT.into(),
                domain: (*domain).into(),
                aggregate_type: "account".into(),
                ..Default::default()
            };
            let key = UniqueKey {
                name: "number".into(),
                value_hash: Sha256::digest(n.as_bytes()).to_vec(),
                ..Default::default()
            };
            vec![(
                "number".to_owned(),
                signing::unique_key_id(&stream, &key).to_vec(),
            )]
        });
        let facts = if *open {
            vec!["open".to_owned()]
        } else {
            Vec::new()
        };
        want.insert(((*domain).to_owned(), id.clone()), (keys, facts));
    }
    let mut got: BTreeMap<(String, String), Held> = BTreeMap::new();
    for (stream, index) in &log.index {
        if !DOMAINS.contains(&stream.domain.as_str()) {
            continue;
        }
        let mut keys: Vec<_> = index
            .keys
            .iter()
            .map(|k| (k.name.clone(), k.id.clone()))
            .collect();
        keys.sort();
        let held = (keys, index.facts.clone());
        got.insert((stream.domain.clone(), stream.aggregate_id.clone()), held);
    }
    for key in want.keys().chain(got.keys()).collect::<BTreeSet<_>>() {
        let (w, g) = (
            want.get(key).cloned().unwrap_or_default(),
            got.get(key).cloned().unwrap_or_default(),
        );
        if w != g {
            violations.push(format!(
                "B4 index: {} {} holds {} key(s) and facts {:?}, the log says {} key(s) and facts {:?}{}",
                key.0,
                key.1,
                g.0.len(),
                g.1,
                w.0.len(),
                w.1,
                if g.0.len() == w.0.len() { " (another number)" } else { "" }
            ));
        }
    }

    // B6: every refusal was justified at some position the kernel may have decided at.
    for r in refusals {
        let c = &r.command;
        let Some(domain) = DOMAINS.iter().find(|d| **d == c.domain) else {
            continue;
        };
        let data: Value = serde_json::from_slice(&c.payload).unwrap_or(Value::Null);
        let before = r.before.unwrap_or(u64::MAX);
        let id = c.aggregate_id.as_str();
        let justified = match (r.code.as_str(), c.command_type.as_str()) {
            ("unique_taken", "Number") => data["number"].as_str().is_some_and(|n| {
                checked
                    .holders
                    .get(&(*domain, n.to_owned()))
                    .is_some_and(|h| {
                        ever(
                            h,
                            r.after,
                            before,
                            |v| matches!(v, Some(Some(other)) if other != id),
                        )
                    })
            }),
            ("requirement_missing", "Withdraw") => data["to"].as_str().is_some_and(|to| {
                let h = checked
                    .opens
                    .get(&(*domain, to.to_owned()))
                    .map_or(&[][..], Vec::as_slice);
                ever(h, r.after, before, |v| v != Some(&true))
            }),
            _ => false,
        };
        if !justified {
            violations.push(format!(
                "B6 justified refusal: {} {} {domain} {id} {} was rejected with {} between \
                 positions {} and {before}",
                c.command_id,
                c.command_type,
                String::from_utf8_lossy(&c.payload),
                r.code,
                r.after
            ));
        }
    }
}
