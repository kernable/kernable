//! A check round, while the load and the faults pause: wait until every listener has caught
//! up, then check every invariant of the store (S1–S9) and of what clients and the audit
//! listener were told (O1–O4), and the apps' own rules against the log:
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
//!   client's commands before and after it (and no other command saw either).

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use kernable::{Kernable, Query};
use kernable_core::{
    proto::{Command, Event, UniqueKey},
    signing,
};
use kernable_store::invariants::Log;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::load::{ACCOUNTS, DOMAINS, Load, Refusal, TENANT};

pub struct Report {
    pub head: u64,
    pub violations: Vec<String>,
    pub took: Duration,
}

/// Read from a fresh read-only open (a file store is a snapshot): the head, every listener's
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

/// Waits until nothing is pending and every listener is at the head, twice in a row.
async fn quiesce(store: &str, limit: Duration) -> Result<u64, String> {
    let started = Instant::now();
    let mut last = None;
    loop {
        let state = positions(store);
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

pub async fn round(store: &str, kernel_key_id: &str, load: &Load, client: &Kernable) -> Report {
    let started = Instant::now();
    let mut violations = Vec::new();
    let head = match quiesce(store, Duration::from_secs(300)).await {
        Ok(head) => head,
        Err(e) => {
            violations.push(e);
            0
        }
    };
    let mut ids = load.observed.lock().unwrap().command_ids();
    ids.extend(load.stats.unanswered.lock().unwrap().iter().cloned());
    let log = kernable_store::open_read_only(store)
        .and_then(|read| Log::read_with(read.as_ref(), &[kernel_key_id.to_owned()], &ids));
    let log = match log {
        Ok(log) => log,
        Err(e) => {
            violations.push(format!("reading the store: {e}"));
            return Report {
                head,
                violations,
                took: started.elapsed(),
            };
        }
    };
    violations.extend(log.check().iter().map(ToString::to_string));
    violations.extend(
        load.observed
            .lock()
            .unwrap()
            .check(&log)
            .iter()
            .map(ToString::to_string),
    );
    let expected = bank_rules(&log.events, &mut violations);
    index_rules(&log, &load.refusals.lock().unwrap(), &mut violations);

    // B3: every account of every app, as its read model answers.
    for domain in DOMAINS {
        for a in 0..ACCOUNTS {
            let id = format!("a{a}");
            let want = expected
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
    Report {
        head,
        violations,
        took: started.elapsed(),
    }
}

/// B1 and B2 over the log; the balance and flags of every account.
fn bank_rules(
    events: &[Event],
    violations: &mut Vec<String>,
) -> BTreeMap<(&'static str, String), (i64, i64)> {
    let mut accounts: BTreeMap<(&'static str, String), (i64, i64)> = BTreeMap::new();
    // Withdrawals of 50 or more by position, and how often each was flagged.
    let mut big: BTreeMap<u64, (&'static str, String)> = BTreeMap::new();
    let mut flagged: BTreeMap<u64, Vec<(&'static str, String)>> = BTreeMap::new();
    for e in events {
        let Some(domain) = DOMAINS.iter().find(|d| **d == e.domain) else {
            continue;
        };
        let data: Value = serde_json::from_slice(&e.payload).unwrap_or(Value::Null);
        let key = (*domain, e.aggregate_id.clone());
        let account = accounts.entry(key.clone()).or_default();
        let amount = data["amount"].as_i64().unwrap_or(0);
        match e.event_type.as_str() {
            "Deposited" => account.0 += amount,
            "Withdrawn" => {
                account.0 -= amount;
                if account.0 < 0 {
                    violations.push(format!(
                        "B1 never below zero: {domain} {} at {} after event {}",
                        e.aggregate_id, account.0, e.global_position
                    ));
                }
                if amount >= 50 {
                    big.insert(e.global_position, key);
                }
            }
            "Flagged" => {
                account.1 += 1;
                let at = data["position"].as_u64().unwrap_or(0);
                flagged.entry(at).or_default().push(key);
            }
            "Opened" | "Closed" | "Numbered" => {}
            other => violations.push(format!("unexpected event {other} at {}", e.global_position)),
        }
    }
    for (position, key) in &big {
        match flagged.get(position).map(Vec::as_slice) {
            Some([one]) if one == key => {}
            Some(flags) => violations.push(format!(
                "B2 flagged once: the withdrawal at {position} ({key:?}) was flagged as {flags:?}"
            )),
            None => violations.push(format!(
                "B2 flagged once: the withdrawal at {position} ({key:?}) was never flagged"
            )),
        }
    }
    for (position, flags) in &flagged {
        if !big.contains_key(position) {
            violations.push(format!(
                "B2 flagged once: {flags:?} flag position {position}, no big withdrawal"
            ));
        }
    }
    accounts
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

/// B4, B5 and B6 over the log, the kernel's index and the refusals the load saw.
fn index_rules(log: &Log, refusals: &[Refusal], violations: &mut Vec<String>) {
    // Per account: its number and whether it is open; per number, who held it; per account,
    // when it opened and closed.
    let mut accounts: BTreeMap<(&str, &str), (Option<&str>, bool)> = BTreeMap::new();
    let mut holders: BTreeMap<(&str, &str), History<Option<&str>>> = BTreeMap::new();
    let mut opens: BTreeMap<(&str, &str), History<bool>> = BTreeMap::new();
    let payloads: Vec<Value> = log
        .events
        .iter()
        .map(|e| serde_json::from_slice(&e.payload).unwrap_or(Value::Null))
        .collect();
    for (e, data) in log.events.iter().zip(&payloads) {
        let Some(domain) = DOMAINS.iter().find(|d| **d == e.domain) else {
            continue;
        };
        let (id, at) = (e.aggregate_id.as_str(), e.global_position);
        let account = accounts.entry((domain, id)).or_default();
        match e.event_type.as_str() {
            "Numbered" => {
                let number = data["number"].as_str();
                if let Some(n) = number
                    && let Some((_, Some(other))) =
                        holders.get(&(*domain, n)).and_then(|h| h.last())
                    && *other != id
                {
                    violations.push(format!(
                        "B4 one holder: {domain} {id} took number {n} at {at}, held by {other}"
                    ));
                }
                if account.0 != number {
                    if let Some(old) = account.0 {
                        holders.entry((domain, old)).or_default().push((at, None));
                    }
                    if let Some(n) = number {
                        holders.entry((domain, n)).or_default().push((at, Some(id)));
                    }
                }
                account.0 = number;
            }
            "Opened" | "Closed" => {
                account.1 = e.event_type == "Opened";
                opens.entry((domain, id)).or_default().push((at, account.1));
            }
            "Withdrawn" => {
                if let Some(to) = data["to"].as_str()
                    && !accounts.get(&(*domain, to)).is_some_and(|a| a.1)
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
    for (&(domain, id), &(number, open)) in &accounts {
        let keys = number.map_or(Vec::new(), |n| {
            let stream = Command {
                tenant_id: TENANT.into(),
                domain: domain.into(),
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
        let facts = if open {
            vec!["open".to_owned()]
        } else {
            Vec::new()
        };
        want.insert((domain.to_owned(), id.to_owned()), (keys, facts));
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
                holders.get(&(*domain, n)).is_some_and(|h| {
                    ever(
                        h,
                        r.after,
                        before,
                        |v| matches!(v, Some(Some(other)) if *other != id),
                    )
                })
            }),
            ("requirement_missing", "Withdraw") => data["to"].as_str().is_some_and(|to| {
                let h = opens.get(&(*domain, to)).map_or(&[][..], Vec::as_slice);
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
