//! A check round, while the load and the faults pause: wait until every listener has caught
//! up, then check every invariant of the store (S1–S8) and of what clients and the audit
//! listener were told (O1–O4), and the apps' own rules against the log:
//!
//! - B1: no account is ever below zero;
//! - B2: every withdrawal of 50 or more is flagged exactly once, and nothing else is;
//! - B3: each app's read model holds the balance and flags the log says.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use kernable::{Kernable, Query};
use kernable_core::proto::Event;
use kernable_store::invariants::Log;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::load::{ACCOUNTS, DOMAINS, Load};

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
