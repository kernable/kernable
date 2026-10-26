use std::collections::BTreeMap;

use kernable_core::{
    proto::{Command, Event, NewEvent, Signature},
    signing::{self, GENESIS},
};
use rusqlite::Connection;

use crate::{EVENT_COLUMNS, Result, event_from_row, join_sig};

/// Outcome of [`crate::SqliteStore::verify`]. The log is intact if `problems` is empty.
#[derive(Debug, Default)]
pub struct Report {
    pub events: u64,
    pub commands: u64,
    pub checkpoints: u64,
    /// Events after the last valid checkpoint; truncating them cannot be detected.
    pub unanchored: u64,
    pub problems: Vec<String>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.problems.is_empty()
    }
}

pub(crate) fn run(conn: &Connection, kernel_key_id: &str) -> Result<Report> {
    let mut report = Report::default();

    // Checkpoints with a valid kernel signature become anchors: position -> chain hash.
    let mut anchors = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT global_position, chain_hash, kernel_key_id, kernel_sig, created_at
         FROM checkpoints ORDER BY global_position",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let (pos, chain_hash, key_id, sig): (u64, Vec<u8>, String, Vec<u8>) =
            (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?);
        report.checkpoints += 1;
        let msg = signing::checkpoint_message(pos, &chain_hash, r.get(4)?);
        if key_id != kernel_key_id {
            report
                .problems
                .push(format!("checkpoint {pos}: signed by unknown key {key_id}"));
        } else if signing::verify(&Signature { key_id, sig }, &msg).is_err() {
            report
                .problems
                .push(format!("checkpoint {pos}: invalid kernel signature"));
        } else {
            anchors.insert(pos, chain_hash);
        }
    }
    let last_anchor = anchors.keys().next_back().copied().unwrap_or(0);

    // Events: recompute the chain and check worker signatures per command.
    let mut stmt = conn.prepare(&format!(
        "SELECT {EVENT_COLUMNS}, prev_hash FROM events ORDER BY global_position"
    ))?;
    let mut rows = stmt.query([])?;
    let mut prev = GENESIS.to_vec();
    let mut group: Vec<Event> = Vec::new();
    while let Some(r) = rows.next()? {
        let e = event_from_row(r)?;
        let prev_hash: Vec<u8> = r.get(12)?;
        let pos = e.global_position;
        report.events += 1;
        if pos > last_anchor {
            report.unanchored += 1;
        }
        if prev_hash != prev {
            report.problems.push(format!(
                "event {pos}: link to previous event broken (deleted or reordered)"
            ));
        }
        if signing::event_hash(&prev_hash, &e)[..] != e.hash[..] {
            report
                .problems
                .push(format!("event {pos}: hash mismatch (modified)"));
        }
        if let Some(anchor) = anchors.remove(&pos)
            && anchor != e.hash
        {
            report.problems.push(format!(
                "checkpoint {pos}: chain hash differs (log rewritten)"
            ));
        }
        if group.first().is_some_and(|g| g.command_id != e.command_id) {
            check_producer(&group, &mut report);
            group.clear();
        }
        prev = e.hash.clone();
        group.push(e);
    }
    check_producer(&group, &mut report);
    for pos in anchors.keys() {
        report
            .problems
            .push(format!("checkpoint {pos}: event missing (log truncated)"));
    }

    // Commands: client signatures.
    let mut stmt = conn.prepare(
        "SELECT command_id, tenant_id, aggregate_type, aggregate_id, command_type, payload,
                client_key_id, client_sig
         FROM commands ORDER BY created_at, rowid",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let cmd = Command {
            command_id: r.get(0)?,
            tenant_id: r.get(1)?,
            aggregate_type: r.get(2)?,
            aggregate_id: r.get(3)?,
            command_type: r.get(4)?,
            payload: r.get(5)?,
            client_sig: join_sig(r.get(6)?, r.get(7)?),
        };
        report.commands += 1;
        let valid = cmd
            .client_sig
            .as_ref()
            .is_some_and(|sig| signing::verify(sig, &signing::command_message(&cmd)).is_ok());
        if !valid {
            report.problems.push(format!(
                "command {}: missing or invalid client signature",
                cmd.command_id
            ));
        }
    }

    Ok(report)
}

/// Checks the worker signature over the events one command produced.
fn check_producer(group: &[Event], report: &mut Report) {
    let Some(first) = group.first() else {
        return;
    };
    let cmd = Command {
        command_id: first.command_id.clone(),
        tenant_id: first.tenant_id.clone(),
        aggregate_type: first.aggregate_type.clone(),
        aggregate_id: first.aggregate_id.clone(),
        ..Default::default()
    };
    let events: Vec<NewEvent> = group
        .iter()
        .map(|e| NewEvent {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
        })
        .collect();
    let msg = signing::result_message(&cmd, first.version.saturating_sub(1), &events);
    let valid = first.producer_sig.as_ref().is_some_and(|sig| {
        group.iter().all(|e| e.producer_sig.as_ref() == Some(sig))
            && signing::verify(sig, &msg).is_ok()
    });
    if !valid {
        report.problems.push(format!(
            "command {}: invalid worker signature on events {}..={}",
            first.command_id,
            first.global_position,
            group.last().map_or(0, |e| e.global_position)
        ));
    }
}
