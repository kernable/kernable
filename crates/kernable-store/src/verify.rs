//! Backend-independent integrity check: hash chain, checkpoints, worker and client signatures.

use std::collections::BTreeMap;

use kernable_core::{
    proto::{Command, Event, NewEvent, SnapshotRecord},
    signing::{self, GENESIS},
};

use crate::Checkpoint;

/// Outcome of [`crate::Store::verify`]. The log is intact if `problems` is empty.
#[derive(Debug, Default)]
pub struct Report {
    pub events: u64,
    pub commands: u64,
    pub checkpoints: u64,
    pub snapshots: u64,
    /// Events after the last valid checkpoint; truncating them cannot be detected.
    pub unanchored: u64,
    pub problems: Vec<String>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Feed checkpoints first, then all events in global order (with their stored `prev_hash`),
/// then snapshots and commands; `finish` returns the report.
pub(crate) struct Verifier {
    report: Report,
    /// Checkpoints with a valid kernel signature: position -> chain hash.
    anchors: BTreeMap<u64, Vec<u8>>,
    last_anchor: u64,
    prev: Vec<u8>,
    /// Consecutive events of one command, checked together against its worker signature.
    group: Vec<Event>,
}

impl Verifier {
    pub fn new(kernel_key_id: &str, checkpoints: impl IntoIterator<Item = Checkpoint>) -> Self {
        let mut report = Report::default();
        let mut anchors = BTreeMap::new();
        for cp in checkpoints {
            report.checkpoints += 1;
            let pos = cp.global_position;
            let msg = signing::checkpoint_message(pos, &cp.chain_hash, cp.created_at);
            if cp.kernel_sig.key_id != kernel_key_id {
                let key = &cp.kernel_sig.key_id;
                report
                    .problems
                    .push(format!("checkpoint {pos}: signed by unknown key {key}"));
            } else if signing::verify(&cp.kernel_sig, &msg).is_err() {
                report
                    .problems
                    .push(format!("checkpoint {pos}: invalid kernel signature"));
            } else {
                anchors.insert(pos, cp.chain_hash);
            }
        }
        let last_anchor = anchors.keys().next_back().copied().unwrap_or(0);
        Verifier {
            report,
            anchors,
            last_anchor,
            prev: GENESIS.to_vec(),
            group: Vec::new(),
        }
    }

    pub fn event(&mut self, e: Event, prev_hash: &[u8]) {
        let pos = e.global_position;
        let report = &mut self.report;
        report.events += 1;
        if pos > self.last_anchor {
            report.unanchored += 1;
        }
        if prev_hash != self.prev {
            report.problems.push(format!(
                "event {pos}: link to previous event broken (deleted or reordered)"
            ));
        }
        if signing::event_hash(prev_hash, &e)[..] != e.hash[..] {
            report
                .problems
                .push(format!("event {pos}: hash mismatch (modified)"));
        }
        if let Some(anchor) = self.anchors.remove(&pos)
            && anchor != e.hash
        {
            report.problems.push(format!(
                "checkpoint {pos}: chain hash differs (log rewritten)"
            ));
        }
        if self
            .group
            .first()
            .is_some_and(|g| g.command_id != e.command_id)
        {
            self.check_producer();
        }
        self.prev = e.hash.clone();
        self.group.push(e);
    }

    /// Checks a snapshot's worker signature and that it belongs to the stored stream:
    /// `event_hash` is the hash of the stream's event at the snapshot's version, if any.
    pub fn snapshot(&mut self, record: &SnapshotRecord, event_hash: Option<&[u8]>) {
        self.report.snapshots += 1;
        let stream = format!(
            "{}/{}/{}",
            record.tenant_id, record.aggregate_type, record.aggregate_id
        );
        let s = record.snapshot.clone().unwrap_or_default();
        let cmd = Command {
            tenant_id: record.tenant_id.clone(),
            aggregate_type: record.aggregate_type.clone(),
            aggregate_id: record.aggregate_id.clone(),
            ..Default::default()
        };
        let msg = signing::snapshot_message(&cmd, s.version, &s.schema, &s.state);
        if s.worker_sig
            .as_ref()
            .is_none_or(|sig| signing::verify(sig, &msg).is_err())
        {
            self.report.problems.push(format!(
                "snapshot of {stream} at version {}: invalid worker signature",
                s.version
            ));
        }
        if event_hash != Some(record.event_hash.as_slice()) {
            self.report.problems.push(format!(
                "snapshot of {stream} at version {}: does not match the stored events",
                s.version
            ));
        }
    }

    pub fn command(&mut self, cmd: &Command) {
        self.report.commands += 1;
        let valid = cmd
            .client_sig
            .as_ref()
            .is_some_and(|sig| signing::verify(sig, &signing::command_message(cmd)).is_ok());
        if !valid {
            self.report.problems.push(format!(
                "command {}: missing or invalid client signature",
                cmd.command_id
            ));
        }
    }

    pub fn finish(mut self) -> Report {
        self.check_producer();
        for pos in self.anchors.keys() {
            self.report
                .problems
                .push(format!("checkpoint {pos}: event missing (log truncated)"));
        }
        self.report
    }

    /// Checks the worker signature over the events one command produced.
    fn check_producer(&mut self) {
        let group = std::mem::take(&mut self.group);
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
            self.report.problems.push(format!(
                "command {}: invalid worker signature on events {}..={}",
                first.command_id,
                first.global_position,
                group.last().map_or(0, |e| e.global_position)
            ));
        }
    }
}
