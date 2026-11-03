//! Backend-independent integrity check: hash chain, checkpoints, worker and client signatures.

use std::collections::BTreeMap;

use kernable_core::{
    proto::{Command, Event, NewEvent, RevocationRecord},
    signing::{self, GENESIS},
};

use crate::Checkpoint;

/// Outcome of [`crate::Store::verify`]. The log is intact if `problems` is empty.
#[derive(Debug, Default)]
pub struct Report {
    pub events: u64,
    pub commands: u64,
    pub checkpoints: u64,
    /// Events after the last valid checkpoint; truncating them cannot be detected.
    pub unanchored: u64,
    pub problems: Vec<String>,
    /// Commands and events signed by a revoked key at or after its revocation's `since`:
    /// intact, but not to be trusted.
    pub suspects: Vec<String>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.problems.is_empty()
    }
}

/// Feed checkpoints first, then all events in global order (with their stored `prev_hash`),
/// then commands; `finish` returns the report.
pub(crate) struct Verifier {
    report: Report,
    /// Checkpoints with a valid kernel signature: position -> chain hash.
    anchors: BTreeMap<u64, Vec<u8>>,
    last_anchor: u64,
    prev: Vec<u8>,
    /// Consecutive events of one command, checked together against its worker signature.
    group: Vec<Event>,
    /// Revoked key ID -> since when (ms since epoch), from kernel-signed revocations.
    revoked: BTreeMap<String, i64>,
}

impl Verifier {
    /// `kernel_key_ids`: every key the kernel signed with, also before a key change.
    pub fn new(
        kernel_key_ids: &[String],
        checkpoints: impl IntoIterator<Item = Checkpoint>,
        revocations: impl IntoIterator<Item = RevocationRecord>,
    ) -> Self {
        let mut report = Report::default();
        let mut anchors = BTreeMap::new();
        let mut revoked = BTreeMap::new();
        for r in revocations {
            let sig = r.kernel_sig.clone().unwrap_or_default();
            let msg = signing::revocation_message(&r.key_id, r.since, r.created_at);
            if !kernel_key_ids.contains(&sig.key_id) || signing::verify(&sig, &msg).is_err() {
                report.problems.push(format!(
                    "revocation of key {}: not signed by a kernel key",
                    r.key_id
                ));
            } else {
                revoked.insert(r.key_id, r.since);
            }
        }
        for cp in checkpoints {
            report.checkpoints += 1;
            let pos = cp.global_position;
            let msg = signing::checkpoint_message(pos, &cp.chain_hash, cp.created_at);
            if !kernel_key_ids.contains(&cp.kernel_sig.key_id) {
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
            revoked,
        }
    }

    /// Whether `sig` is of a key revoked at or before `at`.
    fn suspect(&self, key_id: &str, at: i64) -> bool {
        self.revoked.get(key_id).is_some_and(|since| at >= *since)
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

    /// A stored command and when the kernel accepted it.
    pub fn command(&mut self, cmd: &Command, created_at: i64) {
        self.report.commands += 1;
        if let Some(sig) = &cmd.client_sig
            && self.suspect(&sig.key_id, created_at)
        {
            self.report.suspects.push(format!(
                "command {}: signed by revoked key {}",
                cmd.command_id, sig.key_id
            ));
        }
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
            domain: first.domain.clone(),
            aggregate_type: first.aggregate_type.clone(),
            aggregate_id: first.aggregate_id.clone(),
            ..Default::default()
        };
        let events: Vec<NewEvent> = group
            .iter()
            .map(|e| NewEvent {
                event_type: e.event_type.clone(),
                payload: e.payload.clone(),
                schema: e.schema,
                vault_refs: e.vault_refs.clone(),
                erases: e.erases.clone(),
                origin_partition: e.origin_partition.clone(),
                origin_position: e.origin_position,
            })
            .collect();
        let msg = signing::result_message(&cmd, first.version.saturating_sub(1), &events);
        let valid = first.producer_sig.as_ref().is_some_and(|sig| {
            group.iter().all(|e| e.producer_sig.as_ref() == Some(sig))
                && signing::verify(sig, &msg).is_ok()
        });
        if let Some(sig) = &first.producer_sig
            && self.suspect(&sig.key_id, first.created_at)
        {
            self.report.suspects.push(format!(
                "command {}: events {}..={} written by revoked key {}",
                first.command_id,
                first.global_position,
                group.last().map_or(0, |e| e.global_position),
                sig.key_id
            ));
        }
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
