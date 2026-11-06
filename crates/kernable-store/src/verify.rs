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
    /// Problems found after the start, by when a check in order would have found them.
    found: Vec<(u64, String)>,
    order: u64,
    /// Signatures still to check, on every core at once: they are most of a check's time.
    jobs: Vec<Job>,
}

/// A signature to check, and the problem it is if it does not hold.
struct Job {
    order: u64,
    sig: kernable_core::proto::Signature,
    msg: Vec<u8>,
    problem: String,
}

/// How many signatures are checked together.
const BATCH: usize = 8192;

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
            found: Vec::new(),
            order: 0,
            jobs: Vec::new(),
        }
    }

    fn problem(&mut self, problem: String) {
        self.found.push((self.order, problem));
        self.order += 1;
    }

    /// Checks `sig` over `msg` later, with others; `problem` if it does not hold.
    fn check_later(&mut self, sig: kernable_core::proto::Signature, msg: Vec<u8>, problem: String) {
        self.jobs.push(Job {
            order: self.order,
            sig,
            msg,
            problem,
        });
        self.order += 1;
        if self.jobs.len() >= BATCH {
            self.check_jobs();
        }
    }

    /// Checks the signatures collected so far, spread over the machine's cores.
    fn check_jobs(&mut self) {
        let jobs = std::mem::take(&mut self.jobs);
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        let chunk = jobs.len().div_ceil(threads).max(1);
        let failed: Vec<(u64, String)> = std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .chunks(chunk)
                .map(|part| {
                    scope.spawn(move || {
                        part.iter()
                            .filter(|j| signing::verify(&j.sig, &j.msg).is_err())
                            .map(|j| (j.order, j.problem.clone()))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().expect("a signature check does not panic"))
                .collect()
        });
        self.found.extend(failed);
    }

    /// Whether `sig` is of a key revoked at or before `at`.
    fn suspect(&self, key_id: &str, at: i64) -> bool {
        self.revoked.get(key_id).is_some_and(|since| at >= *since)
    }

    pub fn event(&mut self, e: Event, prev_hash: &[u8]) {
        let pos = e.global_position;
        self.report.events += 1;
        if pos > self.last_anchor {
            self.report.unanchored += 1;
        }
        if prev_hash != self.prev {
            self.problem(format!(
                "event {pos}: link to previous event broken (deleted or reordered)"
            ));
        }
        if signing::event_hash(prev_hash, &e)[..] != e.hash[..] {
            self.problem(format!("event {pos}: hash mismatch (modified)"));
        }
        if let Some(anchor) = self.anchors.remove(&pos)
            && anchor != e.hash
        {
            self.problem(format!(
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
        let problem = format!(
            "command {}: missing or invalid client signature",
            cmd.command_id
        );
        match &cmd.client_sig {
            Some(sig) => self.check_later(sig.clone(), signing::command_message(cmd), problem),
            None => self.problem(problem),
        }
    }

    pub fn finish(mut self) -> Report {
        self.check_producer();
        self.check_jobs();
        let missing: Vec<u64> = self.anchors.keys().copied().collect();
        for pos in missing {
            self.problem(format!("checkpoint {pos}: event missing (log truncated)"));
        }
        // In the order a check one by one finds them.
        self.found.sort_by_key(|(order, _)| *order);
        self.report
            .problems
            .extend(std::mem::take(&mut self.found).into_iter().map(|(_, p)| p));
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
        let same = first
            .producer_sig
            .as_ref()
            .filter(|sig| group.iter().all(|e| e.producer_sig.as_ref() == Some(*sig)))
            .cloned();
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
        let problem = format!(
            "command {}: invalid worker signature on events {}..={}",
            first.command_id,
            first.global_position,
            group.last().map_or(0, |e| e.global_position)
        );
        match same {
            Some(sig) => self.check_later(sig, msg, problem),
            None => self.problem(problem),
        }
    }
}
