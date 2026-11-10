//! The record log a primary sends its standby (design/replication.md).
//!
//! Every store write is a [`LogRecord`]. Numbered from 1 and chained by a hash, the records
//! form a log that two stores can compare with one number and one hash: equal sequence number
//! and hash, equal history. The file log is such a log by itself; SQLite keeps one in a table
//! while replication is on ([`crate::Store::keep_record_log`]).
//!
//! A standby applies the primary's records with [`crate::Store::apply`], which checks them
//! before it writes anything. One that is empty or too far behind first takes a full image
//! ([`crate::Store::image`], [`crate::Store::apply_image`]) and then starts its log where the
//! image stands ([`crate::Store::start_record_log`]).

use kernable_core::{
    proto::{Event, LogRecord, LoggedEvent, ReplicatedRecord, log_record::Record},
    signing,
};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// The hash before the first record.
pub const ZERO: [u8; 32] = [0; 32];

/// Where a record log stands: its newest record's sequence number and hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub seq: u64,
    pub hash: Vec<u8>,
}

impl Default for Head {
    fn default() -> Self {
        Head {
            seq: 0,
            hash: ZERO.to_vec(),
        }
    }
}

/// The hash of record `seq`, whose encoded body has the SHA-256 `digest`, after a record with
/// hash `prev`.
pub(crate) fn record_hash(prev: &[u8], seq: u64, digest: &[u8]) -> [u8; 32] {
    Sha256::new()
        .chain_update(b"kernable.v1.replication")
        .chain_update(prev)
        .chain_update(seq.to_be_bytes())
        .chain_update(digest)
        .finalize()
        .into()
}

/// The SHA-256 of an encoded record body.
pub fn digest(body: &[u8]) -> [u8; 32] {
    Sha256::digest(body).into()
}

/// The encoded body of `record`, as the record log hashes it.
pub fn body(record: &Record) -> Vec<u8> {
    LogRecord {
        record: Some(record.clone()),
    }
    .encode_to_vec()
}

/// Numbers and chains `record` after `head`.
pub fn next(head: &Head, record: Record) -> ReplicatedRecord {
    let seq = head.seq + 1;
    let hash = record_hash(&head.hash, seq, &digest(&body(&record)));
    ReplicatedRecord {
        seq,
        hash: hash.to_vec(),
        record: Some(LogRecord {
            record: Some(record),
        }),
    }
}

/// Checks that `records` follow `head` without a gap and that each hash chains; returns the
/// inner records with the head after them. Nothing about what the records say is checked here.
pub fn chained<'a>(
    head: &Head,
    records: &'a [ReplicatedRecord],
) -> Result<(Vec<&'a Record>, Head)> {
    let mut head = head.clone();
    let mut inner = Vec::with_capacity(records.len());
    for r in records {
        if r.seq != head.seq + 1 {
            return Err(diverged(format!(
                "record {} does not follow record {}",
                r.seq, head.seq
            )));
        }
        let record = r
            .record
            .as_ref()
            .and_then(|l| l.record.as_ref())
            .ok_or_else(|| diverged(format!("record {} is empty", r.seq)))?;
        if matches!(record, Record::Format(_) | Record::ReplicationHead(_)) {
            return Err(diverged(format!(
                "record {} starts a log; it cannot follow another record",
                r.seq
            )));
        }
        let hash = record_hash(&head.hash, r.seq, &digest(&body(record)));
        if hash[..] != r.hash[..] {
            return Err(diverged(format!(
                "record {} does not match its hash",
                r.seq
            )));
        }
        head = Head {
            seq: r.seq,
            hash: r.hash.clone(),
        };
        inner.push(record);
    }
    Ok((inner, head))
}

/// Checks that the events of a commit continue the chain after an event with hash `prev` at
/// position `position`: positions without gaps, each hash over its content and predecessor.
/// Returns the hash and position of the last event.
pub(crate) fn check_events(
    mut prev: Vec<u8>,
    mut position: u64,
    events: &[LoggedEvent],
) -> Result<(Vec<u8>, u64)> {
    for logged in events {
        let event: &Event = logged
            .event
            .as_ref()
            .ok_or_else(|| diverged("a commit without its event".into()))?;
        position += 1;
        if event.global_position != position {
            return Err(diverged(format!(
                "event {} where event {position} was due",
                event.global_position
            )));
        }
        if logged.prev_hash != prev {
            return Err(diverged(format!(
                "event {position} does not follow the event before it"
            )));
        }
        if signing::event_hash(&prev, event)[..] != event.hash[..] {
            return Err(diverged(format!(
                "event {position} does not match its hash"
            )));
        }
        prev = event.hash.clone();
    }
    Ok((prev, position))
}

/// A record log that does not continue the one applied so far: the standby stops.
pub fn diverged(what: String) -> Error {
    Error::Diverged(what)
}
