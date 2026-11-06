//! Continuous backup: a backup follower writes a kernel's records to object storage, and
//! [`restore`] builds stores from them again, up to a record or a time.
//!
//! The follower (`kernable backup`) follows a kernel started with `--backup on` like an
//! asynchronous standby ([`crate::replication`]): it never counts for `--replication sync`, and
//! gets the vault values sealed with the kernel's vault key. What it writes is grouped into
//! **generations**, each a base and the segments after it:
//!
//! - `<generation>/base-000001`..: a full image (ImageBatch messages, length-delimited), in
//!   parts of at most [`PART_BYTES`], then `<generation>/base.done` (a [`BackupBase`]): written
//!   last, so a base cut short by a crash is never used. A kernel whose record log reaches back
//!   to the start sends no image: its base is empty, at record 0.
//! - `<generation>/seg-<first>-<last>-<hash>`: the record batches it took ([`BackupBatch`]),
//!   one segment every `--segment-seconds` or [`SEGMENT_BYTES`]; the name says where the
//!   follower continues after a restart.
//!
//! A new generation starts every `--base-hours`, when the kernel no longer keeps the records
//! the follower needs, or when the kernel's log diverged from the backup's (a kernel restored
//! from an older backup); the oldest beyond `--keep` are deleted. Every object is encrypted with
//! the backup key ([`crypt`]). The generation's name is `g` and the time it began, so names sort
//! in time.

pub mod crypt;
pub mod target;

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        BackupBase, BackupBatch, ChallengeResponse, FollowAck, FollowHello, FollowMode,
        FollowRequest, FollowResponse, ImageBatch, RecordBatch, follow_request, follow_response,
        log_record::Record, replication_service_client::ReplicationServiceClient,
    },
    signing,
};
use kernable_store::Head;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Streaming};

use crate::{BoxError, GRANT_HEADER, now_ms, replication, standby};

use crypt::BackupKey;
use target::Target;

/// Plaintext bytes of one base part at most.
pub const PART_BYTES: usize = 64 << 20;
/// Plaintext bytes after which a segment is written, whatever the time.
pub const SEGMENT_BYTES: usize = 16 << 20;
/// Records after which a segment is written: the follower acknowledges records only once they
/// are in the target, and the kernel sends at most 8,192 ahead of its acknowledgements.
pub const SEGMENT_RECORDS: u64 = 4_096;

/// How a backup follower runs.
pub struct Follower {
    /// The kernels to follow, tried in turn: both kernels of a pair.
    pub addrs: Vec<String>,
    /// Its key, granted `backup`, and the grant.
    pub key: SigningKey,
    pub grant: String,
    pub target: Arc<dyn Target>,
    pub crypt: Arc<BackupKey>,
    /// A segment at least this often while records come.
    pub segment_every: Duration,
    /// A new generation this often.
    pub base_every: Duration,
    /// Generations to keep, the current one included.
    pub keep: usize,
    /// Reads the newest generation through this often ([`check`]); `None`: never.
    pub check_every: Option<Duration>,
    /// Where it notes how far it uploaded and what its last check found ([`Status`]), for
    /// whoever watches it.
    pub status_file: Option<std::path::PathBuf>,
}

/// What the follower last did, as `--status-file` holds it (JSON): when it last put something
/// into the target and up to which record, and when it last checked the backup and what it
/// found. Times in ms since epoch, 0 for never.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Status {
    pub uploaded_at: i64,
    pub record: u64,
    /// When it was last connected to the kernel and had put everything it got into the target:
    /// a backup that is current although nothing new came.
    #[serde(default)]
    pub current_at: i64,
    pub checked_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_error: Option<String>,
}

/// Updates the status file at `path`, if there is one: read, changed, written in one step.
fn note(path: &Option<std::path::PathBuf>, update: impl FnOnce(&mut Status)) {
    let Some(path) = path else {
        return;
    };
    let mut status: Status = std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    update(&mut status);
    let next = path.with_extension("next");
    let written = std::fs::write(&next, serde_json::to_vec(&status).expect("serializes"))
        .and_then(|()| std::fs::rename(&next, path));
    if let Err(e) = written {
        tracing::warn!(path = %path.display(), error = %e, "writing the backup's status failed");
    }
}

/// What [`check`] read of a backup's newest generation.
#[derive(Debug)]
pub struct Checked {
    pub generation: String,
    /// The base's record, and the records its image holds.
    pub base: u64,
    pub image_records: u64,
    /// The last record the segments reach, chained from the base.
    pub record: u64,
    pub segments: usize,
    pub bytes: u64,
    /// When the newest segment (or the base) was taken, ms since epoch.
    pub taken_at: i64,
}

/// Reads the newest finished generation of the backup at `target` all through: every object
/// opens with the key, every batch decodes, and the segments' records chain from the base's
/// hash to the last one without a gap. What a restore would need, without writing a store.
pub fn check(target: &dyn Target, crypt: &BackupKey) -> Result<Checked, BoxError> {
    let gens = generations(target, crypt)?;
    let generation = gens
        .iter()
        .rev()
        .find(|g| g.base.is_some())
        .ok_or_else(|| match gens.iter().find_map(|g| g.unreadable.as_ref()) {
            Some(why) => format!("the backup cannot be read: {why}"),
            None => "the backup holds no finished generation".into(),
        })?;
    let base = generation.base.clone().expect("found with a base");
    let mut checked = Checked {
        generation: generation.name.clone(),
        base: base.seq,
        image_records: 0,
        record: base.seq,
        segments: 0,
        bytes: 0,
        taken_at: base.created_at,
    };
    for n in 1..=base.parts {
        let part = part_name(&generation.name, n);
        let sealed = target.get(&part)?;
        checked.bytes += sealed.len() as u64;
        let plain = crypt.open(&part, &sealed)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let batch = ImageBatch::decode_length_delimited(&mut rest)?;
            checked.image_records += batch.records.len() as u64;
        }
    }
    if checked.image_records != base.records {
        return Err(format!(
            "generation {}: its base holds {} records, its marker says {}",
            generation.name, checked.image_records, base.records
        )
        .into());
    }
    let mut head = Head {
        seq: base.seq,
        hash: base.hash.clone(),
    };
    for segment in &generation.segments {
        if segment.first != head.seq + 1 {
            return Err(format!(
                "generation {}: a gap from record {} to {}",
                generation.name, head.seq, segment.first
            )
            .into());
        }
        let sealed = target.get(&segment.name)?;
        checked.bytes += sealed.len() as u64;
        let plain = crypt.open(&segment.name, &sealed)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let taken = BackupBatch::decode_length_delimited(&mut rest)?;
            let batch = taken.batch.unwrap_or_default();
            let (_, next) = kernable_store::replication::chained(&head, &batch.records)
                .map_err(|e| format!("segment {}: {e}", segment.name))?;
            head = next;
            checked.taken_at = taken.taken_at;
        }
        if head.seq != segment.last || head.hash[..] != segment.hash[..] {
            return Err(format!(
                "segment {}: its records end at {}, its name says {}",
                segment.name, head.seq, segment.last
            )
            .into());
        }
        checked.segments += 1;
    }
    checked.record = head.seq;
    Ok(checked)
}

/// Checks the backup every `every` ([`check`]), logging what it found and noting it in the
/// status file; runs beside the follower.
async fn check_now_and_then(
    target: Arc<dyn Target>,
    crypt: Arc<BackupKey>,
    every: Duration,
    status_file: Option<std::path::PathBuf>,
) {
    loop {
        tokio::time::sleep(every).await;
        let started = Instant::now();
        let (target, crypt) = (Arc::clone(&target), Arc::clone(&crypt));
        let checked = blocking(move || check(target.as_ref(), &crypt)).await;
        note(&status_file, |s| {
            s.checked_at = now_ms();
            s.check_error = checked.as_ref().err().map(|e| e.to_string());
        });
        match checked {
            Ok(c) => tracing::info!(
                generation = %c.generation,
                record = c.record,
                segments = c.segments,
                bytes = c.bytes,
                age_s = (now_ms() - c.taken_at) / 1000,
                took_ms = started.elapsed().as_millis() as u64,
                "the backup checks out: it restores to record {}", c.record
            ),
            Err(e) => tracing::error!(error = %e, "the backup does not check out"),
        }
    }
}

/// A generation as listed in the target.
#[derive(Debug)]
pub struct Generation {
    pub name: String,
    /// The finished base, if it is.
    pub base: Option<BackupBase>,
    /// Why its base marker could not be read, if it could not.
    pub unreadable: Option<String>,
    pub segments: Vec<Segment>,
}

/// A segment, as its name says.
#[derive(Debug, Clone)]
pub struct Segment {
    pub name: String,
    pub first: u64,
    pub last: u64,
    pub hash: Vec<u8>,
}

impl Generation {
    /// Where the generation stands: its last segment, else its base.
    fn head(&self) -> Option<Head> {
        match (self.segments.last(), &self.base) {
            (Some(s), _) => Some(Head {
                seq: s.last,
                hash: s.hash.clone(),
            }),
            (None, Some(b)) => Some(Head {
                seq: b.seq,
                hash: b.hash.clone(),
            }),
            (None, None) => None,
        }
    }

    fn began(&self) -> i64 {
        self.name
            .strip_prefix('g')
            .and_then(|t| t.parse().ok())
            .unwrap_or(0)
    }
}

fn generation_name(at: i64) -> String {
    format!("g{at:013}")
}

fn part_name(generation: &str, n: u32) -> String {
    format!("{generation}/base-{n:06}")
}

fn segment_name(generation: &str, first: u64, last: u64, hash: &[u8]) -> String {
    format!(
        "{generation}/seg-{first:020}-{last:020}-{}",
        hex::encode(hash)
    )
}

fn parse_segment(name: &str) -> Option<Segment> {
    let file = name.rsplit('/').next()?;
    let mut parts = file.strip_prefix("seg-")?.split('-');
    let first = parts.next()?.parse().ok()?;
    let last = parts.next()?.parse().ok()?;
    let hash = hex::decode(parts.next()?).ok()?;
    Some(Segment {
        name: name.to_owned(),
        first,
        last,
        hash,
    })
}

/// The generations in `target`, oldest first, with their bases read.
pub fn generations(target: &dyn Target, crypt: &BackupKey) -> Result<Vec<Generation>, BoxError> {
    let mut gens: Vec<Generation> = Vec::new();
    for name in target.list("")? {
        let Some((generation, object)) = name.split_once('/') else {
            continue;
        };
        if !generation.starts_with('g') {
            continue;
        }
        if gens.last().is_none_or(|g| g.name != generation) {
            gens.push(Generation {
                name: generation.to_owned(),
                base: None,
                unreadable: None,
                segments: Vec::new(),
            });
        }
        let g = gens.last_mut().expect("pushed above");
        if object == "base.done" {
            // One unreadable base leaves its generation unfinished, not the backup unusable.
            let read = target
                .get(&name)
                .and_then(|sealed| crypt.open(&name, &sealed))
                .and_then(|plain| Ok(BackupBase::decode(plain.as_slice())?));
            match read {
                Ok(base) => g.base = Some(base),
                Err(e) => {
                    tracing::warn!(object = %name, error = %e, "a base cannot be read: its generation is left out");
                    g.unreadable = Some(e.to_string());
                }
            }
        } else if let Some(s) = parse_segment(&name) {
            g.segments.push(s);
        }
    }
    for g in &mut gens {
        g.segments.sort_by_key(|s| s.first);
    }
    Ok(gens)
}

/// Runs the follower until it fails to start; reconnects whenever the stream ends.
pub async fn run(follower: Follower) -> Result<(), BoxError> {
    let follower = Arc::new(follower);
    let mut fresh = false;
    let mut delay = Duration::from_secs(1);
    let mut turn = 0;
    tracing::info!(to = %follower.target.describe(), key = %follower.crypt.id(), "backup follower starting");
    let _checks = follower.check_every.map(|every| {
        crate::AbortOnDrop(tokio::spawn(check_now_and_then(
            Arc::clone(&follower.target),
            Arc::clone(&follower.crypt),
            every,
            follower.status_file.clone(),
        )))
    });
    loop {
        let addr = follower.addrs[turn % follower.addrs.len()].clone();
        match follow_once(&follower, &addr, &mut fresh).await {
            Ok(()) => delay = Duration::from_secs(1),
            Err(e) => {
                tracing::warn!(kernel = %addr, error = %e, "the backup follower lost the kernel; connecting again in {delay:?}");
                turn += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Runs `f` on a blocking thread: the target's requests block.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, BoxError> + Send + 'static,
) -> Result<T, BoxError> {
    tokio::task::spawn_blocking(f).await?
}

/// One connection: continue the current generation, or begin a new one.
async fn follow_once(
    follower: &Arc<Follower>,
    addr: &str,
    fresh: &mut bool,
) -> Result<(), BoxError> {
    let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
    let gens = blocking(move || generations(target.as_ref(), &crypt)).await?;
    let base_age = Duration::from_millis(follower.base_every.as_millis() as u64);
    let current = gens
        .iter()
        .rev()
        .find(|g| g.base.is_some())
        .filter(|g| !*fresh && (now_ms() - g.began()) < base_age.as_millis() as i64);
    let from = match current.and_then(Generation::head) {
        Some(head) => head,
        None => Head {
            seq: 0,
            hash: kernable_store::replication::ZERO.to_vec(),
        },
    };
    let mut generation = current.map(|g| g.name.clone());

    let mut client = ReplicationServiceClient::new(standby::connect(addr).await?)
        .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let (tx, rx) = mpsc::channel(16);
    let say = |msg| FollowRequest { msg: Some(msg) };
    tx.send(say(follow_request::Msg::Hello(FollowHello {
        key_id: signing::key_id(&follower.key.verifying_key()),
        seq: from.seq,
        hash: from.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        backup: true,
    })))
    .await?;
    let mut request = Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert(GRANT_HEADER, follower.grant.parse()?);
    let mut inbound = client.follow(request).await?.into_inner();
    let Some(follow_response::Msg::Challenge(challenge)) = next(&mut inbound).await? else {
        return Err("the kernel sent no challenge".into());
    };
    let proof = signing::sign(&follower.key, &signing::challenge_message(&challenge.nonce));
    tx.send(say(follow_request::Msg::ChallengeResponse(
        ChallengeResponse { sig: proof.sig },
    )))
    .await?;
    let Some(follow_response::Msg::Start(start)) = next(&mut inbound).await? else {
        return Err("the kernel did not start the stream".into());
    };
    let mut head = match start.mode() {
        FollowMode::Follow if generation.is_some() => {
            tracing::info!(
                generation = generation.as_deref().unwrap_or(""),
                from = from.seq,
                "backing up"
            );
            from
        }
        FollowMode::Follow => {
            // The kernel keeps its whole record log: the generation starts at record 0.
            let name = generation_name(now_ms());
            write_base(follower, &name, 0, &from, (0, 0, 0)).await?;
            generation = Some(name);
            from
        }
        FollowMode::Image => {
            let name = generation_name(now_ms());
            let head = take_image(follower, &name, &mut inbound).await?;
            tx.send(say(follow_request::Msg::Ack(FollowAck { seq: head.seq })))
                .await?;
            generation = Some(name);
            head
        }
        FollowMode::Diverged | FollowMode::Unspecified => {
            tracing::warn!(message = %start.message, "the kernel's log differs from the backup's: a new generation begins");
            *fresh = true;
            return Ok(());
        }
    };
    *fresh = false;
    let generation = generation.expect("set above");
    prune(follower, &generation).await;

    let mut segment: Vec<u8> = Vec::new();
    let mut first: Option<u64> = None;
    let mut deadline = Instant::now() + follower.segment_every;
    let flush = |segment: &mut Vec<u8>, first: &mut Option<u64>, head: &Head| {
        let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
        let plain = std::mem::take(segment);
        let name = first
            .take()
            .map(|first| segment_name(&generation, first, head.seq, &head.hash));
        let seq = head.seq;
        let tx = tx.clone();
        let status_file = follower.status_file.clone();
        async move {
            let Some(name) = name else {
                return Ok::<(), BoxError>(());
            };
            blocking(move || target.put(&name, &crypt.seal(&name, &plain)?)).await?;
            note(&status_file, |s| {
                s.uploaded_at = now_ms();
                s.record = seq;
            });
            // In the target: the kernel may drop these records from its log now.
            tx.send(FollowRequest {
                msg: Some(follow_request::Msg::Ack(FollowAck { seq })),
            })
            .await?;
            Ok(())
        }
    };
    loop {
        tokio::select! {
            message = next(&mut inbound) => match message? {
                Some(follow_response::Msg::Records(batch)) => {
                    let Some(last) = batch.records.last() else { continue };
                    if batch.records[0].seq != head.seq + 1 {
                        return Err(format!(
                            "the kernel sent record {} after {}", batch.records[0].seq, head.seq
                        ).into());
                    }
                    head = Head { seq: last.seq, hash: last.hash.clone() };
                    first.get_or_insert(batch.records[0].seq);
                    BackupBatch { taken_at: now_ms(), batch: Some(batch) }
                        .encode_length_delimited(&mut segment)?;
                    let held = first.map_or(0, |f| head.seq + 1 - f);
                    if segment.len() >= SEGMENT_BYTES || held >= SEGMENT_RECORDS {
                        flush(&mut segment, &mut first, &head).await?;
                    }
                }
                None => {
                    flush(&mut segment, &mut first, &head).await?;
                    return Ok(());
                }
                Some(_) => return Err("the kernel sent something other than records".into()),
            },
            () = tokio::time::sleep_until(deadline.into()) => {
                flush(&mut segment, &mut first, &head).await?;
                note(&follower.status_file, |s| s.current_at = now_ms());
                deadline = Instant::now() + follower.segment_every;
                let began = generation.strip_prefix('g').and_then(|t| t.parse::<i64>().ok()).unwrap_or(0);
                if now_ms() - began >= base_age.as_millis() as i64 {
                    tracing::info!(generation = %generation, "the generation is old enough: a new one begins");
                    *fresh = true;
                    return Ok(());
                }
            }
        }
    }
}

async fn next(
    inbound: &mut Streaming<FollowResponse>,
) -> Result<Option<follow_response::Msg>, BoxError> {
    Ok(inbound.message().await?.and_then(|m| m.msg))
}

/// Receives an image into the base parts of `generation`; returns where it stands.
async fn take_image(
    follower: &Arc<Follower>,
    generation: &str,
    inbound: &mut Streaming<FollowResponse>,
) -> Result<Head, BoxError> {
    let mut part: Vec<u8> = Vec::new();
    let mut parts = 0u32;
    let mut counts = (0u64, 0u64, 0u64);
    let put = |n: u32, plain: Vec<u8>| {
        let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
        let name = part_name(generation, n);
        blocking(move || target.put(&name, &crypt.seal(&name, &plain)?))
    };
    loop {
        match next(inbound).await? {
            Some(follow_response::Msg::Image(batch)) => {
                counts.0 += batch.records.len() as u64;
                counts.1 += batch.vault.len() as u64;
                counts.2 += batch.files.len() as u64;
                batch.encode_length_delimited(&mut part)?;
                if part.len() >= PART_BYTES {
                    parts += 1;
                    put(parts, std::mem::take(&mut part)).await?;
                }
            }
            Some(follow_response::Msg::ImageEnd(end)) => {
                if !part.is_empty() {
                    parts += 1;
                    put(parts, part).await?;
                }
                let head = Head {
                    seq: end.seq,
                    hash: end.hash,
                };
                write_base(follower, generation, parts, &head, counts).await?;
                tracing::info!(%generation, records = counts.0, record = head.seq, "a new backup generation: its base is written");
                return Ok(head);
            }
            None => return Err("the kernel ended the stream during the image".into()),
            Some(_) => return Err("the kernel sent something else during the image".into()),
        }
    }
}

async fn write_base(
    follower: &Arc<Follower>,
    generation: &str,
    parts: u32,
    head: &Head,
    (records, vault_values, files): (u64, u64, u64),
) -> Result<(), BoxError> {
    let base = BackupBase {
        seq: head.seq,
        hash: head.hash.clone(),
        parts,
        created_at: now_ms(),
        records,
        vault_values,
        files,
    };
    let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
    let name = format!("{generation}/base.done");
    let seq = base.seq;
    blocking(move || target.put(&name, &crypt.seal(&name, &base.encode_to_vec())?)).await?;
    note(&follower.status_file, |s| {
        s.uploaded_at = now_ms();
        s.record = seq;
    });
    Ok(())
}

/// Deletes the generations beyond `--keep` (oldest first) and unfinished ones before
/// `current`; logs what fails, and tries again with the next generation.
async fn prune(follower: &Arc<Follower>, current: &str) {
    let (target, crypt, keep) = (
        Arc::clone(&follower.target),
        Arc::clone(&follower.crypt),
        follower.keep.max(1),
    );
    let current = current.to_owned();
    let pruned = blocking(move || {
        let gens = generations(target.as_ref(), &crypt)?;
        let finished: Vec<&Generation> = gens.iter().filter(|g| g.base.is_some()).collect();
        let mut doomed: HashSet<&str> = finished
            .iter()
            .take(finished.len().saturating_sub(keep))
            .map(|g| g.name.as_str())
            .collect();
        doomed.extend(
            gens.iter()
                .filter(|g| g.base.is_none() && g.name.as_str() < current.as_str())
                .map(|g| g.name.as_str()),
        );
        doomed.remove(current.as_str());
        for name in &doomed {
            // The marker first: a generation half deleted is never taken for a whole one.
            target.delete(&format!("{name}/base.done"))?;
            for object in target.list(&format!("{name}/"))? {
                target.delete(&object)?;
            }
            tracing::info!(generation = %name, "deleted an old backup generation");
        }
        Ok(())
    })
    .await;
    if let Err(e) = pruned {
        tracing::warn!(error = %e, "deleting old backup generations failed");
    }
}

/// How far [`restore`] goes.
#[derive(Debug, Clone, Copy, Default)]
pub enum Until {
    /// Everything the backup holds.
    #[default]
    End,
    /// Up to and with this record.
    Record(u64),
    /// The batches the follower took up to this time (ms since epoch).
    Time(i64),
}

/// What [`restore`] did.
#[derive(Debug)]
pub struct Restored {
    pub generation: String,
    pub base: u64,
    /// The last record applied.
    pub record: u64,
    /// When the follower took the last batch applied (the base's time without one).
    pub taken_at: i64,
    /// The kernel keys that signed the restored checkpoints, by key ID.
    pub kernel_keys: Vec<String>,
    /// Why the restore ends before the point it was asked for, if it does: the backup ends
    /// there, or holds nothing until the next generation's base.
    pub short: Option<String>,
}

/// Builds `stores` (new and empty) from the backup in `target`: the newest generation that
/// begins before `until`, its base, then its segments up to `until`. `kernel_keys`: the source's
/// kernel key IDs its checkpoints must be signed by; empty takes the keys the backup names
/// (and says so in [`Restored::kernel_keys`]).
pub fn restore(
    target: &dyn Target,
    crypt: &BackupKey,
    stores: &standby::Stores,
    until: Until,
    kernel_keys: &[String],
) -> Result<Restored, BoxError> {
    if !crate::export::empty(stores.store.as_ref())? {
        return Err("the store holds data already: restore into new, empty stores".into());
    }
    let gens = generations(target, crypt)?;
    let usable = |g: &&Generation| match (until, &g.base) {
        (_, None) => false,
        (Until::End, _) => true,
        (Until::Record(n), Some(b)) => b.seq <= n,
        (Until::Time(t), Some(b)) => b.created_at <= t,
    };
    let Some(generation) = gens.iter().rev().find(usable) else {
        return Err(match gens.iter().find_map(|g| g.base.as_ref()) {
            None => match gens.iter().find_map(|g| g.unreadable.as_ref()) {
                Some(why) => format!("the backup cannot be read: {why}").into(),
                None => "the backup holds no finished generation".into(),
            },
            Some(b) => format!(
                "the backup begins later: its oldest base is at record {} ({})",
                b.seq,
                crate::config::utc(b.created_at)
            )
            .into(),
        });
    };
    let base = generation.base.clone().expect("usable");
    let mut keys: Vec<String> = kernel_keys.to_vec();
    let named = kernel_keys.is_empty();
    let mut learn = |records: &mut dyn Iterator<Item = &Record>| {
        if named {
            for r in records {
                if let Record::Checkpoint(c) = r
                    && let Some(sig) = &c.kernel_sig
                    && !keys.contains(&sig.key_id)
                {
                    keys.push(sig.key_id.clone());
                }
            }
        }
        keys.clone()
    };
    stores.store.keep_record_log(true)?;
    for n in 1..=base.parts {
        let part = part_name(&generation.name, n);
        let plain = crypt.open(&part, &target.get(&part)?)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let batch = ImageBatch::decode_length_delimited(&mut rest)?;
            let keys = learn(&mut batch.records.iter().filter_map(|l| l.record.as_ref()));
            check_vault(stores, &batch.vault)?;
            standby::apply_image(stores, batch, &keys)?;
        }
    }
    // An empty base at record 0: the store's own record log starts there already.
    if base.parts > 0 || base.seq > 0 {
        stores.store.start_record_log(&Head {
            seq: base.seq,
            hash: base.hash.clone(),
        })?;
    }
    let mut restored = Restored {
        generation: generation.name.clone(),
        base: base.seq,
        record: base.seq,
        taken_at: base.created_at,
        kernel_keys: Vec::new(),
        short: None,
    };
    'segments: for segment in &generation.segments {
        if segment.first != restored.record + 1 {
            return Err(format!(
                "the backup has a gap: generation {} goes from record {} to {}",
                generation.name, restored.record, segment.first
            )
            .into());
        }
        if matches!(until, Until::Record(n) if segment.first > n) {
            break;
        }
        let plain = crypt.open(&segment.name, &target.get(&segment.name)?)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let taken = BackupBatch::decode_length_delimited(&mut rest)?;
            if matches!(until, Until::Time(t) if taken.taken_at > t) {
                break 'segments;
            }
            let mut batch = taken.batch.unwrap_or_default();
            if let Until::Record(n) = until {
                cut(&mut batch, n);
                if batch.records.is_empty() {
                    break 'segments;
                }
            }
            let keys = learn(
                &mut batch
                    .records
                    .iter()
                    .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref())),
            );
            check_vault(stores, &batch.vault)?;
            let cut_short = matches!(until, Until::Record(n) if batch.records.last().is_some_and(|r| r.seq >= n));
            restored.record = standby::apply(stores, batch, &keys)?;
            restored.taken_at = taken.taken_at;
            if cut_short {
                break 'segments;
            }
        }
    }
    stores.store.sync()?;
    restored.kernel_keys = keys;
    let reached = match until {
        Until::End => true,
        Until::Record(n) => restored.record >= n,
        Until::Time(t) => {
            restored.taken_at >= t
                || generation
                    .segments
                    .last()
                    .is_some_and(|s| s.last > restored.record)
        }
    };
    if !reached {
        let next = gens
            .iter()
            .filter(|g| g.name > generation.name)
            .find_map(|g| g.base.as_ref());
        restored.short = Some(match next {
            Some(b) => format!(
                "the backup holds nothing between record {} ({}) and the next generation's base at record {} ({}): restored up to record {}",
                restored.record,
                crate::config::utc(restored.taken_at),
                b.seq,
                crate::config::utc(b.created_at),
                restored.record
            ),
            None => format!(
                "the backup ends at record {} ({}): restored up to there",
                restored.record,
                crate::config::utc(restored.taken_at)
            ),
        });
    }
    Ok(restored)
}

fn check_vault(
    stores: &standby::Stores,
    rows: &[kernable_core::proto::VaultRow],
) -> Result<(), BoxError> {
    match (&stores.vault, rows.first()) {
        (_, None) => Ok(()),
        (None, Some(_)) => {
            Err("the backup holds vault values: give the vault and the source's vault key".into())
        }
        (Some(vault), Some(row)) => Ok(vault.check_sealed(std::slice::from_ref(row))?),
    }
}

/// Drops the records of `batch` after record `n`, and the files only they refer to.
fn cut(batch: &mut RecordBatch, n: u64) {
    if batch.records.last().is_none_or(|r| r.seq <= n) {
        return;
    }
    batch.records.retain(|r| r.seq <= n);
    let kept: HashSet<(String, String, String)> = batch
        .records
        .iter()
        .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref()))
        .filter_map(|r| match r {
            Record::DataChanged(d) => Some((d.tenant_id.clone(), d.domain.clone(), d.key.clone())),
            _ => None,
        })
        .collect();
    batch
        .files
        .retain(|f| kept.contains(&(f.tenant_id.clone(), f.domain.clone(), f.key.clone())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_names_say_where_they_stand_and_sort_in_order() {
        let a = segment_name("g0000000000001", 1, 512, &[0xab; 32]);
        let b = segment_name("g0000000000001", 513, 9000, &[0xcd; 32]);
        assert!(a < b);
        let s = parse_segment(&b).unwrap();
        assert_eq!((s.first, s.last, s.hash), (513, 9000, vec![0xcd; 32]));
        assert!(parse_segment("g1/base.done").is_none());
        assert!(generation_name(5) < generation_name(1_000_000_000_000));
    }

    #[test]
    fn a_batch_cut_at_a_record_keeps_only_its_files() {
        use kernable_core::proto::{DataChangedRecord, DataFile, LogRecord, ReplicatedRecord};
        let changed = |seq: u64, key: &str| ReplicatedRecord {
            seq,
            hash: vec![seq as u8],
            record: Some(LogRecord {
                record: Some(Record::DataChanged(DataChangedRecord {
                    tenant_id: "t".into(),
                    domain: "shop".into(),
                    key: key.into(),
                })),
            }),
        };
        let file = |key: &str| DataFile {
            tenant_id: "t".into(),
            domain: "shop".into(),
            key: key.into(),
            ..Default::default()
        };
        let mut batch = RecordBatch {
            records: vec![changed(7, "a"), changed(8, "b"), changed(9, "c")],
            files: vec![file("a"), file("b"), file("c")],
            ..Default::default()
        };
        cut(&mut batch, 8);
        assert_eq!(batch.records.len(), 2);
        let keys: Vec<&str> = batch.files.iter().map(|f| f.key.as_str()).collect();
        assert_eq!(keys, ["a", "b"]);
    }
}
