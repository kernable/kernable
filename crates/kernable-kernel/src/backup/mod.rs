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
pub mod signer;
pub mod target;

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        BackupBase, BackupBatch, ChallengeResponse, Command, FollowAck, FollowHello, FollowMode,
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

/// The grant the follower sends: its file as it reads now, else the one it started with.
fn current_grant(file: Option<&std::path::Path>, started_with: &str) -> String {
    file.and_then(|f| std::fs::read_to_string(f).ok())
        .map(|g| g.trim().to_owned())
        .filter(|g| !g.is_empty())
        .unwrap_or_else(|| started_with.to_owned())
}

/// How a backup follower runs.
pub struct Follower {
    /// The kernels to follow, tried in turn: both kernels of a pair.
    pub addrs: Vec<String>,
    /// Its key, granted `backup`, and the grant.
    pub key: SigningKey,
    pub grant: String,
    /// The grant's file, read again on every connection, so a grant renewed beside the key
    /// (a new one for a new key, say) is taken without a restart; `grant` while it does not
    /// read.
    pub grant_file: Option<std::path::PathBuf>,
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
    pub current_at: i64,
    pub checked_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_error: Option<String>,
    /// While the target (its storage, or its signer) cannot be written: why the last attempt
    /// failed, and since when (ms); both cleared by the next upload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_error: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub failing_since: i64,
}

fn is_zero(n: &i64) -> bool {
    *n == 0
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
#[derive(Debug, Clone)]
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
        began(&self.name)
    }
}

/// When the generation `name` began, as its name says.
fn began(name: &str) -> i64 {
    name.strip_prefix('g')
        .and_then(|t| t.parse().ok())
        .unwrap_or(0)
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
            // A target that cannot be reached is an error, not an unreadable base.
            let sealed = target.get(&name)?;
            let read = crypt
                .open(&name, &sealed)
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

/// An error if generations were finished but none of them can be read with `crypt`: the
/// backup was written with another key, every base looks unfinished, and pruning would delete
/// the whole backup. The follower then never writes into or deletes from it. Some unreadable
/// generations beside readable ones (a damaged object) are left out, kept and never pruned.
fn refuse_unreadable(gens: &[Generation], crypt: &BackupKey) -> Result<(), BoxError> {
    if gens.iter().any(|g| g.base.is_some()) {
        return Ok(());
    }
    match gens.iter().find(|g| g.unreadable.is_some()) {
        Some(g) => Err(format!(
            "no generation of the backup can be read with the backup key {} \
             (generation {}: {}); give the key the backup was written with",
            crypt.id(),
            g.name,
            g.unreadable.as_deref().unwrap_or_default()
        )
        .into()),
        None => Ok(()),
    }
}

/// Runs the follower until it fails to start; reconnects whenever the stream ends. Refuses to
/// start when the target holds finished generations but none can be read with its key.
pub async fn run(follower: Follower) -> Result<(), BoxError> {
    let follower = Arc::new(Running {
        follower,
        failing_since: AtomicI64::new(0),
    });
    let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
    match blocking(move || generations(target.as_ref(), &crypt)).await {
        Ok(gens) => refuse_unreadable(&gens, &follower.crypt)?,
        // The target may come back: the follower tries again (and checks again) when it connects.
        Err(e) => tracing::warn!(error = %e, "listing the backup's generations failed"),
    }
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
                match e.downcast_ref::<target::StorageError>() {
                    // Nothing is acknowledged that is not in the target: the kernel keeps the
                    // records, and the follower takes them once the target is back.
                    Some(storage) => {
                        let since = failing(&follower, &e.to_string());
                        tracing::error!(
                            to = %follower.target.describe(),
                            signer = storage.signer,
                            failing_s = (now_ms() - since) / 1000,
                            error = %e,
                            "the backup is falling behind: its target cannot be written; the kernel keeps the records, trying again in {delay:?}"
                        );
                    }
                    None => {
                        tracing::warn!(kernel = %addr, error = %e, "the backup follower lost the kernel; connecting again in {delay:?}");
                        turn += 1;
                    }
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// A follower while it runs, and since when its target cannot be written (ms; 0: it can).
struct Running {
    follower: Follower,
    failing_since: AtomicI64,
}

impl std::ops::Deref for Running {
    type Target = Follower;

    fn deref(&self) -> &Follower {
        &self.follower
    }
}

/// Notes that the target cannot be written (`why`), in the status file too; returns since when
/// it fails.
fn failing(running: &Running, why: &str) -> i64 {
    let now = now_ms();
    let since =
        match running
            .failing_since
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => now,
            Err(since) => since,
        };
    note(&running.status_file, |s| {
        s.failing_since = since;
        s.upload_error = Some(why.to_owned());
    });
    since
}

/// Notes an upload up to `record`; after a time the target could not be written, logs that it
/// can again.
fn uploaded(running: &Running, record: u64) {
    let since = running.failing_since.swap(0, Ordering::Relaxed);
    if since != 0 {
        tracing::info!(
            record,
            failed_s = (now_ms() - since) / 1000,
            "the backup's target can be written again: the backup catches up"
        );
    }
    note(&running.status_file, |s| {
        s.uploaded_at = now_ms();
        s.record = record;
        s.upload_error = None;
        s.failing_since = 0;
    });
}

/// Runs `f` on a blocking thread: the target's requests block.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, BoxError> + Send + 'static,
) -> Result<T, BoxError> {
    tokio::task::spawn_blocking(f).await?
}

/// One connection: continue the current generation, or begin a new one.
async fn follow_once(
    follower: &Arc<Running>,
    addr: &str,
    fresh: &mut bool,
) -> Result<(), BoxError> {
    let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
    let gens = blocking(move || generations(target.as_ref(), &crypt)).await?;
    refuse_unreadable(&gens, &follower.crypt)?;
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
    // The vault key the current generation's values are sealed with.
    let current_key = current
        .and_then(|g| g.base.as_ref())
        .map(|b| b.vault_key.clone());

    let mut client = ReplicationServiceClient::new(standby::connect(addr).await?)
        .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let (tx, rx) = mpsc::channel(16);
    let say = |msg| FollowRequest { msg: Some(msg) };
    tx.send(say(follow_request::Msg::Hello(FollowHello {
        reader: false,
        key_id: signing::key_id(&follower.key.verifying_key()),
        seq: from.seq,
        hash: from.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        backup: true,
        rewind_points: Vec::new(),
        witness_generation: None,
        copy: String::new(),
    })))
    .await?;
    let mut request = Request::new(ReceiverStream::new(rx));
    request.metadata_mut().insert(
        GRANT_HEADER,
        current_grant(follower.grant_file.as_deref(), &follower.grant).parse()?,
    );
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
    // One vault key per generation: a kernel whose vault key changed (rekeyed, or restored
    // with another key) begins a new one, which a restore opens with the new key alone.
    if current_key.is_some_and(|k| k != start.vault_key) {
        tracing::info!(
            vault_key = %start.vault_key,
            "the kernel's vault key changed: a new generation begins"
        );
        *fresh = true;
        return Ok(());
    }
    let vault_key = start.vault_key.clone();
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
            write_base(follower, &name, 0, &from, (0, 0, 0), &vault_key).await?;
            generation = Some(name);
            from
        }
        FollowMode::Image => {
            let name = generation_name(now_ms());
            let head = take_image(follower, &name, &mut inbound, &vault_key).await?;
            tx.send(say(follow_request::Msg::Ack(FollowAck { seq: head.seq })))
                .await?;
            generation = Some(name);
            head
        }
        // It offers no records to go back to, so it is never told to rewind.
        FollowMode::Diverged | FollowMode::Rewind | FollowMode::Unspecified => {
            tracing::warn!(message = %start.message, "the kernel's log differs from the backup's: a new generation begins");
            *fresh = true;
            return Ok(());
        }
    };
    *fresh = false;
    let generation = generation.expect("set above");
    prune(follower, &generation, &vault_key).await;

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
        let running = Arc::clone(follower);
        async move {
            let Some(name) = name else {
                return Ok::<(), BoxError>(());
            };
            blocking(move || target.put(&name, &crypt.seal(&name, &plain)?)).await?;
            uploaded(&running, seq);
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
                if now_ms() - began(&generation) >= base_age.as_millis() as i64 {
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
    follower: &Arc<Running>,
    generation: &str,
    inbound: &mut Streaming<FollowResponse>,
    vault_key: &str,
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
                write_base(follower, generation, parts, &head, counts, vault_key).await?;
                tracing::info!(%generation, records = counts.0, record = head.seq, "a new backup generation: its base is written");
                return Ok(head);
            }
            None => return Err("the kernel ended the stream during the image".into()),
            Some(_) => return Err("the kernel sent something else during the image".into()),
        }
    }
}

async fn write_base(
    follower: &Arc<Running>,
    generation: &str,
    parts: u32,
    head: &Head,
    (records, vault_values, files): (u64, u64, u64),
    vault_key: &str,
) -> Result<(), BoxError> {
    let base = BackupBase {
        seq: head.seq,
        hash: head.hash.clone(),
        parts,
        created_at: now_ms(),
        records,
        vault_values,
        files,
        vault_key: vault_key.to_owned(),
    };
    let (target, crypt) = (Arc::clone(&follower.target), Arc::clone(&follower.crypt));
    let name = format!("{generation}/base.done");
    let seq = base.seq;
    blocking(move || target.put(&name, &crypt.seal(&name, &base.encode_to_vec())?)).await?;
    uploaded(follower, seq);
    Ok(())
}

/// Deletes the generations beyond `--keep` (oldest first) and unfinished ones before
/// `current`; deletes nothing while no generation can be read with the key, and never a
/// generation whose base cannot be read. Logs what fails, and tries again with the next
/// generation.
async fn prune(follower: &Arc<Running>, current: &str, vault_key: &str) {
    let (target, crypt, keep) = (
        Arc::clone(&follower.target),
        Arc::clone(&follower.crypt),
        follower.keep.max(1),
    );
    let (current, vault_key) = (current.to_owned(), vault_key.to_owned());
    let pruned = blocking(move || {
        let gens = generations(target.as_ref(), &crypt)?;
        refuse_unreadable(&gens, &crypt)?;
        let doomed = doomed(&gens, keep, &current);
        for name in &doomed {
            // The marker first: a generation half deleted is never taken for a whole one.
            target.delete(&format!("{name}/base.done"))?;
            for object in target.list(&format!("{name}/"))? {
                target.delete(&object)?;
            }
            tracing::info!(generation = %name, "deleted an old backup generation");
        }
        // After its last generation: what is kept per vault key (an escrow of the key, say).
        for key in retired_keys(&gens, &doomed, &vault_key) {
            for object in target.list(&format!("{VAULT_KEYS}{key}/"))? {
                target.delete(&object)?;
            }
            tracing::info!(vault_key = %key, "deleted what was kept for a vault key no generation needs");
        }
        Ok(())
    })
    .await;
    if let Err(e) = pruned {
        tracing::warn!(error = %e, "deleting old backup generations failed");
    }
}

/// Where files kept per vault key live beside the generations: `vault-keys/<key id>/…`, for
/// example an escrow of the key. Neither a restore nor the follower reads them; the follower
/// deletes them with the last generation whose values are sealed with that key.
pub const VAULT_KEYS: &str = "vault-keys/";

/// The vault keys whose files go once `doomed` is deleted: named by a doomed generation and by
/// no other one, and not the kernel's key now (`current`). None while a generation that stays
/// cannot be read: it may need one of them.
fn retired_keys(gens: &[Generation], doomed: &HashSet<&str>, current: &str) -> Vec<String> {
    let staying = || gens.iter().filter(|g| !doomed.contains(g.name.as_str()));
    if staying().any(|g| g.unreadable.is_some()) {
        return Vec::new();
    }
    let needed: HashSet<&str> = staying()
        .filter_map(|g| g.base.as_ref())
        .map(|b| b.vault_key.as_str())
        .chain([current])
        .collect();
    let mut retired: Vec<String> = gens
        .iter()
        .filter(|g| doomed.contains(g.name.as_str()))
        .filter_map(|g| g.base.as_ref())
        .map(|b| b.vault_key.as_str())
        .filter(|k| !k.is_empty() && !needed.contains(k))
        .map(str::to_owned)
        .collect();
    retired.sort();
    retired.dedup();
    retired
}

/// The generations [`prune`] deletes: the finished ones beyond the newest `keep`, and the
/// unfinished ones before `current`; never `current`, and never one whose base cannot be read.
fn doomed<'a>(gens: &'a [Generation], keep: usize, current: &str) -> HashSet<&'a str> {
    let finished: Vec<&Generation> = gens.iter().filter(|g| g.base.is_some()).collect();
    let mut doomed: HashSet<&str> = finished
        .iter()
        .take(finished.len().saturating_sub(keep))
        .map(|g| g.name.as_str())
        .collect();
    doomed.extend(
        gens.iter()
            .filter(|g| g.base.is_none() && g.unreadable.is_none() && g.name.as_str() < current)
            .map(|g| g.name.as_str()),
    );
    doomed.remove(current);
    doomed
}

/// The generation a restore up to `until` builds from: the newest finished one that begins by
/// then.
fn choose(gens: &[Generation], until: Until) -> Result<&Generation, BoxError> {
    let usable = |g: &&Generation| match (until, &g.base) {
        (_, None) => false,
        (Until::End, _) => true,
        (Until::Record(n), Some(b)) => b.seq <= n,
        (Until::Time(t), Some(b)) => b.created_at <= t,
    };
    gens.iter()
        .rev()
        .find(usable)
        .ok_or_else(|| match gens.iter().find_map(|g| g.base.as_ref()) {
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
        })
}

/// An erasure the backup holds after a restore point: an event that erased a subject's vault
/// values, committed after the point. A restore to the point brings those values back.
#[derive(Debug, Clone, PartialEq)]
pub struct Erasure {
    /// The record and the event's position in the log, and when it was committed (ms).
    pub record: u64,
    pub position: u64,
    pub at: i64,
    pub tenant: String,
    pub domain: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    /// The subject whose values it erased.
    pub subject: String,
    pub command_id: String,
    /// The command that wrote it, if the backup holds it after the point; `None`: it was
    /// queued before the point, so the restored store holds it.
    pub command: Option<Command>,
}

/// What a restore up to a point uses, and what it leaves out ([`plan`]).
#[derive(Debug, Default)]
pub struct Plan {
    pub generation: String,
    /// The vault key the generation's values are sealed with (empty: no vault).
    pub vault_key: String,
    /// The erasures committed after the point, oldest first.
    pub erasures: Vec<Erasure>,
    /// Why the list may be incomplete: the backup holds records after the point only in a
    /// later generation's base.
    pub note: Option<String>,
}

/// What a restore up to `until` uses ([`restore`] picks the same generation), and the erasures
/// the backup holds after that point: in the rest of that generation and in the segments of
/// every later one. Reads the backup, writes nothing.
pub fn plan(target: &dyn Target, crypt: &BackupKey, until: Until) -> Result<Plan, BoxError> {
    let gens = generations(target, crypt)?;
    let generation = choose(&gens, until)?;
    let base = generation.base.as_ref().expect("chosen with a base");
    let mut plan = Plan {
        generation: generation.name.clone(),
        vault_key: base.vault_key.clone(),
        ..Plan::default()
    };
    if matches!(until, Until::End) {
        return Ok(plan);
    }
    let mut found = Found::default();
    // The chosen generation: what the restore leaves out of it.
    let mut seen = base.seq;
    let mut after = false;
    for segment in &generation.segments {
        let plain = crypt.open(&segment.name, &target.get(&segment.name)?)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let taken = BackupBatch::decode_length_delimited(&mut rest)?;
            let batch = taken.batch.unwrap_or_default();
            after |= matches!(until, Until::Time(t) if taken.taken_at > t);
            for r in &batch.records {
                seen = seen.max(r.seq);
                let later = after || matches!(until, Until::Record(n) if r.seq > n);
                if let (true, Some(record)) =
                    (later, r.record.as_ref().and_then(|l| l.record.as_ref()))
                {
                    found.take(r.seq, record);
                }
            }
        }
    }
    // Later generations: their segments after what was seen; a base beyond it holds the
    // records between only in its image, which is not read.
    for g in gens.iter().filter(|g| g.name > generation.name) {
        let Some(b) = &g.base else { continue };
        if b.seq > seen && plan.note.is_none() {
            plan.note = Some(format!(
                "records {} to {} are only in generation {}'s base: erasures among them are not listed",
                seen + 1,
                b.seq,
                g.name
            ));
        }
        seen = seen.max(b.seq);
        for segment in &g.segments {
            if segment.last <= seen {
                continue;
            }
            let plain = crypt.open(&segment.name, &target.get(&segment.name)?)?;
            let mut rest = plain.as_slice();
            while !rest.is_empty() {
                let taken = BackupBatch::decode_length_delimited(&mut rest)?;
                for r in taken.batch.unwrap_or_default().records {
                    if r.seq <= seen {
                        continue;
                    }
                    seen = r.seq;
                    if let Some(record) = r.record.as_ref().and_then(|l| l.record.as_ref()) {
                        found.take(r.seq, record);
                    }
                }
            }
        }
    }
    plan.erasures = found.erasures();
    Ok(plan)
}

/// What [`plan`] collects from the records after the point.
#[derive(Default)]
struct Found {
    erasures: Vec<Erasure>,
    commands: std::collections::HashMap<String, Command>,
}

impl Found {
    fn take(&mut self, seq: u64, record: &Record) {
        match record {
            Record::Enqueued(e) => {
                if let Some(c) = &e.command {
                    self.commands.insert(c.command_id.clone(), c.clone());
                }
            }
            Record::Committed(c) => {
                for e in c.events.iter().filter_map(|l| l.event.as_ref()) {
                    if e.erases.is_empty()
                        || self
                            .erasures
                            .iter()
                            .any(|x| x.command_id == e.command_id && x.subject == e.erases)
                    {
                        continue;
                    }
                    self.erasures.push(Erasure {
                        record: seq,
                        position: e.global_position,
                        at: e.created_at,
                        tenant: e.tenant_id.clone(),
                        domain: e.domain.clone(),
                        aggregate_type: e.aggregate_type.clone(),
                        aggregate_id: e.aggregate_id.clone(),
                        event_type: e.event_type.clone(),
                        subject: e.erases.clone(),
                        command_id: e.command_id.clone(),
                        command: None,
                    });
                }
            }
            _ => {}
        }
    }

    fn erasures(mut self) -> Vec<Erasure> {
        for e in &mut self.erasures {
            e.command = self.commands.get(&e.command_id).cloned();
        }
        self.erasures
    }
}

impl Erasure {
    /// One line for people: when, which subject in which tenant, by which event.
    pub fn line(&self) -> String {
        format!(
            "{}  {}/{}  {}.{} {} {} (command {})",
            crate::config::utc(self.at),
            self.tenant,
            self.subject,
            self.domain,
            self.aggregate_type,
            self.aggregate_id,
            self.event_type,
            self.command_id
        )
    }

    /// Whether `kernable erasures apply` can submit its command again: the backup holds it,
    /// and it carries no vault values (those are not in the list).
    pub fn reapplicable(&self) -> Result<&Command, String> {
        match &self.command {
            None => Err("its command was queued before the point: the restored store holds it, and the kernel runs it".into()),
            Some(c) if !c.vault_refs.is_empty() => Err("its command carries vault values: submit it again from your application".into()),
            Some(c) => Ok(c),
        }
    }

    /// The command that erases the subject again: the original's, with a new ID (`id`), its
    /// context kept (the domain authorizes it as before) but without an expected version, and
    /// the original named as its cause. Signed by whoever submits it.
    pub fn again(&self, id: &str) -> Result<Command, String> {
        let original = self.reapplicable()?;
        let mut context = original.context.clone().unwrap_or_default();
        context.expected_version = None;
        context.causation_id = original.command_id.clone();
        Ok(Command {
            command_id: id.to_owned(),
            client_sig: None,
            context: Some(context),
            deliver_at: 0,
            deliver_until: 0,
            ..original.clone()
        })
    }

    /// Whether its command's context sets `bypass_authorization`.
    pub fn privileged(&self) -> bool {
        self.command
            .as_ref()
            .and_then(|c| c.context.as_ref())
            .is_some_and(|c| c.bypass_authorization)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "record": self.record,
            "position": self.position,
            "at": crate::config::utc(self.at),
            "at_ms": self.at,
            "tenant": self.tenant,
            "domain": self.domain,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "subject": self.subject,
            "command_id": self.command_id,
            // The command as stored (protobuf, hex): what `kernable erasures apply` submits again.
            "command": self.command.as_ref().map(|c| hex::encode(c.encode_to_vec())),
            "command_type": self.command.as_ref().map(|c| c.command_type.clone()),
            // Its context sets bypass_authorization: whoever applies it again needs a key
            // granted privileged for it.
            "privileged": self.privileged(),
        })
    }

    pub fn from_json(v: &serde_json::Value) -> Result<Self, String> {
        let text = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_str())
                .map(str::to_owned)
                .ok_or_else(|| format!("an erasure without {k}"))
        };
        let number = |k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
        let command = match v.get("command").and_then(|c| c.as_str()) {
            Some(h) => Some(
                hex::decode(h)
                    .ok()
                    .and_then(|b| Command::decode(b.as_slice()).ok())
                    .ok_or("an erasure whose command does not decode")?,
            ),
            None => None,
        };
        Ok(Erasure {
            record: number("record"),
            position: number("position"),
            at: v.get("at_ms").and_then(|x| x.as_i64()).unwrap_or(0),
            tenant: text("tenant")?,
            domain: text("domain")?,
            aggregate_type: text("aggregate_type")?,
            aggregate_id: text("aggregate_id")?,
            event_type: text("event_type")?,
            subject: text("subject")?,
            command_id: text("command_id")?,
            command,
        })
    }
}

/// What `kernable backup plan --json` and `kernable restore --reapply-erasures` write: the
/// point, the generation and its vault key, and the erasures after the point.
pub fn plan_json(point: &str, plan: &Plan) -> String {
    let list = serde_json::json!({
        "point": point,
        "generation": plan.generation,
        "vault_key": plan.vault_key,
        "note": plan.note,
        "erasures": plan.erasures.iter().map(Erasure::to_json).collect::<Vec<_>>(),
    });
    serde_json::to_string_pretty(&list).expect("serializes") + "\n"
}

/// The erasures of what [`plan_json`] wrote.
pub fn read_erasures(text: &str) -> Result<Vec<Erasure>, String> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    v.get("erasures")
        .and_then(|e| e.as_array())
        .ok_or("no erasures in it")?
        .iter()
        .map(Erasure::from_json)
        .collect()
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
    restore_masked(target, crypt, stores, until, kernel_keys, None)
}

/// [`restore`], or with `mask` a masked copy ([`crate::copy`]): the backup's vault values are
/// left out (`stores` has no vault; the copy's own comes after), and its files unless asked.
/// Without `mask`, a backup of a masked copy is refused.
pub fn restore_masked(
    target: &dyn Target,
    crypt: &BackupKey,
    stores: &standby::Stores,
    until: Until,
    kernel_keys: &[String],
    mask: Option<crate::copy::Mask>,
) -> Result<Restored, BoxError> {
    if !crate::export::empty(stores.store.as_ref())? {
        return Err("the store holds data already: restore into new, empty stores".into());
    }
    let gens = generations(target, crypt)?;
    let generation = choose(&gens, until)?;
    let base = generation.base.clone().expect("usable");
    // The generation's vault values open with one vault key: the one it names.
    match &stores.vault {
        _ if mask.is_some() => {}
        Some(vault) if !base.vault_key.is_empty() && vault.key_id() != base.vault_key => {
            return Err(format!(
                "generation {} holds vault values sealed with vault key {}; the vault key given is {}: give the source's vault key of that time",
                generation.name,
                base.vault_key,
                vault.key_id()
            )
            .into());
        }
        None if !base.vault_key.is_empty() && base.vault_values > 0 => {
            return Err(format!(
                "generation {} holds vault values (vault key {}): give the vault and that key",
                generation.name, base.vault_key
            )
            .into());
        }
        _ => {}
    }
    let mut keys: Vec<String> = kernel_keys.to_vec();
    let named = kernel_keys.is_empty();
    // Without keys named, the ones the backup names; with them, a checkpoint signed by another
    // (a kernel key changed, or a restored kernel's own key) says which key is missing.
    let mut learn = |records: &mut dyn Iterator<Item = &Record>| -> Result<Vec<String>, BoxError> {
        for r in records {
            if let Record::Checkpoint(c) = r
                && let Some(sig) = &c.kernel_sig
                && !keys.contains(&sig.key_id)
            {
                if !named {
                    return Err(format!(
                        "the checkpoint of event {} is signed by the kernel key {}, which no --kernel-pub names: name every kernel key the source had, or none to take the ones the backup names",
                        c.global_position, sig.key_id
                    )
                    .into());
                }
                keys.push(sig.key_id.clone());
            }
        }
        Ok(keys.clone())
    };
    stores.store.keep_record_log(true)?;
    for n in 1..=base.parts {
        let part = part_name(&generation.name, n);
        let plain = crypt.open(&part, &target.get(&part)?)?;
        let mut rest = plain.as_slice();
        while !rest.is_empty() {
            let mut batch = ImageBatch::decode_length_delimited(&mut rest)?;
            match &mask {
                Some(mask) => mask.image(&mut batch),
                None => crate::copy::refuse_marked(
                    batch.records.iter().filter_map(|l| l.record.as_ref()),
                    "the backup",
                )?,
            }
            let keys = learn(&mut batch.records.iter().filter_map(|l| l.record.as_ref()))?;
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
            let records = || {
                batch
                    .records
                    .iter()
                    .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref()))
            };
            match &mask {
                Some(mask) => mask.records(&mut batch),
                None => crate::copy::refuse_marked(records(), "the backup")?,
            }
            let keys = learn(
                &mut batch
                    .records
                    .iter()
                    .filter_map(|r| r.record.as_ref().and_then(|l| l.record.as_ref())),
            )?;
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
    fn the_grant_is_read_again_from_its_file() {
        let file = std::env::temp_dir().join(format!("kn-backup-grant-{}", std::process::id()));
        std::fs::write(&file, "abcd\n").unwrap();
        assert_eq!(current_grant(Some(&file), "old"), "abcd");
        std::fs::write(&file, "ef01").unwrap();
        assert_eq!(current_grant(Some(&file), "old"), "ef01");
        std::fs::remove_file(&file).unwrap();
        assert_eq!(current_grant(Some(&file), "old"), "old");
        assert_eq!(current_grant(None, "old"), "old");
    }

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
    fn a_backup_written_with_another_key_is_refused_not_pruned() {
        let dir = std::env::temp_dir().join(format!("kn-backup-wrong-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let target = target::open(&format!("file:{}", dir.display()), None).unwrap();
        let right = BackupKey::new(&[7; 32]);
        for at in [1_000, 2_000] {
            let name = format!("{}/base.done", generation_name(at));
            let base = BackupBase {
                seq: 0,
                hash: vec![0; 32],
                ..Default::default()
            };
            target
                .put(&name, &right.seal(&name, &base.encode_to_vec()).unwrap())
                .unwrap();
        }
        let gens = generations(target.as_ref(), &right).unwrap();
        assert!(gens.iter().all(|g| g.base.is_some()));
        refuse_unreadable(&gens, &right).unwrap();

        let wrong = BackupKey::new(&[8; 32]);
        let gens = generations(target.as_ref(), &wrong).unwrap();
        assert!(
            gens.iter()
                .all(|g| g.base.is_none() && g.unreadable.is_some())
        );
        let refused = refuse_unreadable(&gens, &wrong).unwrap_err().to_string();
        assert!(
            refused.contains("can be read with the backup key"),
            "{refused}"
        );
        assert!(doomed(&gens, 1, &generation_name(3_000)).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn one_damaged_generation_is_left_out_and_kept_not_refused() {
        let dir = std::env::temp_dir().join(format!("kn-backup-damaged-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let target = target::open(&format!("file:{}", dir.display()), None).unwrap();
        let key = BackupKey::new(&[7; 32]);
        for at in [1_000, 2_000, 3_000] {
            let name = format!("{}/base.done", generation_name(at));
            let base = BackupBase {
                seq: 0,
                hash: vec![0; 32],
                ..Default::default()
            };
            let mut sealed = key.seal(&name, &base.encode_to_vec()).unwrap();
            if at == 1_000 {
                // Bit rot in the oldest generation's marker.
                let last = sealed.len() - 1;
                sealed[last] ^= 0x01;
            }
            target.put(&name, &sealed).unwrap();
        }
        let gens = generations(target.as_ref(), &key).unwrap();
        assert!(gens[0].base.is_none() && gens[0].unreadable.is_some());
        assert!(gens[1..].iter().all(|g| g.base.is_some()));
        refuse_unreadable(&gens, &key).unwrap();
        // Pruned to one: the older readable generation goes, the damaged one stays.
        let current = generation_name(4_000);
        let doomed = doomed(&gens, 1, &current);
        assert_eq!(doomed, HashSet::from([generation_name(2_000).as_str()]));
        let _ = std::fs::remove_dir_all(&dir);
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

    /// A record of a command `id` queued (`erases` empty) or committed with one event that
    /// erases `erases`.
    fn record(seq: u64, id: &str, erases: Option<&str>) -> kernable_core::proto::ReplicatedRecord {
        use kernable_core::proto::{
            CommittedRecord, EnqueuedRecord, Event, LogRecord, LoggedEvent, ReplicatedRecord,
            RequestContext,
        };
        let record = match erases {
            None => Record::Enqueued(EnqueuedRecord {
                command: Some(Command {
                    command_id: id.into(),
                    tenant_id: "t1".into(),
                    domain: "shop".into(),
                    aggregate_type: "customer".into(),
                    aggregate_id: "c1".into(),
                    command_type: "Erase".into(),
                    payload: b"{}".to_vec(),
                    context: Some(RequestContext {
                        sender_identity: "support".into(),
                        expected_version: Some(3),
                        // c4 a privileged one.
                        bypass_authorization: id == "c4",
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                created_at: 0,
            }),
            Some(subject) => Record::Committed(CommittedRecord {
                command_id: id.into(),
                events: vec![LoggedEvent {
                    event: Some(Event {
                        global_position: seq,
                        tenant_id: "t1".into(),
                        domain: "shop".into(),
                        aggregate_type: "customer".into(),
                        aggregate_id: "c1".into(),
                        event_type: "CustomerErased".into(),
                        command_id: id.into(),
                        created_at: seq as i64 * 1000,
                        erases: subject.into(),
                        ..Default::default()
                    }),
                    prev_hash: vec![],
                }],
                index: None,
            }),
        };
        ReplicatedRecord {
            seq,
            hash: vec![seq as u8; 32],
            record: Some(LogRecord {
                record: Some(record),
            }),
        }
    }

    /// Writes generation `at` with an empty base at `base` (sealed with vault key `vault_key`)
    /// and one segment per batch, each `(taken_at, records)`.
    fn write_generation(
        target: &dyn Target,
        key: &BackupKey,
        at: i64,
        base: u64,
        vault_key: &str,
        batches: Vec<(i64, Vec<kernable_core::proto::ReplicatedRecord>)>,
    ) {
        let name = generation_name(at);
        let marker = format!("{name}/base.done");
        let b = BackupBase {
            seq: base,
            hash: vec![0; 32],
            created_at: at,
            vault_key: vault_key.into(),
            ..Default::default()
        };
        target
            .put(&marker, &key.seal(&marker, &b.encode_to_vec()).unwrap())
            .unwrap();
        for (taken_at, records) in batches {
            let (first, last) = (records[0].seq, records.last().unwrap().seq);
            let segment = segment_name(&name, first, last, &[last as u8; 32]);
            let mut plain = Vec::new();
            BackupBatch {
                taken_at,
                batch: Some(RecordBatch {
                    records,
                    ..Default::default()
                }),
            }
            .encode_length_delimited(&mut plain)
            .unwrap();
            target
                .put(&segment, &key.seal(&segment, &plain).unwrap())
                .unwrap();
        }
    }

    #[test]
    fn a_plan_lists_the_erasures_after_the_point_with_their_commands() {
        let dir = std::env::temp_dir().join(format!("kn-backup-plan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let target = target::open(&format!("file:{}", dir.display()), None).unwrap();
        let key = BackupKey::new(&[7; 32]);
        // Generation 1: c1 erases ann before 2_000; c2 is queued before and commits after it;
        // c3 is queued and erases bob after it.
        write_generation(
            target.as_ref(),
            &key,
            1_000,
            0,
            "k1",
            vec![
                (
                    1_500,
                    vec![record(1, "c1", None), record(2, "c1", Some("ann"))],
                ),
                (1_900, vec![record(3, "c2", None)]),
                (
                    2_500,
                    vec![
                        record(4, "c2", Some("cid")),
                        record(5, "c3", None),
                        record(6, "c3", Some("bob")),
                    ],
                ),
            ],
        );
        // Generation 2 goes on from record 6 (its base), with another vault key: c4 erases eve.
        write_generation(
            target.as_ref(),
            &key,
            3_000,
            6,
            "k2",
            vec![(
                3_500,
                vec![record(7, "c4", None), record(8, "c4", Some("eve"))],
            )],
        );

        let plan = plan(target.as_ref(), &key, Until::Time(2_000)).unwrap();
        assert_eq!(
            (plan.generation.as_str(), plan.vault_key.as_str()),
            (generation_name(1_000).as_str(), "k1")
        );
        let subjects: Vec<&str> = plan.erasures.iter().map(|e| e.subject.as_str()).collect();
        assert_eq!(
            subjects,
            ["cid", "bob", "eve"],
            "after the point, oldest first"
        );
        assert_eq!(plan.note, None);
        // c2 was queued before the point: the restored store holds it, the kernel runs it.
        assert!(plan.erasures[0].command.is_none());
        assert!(
            plan.erasures[0]
                .reapplicable()
                .unwrap_err()
                .contains("queued before")
        );
        // c3's command again: a new ID, its context without the expected version, c3 its cause.
        let again = plan.erasures[1].again("n1").unwrap();
        assert_eq!(
            (again.command_id.as_str(), again.command_type.as_str()),
            ("n1", "Erase")
        );
        let context = again.context.unwrap();
        assert_eq!(context.expected_version, None);
        assert_eq!(
            (
                context.causation_id.as_str(),
                context.sender_identity.as_str()
            ),
            ("c3", "support")
        );
        assert!(again.client_sig.is_none(), "signed by whoever submits it");
        // The list as written, read back.
        let text = plan_json("2026-11-04T13:39:00Z", &plan);
        assert_eq!(read_erasures(&text).unwrap(), plan.erasures);
        assert!(text.contains("\"vault_key\": \"k1\""), "{text}");
        // c4 bypasses the domain's authorization: marked, for whoever applies it again.
        assert!(!plan.erasures[1].privileged() && plan.erasures[2].privileged());
        assert!(text.contains("\"privileged\": true"), "{text}");

        // To a record: the same, from the record on.
        let plan_r = super::plan(target.as_ref(), &key, Until::Record(4)).unwrap();
        let subjects: Vec<&str> = plan_r.erasures.iter().map(|e| e.subject.as_str()).collect();
        assert_eq!(subjects, ["bob", "eve"]);
        // To the newest: nothing after it, from the newest generation and its key.
        let end = super::plan(target.as_ref(), &key, Until::End).unwrap();
        assert!(end.erasures.is_empty());
        assert_eq!(end.vault_key, "k2");
        // A later generation whose base lies beyond the segments: those records are not read.
        write_generation(target.as_ref(), &key, 4_000, 20, "k2", vec![]);
        let noted = super::plan(target.as_ref(), &key, Until::Time(2_000)).unwrap();
        assert!(noted.note.unwrap().contains("records 9 to 20"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn files_kept_per_vault_key_go_with_the_last_generation_that_needs_the_key() {
        let generation = |name: &str, key: Option<&str>| Generation {
            name: name.into(),
            base: key.map(|k| BackupBase {
                vault_key: k.into(),
                ..Default::default()
            }),
            unreadable: None,
            segments: vec![],
        };
        let gens = vec![
            generation("g1", Some("k1")),
            generation("g2", Some("k1")),
            generation("g3", Some("k2")),
            generation("g4", Some("k3")),
        ];
        let doomed = |names: &[&'static str]| names.iter().copied().collect::<HashSet<&str>>();
        // g1 goes, g2 still needs k1.
        assert!(retired_keys(&gens, &doomed(&["g1"]), "k3").is_empty());
        // g1 and g2 go: k1's files go with them; k2 stays with g3.
        assert_eq!(retired_keys(&gens, &doomed(&["g1", "g2"]), "k3"), ["k1"]);
        assert_eq!(
            retired_keys(&gens, &doomed(&["g1", "g2", "g3"]), "k3"),
            ["k1", "k2"]
        );
        // The kernel's key now is kept, whatever generation goes; no key, nothing to delete.
        assert!(retired_keys(&gens, &doomed(&["g1", "g2"]), "k1").is_empty());
        let keyless = vec![generation("g1", Some("")), generation("g2", Some(""))];
        assert!(retired_keys(&keyless, &doomed(&["g1"]), "").is_empty());
        // A generation that stays but cannot be read may need any of them: none goes.
        let mut damaged = gens.clone();
        damaged.push(Generation {
            name: "g0".into(),
            base: None,
            unreadable: Some("bit rot".into()),
            segments: vec![],
        });
        assert!(retired_keys(&damaged, &doomed(&["g1", "g2"]), "k3").is_empty());
    }

    #[test]
    fn a_restore_refuses_a_vault_key_other_than_its_generation_s() {
        let dir = std::env::temp_dir().join(format!("kn-backup-vault-key-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let target = target::open(&format!("file:{}", dir.join("backup").display()), None).unwrap();
        let key = BackupKey::new(&[7; 32]);
        let vault_key = [5u8; 32];
        let other = kernable_core::signing::vault_key_id(&[6u8; 32]);
        write_generation(target.as_ref(), &key, 1_000, 0, &other, vec![]);
        let stores = standby::Stores {
            store: kernable_store::open(&format!("sqlite:{}", dir.join("k.db").display())).unwrap(),
            vault: Some(
                kernable_store::vault::Vault::open(
                    &format!("sqlite:{}", dir.join("v.db").display()),
                    &vault_key,
                )
                .unwrap(),
            ),
            data: kernable_store::data::open(&format!("file:{}", dir.join("data").display()))
                .unwrap(),
            snapshots: kernable_store::snapshots::open("memory:").unwrap(),
        };
        let refused = restore(target.as_ref(), &key, &stores, Until::End, &[])
            .unwrap_err()
            .to_string();
        assert!(refused.contains(&other), "{refused}");
        assert!(
            refused.contains(&kernable_core::signing::vault_key_id(&vault_key)),
            "{refused}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
