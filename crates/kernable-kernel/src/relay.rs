//! A relay (`kernable relay`): copies of writers' records outside their networks, for readers
//! there (docs/how-to/relays.md).
//!
//! A pusher beside the writer (`kernable push`, [`crate::push`]) follows the writer as a reader
//! does and sends what it gets here over a connection it opens itself (`RelayService.Push`). The
//! relay writes it to the copy's log and serves it to readers as a writer would
//! (`ReplicationService.Follow`, `reader`, with `copy` naming the copy): an image when a reader
//! is new or behind the copy's image, then the record batches. It applies nothing and checks no
//! record; every reader checks each one against the writer's checkpoints as it does from the
//! writer, so a relay can hold records back but never change one.
//!
//! Each copy is a directory `<dir>/<copy>/` with one file, `log`: frames of what the pusher sent
//! (image batches and the image's end, then record batches), each with its length, its kind, the
//! first and last record it holds and a checksum. A new image (or an empty one, which empties the
//! copy) is written to `log.new` and replaces `log` once it ended. On start the relay reads each
//! log up to its last whole frame and cuts off the rest.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering::SeqCst},
    },
    time::Duration,
};

use kernable_core::{
    proto::{
        Challenge, FollowMode, FollowRequest, FollowResponse, FollowStart, Grant, ImageEnd,
        PromoteRequest, PromoteResult, PushRequest, PushResponse, RecordBatch, RecordPoint,
        SwitchoverRequest, SwitchoverResult, follow_request, follow_response, push_request,
        push_response,
        relay_service_server::{RelayService, RelayServiceServer},
        replication_service_server::{ReplicationService, ReplicationServiceServer},
    },
    signing, transport,
};
use kernable_store::replication::ZERO;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

use crate::{BoxError, GRANT_HEADER, Permissions, config, protocol, replication};

/// How a relay runs.
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub listen: String,
    /// Where the copies are kept, one directory each.
    pub dir: PathBuf,
    /// Root key IDs: a reader's grant must be signed by one of them (or delegated from one) and
    /// allow `read`.
    pub roots: Vec<String>,
    /// Key IDs of the pushers it takes copies from.
    pub pushers: Vec<String>,
    pub tls: Option<config::Tls>,
}

/// How often a reader hears where the copy stands while nothing new comes, as from a writer.
const BEAT: Duration = Duration::from_secs(1);

/// A frame's header: length (4), kind (1), first and last record (8 each), checksum (8).
const HEADER: usize = 29;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Image = 1,
    End = 2,
    Records = 3,
}

impl Kind {
    fn of(byte: u8) -> Option<Kind> {
        match byte {
            1 => Some(Kind::Image),
            2 => Some(Kind::End),
            3 => Some(Kind::Records),
            _ => None,
        }
    }
}

/// Where a frame is in the log, and the records it holds (an image's end: its record, twice).
#[derive(Debug, Clone, Copy)]
struct Frame {
    kind: Kind,
    offset: u64,
    len: u32,
    first: u64,
    last: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct Point {
    seq: u64,
    hash: Vec<u8>,
}

impl Point {
    fn zero() -> Point {
        Point {
            seq: 0,
            hash: ZERO.to_vec(),
        }
    }
}

fn sum(payload: &[u8]) -> [u8; 8] {
    let digest = Sha256::digest(payload);
    let mut out = [0; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

/// Appends one frame to `file` at `offset`; returns it.
fn append(
    file: &mut File,
    offset: u64,
    kind: Kind,
    first: u64,
    last: u64,
    msg: &PushRequest,
) -> std::io::Result<Frame> {
    let payload = msg.encode_to_vec();
    let len = u32::try_from(payload.len()).map_err(std::io::Error::other)?;
    let mut buf = Vec::with_capacity(HEADER + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.push(kind as u8);
    buf.extend_from_slice(&first.to_be_bytes());
    buf.extend_from_slice(&last.to_be_bytes());
    buf.extend_from_slice(&sum(&payload));
    buf.extend_from_slice(&payload);
    file.write_all(&buf)?;
    Ok(Frame {
        kind,
        offset,
        len,
        first,
        last,
    })
}

/// The frames of the log at `path` up to the last whole one, and where that one ends.
fn scan(path: &Path) -> std::io::Result<(Vec<Frame>, u64)> {
    let mut file = std::io::BufReader::new(File::open(path)?);
    let (mut frames, mut offset) = (Vec::new(), 0u64);
    loop {
        let mut header = [0u8; HEADER];
        if file.read_exact(&mut header).is_err() {
            break;
        }
        let len = u32::from_be_bytes(header[0..4].try_into().expect("4 bytes"));
        let Some(kind) = Kind::of(header[4]) else {
            break;
        };
        let first = u64::from_be_bytes(header[5..13].try_into().expect("8 bytes"));
        let last = u64::from_be_bytes(header[13..21].try_into().expect("8 bytes"));
        let mut payload = vec![0; len as usize];
        if file.read_exact(&mut payload).is_err() || sum(&payload) != header[21..29] {
            break;
        }
        frames.push(Frame {
            kind,
            offset,
            len,
            first,
            last,
        });
        offset += (HEADER + len as usize) as u64;
    }
    Ok((frames, offset))
}

fn read_frame(file: &File, frame: &Frame) -> std::io::Result<PushRequest> {
    let mut payload = vec![0; frame.len as usize];
    file.read_exact_at(&mut payload, frame.offset + HEADER as u64)?;
    PushRequest::decode(payload.as_slice()).map_err(std::io::Error::other)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

/// One copy's log as it stands.
struct Log {
    dir: PathBuf,
    file: File,
    end: u64,
    frames: Vec<Frame>,
    /// Where its image stands (the zero point without one): its records follow from there.
    base: Point,
    /// Its newest record.
    head: Point,
    /// Counts the times it began anew (a new image): a reader's stream from an older one ends.
    generation: u64,
}

impl Log {
    fn open(dir: &Path) -> Result<Log, BoxError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("log");
        // A new image that did not end is not the copy.
        let _ = std::fs::remove_file(dir.join("log.new"));
        if !path.exists() {
            File::create(&path)?.sync_all()?;
            sync_dir(dir)?;
        }
        let (frames, end) = scan(&path)?;
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        if file.metadata()?.len() > end {
            tracing::warn!(path = %path.display(), at = end, "the copy's log ends in a torn frame: cut off");
            file.set_len(end)?;
            file.sync_all()?;
        }
        let mut log = Log {
            dir: dir.to_owned(),
            file,
            end,
            frames,
            base: Point::zero(),
            head: Point::zero(),
            generation: 0,
        };
        log.points()?;
        Ok(log)
    }

    /// Sets `base` and `head` from the frames.
    fn points(&mut self) -> Result<(), BoxError> {
        self.base = match self.frames.iter().find(|f| f.kind == Kind::End) {
            Some(end) => match read_frame(&self.file, end)?.msg {
                Some(push_request::Msg::ImageEnd(e)) => Point {
                    seq: e.seq,
                    hash: e.hash,
                },
                _ => return Err("an image's end frame holds no image end".into()),
            },
            None => Point::zero(),
        };
        self.head = match self.frames.last() {
            Some(f) if f.kind == Kind::Records => match read_frame(&self.file, f)?.msg {
                Some(push_request::Msg::Records(batch)) => {
                    let last = batch.records.last().ok_or("an empty record frame")?;
                    Point {
                        seq: last.seq,
                        hash: last.hash.clone(),
                    }
                }
                _ => return Err("a record frame holds no records".into()),
            },
            _ => self.base.clone(),
        };
        Ok(())
    }

    /// Whether it holds anything: an image, an emptied copy, or records.
    fn holds(&self) -> bool {
        !self.frames.is_empty()
    }

    /// The hash of record `seq`, if the copy holds it (or stands on it).
    fn hash_at(&self, seq: u64) -> Result<Option<Vec<u8>>, BoxError> {
        if seq == self.base.seq {
            return Ok(Some(self.base.hash.clone()));
        }
        let at = self
            .frames
            .partition_point(|f| f.kind != Kind::Records || f.last < seq);
        let Some(frame) = self.frames.get(at).filter(|f| f.first <= seq) else {
            return Ok(None);
        };
        match read_frame(&self.file, frame)?.msg {
            Some(push_request::Msg::Records(batch)) => Ok(batch
                .records
                .into_iter()
                .find(|r| r.seq == seq)
                .map(|r| r.hash)),
            _ => Ok(None),
        }
    }

    /// Appends a record batch that continues the copy, synced.
    fn add(&mut self, batch: RecordBatch) -> Result<u64, Status> {
        let (Some(first), Some(last)) = (batch.records.first(), batch.records.last()) else {
            return Ok(self.head.seq);
        };
        if first.seq != self.head.seq + 1
            || batch.records.windows(2).any(|w| w[1].seq != w[0].seq + 1)
        {
            return Err(Status::invalid_argument(format!(
                "records {}..{} do not continue the copy at {}",
                first.seq, last.seq, self.head.seq
            )));
        }
        let (first, last) = (first.seq, last.seq);
        let head = Point {
            seq: last,
            hash: batch.records.last().expect("not empty").hash.clone(),
        };
        let msg = PushRequest {
            msg: Some(push_request::Msg::Records(batch)),
        };
        let frame = append(&mut self.file, self.end, Kind::Records, first, last, &msg)
            .and_then(|f| self.file.sync_data().map(|()| f))
            .map_err(|e| Status::internal(format!("writing the copy failed: {e}")))?;
        self.end += (HEADER + frame.len as usize) as u64;
        self.frames.push(frame);
        self.head = head;
        Ok(last)
    }
}

/// A new image being written, until it ends.
struct Image {
    file: File,
    end: u64,
    frames: Vec<Frame>,
}

impl Image {
    fn begin(dir: &Path) -> std::io::Result<Image> {
        Ok(Image {
            file: File::create(dir.join("log.new"))?,
            end: 0,
            frames: Vec::new(),
        })
    }

    fn push(&mut self, kind: Kind, seq: u64, msg: &PushRequest) -> std::io::Result<()> {
        let frame = append(&mut self.file, self.end, kind, seq, seq, msg)?;
        self.end += (HEADER + frame.len as usize) as u64;
        self.frames.push(frame);
        Ok(())
    }
}

/// One copy: its log, who pushes to it, and what readers wait on.
struct Copy {
    name: String,
    log: Mutex<Log>,
    pushing: AtomicBool,
    /// (generation, head): readers wake when it changes.
    changed: watch::Sender<(u64, u64)>,
}

impl Copy {
    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn tell(&self, log: &Log) {
        self.changed.send_replace((log.generation, log.head.seq));
    }

    /// Replaces the log with the image just written (`log.new`), synced.
    fn replace(&self, image: Image) -> Result<(), BoxError> {
        image.file.sync_all()?;
        let mut log = self.log();
        let (new, path) = (log.dir.join("log.new"), log.dir.join("log"));
        std::fs::rename(&new, &path)?;
        sync_dir(&log.dir)?;
        log.file = OpenOptions::new().read(true).append(true).open(&path)?;
        log.end = image.end;
        log.frames = image.frames;
        log.generation += 1;
        log.points()?;
        self.tell(&log);
        Ok(())
    }
}

/// Marks a copy pushed to while held.
struct Pushing(Arc<Copy>);

impl Drop for Pushing {
    fn drop(&mut self) {
        self.0.pushing.store(false, SeqCst);
    }
}

struct Relay {
    config: RelayConfig,
    copies: Mutex<HashMap<String, Arc<Copy>>>,
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name.len() <= 63
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
}

impl Relay {
    /// The copy named `name`; with `create`, a new one if it has none.
    fn copy(&self, name: &str, create: bool) -> Result<Arc<Copy>, Status> {
        if !valid_name(name) {
            return Err(Status::invalid_argument(format!(
                "{name:?} is no copy name: lowercase letters, digits, '.', '_' and '-', at most 63"
            )));
        }
        let mut copies = self.copies.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(copy) = copies.get(name) {
            return Ok(Arc::clone(copy));
        }
        let dir = self.config.dir.join(name);
        if !create && !dir.join("log").exists() {
            return Err(Status::unavailable(format!(
                "this relay holds no copy {name} yet"
            )));
        }
        let log = Log::open(&dir).map_err(|e| Status::internal(format!("copy {name}: {e}")))?;
        let (changed, _) = watch::channel((log.generation, log.head.seq));
        let copy = Arc::new(Copy {
            name: name.to_owned(),
            log: Mutex::new(log),
            pushing: AtomicBool::new(false),
            changed,
        });
        copies.insert(name.to_owned(), Arc::clone(&copy));
        Ok(copy)
    }

    /// A reader's grant, presented with its stream: signed by a root (or delegated from one)
    /// and allowing `read`.
    fn reader_may(&self, metadata: &tonic::metadata::MetadataMap, key: &str) -> Result<(), Status> {
        let grant = metadata
            .get(GRANT_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| hex::decode(v.trim()).ok())
            .and_then(|b| Grant::decode(b.as_slice()).ok())
            .filter(|g| g.key_id == key)
            .filter(|g| crate::grants::chain(g, &self.config.roots, None).is_ok())
            .ok_or_else(|| {
                Status::permission_denied("present the reader's grant, signed by a trusted root")
            })?;
        let grants = HashMap::from([(grant.key_id.clone(), grant)]);
        if !config::allowed(&Permissions::new(&grants, &HashMap::new()).read, key, "*") {
            return Err(Status::permission_denied(
                "the key may not follow as a reader: its grant needs read",
            ));
        }
        Ok(())
    }
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

async fn challenge<T, F>(
    inbound: &mut Streaming<T>,
    key: &str,
    send_nonce: impl AsyncFnOnce(Vec<u8>) -> Result<(), Status>,
    signature: F,
) -> Result<(), Status>
where
    F: Fn(T) -> Option<Vec<u8>>,
{
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| Status::internal("no randomness"))?;
    send_nonce(nonce.to_vec()).await?;
    let sig = inbound
        .message()
        .await?
        .and_then(signature)
        .ok_or_else(|| Status::invalid_argument("expected ChallengeResponse"))?;
    let proof = kernable_core::proto::Signature {
        key_id: key.to_owned(),
        sig,
    };
    signing::verify(&proof, &signing::challenge_message(&nonce))
        .map_err(|_| Status::unauthenticated("challenge failed"))
}

#[derive(Clone)]
struct Api(Arc<Relay>);

type PushOut = mpsc::Sender<Result<PushResponse, Status>>;

async fn say(out: &PushOut, msg: push_response::Msg) -> Result<(), Status> {
    out.send(Ok(PushResponse { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("the pusher left"))
}

async fn push(
    relay: Arc<Relay>,
    inbound: &mut Streaming<PushRequest>,
    out: &PushOut,
) -> Result<(), Status> {
    let Some(push_request::Msg::Hello(hello)) = inbound.message().await?.and_then(|m| m.msg) else {
        return Err(Status::invalid_argument("expected PushHello"));
    };
    protocol(hello.protocol)?;
    if !relay.config.pushers.contains(&hello.key_id) {
        return Err(Status::permission_denied(
            "this relay takes no pushes from the key (--allow-push)",
        ));
    }
    challenge(
        inbound,
        &hello.key_id,
        async |nonce| say(out, push_response::Msg::Challenge(Challenge { nonce })).await,
        |m: PushRequest| match m.msg {
            Some(push_request::Msg::ChallengeResponse(r)) => Some(r.sig),
            _ => None,
        },
    )
    .await?;
    let copy = relay.copy(&hello.copy, true)?;
    if copy.pushing.swap(true, SeqCst) {
        return Err(Status::already_exists(format!(
            "copy {}: another pusher pushes to it",
            copy.name
        )));
    }
    let _pushing = Pushing(Arc::clone(&copy));
    let start = copy.log().head.clone();
    tracing::info!(copy = %copy.name, key = %hello.key_id, at = start.seq, "a pusher pushes");
    say(
        out,
        push_response::Msg::Start(RecordPoint {
            seq: start.seq,
            hash: start.hash,
        }),
    )
    .await?;
    let mut image: Option<Image> = None;
    let dir = copy.log().dir.clone();
    while let Some(message) = inbound.message().await? {
        let ack = match message.msg {
            Some(push_request::Msg::Image(batch)) => {
                if image.is_none() {
                    tracing::info!(copy = %copy.name, "a new image begins");
                    image = Some(Image::begin(&dir).map_err(internal)?);
                }
                let msg = PushRequest {
                    msg: Some(push_request::Msg::Image(batch)),
                };
                let writing = image.as_mut().expect("begun above");
                writing.push(Kind::Image, 0, &msg).map_err(internal)?;
                None
            }
            Some(push_request::Msg::ImageEnd(end)) => {
                let mut writing = match image.take() {
                    Some(writing) => writing,
                    // The writer's log no longer continues the copy: it begins anew, empty.
                    None if end.seq == 0 => Image::begin(&dir).map_err(internal)?,
                    None => {
                        return Err(Status::invalid_argument(
                            "an image's end without an image (only an empty copy, at 0, has none)",
                        ));
                    }
                };
                let seq = end.seq;
                let msg = PushRequest {
                    msg: Some(push_request::Msg::ImageEnd(end)),
                };
                writing.push(Kind::End, seq, &msg).map_err(internal)?;
                copy.replace(writing).map_err(internal)?;
                tracing::info!(copy = %copy.name, at = seq, "the copy begins anew");
                Some(seq)
            }
            Some(push_request::Msg::Records(batch)) if image.is_none() => {
                if batch.records.is_empty() {
                    None
                } else {
                    let copy = Arc::clone(&copy);
                    let seq = tokio::task::spawn_blocking(move || {
                        let mut log = copy.log();
                        let seq = log.add(batch)?;
                        copy.tell(&log);
                        Ok::<_, Status>(seq)
                    })
                    .await
                    .map_err(internal)??;
                    Some(seq)
                }
            }
            _ => return Err(Status::invalid_argument("unexpected message")),
        };
        if let Some(seq) = ack {
            say(
                out,
                push_response::Msg::Ack(kernable_core::proto::FollowAck { seq }),
            )
            .await?;
        }
    }
    Ok(())
}

type FollowOut = mpsc::Sender<Result<FollowResponse, Status>>;

async fn tell(out: &FollowOut, msg: follow_response::Msg) -> Result<(), Status> {
    out.send(Ok(FollowResponse { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("the reader left"))
}

/// Reads frames off the thread pool.
async fn frames(file: Arc<File>, frames: Vec<Frame>) -> Result<Vec<PushRequest>, Status> {
    tokio::task::spawn_blocking(move || {
        frames
            .iter()
            .map(|f| read_frame(&file, f))
            .collect::<std::io::Result<Vec<_>>>()
    })
    .await
    .map_err(internal)?
    .map_err(internal)
}

async fn follow(
    relay: Arc<Relay>,
    metadata: &tonic::metadata::MetadataMap,
    inbound: &mut Streaming<FollowRequest>,
    out: &FollowOut,
) -> Result<(), Status> {
    let Some(follow_request::Msg::Hello(hello)) = inbound.message().await?.and_then(|m| m.msg)
    else {
        return Err(Status::invalid_argument("expected FollowHello"));
    };
    protocol(hello.protocol)?;
    if !hello.reader || hello.backup {
        return Err(Status::failed_precondition(
            "a relay serves readers only: a standby or a backup follower follows the writer",
        ));
    }
    if hello.copy.is_empty() {
        return Err(Status::invalid_argument(
            "name the copy to follow at this relay (--reader-copy)",
        ));
    }
    relay.reader_may(metadata, &hello.key_id)?;
    challenge(
        inbound,
        &hello.key_id,
        async |nonce| tell(out, follow_response::Msg::Challenge(Challenge { nonce })).await,
        |m: FollowRequest| match m.msg {
            Some(follow_request::Msg::ChallengeResponse(r)) => Some(r.sig),
            _ => None,
        },
    )
    .await?;
    let copy = relay.copy(&hello.copy, false)?;
    let mut changed = copy.changed.subscribe();

    // Where the reader stands against the copy.
    let (file, generation, all, base, head) = {
        let log = copy.log();
        if !log.holds() {
            return Err(Status::unavailable(format!(
                "the copy {} holds nothing yet",
                copy.name
            )));
        }
        (
            Arc::new(log.file.try_clone().map_err(internal)?),
            log.generation,
            log.frames.clone(),
            log.base.clone(),
            log.head.clone(),
        )
    };
    let mode = if hello.seq < base.seq {
        FollowMode::Image
    } else if hello.seq > head.seq {
        // The reader followed the writer, or another relay, further than this copy holds.
        return Err(Status::unavailable(format!(
            "the copy {} holds records up to {}, the reader up to {}: it waits for the copy",
            copy.name, head.seq, hello.seq
        )));
    } else {
        let log_hash = {
            let copy = Arc::clone(&copy);
            let seq = hello.seq;
            tokio::task::spawn_blocking(move || copy.log().hash_at(seq))
                .await
                .map_err(internal)?
                .map_err(internal)?
        };
        if log_hash.as_ref() == Some(&hello.hash) {
            FollowMode::Follow
        } else {
            FollowMode::Diverged
        }
    };
    tracing::info!(copy = %copy.name, reader = %hello.key_id, from = hello.seq, mode = ?mode, "a reader follows");
    tell(
        out,
        follow_response::Msg::Start(FollowStart {
            mode: mode.into(),
            message: match mode {
                FollowMode::Diverged => {
                    format!("the reader's record {} differs from the copy's", hello.seq)
                }
                _ => String::new(),
            },
            ..Default::default()
        }),
    )
    .await?;
    let mut sent = match mode {
        FollowMode::Follow => hello.seq,
        FollowMode::Image => {
            for frame in all.iter().filter(|f| f.kind != Kind::Records) {
                let msg = frames(Arc::clone(&file), vec![*frame]).await?;
                match msg.into_iter().next().and_then(|m| m.msg) {
                    Some(push_request::Msg::Image(batch)) => {
                        tell(out, follow_response::Msg::Image(batch)).await?;
                    }
                    Some(push_request::Msg::ImageEnd(ImageEnd { seq, hash })) => {
                        tell(out, follow_response::Msg::ImageEnd(ImageEnd { seq, hash })).await?;
                    }
                    _ => return Err(internal("the copy's image holds something else")),
                }
            }
            base.seq
        }
        _ => return Ok(()),
    };
    let mut next = all.partition_point(|f| f.kind != Kind::Records || f.last <= sent);
    let mut beat = tokio::time::Instant::now();
    loop {
        let (pending, head) = {
            let log = copy.log();
            if log.generation != generation {
                return Err(Status::unavailable(format!(
                    "the copy {} began anew: follow it again",
                    copy.name
                )));
            }
            (
                log.frames[next.min(log.frames.len())..].to_vec(),
                log.head.seq,
            )
        };
        if !pending.is_empty() {
            next += pending.len();
            // A few at a time: a reader far behind gets them as it takes them.
            for chunk in pending.chunks(8) {
                for msg in frames(Arc::clone(&file), chunk.to_vec()).await? {
                    let Some(push_request::Msg::Records(mut batch)) = msg.msg else {
                        continue;
                    };
                    batch.records.retain(|r| r.seq > sent);
                    let Some(last) = batch.records.last() else {
                        continue;
                    };
                    sent = last.seq;
                    batch.head = head.max(sent);
                    tell(out, follow_response::Msg::Records(batch)).await?;
                }
            }
            beat = tokio::time::Instant::now();
            continue;
        }
        if beat.elapsed() >= BEAT {
            beat = tokio::time::Instant::now();
            tell(
                out,
                follow_response::Msg::Records(RecordBatch {
                    head,
                    ..Default::default()
                }),
            )
            .await?;
        }
        tokio::select! {
            message = inbound.message() => match message?.and_then(|m| m.msg) {
                // What the reader applied: the relay keeps the whole copy, nothing to free.
                Some(follow_request::Msg::Ack(_)) => {}
                Some(follow_request::Msg::Leaving(_)) | None => return Ok(()),
                Some(_) => return Err(Status::invalid_argument("unexpected message")),
            },
            _ = changed.changed() => {}
            () = tokio::time::sleep_until(beat + BEAT) => {}
        }
    }
}

#[tonic::async_trait]
impl RelayService for Api {
    type PushStream = ReceiverStream<Result<PushResponse, Status>>;

    async fn push(
        &self,
        request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        let relay = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = push(relay, &mut inbound, &tx).await {
                tracing::warn!(error = %status.message(), "a push ended");
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl ReplicationService for Api {
    type FollowStream = ReceiverStream<Result<FollowResponse, Status>>;

    async fn follow(
        &self,
        request: Request<Streaming<FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let (tx, rx) = mpsc::channel(16);
        let relay = Arc::clone(&self.0);
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            if let Err(status) = follow(relay, &metadata, &mut inbound, &tx).await {
                tracing::debug!(error = %status.message(), "a reader's stream ended");
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn promote(
        &self,
        _request: Request<PromoteRequest>,
    ) -> Result<Response<PromoteResult>, Status> {
        Err(Status::failed_precondition(
            "this is a relay: it holds copies, it is never promoted",
        ))
    }

    async fn switchover(
        &self,
        _request: Request<SwitchoverRequest>,
    ) -> Result<Response<SwitchoverResult>, Status> {
        Err(Status::failed_precondition(
            "this is a relay: ask the writer to switch over",
        ))
    }
}

/// Serves the relay until it fails.
pub async fn run(config: RelayConfig) -> Result<(), BoxError> {
    std::fs::create_dir_all(&config.dir)?;
    let listen = config.listen.clone();
    if config.tls.is_none() && transport::unix_path(&listen).is_none() && !crate::loopback(&listen)
    {
        return Err(format!(
            "{listen} is reachable from other machines: use mutual TLS (--tls-cert, --tls-key, --tls-client-ca)"
        )
        .into());
    }
    let mut builder = Server::builder();
    if let Some(tls) = &config.tls {
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let client_ca = Certificate::from_pem(std::fs::read(&tls.client_ca)?);
        builder = builder.tls_config(
            ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca),
        )?;
    }
    tracing::info!(%listen, dir = %config.dir.display(), "relay starting");
    let api = Api(Arc::new(Relay {
        config,
        copies: Mutex::default(),
    }));
    let router = builder
        .add_service(
            RelayServiceServer::new(api.clone())
                .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(replication::MAX_MESSAGE_BYTES),
        )
        .add_service(
            ReplicationServiceServer::new(api)
                .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
                .max_encoding_message_size(replication::MAX_MESSAGE_BYTES),
        );
    match transport::unix_path(&listen) {
        Some(path) => {
            use std::os::unix::fs::FileTypeExt;
            if std::fs::metadata(path).is_ok_and(|m| m.file_type().is_socket()) {
                std::fs::remove_file(path)?;
            }
            let listener = tokio::net::UnixListener::bind(path)?;
            crate::socket_mode(path)?;
            router
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await?;
        }
        None => {
            let addr = tokio::net::lookup_host(&listen)
                .await?
                .next()
                .ok_or_else(|| format!("{listen}: no address"))?;
            router.serve(addr).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernable_core::proto::{ImageBatch, ReplicatedRecord};

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kn-relay-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn batch(from: u64, to: u64) -> RecordBatch {
        RecordBatch {
            records: (from..=to)
                .map(|seq| ReplicatedRecord {
                    seq,
                    hash: vec![seq as u8; 32],
                    record: None,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_log_keeps_what_was_added_and_cuts_off_a_torn_frame() {
        let d = dir("torn");
        {
            let mut log = Log::open(&d).unwrap();
            assert!(!log.holds());
            assert_eq!(log.add(batch(1, 3)).unwrap(), 3);
            assert_eq!(log.add(batch(4, 5)).unwrap(), 5);
            assert!(log.add(batch(7, 8)).is_err(), "a gap is refused");
        }
        // Half a frame more, as from a crash while writing.
        let mut file = OpenOptions::new().append(true).open(d.join("log")).unwrap();
        file.write_all(&[0, 0, 1, 0, 3, 9]).unwrap();
        drop(file);
        let log = Log::open(&d).unwrap();
        assert_eq!(log.head.seq, 5);
        assert_eq!(log.hash_at(4).unwrap(), Some(vec![4; 32]));
        assert_eq!(log.hash_at(0).unwrap(), Some(ZERO.to_vec()));
        assert_eq!(log.hash_at(6).unwrap(), None);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn an_image_replaces_the_log() {
        let d = dir("image");
        let copy = {
            let log = Log::open(&d).unwrap();
            let (changed, _) = watch::channel((0, 0));
            Copy {
                name: "c".into(),
                log: Mutex::new(log),
                pushing: AtomicBool::new(false),
                changed,
            }
        };
        copy.log().add(batch(1, 4)).unwrap();
        let mut image = Image::begin(&d).unwrap();
        let msg = |m| PushRequest { msg: Some(m) };
        image
            .push(
                Kind::Image,
                0,
                &msg(push_request::Msg::Image(ImageBatch::default())),
            )
            .unwrap();
        image
            .push(
                Kind::End,
                10,
                &msg(push_request::Msg::ImageEnd(ImageEnd {
                    seq: 10,
                    hash: vec![10; 32],
                })),
            )
            .unwrap();
        copy.replace(image).unwrap();
        {
            let mut log = copy.log();
            assert_eq!((log.base.seq, log.head.seq, log.generation), (10, 10, 1));
            assert!(log.add(batch(5, 6)).is_err());
            assert_eq!(log.add(batch(11, 12)).unwrap(), 12);
        }
        let log = Log::open(&d).unwrap();
        assert_eq!((log.base.seq, log.head.seq), (10, 12));
        assert_eq!(log.hash_at(10).unwrap(), Some(vec![10; 32]));
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn copy_names_are_plain() {
        assert!(valid_name("shop-eu.1"));
        assert!(!valid_name("../x"));
        assert!(!valid_name(""));
        assert!(!valid_name("Shop"));
    }
}
