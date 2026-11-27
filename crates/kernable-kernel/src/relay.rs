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
//!
//! Beside the log, for whoever operates the relay: `max-bytes`, if it exists, the most the log may
//! hold (a push that would make it larger is refused with `RESOURCE_EXHAUSTED`, and the copy
//! stops following until the limit rises), and `served`, the bytes sent to readers since the copy
//! began, written every ten seconds.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    },
    time::Duration,
};

use kernable_core::{
    proto::{
        CallsRequest, CallsResponse, Challenge, FollowMode, FollowRequest, FollowResponse,
        FollowStart, PromoteRequest, PromoteResult, PushRequest, PushResponse, RecordBatch,
        RecordPoint, SealedCall, SealedReply, SwitchoverRequest, SwitchoverResult, calls_request,
        calls_response, follow_request, follow_response, push_request, push_response,
        relay_service_server::{RelayService, RelayServiceServer},
        replication_service_server::{ReplicationService, ReplicationServiceServer},
    },
    signing, transport,
};
use kernable_store::replication::ZERO;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Certificate, Identity, Server, ServerTlsConfig},
};

use crate::{BoxError, MAX_REQUEST_AGE_MS, now_ms, protocol, replication};

/// How a relay runs.
#[derive(Debug, Clone)]
pub struct RelayConfig {
    pub listen: String,
    /// Where the copies are kept, one directory each.
    pub dir: PathBuf,
    /// Key IDs of the readers it serves every copy to (`--allow-read`).
    pub readers: Vec<String>,
    /// Key IDs of the pushers it takes copies from (`--allow-push`).
    pub pushers: Vec<String>,
    /// `--access`: per copy, the keys it takes and their state, for a relay several customers
    /// share. With it, a copy it does not name is refused, and `readers` and `pushers` count for
    /// none of them.
    pub access: Option<PathBuf>,
    pub tls: Option<RelayTls>,
}

/// The relay's TLS: its certificate and key (a public CA's, say Let's Encrypt), and, only for
/// mutual TLS, the CA its clients' certificates must chain to. Without one, any client may
/// connect: pushers and readers prove their keys to the relay all the same (a pusher answers a
/// challenge, a reader shows a grant its root signed, each call signed), and what a copy holds
/// is sealed with a key the relay never sees.
#[derive(Debug, Clone)]
pub struct RelayTls {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: Option<PathBuf>,
}

/// `--access`: who may push to and read each copy, by key (design/writer-from-copy.md): per copy,
/// each key ID with its role and its state.
///
/// ```toml
/// [copies."acme.shop.eu".keys]
/// "<key ID>" = { role = "push", state = "active" }
/// "<key ID>" = { role = "read", state = "active" }
/// ```
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Access {
    #[serde(default)]
    pub copies: HashMap<String, CopyAccess>,
}

#[derive(Debug, Default, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopyAccess {
    /// The keys it takes, by key ID.
    #[serde(default)]
    pub keys: HashMap<String, KeyAccess>,
}

/// What a key may do with a copy, and whether it may now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyAccess {
    pub role: KeyRole,
    #[serde(default)]
    pub state: KeyState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyRole {
    /// Pushes the copy (and takes its readers' calls): a writer's pusher.
    Push,
    /// Follows the copy (and sends calls through it): a reader, or a writer starting from it.
    Read,
}

/// A key's state: only an active key is let in; a stopped or revoked one is refused, at once on
/// a connection it holds (checked every few seconds). How the instance behind it ends differs
/// (design/writer-from-copy.md): a stopped writer first pushes what it has, a revoked one not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyState {
    #[default]
    Active,
    Stopped,
    Revoked,
}

impl Access {
    /// Read at every check, so a change applies at once without a restart.
    pub fn read(path: &Path) -> Result<Access, BoxError> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?)
    }
}

/// How often a reader hears where the copy stands while nothing new comes, as from a writer.
const BEAT: Duration = Duration::from_secs(1);

/// How long the copy counts as following its writer after the pusher last sent anything (the
/// writer's beats come every second). Silent longer, the relay stops beating its readers, and
/// they count their writer as away, as when they lose the writer itself.
const PUSHER_SILENT: Duration = Duration::from_secs(3);

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

/// A frame of the log, checked against its checksum: a log changed on disk since it was
/// written is told as such, never passed on.
fn read_frame(file: &File, frame: &Frame) -> std::io::Result<PushRequest> {
    let mut buf = vec![0; HEADER + frame.len as usize];
    file.read_exact_at(&mut buf, frame.offset)?;
    let (header, payload) = buf.split_at(HEADER);
    if sum(payload) != header[21..29] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "the copy's log is damaged: the frame at byte {} does not match its checksum",
                frame.offset
            ),
        ));
    }
    PushRequest::decode(payload).map_err(std::io::Error::other)
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
        within_limit(&self.dir, self.end + (HEADER + msg.encoded_len()) as u64)?;
        let frame = append(&mut self.file, self.end, Kind::Records, first, last, &msg)
            .and_then(|f| self.file.sync_data().map(|()| f))
            .map_err(|e| Status::internal(format!("writing the copy failed: {e}")))?;
        self.end += (HEADER + frame.len as usize) as u64;
        self.frames.push(frame);
        self.head = head;
        Ok(last)
    }
}

/// The most a copy in `dir` may hold (`max-bytes`), if it is limited.
fn limit(dir: &Path) -> Option<u64> {
    std::fs::read_to_string(dir.join("max-bytes"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

/// RESOURCE_EXHAUSTED when a copy in `dir` would hold `bytes`, more than its limit.
fn within_limit(dir: &Path, bytes: u64) -> Result<(), Status> {
    match limit(dir) {
        Some(max) if bytes > max => Err(Status::resource_exhausted(format!(
            "the copy is full: {bytes} bytes would exceed its limit of {max} (max-bytes)"
        ))),
        _ => Ok(()),
    }
}

/// A new image being written, until it ends.
struct Image {
    dir: PathBuf,
    file: File,
    end: u64,
    frames: Vec<Frame>,
}

impl Image {
    fn begin(dir: &Path) -> std::io::Result<Image> {
        Ok(Image {
            dir: dir.to_owned(),
            file: File::create(dir.join("log.new"))?,
            end: 0,
            frames: Vec::new(),
        })
    }

    fn push(&mut self, kind: Kind, seq: u64, msg: &PushRequest) -> Result<(), Status> {
        within_limit(&self.dir, self.end + (HEADER + msg.encoded_len()) as u64)?;
        let frame = append(&mut self.file, self.end, kind, seq, seq, msg).map_err(internal)?;
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
    /// How far the writer said readers may follow, and when the pusher last sent anything.
    writer: Mutex<Option<(u64, std::time::Instant)>>,
    /// (generation, head): readers wake when it changes.
    changed: watch::Sender<(u64, u64)>,
    /// The pusher's stream for calls, while it is connected.
    calls: Mutex<Option<mpsc::Sender<Result<CallsResponse, Status>>>>,
    /// Calls sent down it, waiting for their answers, by ID.
    pending: Mutex<HashMap<u64, oneshot::Sender<SealedReply>>>,
    /// Bytes sent to readers since the copy began (`served`).
    served: AtomicU64,
    /// Who is connected now, for `status.json`.
    who: Mutex<Who>,
    /// Per reader key, the generation of the copy it followed last (since the relay started):
    /// a reader of an older one that is ahead of the copy holds records of a copy that is gone.
    followed: Mutex<HashMap<String, u64>>,
}

/// A copy's pusher and readers as they are connected now (`status.json`, every ten seconds):
/// for whoever operates the relay, never a record's content.
#[derive(Default, serde::Serialize)]
struct Who {
    /// The pusher's key and since when (ms).
    pusher: Option<(String, i64)>,
    /// Each reader's stream: its key, the newest record sent to it, since when (ms).
    readers: HashMap<u64, (String, u64, i64)>,
}

/// One reader's stream, counted in [`Who`] while it lasts.
struct Following(Arc<Copy>, u64);

impl Drop for Following {
    fn drop(&mut self) {
        self.0
            .who
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .readers
            .remove(&self.1);
    }
}

impl Copy {
    fn who(&self) -> std::sync::MutexGuard<'_, Who> {
        self.who.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `status.json`: the head, the pusher, the readers, when it was written.
    fn status(&self) -> serde_json::Value {
        let head = self.log().head.seq;
        let who = self.who();
        let mut readers: Vec<_> = who
            .readers
            .values()
            .map(|(key, at, since)| serde_json::json!({ "key": key, "position": at, "since": since }))
            .collect();
        readers.sort_by_key(|r| r["key"].as_str().unwrap_or_default().to_owned());
        serde_json::json!({
            "head": head,
            "pusher": who.pusher.as_ref().map(|(key, since)| serde_json::json!({ "key": key, "since": since })),
            "readers": readers,
            "written_at": now_ms(),
        })
    }
}

impl Copy {
    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The pusher sent something: the writer's head with a record batch, or nothing new.
    fn heard(&self, head: Option<u64>) {
        let mut writer = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let head = head.or(writer.map(|(h, _)| h)).unwrap_or(0);
        *writer = Some((head, std::time::Instant::now()));
    }

    /// The writer's head as last heard, while the pusher is heard from.
    fn writer_head(&self) -> Option<u64> {
        self.writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .filter(|(_, at)| at.elapsed() < PUSHER_SILENT)
            .map(|(head, _)| head)
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
        self.0.who().pusher = None;
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
        && name.len() <= 127
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-".contains(c))
}

/// Signs the request that opens a stream (`rpc`: push, calls, follow) to `copy` at a relay with
/// `key`: the relay checks it before it reads any message.
pub fn sign_stream<T>(
    request: &mut tonic::Request<T>,
    key: &ed25519_dalek::SigningKey,
    rpc: &str,
    copy: &str,
) -> Result<(), BoxError> {
    let key_id = signing::key_id(&key.verifying_key());
    let at = now_ms();
    let sig = signing::sign(key, &signing::relay_stream_message(rpc, copy, &key_id, at));
    let headers = request.metadata_mut();
    headers.insert(signing::RELAY_KEY_HEADER, key_id.parse()?);
    headers.insert(signing::RELAY_COPY_HEADER, copy.parse()?);
    headers.insert(signing::RELAY_AT_HEADER, at.to_string().parse()?);
    headers.insert(signing::RELAY_SIG_HEADER, hex::encode(&sig.sig).parse()?);
    Ok(())
}

/// The key and copy a stream's request names, its signature checked, fresh: before any message
/// of it is read, so nobody without a key the relay takes gets it to buffer anything.
fn signed_stream(
    metadata: &tonic::metadata::MetadataMap,
    rpc: &str,
) -> Result<(String, String), Status> {
    let get = |name: &str| metadata.get(name).and_then(|v| v.to_str().ok());
    let (Some(key), Some(copy), Some(at), Some(sig)) = (
        get(signing::RELAY_KEY_HEADER),
        get(signing::RELAY_COPY_HEADER),
        get(signing::RELAY_AT_HEADER),
        get(signing::RELAY_SIG_HEADER),
    ) else {
        return Err(Status::unauthenticated(
            "a stream at a relay names its key and copy, signed, in its request (kernable-relay-*): \
             a kernable older than this relay?",
        ));
    };
    if !valid_name(copy) {
        return Err(invalid_name());
    }
    let at: i64 = at
        .parse()
        .map_err(|_| Status::unauthenticated("kernable-relay-at is no time"))?;
    if (now_ms() - at).abs() > MAX_REQUEST_AGE_MS {
        return Err(Status::unauthenticated(
            "the stream's signature is too old or too new: check the clock",
        ));
    }
    let proof = kernable_core::proto::Signature {
        key_id: key.to_owned(),
        sig: hex::decode(sig)
            .map_err(|_| Status::unauthenticated("kernable-relay-sig is no hex"))?,
    };
    signing::verify(&proof, &signing::relay_stream_message(rpc, copy, key, at))
        .map_err(|_| Status::unauthenticated("the stream's signature does not hold"))?;
    Ok((key.to_owned(), copy.to_owned()))
}

/// The hello must name the key and copy the request was signed for.
fn same_as_signed(
    hello_key: &str,
    hello_copy: &str,
    signed: &(String, String),
) -> Result<(), Status> {
    if (hello_key, hello_copy) != (signed.0.as_str(), signed.1.as_str()) {
        return Err(Status::invalid_argument(
            "the hello names another key or copy than its request",
        ));
    }
    Ok(())
}

/// A name that is no copy's, not repeated back: whatever a client sent stays out of the answer
/// and the log.
fn invalid_name() -> Status {
    Status::invalid_argument(
        "no copy name: lowercase letters, digits, '.', '_' and '-', at most 127, starting with a \
         letter or digit",
    )
}

impl Relay {
    /// The copy named `name`; with `create`, a new one if it has none.
    fn copy(&self, name: &str, create: bool) -> Result<Arc<Copy>, Status> {
        if !valid_name(name) {
            return Err(invalid_name());
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
            writer: Mutex::default(),
            changed,
            calls: Mutex::default(),
            pending: Mutex::default(),
            who: Mutex::default(),
            followed: Mutex::default(),
            served: AtomicU64::new(
                std::fs::read_to_string(dir.join("served"))
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(0),
            ),
        });
        copies.insert(name.to_owned(), Arc::clone(&copy));
        Ok(copy)
    }

    /// Whether `key` (proven by its signature) may act on `copy` in `role` now: named for it in
    /// `--access`, active; else among the relay's own `--allow-push` or `--allow-read`. A key
    /// that may push a copy may read it too: one key per instance, so a reader that became the
    /// writer follows and pushes with the same key.
    fn may(&self, key: &str, copy: &str, role: KeyRole) -> Result<(), Status> {
        if !valid_name(copy) {
            return Err(invalid_name());
        }
        let what = match role {
            KeyRole::Push => "push to",
            KeyRole::Read => "read",
        };
        let Some(path) = &self.config.access else {
            let pusher = self.config.pushers.iter().any(|k| k == key);
            let allowed = match role {
                KeyRole::Push => pusher,
                KeyRole::Read => pusher || self.config.readers.iter().any(|k| k == key),
            };
            return if allowed {
                Ok(())
            } else {
                Err(Status::permission_denied(format!(
                    "this relay lets the key {} not {what} {copy} (--allow-push, --allow-read)",
                    short(key)
                )))
            };
        };
        let file = Access::read(path).map_err(|e| {
            tracing::error!(error = %e, "the access file does not read: every copy refused");
            Status::unavailable("the relay cannot read its access file")
        })?;
        let entry = file.copies.get(copy).ok_or_else(|| {
            Status::permission_denied(format!("this relay holds no copy {copy} for anyone"))
        })?;
        match entry.keys.get(key) {
            Some(KeyAccess { role: r, state }) if *r == role || *r == KeyRole::Push => {
                match state {
                    KeyState::Active => Ok(()),
                    KeyState::Stopped => Err(Status::permission_denied(format!(
                        "the key {} is stopped for {copy}: it may no longer {what} it",
                        short(key)
                    ))),
                    KeyState::Revoked => Err(Status::permission_denied(format!(
                        "the key {} is revoked for {copy}",
                        short(key)
                    ))),
                }
            }
            _ => Err(Status::permission_denied(format!(
                "the key {} may not {what} {copy}: its access list does not name it so",
                short(key)
            ))),
        }
    }

    /// Whether `key` may push to (and take calls for) `copy`.
    fn pusher_may(&self, key: &str, copy: &str) -> Result<(), Status> {
        self.may(key, copy, KeyRole::Push)
    }

    /// Whether `key` may follow `copy` (and send calls through it).
    fn reader_may(&self, key: &str, copy: &str) -> Result<(), Status> {
        self.may(key, copy, KeyRole::Read)
    }
}

/// A key ID as said in a refusal: its first twelve digits.
fn short(key: &str) -> &str {
    &key[..key.len().min(12)]
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

/// How long a stream may take from its start to its hello, and from the challenge to its
/// answer: a client that sends nothing, or never proves its key, holds nothing longer.
pub const HANDSHAKE: Duration = Duration::from_secs(10);

/// The first message of a stream, within [`HANDSHAKE`].
async fn first<T>(inbound: &mut Streaming<T>) -> Result<Option<T>, Status> {
    tokio::time::timeout(HANDSHAKE, inbound.message())
        .await
        .map_err(|_| Status::deadline_exceeded("no hello within the handshake's time"))?
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
    let sig = tokio::time::timeout(HANDSHAKE, inbound.message())
        .await
        .map_err(|_| Status::deadline_exceeded("the challenge was not answered in time"))??
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
    signed: (String, String),
    inbound: &mut Streaming<PushRequest>,
    out: &PushOut,
) -> Result<(), Status> {
    let Some(push_request::Msg::Hello(hello)) = first(inbound).await?.and_then(|m| m.msg) else {
        return Err(Status::invalid_argument("expected PushHello"));
    };
    protocol(hello.protocol)?;
    same_as_signed(&hello.key_id, &hello.copy, &signed)?;
    relay.pusher_may(&hello.key_id, &hello.copy)?;
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
    copy.who().pusher = Some((hello.key_id.clone(), now_ms()));
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
        copy.heard(match &message.msg {
            Some(push_request::Msg::Records(batch)) => Some(batch.head),
            _ => None,
        });
        // It keeps only what is sealed: a pusher that sends plain values is misconfigured.
        if !sealed(&message) {
            return Err(Status::invalid_argument(
                "a relay keeps sealed records only: give the pusher --copy-key",
            ));
        }
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
                writing.push(Kind::Image, 0, &msg)?;
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
                writing.push(Kind::End, seq, &msg)?;
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

/// Whether `message` carries nothing in plain but positions, hashes and sizes.
fn sealed(message: &PushRequest) -> bool {
    match &message.msg {
        Some(push_request::Msg::Image(b)) => {
            b.records.is_empty() && b.vault.is_empty() && b.files.is_empty() && !b.sealed.is_empty()
        }
        Some(push_request::Msg::ImageEnd(e)) => !e.sealed.is_empty(),
        Some(push_request::Msg::Records(b)) => {
            b.vault.is_empty()
                && b.files.is_empty()
                && b.records
                    .iter()
                    .all(|r| r.record.is_none() && !r.sealed.is_empty())
                && (b.records.is_empty() || !b.sealed.is_empty())
        }
        _ => true,
    }
}

/// How long a reader's call waits for the writer's answer.
const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// The pusher's stream for calls: readers' writes go down it, the writer's answers come up.
async fn calls(
    relay: Arc<Relay>,
    signed: (String, String),
    inbound: &mut Streaming<CallsRequest>,
    out: &mpsc::Sender<Result<CallsResponse, Status>>,
) -> Result<(), Status> {
    let Some(calls_request::Msg::Hello(hello)) = first(inbound).await?.and_then(|m| m.msg) else {
        return Err(Status::invalid_argument("expected PushHello"));
    };
    protocol(hello.protocol)?;
    same_as_signed(&hello.key_id, &hello.copy, &signed)?;
    relay.pusher_may(&hello.key_id, &hello.copy)?;
    challenge(
        inbound,
        &hello.key_id,
        async |nonce| {
            out.send(Ok(CallsResponse {
                msg: Some(calls_response::Msg::Challenge(Challenge { nonce })),
            }))
            .await
            .map_err(|_| Status::cancelled("the pusher left"))
        },
        |m: CallsRequest| match m.msg {
            Some(calls_request::Msg::ChallengeResponse(r)) => Some(r.sig),
            _ => None,
        },
    )
    .await?;
    let copy = relay.copy(&hello.copy, true)?;
    // The newest pusher's stream takes the calls.
    *copy.calls.lock().unwrap_or_else(PoisonError::into_inner) = Some(out.clone());
    tracing::info!(copy = %copy.name, key = %hello.key_id, "a pusher takes calls");
    while let Some(message) = inbound.message().await? {
        if let Some(calls_request::Msg::Reply(reply)) = message.msg {
            let waiting = copy
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&reply.id);
            if let Some(waiting) = waiting {
                let _ = waiting.send(reply);
            }
        }
    }
    Ok(())
}

/// A reader's write, passed to the copy's pusher; the writer's answer, as the pusher sent it.
async fn call(relay: Arc<Relay>, call: SealedCall) -> Result<SealedReply, Status> {
    let sig = call
        .sig
        .as_ref()
        .ok_or_else(|| Status::unauthenticated("the call is not signed"))?;
    relay.reader_may(&sig.key_id, &call.copy)?;
    let msg = signing::relay_call_message(&call.copy, call.id, &call.sealed, call.issued_at);
    if signing::verify(sig, &msg).is_err() {
        return Err(Status::unauthenticated("invalid signature"));
    }
    if (now_ms() - call.issued_at).abs() > MAX_REQUEST_AGE_MS {
        return Err(Status::unauthenticated("the call expired; check the clock"));
    }
    let copy = relay.copy(&call.copy, false)?;
    let out = copy
        .calls
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .filter(|o| !o.is_closed())
        .ok_or_else(|| {
            Status::unavailable(format!(
                "writer_away: no pusher of the copy {} is connected to this relay; try again",
                copy.name
            ))
        })?;
    let (tx, rx) = oneshot::channel();
    {
        let mut pending = copy.pending.lock().unwrap_or_else(PoisonError::into_inner);
        if pending.contains_key(&call.id) {
            return Err(Status::already_exists("a call with this ID waits already"));
        }
        pending.insert(call.id, tx);
    }
    let id = call.id;
    let forget = || {
        copy.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
    };
    if out
        .send(Ok(CallsResponse {
            msg: Some(calls_response::Msg::Call(call)),
        }))
        .await
        .is_err()
    {
        forget();
        return Err(Status::unavailable(
            "writer_away: the copy's pusher left; try again",
        ));
    }
    match tokio::time::timeout(CALL_TIMEOUT, rx).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(_)) => Err(Status::unavailable(
            "writer_away: the copy's pusher left before it answered; try again",
        )),
        Err(_) => {
            forget();
            Err(Status::deadline_exceeded(
                "the writer did not answer through the relay in time",
            ))
        }
    }
}

type FollowOut = mpsc::Sender<Result<FollowResponse, Status>>;

async fn tell(out: &FollowOut, msg: follow_response::Msg) -> Result<(), Status> {
    out.send(Ok(FollowResponse { msg: Some(msg) }))
        .await
        .map_err(|_| Status::cancelled("the reader left"))
}

/// Takes the reader's messages that already came, without waiting; true if it left. A reader
/// acknowledges every batch: left unread while it catches up, the small frames pile up and
/// HTTP/2 ends the whole connection ("too_many_data_frames").
fn took_what_came(inbound: &mut Streaming<FollowRequest>) -> Result<bool, Status> {
    loop {
        let next = {
            let mut message = std::pin::pin!(inbound.message());
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            match message.as_mut().poll(&mut cx) {
                std::task::Poll::Ready(next) => next,
                std::task::Poll::Pending => return Ok(false),
            }
        };
        match next?.and_then(|m| m.msg) {
            Some(follow_request::Msg::Ack(_)) => {}
            Some(follow_request::Msg::Leaving(_)) | None => return Ok(true),
            Some(_) => return Err(Status::invalid_argument("unexpected message")),
        }
    }
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
    .map_err(|e| match e.kind() {
        std::io::ErrorKind::InvalidData => Status::data_loss(e.to_string()),
        _ => internal(e),
    })
}

async fn follow(
    relay: Arc<Relay>,
    signed: (String, String),
    inbound: &mut Streaming<FollowRequest>,
    out: &FollowOut,
) -> Result<(), Status> {
    let Some(follow_request::Msg::Hello(hello)) = first(inbound).await?.and_then(|m| m.msg) else {
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
    same_as_signed(&hello.key_id, &hello.copy, &signed)?;
    relay.reader_may(&hello.key_id, &hello.copy)?;
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
    let before = copy
        .followed
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&hello.key_id)
        .copied();
    let mode = if hello.seq < base.seq {
        FollowMode::Image
    } else if hello.seq > head.seq && before.is_some_and(|g| g < generation) {
        // It followed this copy before it began anew: what it holds beyond the copy's head is
        // of the copy that is gone (the new one is another writer's log, or it would have
        // begun from an image at or past the reader). Waiting would wait for nothing.
        FollowMode::Diverged
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
    // Told it diverged, it has not followed this generation: until it starts over, it is told
    // so again.
    if mode != FollowMode::Diverged {
        copy.followed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hello.key_id.clone(), generation);
    }
    static STREAMS: AtomicU64 = AtomicU64::new(0);
    let stream = STREAMS.fetch_add(1, SeqCst);
    copy.who()
        .readers
        .insert(stream, (hello.key_id.clone(), hello.seq, now_ms()));
    let _following = Following(Arc::clone(&copy), stream);
    tell(
        out,
        follow_response::Msg::Start(FollowStart {
            mode: mode.into(),
            message: match mode {
                FollowMode::Diverged if hello.seq > head.seq => format!(
                    "the copy began anew since the reader followed it: its records up to {} are of \
                     the copy before",
                    hello.seq
                ),
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
                        copy.served.fetch_add(batch.encoded_len() as u64, SeqCst);
                        tell(out, follow_response::Msg::Image(batch)).await?;
                    }
                    Some(push_request::Msg::ImageEnd(end)) => {
                        tell(out, follow_response::Msg::ImageEnd(end)).await?;
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
                    if let Some(reader) = copy.who().readers.get_mut(&stream) {
                        reader.1 = sent;
                    }
                    batch.head = copy.writer_head().unwrap_or(head).max(sent);
                    copy.served.fetch_add(batch.encoded_len() as u64, SeqCst);
                    if took_what_came(inbound)? {
                        return Ok(());
                    }
                    tell(out, follow_response::Msg::Records(batch)).await?;
                }
            }
            beat = tokio::time::Instant::now();
            continue;
        }
        // Beats only while the pusher is heard from: a reader whose copy no longer follows the
        // writer counts the writer as away, as it would without a relay.
        if beat.elapsed() >= BEAT {
            beat = tokio::time::Instant::now();
            if let Some(writer) = copy.writer_head() {
                tell(
                    out,
                    follow_response::Msg::Records(RecordBatch {
                        head: writer.max(sent),
                        ..Default::default()
                    }),
                )
                .await?;
            }
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

/// How often an open stream's access is looked at again: a copy taken out of the access file (its
/// customer unlinked it) or a key taken off it ends the stream within this.
const RECHECK: std::time::Duration = std::time::Duration::from_secs(5);

/// Runs `stream` while `may` still lets it: the refusal ends it. Only a refusal does, not an
/// access file that does not read for a moment.
async fn while_allowed(
    stream: impl std::future::Future<Output = Result<(), Status>>,
    may: impl Fn() -> Result<(), Status>,
) -> Result<(), Status> {
    let refused = async {
        loop {
            tokio::time::sleep(RECHECK).await;
            if let Err(s) = may()
                && s.code() == tonic::Code::PermissionDenied
            {
                return s;
            }
        }
    };
    tokio::select! {
        r = stream => r,
        s = refused => Err(s),
    }
}

#[tonic::async_trait]
impl RelayService for Api {
    type PushStream = ReceiverStream<Result<PushResponse, Status>>;

    async fn push(
        &self,
        request: Request<Streaming<PushRequest>>,
    ) -> Result<Response<Self::PushStream>, Status> {
        // Known before any message is read: a stranger gets nothing buffered.
        let signed = signed_stream(request.metadata(), "push")?;
        self.0.pusher_may(&signed.0, &signed.1)?;
        let (tx, rx) = mpsc::channel(16);
        let relay = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            let (key, copy) = signed.clone();
            let still = Arc::clone(&relay);
            let pushing = push(relay, signed, &mut inbound, &tx);
            if let Err(status) = while_allowed(pushing, || still.pusher_may(&key, &copy)).await {
                tracing::warn!(error = %status.message(), "a push ended");
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type CallsStream = ReceiverStream<Result<CallsResponse, Status>>;

    async fn calls(
        &self,
        request: Request<Streaming<CallsRequest>>,
    ) -> Result<Response<Self::CallsStream>, Status> {
        let signed = signed_stream(request.metadata(), "calls")?;
        self.0.pusher_may(&signed.0, &signed.1)?;
        let (tx, rx) = mpsc::channel(64);
        let relay = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            let (key, copy) = signed.clone();
            let still = Arc::clone(&relay);
            let taking = calls(relay, signed, &mut inbound, &tx);
            if let Err(status) = while_allowed(taking, || still.pusher_may(&key, &copy)).await {
                tracing::warn!(error = %status.message(), "a pusher's calls ended");
                let _ = tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn call(&self, request: Request<SealedCall>) -> Result<Response<SealedReply>, Status> {
        call(Arc::clone(&self.0), request.into_inner())
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl ReplicationService for Api {
    type FollowStream = ReceiverStream<Result<FollowResponse, Status>>;

    async fn follow(
        &self,
        request: Request<Streaming<FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let signed = signed_stream(request.metadata(), "follow")?;
        self.0.reader_may(&signed.0, &signed.1)?;
        let (tx, rx) = mpsc::channel(16);
        let relay = Arc::clone(&self.0);
        let mut inbound = request.into_inner();
        tokio::spawn(async move {
            let (key, copy) = signed.clone();
            let still = Arc::clone(&relay);
            let following = follow(relay, signed, &mut inbound, &tx);
            let may = || still.reader_may(&key, &copy);
            if let Err(status) = while_allowed(following, may).await {
                // A damaged copy is the operator's to know; a reader leaving is not.
                if status.code() == tonic::Code::DataLoss {
                    tracing::error!(error = %status.message(), "a reader's stream ended");
                } else if status.code() == tonic::Code::PermissionDenied {
                    tracing::warn!(error = %status.message(), "a reader's stream ended");
                } else {
                    tracing::debug!(error = %status.message(), "a reader's stream ended");
                }
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
            "{listen} is reachable from other machines: use TLS (--tls-cert, --tls-key; \
             --tls-client-ca too for mutual TLS)"
        )
        .into());
    }
    let mut builder = Server::builder();
    if let Some(tls) = &config.tls {
        let identity = Identity::from_pem(std::fs::read(&tls.cert)?, std::fs::read(&tls.key)?);
        let mut server = ServerTlsConfig::new().identity(identity);
        if let Some(ca) = &tls.client_ca {
            server = server.client_ca_root(Certificate::from_pem(std::fs::read(ca)?));
        }
        builder = builder.tls_config(server)?;
    }
    tracing::info!(%listen, dir = %config.dir.display(), "relay starting");
    let relay = Arc::new(Relay {
        config,
        copies: Mutex::default(),
    });
    // What each copy served, for whoever operates the relay, every ten seconds.
    let counting = Arc::clone(&relay);
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_secs(10));
        loop {
            every.tick().await;
            let copies: Vec<Arc<Copy>> = counting
                .copies
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .values()
                .cloned()
                .collect();
            for copy in copies {
                let dir = copy.log().dir.clone();
                let served = copy.served.load(SeqCst);
                let written = std::fs::write(dir.join("served.new"), format!("{served}\n"))
                    .and_then(|()| std::fs::rename(dir.join("served.new"), dir.join("served")))
                    .and_then(|()| {
                        std::fs::write(dir.join("status.json.new"), copy.status().to_string())
                    })
                    .and_then(|()| {
                        std::fs::rename(dir.join("status.json.new"), dir.join("status.json"))
                    });
                if let Err(e) = written {
                    tracing::warn!(copy = %copy.name, error = %e, "writing what the copy served failed");
                }
            }
        }
    });
    let api = Api(relay);
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
    use kernable_core::proto::{ImageBatch, ImageEnd, ReplicatedRecord};

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("kn-relay-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    fn batch(from: u64, to: u64) -> RecordBatch {
        RecordBatch {
            records: (from..=to)
                .map(|seq| ReplicatedRecord {
                    sealed: Vec::new(),
                    seq,
                    hash: vec![seq as u8; 32],
                    record: None,
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_frame_changed_on_disk_is_told_not_passed_on() {
        let d = dir("damaged");
        let log = {
            let mut log = Log::open(&d).unwrap();
            log.add(batch(1, 3)).unwrap();
            log.add(batch(4, 5)).unwrap();
            log
        };
        assert_eq!(log.hash_at(2).unwrap(), Some(vec![2; 32]));
        // One byte of the first frame's payload changed, as by someone with the disk.
        let file = OpenOptions::new().write(true).open(d.join("log")).unwrap();
        file.write_all_at(&[0xff], HEADER as u64 + 3).unwrap();
        let e = log.hash_at(2).unwrap_err();
        assert_eq!(
            e.downcast_ref::<std::io::Error>().map(|e| e.kind()),
            Some(std::io::ErrorKind::InvalidData)
        );
        assert!(e.to_string().contains("damaged"), "{e}");
        // The other frame still reads.
        assert_eq!(log.hash_at(5).unwrap(), Some(vec![5; 32]));
        let _ = std::fs::remove_dir_all(d);
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
                writer: Mutex::default(),
                changed,
                calls: Mutex::default(),
                pending: Mutex::default(),
                served: AtomicU64::new(0),
                who: Mutex::default(),
                followed: Mutex::default(),
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
                    sealed: Vec::new(),
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
    fn a_copy_takes_no_more_than_its_limit() {
        let d = dir("limit");
        let mut log = Log::open(&d).unwrap();
        log.add(batch(1, 2)).unwrap();
        let size = log.end;
        std::fs::write(d.join("max-bytes"), format!("{}\n", size + 10)).unwrap();
        let full = log.add(batch(3, 4)).unwrap_err();
        assert_eq!(full.code(), tonic::Code::ResourceExhausted, "{full:?}");
        assert_eq!((log.end, log.head.seq), (size, 2), "nothing written");
        // The limit rises: it follows on.
        std::fs::write(d.join("max-bytes"), "1000000\n").unwrap();
        assert_eq!(log.add(batch(3, 4)).unwrap(), 4);
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn copy_names_are_plain() {
        assert!(valid_name("shop-eu.1"));
        // <team>.<project>.<partition>, a long team's too (kernable Cloud's copies).
        assert!(valid_name(&format!("{}.shop.eu.2", "t".repeat(63))));
        assert!(!valid_name(&"a".repeat(128)));
        assert!(!valid_name("../x"));
        assert!(!valid_name(""));
        assert!(!valid_name("Shop"));
    }
}
