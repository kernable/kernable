//! A pusher (`kernable push`): sends a writer's records to its copy at a relay
//! (docs/how-to/relays.md).
//!
//! It runs beside the writer and follows it as a reader does (its key granted `read`, the writer
//! `--readers on`), so it gets every record the writer may tell, and opens a connection of its
//! own to the relay (`RelayService.Push`): the writer takes no connection from outside its
//! network. It starts where the relay's copy stands; when the writer no longer keeps the records
//! after that, the writer sends an image and the copy begins anew from it. When the writer's log
//! no longer continues the copy (restored from an older backup, or another writer started by
//! mistake), the copy keeps its history: the pusher refuses to push, says so as an error, and
//! tries again every minute. Only `--start-anew <position>`, the copy's position as the pusher
//! reported it, lets it empty the copy so the writer's records go there from the first. The
//! relay's acknowledgements go on to the writer, which keeps its records until the copy holds
//! them, as it does for a slow reader.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        CallsRequest, ChallengeResponse, CopyKeyReply, CopyKeyRequest, FollowAck, FollowHello,
        FollowMode, FollowRequest, ForwardedCall, ForwardedReply, Header, ImageEnd, PushHello,
        PushRequest, SealedReply, calls_request, calls_response, follow_request, follow_response,
        push_request, push_response, relay_service_client::RelayServiceClient,
    },
    signing,
};
use kernable_store::replication::ZERO;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{BoxError, replication, seal::CopyKey, standby};

/// How a pusher runs.
pub struct Pusher {
    /// The writer's addresses, tried in turn: both kernels of a pair.
    pub follow: Vec<String>,
    /// Its key, granted `read` by the writer, and taken by the relay (`--allow-push`).
    pub key: SigningKey,
    /// Its grant's file, read again on every connection.
    pub grant_file: PathBuf,
    /// The relay's address.
    pub relay: String,
    /// The copy's name there.
    pub copy: String,
    /// The key the copy is sealed with: the relay never holds it.
    pub copy_key: Arc<CopyKey>,
    /// `--start-anew <position>`: the copy may be emptied when the writer's log no longer
    /// continues it, but only while it stands at this position (as the refusal named it).
    pub start_anew: Option<u64>,
    /// Who may be handed the copy key: keys whose grants, signed by these roots, allow `copy`
    /// for this copy, and that are not revoked. No root: nobody (`--root`).
    pub trust: Trust,
}

/// The roots a pusher trusts, and the keys the writer revoked, as the pusher saw them pass
/// (kept in a file, so a pusher started again still knows them).
pub struct Trust {
    pub roots: Vec<String>,
    revoked: Mutex<std::collections::HashSet<String>>,
    file: Option<PathBuf>,
}

impl Trust {
    /// `roots`, and the revocations kept in `file` (one key ID a line), if it exists.
    pub fn load(roots: Vec<String>, file: Option<PathBuf>) -> Result<Trust, BoxError> {
        let revoked = match &file {
            Some(f) if f.exists() => std::fs::read_to_string(f)?
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_owned)
                .collect(),
            _ => Default::default(),
        };
        Ok(Trust {
            roots,
            revoked: Mutex::new(revoked),
            file,
        })
    }

    fn note_revoked(&self, key: &str) {
        let new = self
            .revoked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key.to_owned());
        if let (true, Some(file)) = (new, &self.file) {
            let line = format!("{key}\n");
            let kept = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(file)
                .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
            if let Err(e) = kept {
                tracing::warn!(file = %file.display(), error = %e, "noting a revoked key failed");
            }
        }
    }

    fn is_revoked(&self, key: &str) -> bool {
        self.revoked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(key)
    }

    /// The revocations among records the writer sent.
    fn notice<'a>(&self, records: impl Iterator<Item = &'a kernable_core::proto::LogRecord>) {
        for r in records {
            if let Some(kernable_core::proto::log_record::Record::Revocation(rev)) = &r.record {
                self.note_revoked(&rev.key_id);
            }
        }
    }

    /// Whether the key that asks may be handed the copy key; why not.
    fn may_have_key(&self, ask: &CopyKeyRequest, copy: &str) -> Result<(), String> {
        if self.roots.is_empty() {
            return Err("this pusher hands out no copy key (it trusts no root: --root)".into());
        }
        if ask.copy != copy {
            return Err("an ask for another copy".into());
        }
        let sig = ask
            .sig
            .as_ref()
            .filter(|s| s.key_id == ask.key_id)
            .ok_or("the ask is not signed by its key")?;
        signing::verify(
            sig,
            &signing::copy_key_message(&ask.copy, &ask.key_id, ask.id, ask.issued_at),
        )
        .map_err(|_| "invalid signature")?;
        if (crate::now_ms() - ask.issued_at).abs() > CALL_AGE_MS {
            return Err("the ask expired; check the clock".into());
        }
        let grant = ask.grant.as_ref().ok_or("no grant")?;
        if grant.key_id != ask.key_id {
            return Err("the grant is another key's".into());
        }
        crate::grants::chain(grant, &self.roots, None)
            .map_err(|e| format!("the grant is not from a root this pusher trusts: {e}"))?;
        let grants = std::collections::HashMap::from([(grant.key_id.clone(), grant.clone())]);
        let permissions =
            crate::grants::Permissions::new(&grants, &std::collections::HashMap::new());
        if !crate::config::allowed(&permissions.copy, &ask.key_id, copy) {
            return Err(format!(
                "the key may not have the copy key: its grant needs copy={copy}"
            ));
        }
        // The key, the delegate that granted it and the root behind it.
        let mut signers = vec![ask.key_id.clone()];
        signers.extend(grant.root_sig.as_ref().map(|s| s.key_id.clone()));
        if let Some(issuer) = &grant.issuer {
            signers.extend(issuer.root_sig.as_ref().map(|s| s.key_id.clone()));
        }
        if let Some(revoked) = signers.iter().find(|k| self.is_revoked(k)) {
            return Err(format!("the key {revoked} is revoked"));
        }
        Ok(())
    }
}

/// The answer to an ask for the copy key: sealed to the asking key and signed, or why not.
fn hand_over(pusher: &Pusher, ask: &CopyKeyRequest) -> CopyKeyReply {
    let decided = pusher
        .trust
        .may_have_key(ask, &pusher.copy)
        .and_then(|()| signing::verifying_key_from_hex(&ask.key_id).map_err(|e| e.to_string()));
    match decided {
        Ok(to) => {
            let sealed = crate::seal::seal_key_for(&to, &pusher.copy, &pusher.copy_key);
            let copy_key_id = pusher.copy_key.id();
            tracing::info!(key = %ask.key_id, copy = %pusher.copy, "the copy key, sealed to a new instance");
            CopyKeyReply {
                id: ask.id,
                sender_sig: Some(signing::sign(
                    &pusher.key,
                    &signing::copy_key_handover_message(
                        &pusher.copy,
                        &ask.key_id,
                        &copy_key_id,
                        &sealed,
                    ),
                )),
                sealed,
                copy_key_id,
                refused: String::new(),
            }
        }
        Err(why) => {
            tracing::warn!(key = %ask.key_id, %why, "a key may not have the copy key");
            CopyKeyReply {
                id: ask.id,
                refused: why,
                ..Default::default()
            }
        }
    }
}

/// The writer's log no longer continues the copy, and the copy keeps its history.
#[derive(Debug)]
struct Diverged {
    copy: String,
    at: u64,
    why: String,
}

impl std::fmt::Display for Diverged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the writer's log no longer continues the copy {} at record {} ({}): the copy keeps its history and nothing is pushed. If this writer is meant to replace it (restored from an older backup, say), start the pusher once with --start-anew {} to empty the copy; its readers start over",
            self.copy, self.at, self.why, self.at
        )
    }
}

impl std::error::Error for Diverged {}

/// How long a pusher waits before it asks again after a refusal to start the copy anew.
const DIVERGED_DELAY: Duration = Duration::from_secs(60);

/// The longest pause between tries.
const MAX_DELAY: Duration = Duration::from_secs(5);

/// How often the connection to the writer is pinged: a writer the network cut off ends it.
const PING: Duration = Duration::from_secs(2);

/// Pushes until stopped; reconnects after every failure, to the writer's next address.
pub async fn run(pusher: Pusher) -> Result<(), BoxError> {
    let pusher = Arc::new(pusher);
    let current = Arc::new(Mutex::new(pusher.follow[0].clone()));
    // Beside the push: the writes the copy's readers send through the relay.
    let calls = tokio::spawn(take_calls(Arc::clone(&pusher), Arc::clone(&current)));
    let _calls = AbortOnDrop(calls);
    let mut at = 0;
    let mut delay = Duration::from_millis(200);
    loop {
        let writer = &pusher.follow[at % pusher.follow.len()];
        *current.lock().unwrap_or_else(PoisonError::into_inner) = writer.clone();
        match push_once(&pusher, writer).await {
            Ok(()) => delay = Duration::from_millis(200),
            // Nothing changes by itself: said as an error, asked again only now and then.
            Err(e) if e.downcast_ref::<Diverged>().is_some() => {
                tracing::error!(%writer, relay = %pusher.relay, error = %e, "not pushing");
                tokio::time::sleep(DIVERGED_DELAY).await;
            }
            Err(e) => {
                tracing::warn!(%writer, relay = %pusher.relay, error = %e, "pushing failed; trying again in {delay:?}");
                at += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
        }
    }
}

/// The relay's push stream for `copy`, held with `key` and nothing sent on it: while it is open
/// no other pusher can push the copy. A kernel that becomes the writer from the copy
/// (`--become-writer`) holds it while it takes the copy's last records, so the old writer's
/// pusher cannot slip in meanwhile.
pub(crate) struct Hold {
    _to_relay: mpsc::Sender<PushRequest>,
    _from_relay: tonic::Streaming<kernable_core::proto::PushResponse>,
}

/// Opens [`Hold`] at `relay`: the relay must take `key` as the copy's pusher (named, active) and
/// have no other pusher connected; otherwise its answer says why not.
pub(crate) async fn hold(relay: &str, copy: &str, key: &SigningKey) -> Result<Hold, String> {
    let held = async {
        let mut client = RelayServiceClient::new(standby::connect(relay).await?);
        let (to_relay, rx) = mpsc::channel(4);
        let say = |msg| PushRequest { msg: Some(msg) };
        to_relay
            .send(say(push_request::Msg::Hello(PushHello {
                key_id: signing::key_id(&key.verifying_key()),
                copy: copy.to_owned(),
                protocol: kernable_core::PROTOCOL,
            })))
            .await?;
        let mut request = tonic::Request::new(ReceiverStream::new(rx));
        crate::relay::sign_stream(&mut request, key, "push", copy)?;
        let mut from_relay = client.push(request).await?.into_inner();
        let Some(push_response::Msg::Challenge(challenge)) =
            from_relay.message().await?.and_then(|m| m.msg)
        else {
            return Err::<Hold, BoxError>("the relay sent no challenge".into());
        };
        let proof = signing::sign(key, &signing::challenge_message(&challenge.nonce));
        to_relay
            .send(say(push_request::Msg::ChallengeResponse(
                ChallengeResponse { sig: proof.sig },
            )))
            .await?;
        match from_relay.message().await?.and_then(|m| m.msg) {
            Some(push_response::Msg::Start(_)) => Ok(Hold {
                _to_relay: to_relay,
                _from_relay: from_relay,
            }),
            _ => Err("the relay did not start the push".into()),
        }
    };
    held.await
        .map_err(|e| match e.downcast_ref::<tonic::Status>() {
            Some(status) => status.message().to_owned(),
            None => e.to_string(),
        })
}

async fn push_once(pusher: &Pusher, writer: &str) -> Result<(), BoxError> {
    let grant = std::fs::read_to_string(&pusher.grant_file)
        .map_err(|e| format!("the pusher's grant {}: {e}", pusher.grant_file.display()))?
        .trim()
        .to_owned();
    let key_id = signing::key_id(&pusher.key.verifying_key());

    // The relay first: where the copy stands.
    let mut relay = RelayServiceClient::new(standby::connect(&pusher.relay).await?)
        .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let (to_relay, rx) = mpsc::channel(16);
    let say = |msg| PushRequest { msg: Some(msg) };
    to_relay
        .send(say(push_request::Msg::Hello(PushHello {
            key_id: key_id.clone(),
            copy: pusher.copy.clone(),
            protocol: kernable_core::PROTOCOL,
        })))
        .await?;
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    crate::relay::sign_stream(&mut request, &pusher.key, "push", &pusher.copy)?;
    let mut from_relay = relay.push(request).await?.into_inner();
    let next = async |s: &mut tonic::Streaming<kernable_core::proto::PushResponse>| {
        Ok::<_, BoxError>(s.message().await?.and_then(|m| m.msg))
    };
    let Some(push_response::Msg::Challenge(challenge)) = next(&mut from_relay).await? else {
        return Err("the relay sent no challenge".into());
    };
    let proof = signing::sign(&pusher.key, &signing::challenge_message(&challenge.nonce));
    to_relay
        .send(say(push_request::Msg::ChallengeResponse(
            ChallengeResponse { sig: proof.sig },
        )))
        .await?;
    let Some(push_response::Msg::Start(start)) = next(&mut from_relay).await? else {
        return Err("the relay did not start the push".into());
    };

    // Then the writer, from there.
    let hello = FollowHello {
        key_id,
        seq: start.seq,
        hash: start.hash.clone(),
        protocol: kernable_core::PROTOCOL,
        reader: true,
        ..Default::default()
    };
    let standby::Opened {
        tx: to_writer,
        inbound: mut from_writer,
        start: begun,
    } = standby::open_stream(writer, &pusher.key, &grant, hello, PING, None).await?;
    match begun.mode() {
        FollowMode::Follow => {
            tracing::info!(%writer, copy = %pusher.copy, from = start.seq, "pushing");
        }
        FollowMode::Image => {
            tracing::info!(%writer, copy = %pusher.copy, "the writer sends an image: the copy begins anew from it");
        }
        FollowMode::Diverged if start.seq > 0 && pusher.start_anew != Some(start.seq) => {
            return Err(Diverged {
                copy: pusher.copy.clone(),
                at: start.seq,
                why: begun.message.clone(),
            }
            .into());
        }
        FollowMode::Diverged if start.seq > 0 => {
            tracing::warn!(%writer, copy = %pusher.copy, at = start.seq, message = %begun.message, "--start-anew: the writer's log no longer continues the copy; it begins anew, empty");
            let mut empty = follow_response::Msg::ImageEnd(ImageEnd {
                seq: 0,
                hash: ZERO.to_vec(),
                sealed: Vec::new(),
            });
            crate::seal::seal(&pusher.copy_key, &mut empty, &mut Default::default());
            let follow_response::Msg::ImageEnd(empty) = empty else {
                unreachable!("sealed in place")
            };
            to_relay
                .send(say(push_request::Msg::ImageEnd(empty)))
                .await?;
            return match next(&mut from_relay).await? {
                Some(push_response::Msg::Ack(_)) => Ok(()),
                _ => Err("the relay did not empty the copy".into()),
            };
        }
        other => {
            return Err(format!("the writer answered {other:?}: {}", begun.message).into());
        }
    }
    // The batches of the image under way, for sealing.
    let mut images = crate::seal::Images::default();
    loop {
        tokio::select! {
            message = from_writer.message() => match message?.and_then(|m| m.msg) {
                Some(mut msg @ (follow_response::Msg::Image(_)
                    | follow_response::Msg::ImageEnd(_)
                    | follow_response::Msg::Records(_))) => {
                    // Revocations pass in plain here, before they are sealed.
                    match &msg {
                        follow_response::Msg::Image(b) => pusher.trust.notice(b.records.iter()),
                        follow_response::Msg::Records(b) => pusher
                            .trust
                            .notice(b.records.iter().filter_map(|r| r.record.as_ref())),
                        _ => {}
                    }
                    crate::seal::seal(&pusher.copy_key, &mut msg, &mut images);
                    let msg = match msg {
                        follow_response::Msg::Image(batch) => push_request::Msg::Image(batch),
                        follow_response::Msg::ImageEnd(end) => {
                            tracing::info!(record = end.seq, "the image ended");
                            push_request::Msg::ImageEnd(end)
                        }
                        follow_response::Msg::Records(batch) => push_request::Msg::Records(batch),
                        _ => unreachable!("matched above"),
                    };
                    if to_relay.send(say(msg)).await.is_err() {
                        return Err(standby::ended("the relay", from_relay.message()).await);
                    }
                }
                Some(_) => {}
                None => return Ok(()),
            },
            message = from_relay.message() => match message?.and_then(|m| m.msg) {
                Some(push_response::Msg::Ack(FollowAck { seq })) => {
                    if to_writer
                        .send(FollowRequest {
                            msg: Some(follow_request::Msg::Ack(FollowAck { seq })),
                        })
                        .await
                        .is_err()
                    {
                        return Err(standby::ended("the writer", from_writer.message()).await);
                    }
                }
                Some(_) => {}
                None => return Err("the relay ended the push".into()),
            },
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Takes the copy's readers' writes from the relay and makes them on the writer, until stopped.
async fn take_calls(pusher: Arc<Pusher>, current: Arc<Mutex<String>>) {
    let mut delay = Duration::from_millis(200);
    loop {
        match calls_once(&pusher, &current).await {
            Ok(()) => delay = Duration::from_millis(200),
            Err(e) => {
                tracing::warn!(relay = %pusher.relay, error = %e, "taking calls failed; trying again in {delay:?}");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
        }
    }
}

async fn calls_once(pusher: &Arc<Pusher>, current: &Arc<Mutex<String>>) -> Result<(), BoxError> {
    let mut relay = RelayServiceClient::new(standby::connect(&pusher.relay).await?)
        .max_decoding_message_size(replication::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(replication::MAX_MESSAGE_BYTES);
    let (tx, rx) = mpsc::channel(64);
    let say = |msg| CallsRequest { msg: Some(msg) };
    tx.send(say(calls_request::Msg::Hello(PushHello {
        key_id: signing::key_id(&pusher.key.verifying_key()),
        copy: pusher.copy.clone(),
        protocol: kernable_core::PROTOCOL,
    })))
    .await?;
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    crate::relay::sign_stream(&mut request, &pusher.key, "calls", &pusher.copy)?;
    let mut inbound = relay.calls(request).await?.into_inner();
    let Some(calls_response::Msg::Challenge(challenge)) =
        inbound.message().await?.and_then(|m| m.msg)
    else {
        return Err("the relay sent no challenge".into());
    };
    let proof = signing::sign(&pusher.key, &signing::challenge_message(&challenge.nonce));
    tx.send(say(calls_request::Msg::ChallengeResponse(
        ChallengeResponse { sig: proof.sig },
    )))
    .await?;
    tracing::info!(relay = %pusher.relay, copy = %pusher.copy, "taking the copy's readers' writes");
    let channels: Arc<Mutex<Option<(String, tonic::transport::Channel)>>> = Arc::default();
    let mut seen = Seen::default();
    while let Some(message) = inbound.message().await? {
        let call = match message.msg {
            Some(calls_response::Msg::Call(call)) => call,
            Some(calls_response::Msg::KeyRequest(ask)) => {
                tx.send(say(calls_request::Msg::KeyReply(hand_over(pusher, &ask))))
                    .await?;
                continue;
            }
            _ => continue,
        };
        // A relay cannot send a call again, or late: each is made once, within a minute.
        if !seen.first(call.id, call.issued_at) {
            tracing::warn!(
                id = call.id,
                "a call through the relay came late or twice: dropped"
            );
            continue;
        }
        let (pusher, tx) = (Arc::clone(pusher), tx.clone());
        let writer = current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let channels = Arc::clone(&channels);
        tokio::spawn(async move {
            let reply = match crate::seal::open_call(
                &pusher.copy_key,
                &pusher.copy,
                call.id,
                call.issued_at,
                &call.sealed,
            ) {
                Ok(forwarded) => make_call(&writer, &channels, forwarded).await,
                Err(e) => {
                    tracing::warn!(error = %e, "a call through the relay does not open: dropped");
                    return;
                }
            };
            let sealed = crate::seal::seal_reply(&pusher.copy_key, &pusher.copy, call.id, &reply);
            let _ = tx
                .send(CallsRequest {
                    msg: Some(calls_request::Msg::Reply(SealedReply {
                        id: call.id,
                        sealed,
                    })),
                })
                .await;
        });
    }
    Ok(())
}

/// How old a call through the relay may be when the pusher gets it.
const CALL_AGE_MS: i64 = 60_000;

/// The calls made lately, so none is made twice.
#[derive(Default)]
struct Seen(std::collections::HashMap<u64, i64>);

impl Seen {
    /// Whether call `id`, sent at `issued_at`, is new and on time; notes it.
    fn first(&mut self, id: u64, issued_at: i64) -> bool {
        let now = crate::now_ms();
        if (now - issued_at).abs() > CALL_AGE_MS {
            return false;
        }
        // Kept until it could no longer be on time.
        self.0.retain(|_, at| now - *at <= 2 * CALL_AGE_MS);
        self.0.insert(id, issued_at).is_none()
    }
}

/// Hop-by-hop headers and those the transport sets again.
fn passes(name: &str) -> bool {
    !matches!(
        name,
        "host" | "connection" | "content-length" | "transfer-encoding" | "keep-alive" | "upgrade"
    )
}

pub(crate) fn headers(map: &tonic::codegen::http::HeaderMap) -> Vec<Header> {
    map.iter()
        .filter(|(name, _)| passes(name.as_str()))
        .map(|(name, value)| Header {
            name: name.as_str().to_owned(),
            value: value.as_bytes().to_vec(),
        })
        .collect()
}

/// The call made on the writer at `writer`, and its answer as it came; an unreachable writer
/// answers UNAVAILABLE, as to a reader that cannot reach it.
async fn make_call(
    writer: &str,
    channels: &Mutex<Option<(String, tonic::transport::Channel)>>,
    call: ForwardedCall,
) -> ForwardedReply {
    use http_body_util::BodyExt;
    use tonic::codegen::http;
    let unavailable = |e: &dyn std::fmt::Display| {
        ForwardedReply {
        status: 200,
        headers: vec![
            Header {
                name: "content-type".into(),
                value: b"application/grpc".to_vec(),
            },
            Header {
                name: "grpc-status".into(),
                value: b"14".to_vec(),
            },
            Header {
                name: "grpc-message".into(),
                value: format!("writer_away: the relay's pusher cannot reach the writer {writer} ({e}); try again")
                    .replace(|c: char| !c.is_ascii() || c.is_ascii_control(), " ")
                    .into_bytes(),
            },
        ],
        ..Default::default()
    }
    };
    // Only what a reader forwards anyway: commands and the writes it passes on as they come.
    // Anything else would open the writer's other services to whoever holds the copy key.
    // And what partitions ask each other, which checks the asking node key itself.
    if call.path != "/kernable.v1.CommandService/Submit"
        && !crate::reader::forwarded(&call.path)
        && !crate::partitions::THROUGH_RELAY.contains(&call.path.as_str())
    {
        return ForwardedReply {
            status: 200,
            headers: vec![
                Header {
                    name: "content-type".into(),
                    value: b"application/grpc".to_vec(),
                },
                Header {
                    name: "grpc-status".into(),
                    value: b"7".to_vec(),
                },
                Header {
                    name: "grpc-message".into(),
                    value: format!("{}: not a call a relay passes on", call.path).into_bytes(),
                },
            ],
            ..Default::default()
        };
    }
    let cached = channels
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .filter(|(at, _)| at == writer);
    let channel = match cached {
        Some((_, channel)) => channel,
        None => match standby::connect(writer).await {
            Ok(channel) => {
                *channels.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some((writer.to_owned(), channel.clone()));
                channel
            }
            Err(e) => return unavailable(&e),
        },
    };
    let mut request = http::Request::builder().method("POST").uri(&call.path);
    for h in &call.headers {
        if passes(&h.name) {
            request = request.header(h.name.as_str(), h.value.as_slice());
        }
    }
    let request = match request.body(tonic::body::Body::new(http_body_util::Full::new(
        tonic::codegen::Bytes::from(call.body),
    ))) {
        Ok(r) => r,
        Err(e) => return unavailable(&e),
    };
    let response = match tower::ServiceExt::oneshot(channel, request).await {
        Ok(r) => r,
        Err(e) => {
            channels
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            return unavailable(&e);
        }
    };
    let (parts, body) = response.into_parts();
    let collected = match body.collect().await {
        Ok(c) => c,
        Err(e) => return unavailable(&e),
    };
    let trailers = collected.trailers().map(headers).unwrap_or_default();
    ForwardedReply {
        status: u32::from(parts.status.as_u16()),
        headers: headers(&parts.headers),
        body: collected.to_bytes().to_vec(),
        trailers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_a_readers_writes_pass_the_relay() {
        let channels = Mutex::new(None);
        let call = |path: &str| ForwardedCall {
            path: path.into(),
            ..Default::default()
        };
        let reply = make_call(
            "unix:/nonexistent",
            &channels,
            call("/kernable.v1.ReplicationService/Promote"),
        )
        .await;
        let status = reply
            .headers
            .iter()
            .find(|h| h.name == "grpc-status")
            .unwrap();
        assert_eq!(status.value, b"7", "refused before the writer is asked");
        let reply = make_call(
            "unix:/nonexistent",
            &channels,
            call("/kernable.v1.CommandService/Submit"),
        )
        .await;
        let status = reply
            .headers
            .iter()
            .find(|h| h.name == "grpc-status")
            .unwrap();
        assert_eq!(status.value, b"14", "passed on (the writer is away here)");
    }

    #[test]
    fn a_call_is_made_once_and_only_while_it_is_new() {
        let mut seen = Seen::default();
        let now = crate::now_ms();
        assert!(seen.first(1, now));
        assert!(!seen.first(1, now), "twice");
        assert!(seen.first(2, now - 1_000));
        assert!(!seen.first(3, now - 2 * CALL_AGE_MS), "late");
        assert!(!seen.first(4, now + 2 * CALL_AGE_MS), "from the future");
    }
}
