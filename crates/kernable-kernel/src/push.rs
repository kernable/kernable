//! A pusher (`kernable push`): sends a writer's records to its copy at a relay
//! (docs/how-to/relays.md).
//!
//! It runs beside the writer and follows it as a reader does (its key granted `read`, the writer
//! `--readers on`), so it gets every record the writer may tell, and opens a connection of its
//! own to the relay (`RelayService.Push`): the writer takes no connection from outside its
//! network. It starts where the relay's copy stands; when the writer no longer keeps the records
//! after that, the writer sends an image and the copy begins anew from it; when the writer's log
//! no longer continues the copy (restored from an older backup, say), the pusher empties the copy
//! and the writer's records go there from the first. The relay's acknowledgements go on to the
//! writer, which keeps its records until the copy holds them, as it does for a slow reader.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        CallsRequest, ChallengeResponse, FollowAck, FollowHello, FollowMode, FollowRequest,
        ForwardedCall, ForwardedReply, Header, ImageEnd, PushHello, PushRequest, SealedReply,
        calls_request, calls_response, follow_request, follow_response, push_request,
        push_response, relay_service_client::RelayServiceClient,
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
}

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
            Err(e) => {
                tracing::warn!(%writer, relay = %pusher.relay, error = %e, "pushing failed; trying again in {delay:?}");
                at += 1;
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_DELAY);
            }
        }
    }
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
    let mut from_relay = relay.push(ReceiverStream::new(rx)).await?.into_inner();
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
        FollowMode::Diverged if start.seq > 0 => {
            tracing::warn!(%writer, copy = %pusher.copy, message = %begun.message, "the writer's log no longer continues the copy: it begins anew, empty");
            let mut empty = follow_response::Msg::ImageEnd(ImageEnd {
                seq: 0,
                hash: ZERO.to_vec(),
                sealed: Vec::new(),
            });
            crate::seal::seal(&pusher.copy_key, &mut empty, &mut 0);
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
    let mut images = 0;
    loop {
        tokio::select! {
            message = from_writer.message() => match message?.and_then(|m| m.msg) {
                Some(mut msg @ (follow_response::Msg::Image(_)
                    | follow_response::Msg::ImageEnd(_)
                    | follow_response::Msg::Records(_))) => {
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
                    to_relay.send(say(msg)).await?;
                }
                Some(_) => {}
                None => return Ok(()),
            },
            message = from_relay.message() => match message?.and_then(|m| m.msg) {
                Some(push_response::Msg::Ack(FollowAck { seq })) => {
                    to_writer
                        .send(FollowRequest {
                            msg: Some(follow_request::Msg::Ack(FollowAck { seq })),
                        })
                        .await?;
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
    let mut inbound = relay.calls(ReceiverStream::new(rx)).await?.into_inner();
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
        let Some(calls_response::Msg::Call(call)) = message.msg else {
            continue;
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
