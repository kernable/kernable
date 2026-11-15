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

use std::{path::PathBuf, sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use kernable_core::{
    proto::{
        ChallengeResponse, FollowAck, FollowHello, FollowMode, FollowRequest, ImageEnd, PushHello,
        PushRequest, follow_request, follow_response, push_request, push_response,
        relay_service_client::RelayServiceClient,
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
    let mut at = 0;
    let mut delay = Duration::from_millis(200);
    loop {
        let writer = &pusher.follow[at % pusher.follow.len()];
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
