//! Any bytes as a wire or log message: decoding, building signed messages, verifying
//! signatures and hashing events never crash.
#![no_main]

use kernable_core::{
    proto::{
        Command, Event, ListenerMessage, LogRecord, QueryRequest, SubmitRequest,
        SubscribeRequest, WorkerMessage, worker_message::Msg,
    },
    signing,
};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|data: &[u8]| {
    if let Ok(r) = SubmitRequest::decode(data)
        && let Some(cmd) = r.command
    {
        let msg = signing::command_message(&cmd);
        if let Some(sig) = &cmd.client_sig {
            let _ = signing::verify(sig, &msg);
        }
    }
    if let Ok(q) = QueryRequest::decode(data) {
        let msg = signing::query_message(&q);
        if let Some(sig) = &q.client_sig {
            let _ = signing::verify(sig, &msg);
        }
    }
    if let Ok(s) = SubscribeRequest::decode(data) {
        let _ = signing::subscribe_message(s.after_position, &s.event_types, s.issued_at);
    }
    if let Ok(w) = WorkerMessage::decode(data) {
        let stream = Command::default();
        match w.msg {
            Some(Msg::Result(r)) => {
                let msg = signing::result_message(&stream, 0, &r.events);
                if let Some(sig) = &r.worker_sig {
                    let _ = signing::verify(sig, &msg);
                }
                if let Some(s) = &r.snapshot {
                    let _ = signing::snapshot_message(&stream, s.version, &s.schema, &s.state);
                }
            }
            Some(Msg::Rejection(r)) => {
                let _ = signing::rejection_message("c", &r.reason.unwrap_or_default());
            }
            Some(Msg::ChallengeResponse(c)) => {
                let _ = signing::verifying_key_from_hex(&String::from_utf8_lossy(&c.sig));
            }
            _ => {}
        }
    }
    let _ = ListenerMessage::decode(data);
    if let Ok(e) = Event::decode(data) {
        let _ = signing::event_hash(&[0; 32], &e);
    }
    let _ = LogRecord::decode(data);
});
