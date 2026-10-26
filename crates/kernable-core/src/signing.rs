//! Signed messages and the event hash chain, as specified in `proto/SIGNING.md`.
//!
//! A key ID is the hex-encoded Ed25519 public key, so any signature can be checked
//! without a key registry; *authorization* of a key is up to the kernel.

use std::{fmt, fs, io, path::Path};

use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

use crate::proto::{Command, Event, NewEvent, Rejected, Signature};

/// Hash that precedes the first event of the chain.
pub const GENESIS: [u8; 32] = [0; 32];

#[derive(Debug, PartialEq)]
pub enum SignError {
    InvalidKey,
    InvalidSignature,
}

impl fmt::Display for SignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignError::InvalidKey => write!(f, "invalid key"),
            SignError::InvalidSignature => write!(f, "invalid signature"),
        }
    }
}

impl std::error::Error for SignError {}

/// Length-prefixed message builder: every field is `u32 BE length || bytes`, integers are 8 bytes BE.
struct Msg(Vec<u8>);

impl Msg {
    fn new(tag: &str) -> Self {
        Msg(Vec::new()).str(tag)
    }

    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend((b.len() as u32).to_be_bytes());
        self.0.extend(b);
        self
    }

    fn str(self, s: &str) -> Self {
        self.bytes(s.as_bytes())
    }

    fn u64(mut self, v: u64) -> Self {
        self.0.extend(v.to_be_bytes());
        self
    }

    fn i64(mut self, v: i64) -> Self {
        self.0.extend(v.to_be_bytes());
        self
    }
}

/// What a client signs when submitting a command.
pub fn command_message(cmd: &Command) -> Vec<u8> {
    Msg::new("kernable.v1.command")
        .str(&cmd.command_id)
        .str(&cmd.tenant_id)
        .str(&cmd.aggregate_type)
        .str(&cmd.aggregate_id)
        .str(&cmd.command_type)
        .bytes(&cmd.payload)
        .0
}

/// What a worker signs when returning events for `cmd` based on `expected_version`.
pub fn result_message(cmd: &Command, expected_version: u64, events: &[NewEvent]) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.result")
        .str(&cmd.command_id)
        .str(&cmd.tenant_id)
        .str(&cmd.aggregate_type)
        .str(&cmd.aggregate_id)
        .u64(expected_version)
        .u64(events.len() as u64);
    for e in events {
        m = m.str(&e.event_type).bytes(&e.payload);
    }
    m.0
}

/// What a worker signs when rejecting a command.
pub fn rejection_message(command_id: &str, reason: &Rejected) -> Vec<u8> {
    Msg::new("kernable.v1.rejection")
        .str(command_id)
        .str(&reason.code)
        .str(&reason.message)
        .0
}

/// What a worker signs to prove it holds its key when registering.
pub fn challenge_message(nonce: &[u8]) -> Vec<u8> {
    Msg::new("kernable.v1.challenge").bytes(nonce).0
}

/// What a subscriber signs to open an event stream.
pub fn subscribe_message(after_position: u64, event_types: &[String], issued_at: i64) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.subscribe")
        .u64(after_position)
        .i64(issued_at)
        .u64(event_types.len() as u64);
    for t in event_types {
        m = m.str(t);
    }
    m.0
}

/// What the kernel signs to anchor the chain at `global_position`.
pub fn checkpoint_message(global_position: u64, chain_hash: &[u8], created_at: i64) -> Vec<u8> {
    Msg::new("kernable.v1.checkpoint")
        .u64(global_position)
        .bytes(chain_hash)
        .i64(created_at)
        .0
}

/// Chain hash of `event`, linked to the hash of the previous event (`GENESIS` for the first).
pub fn event_hash(prev: &[u8], event: &Event) -> [u8; 32] {
    let sig = event.producer_sig.clone().unwrap_or_default();
    let msg = Msg::new("kernable.v1.event")
        .bytes(prev)
        .u64(event.global_position)
        .str(&event.tenant_id)
        .str(&event.aggregate_type)
        .str(&event.aggregate_id)
        .u64(event.version)
        .str(&event.event_type)
        .bytes(&event.payload)
        .str(&event.command_id)
        .i64(event.created_at)
        .str(&sig.key_id)
        .bytes(&sig.sig);
    Sha256::digest(msg.0).into()
}

pub fn key_id(key: &VerifyingKey) -> String {
    hex::encode(key.as_bytes())
}

pub fn generate_key() -> SigningKey {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).expect("OS randomness unavailable");
    SigningKey::from_bytes(&seed)
}

pub fn sign(key: &SigningKey, msg: &[u8]) -> Signature {
    Signature {
        key_id: key_id(&key.verifying_key()),
        sig: key.sign(msg).to_bytes().to_vec(),
    }
}

pub fn verify(sig: &Signature, msg: &[u8]) -> Result<(), SignError> {
    let key = verifying_key_from_hex(&sig.key_id)?;
    let sig = Ed25519Signature::from_slice(&sig.sig).map_err(|_| SignError::InvalidSignature)?;
    key.verify_strict(msg, &sig)
        .map_err(|_| SignError::InvalidSignature)
}

pub fn verifying_key_from_hex(s: &str) -> Result<VerifyingKey, SignError> {
    let bytes: [u8; 32] = hex::decode(s.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or(SignError::InvalidKey)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| SignError::InvalidKey)
}

pub fn signing_key_from_hex(s: &str) -> Result<SigningKey, SignError> {
    let seed: [u8; 32] = hex::decode(s.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or(SignError::InvalidKey)?;
    Ok(SigningKey::from_bytes(&seed))
}

/// Reads a `.key` file (hex-encoded 32-byte seed).
pub fn read_signing_key(path: impl AsRef<Path>) -> io::Result<SigningKey> {
    signing_key_from_hex(&fs::read_to_string(path)?).map_err(invalid_data)
}

/// Reads a `.pub` file (hex-encoded public key, i.e. the key ID).
pub fn read_verifying_key(path: impl AsRef<Path>) -> io::Result<VerifyingKey> {
    verifying_key_from_hex(&fs::read_to_string(path)?).map_err(invalid_data)
}

fn invalid_data(e: SignError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}
