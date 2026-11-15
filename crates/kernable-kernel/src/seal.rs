//! What goes through a relay is sealed with the copy key (docs/how-to/relays.md): the pusher
//! seals what the writer sends, the copy's readers open it, and the relay, which never holds the
//! key, sees records' positions, hashes and sizes, nothing else.
//!
//! A sealed value is the key's ID (the first 8 bytes of its SHA-256), a random 24-byte nonce and
//! the XChaCha20-Poly1305 ciphertext with its tag. Its associated data names what it is and where
//! it belongs, so a relay cannot move it elsewhere unnoticed:
//!
//! - a record: `kernable.v1.sealed.record`, its seq (8 bytes, big-endian) and hash;
//! - a record batch's vault values and files: `kernable.v1.sealed.attachments`, the batch's last
//!   record's seq and hash (a relay may send a reader only the end of a batch: the last stays);
//! - an image batch: `kernable.v1.sealed.image` and its place in the image (8 bytes);
//! - an image's end: `kernable.v1.sealed.image-end`, its seq and hash; the plaintext is the
//!   number of batches (8 bytes), so a reader notices a batch left out;
//! - a write a reader sends through the relay: `kernable.v1.sealed.call`, the call's ID, the
//!   copy's name and when it was sent (8 bytes), so the pusher can refuse it late or twice; the
//!   writer's answer: `kernable.v1.sealed.reply`, the call's ID and the copy's name, so an
//!   answer cannot go to another call.

use std::{path::Path, sync::Arc};

use chacha20poly1305::{
    KeyInit, XChaCha20Poly1305, XNonce,
    aead::{Aead, Payload},
};
use kernable_core::proto::{
    ForwardedCall, ForwardedReply, ImageBatch, ImageEnd, LogRecord, RecordBatch, SealedAttachments,
    follow_response,
};
use prost::Message;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::BoxError;

const ID: usize = 8;
const NONCE: usize = 24;

/// The key a copy at a relay is sealed with.
pub struct CopyKey {
    cipher: XChaCha20Poly1305,
    id: [u8; ID],
    /// The key itself, for sealing it for a new reader.
    raw: Zeroizing<[u8; 32]>,
}

impl CopyKey {
    pub fn new(key: &[u8; 32]) -> Self {
        let digest = Sha256::digest(key);
        let mut id = [0u8; ID];
        id.copy_from_slice(&digest[..ID]);
        CopyKey {
            cipher: XChaCha20Poly1305::new(key.into()),
            id,
            raw: Zeroizing::new(*key),
        }
    }

    /// The key in `path` (64 hex characters).
    pub fn read(path: &Path) -> Result<Arc<Self>, BoxError> {
        let key = kernable_core::signing::read_secret(path)
            .map_err(|e| format!("the copy key {}: {e}", path.display()))?;
        Ok(Arc::new(CopyKey::new(&key)))
    }

    /// Writes a new key file, readable by its owner only; never overwrites one.
    pub fn create(path: &Path) -> Result<(), BoxError> {
        let mut key = Zeroizing::new([0u8; 32]);
        getrandom::fill(key.as_mut()).map_err(|_| "no randomness")?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options
            .open(path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        std::io::Write::write_all(&mut file, format!("{}\n", hex::encode(*key)).as_bytes())?;
        Ok(())
    }

    pub(crate) fn bytes(&self) -> &[u8; 32] {
        &self.raw
    }

    /// The key's ID, in hex.
    pub fn id(&self) -> String {
        hex::encode(self.id)
    }

    fn seal(&self, aad: &[u8], plain: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE];
        getrandom::fill(&mut nonce).expect("randomness");
        let sealed = self
            .cipher
            .encrypt(XNonce::from_slice(&nonce), Payload { msg: plain, aad })
            .expect("sealing does not fail");
        let mut out = Vec::with_capacity(ID + NONCE + sealed.len());
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        out
    }

    fn open(&self, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, BoxError> {
        if sealed.len() < ID + NONCE {
            return Err("a sealed value is cut off".into());
        }
        if sealed[..ID] != self.id {
            return Err(format!(
                "sealed with the copy key {}, not with this one ({})",
                hex::encode(&sealed[..ID]),
                self.id()
            )
            .into());
        }
        let nonce = XNonce::from_slice(&sealed[ID..ID + NONCE]);
        self.cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &sealed[ID + NONCE..],
                    aad,
                },
            )
            .map_err(|_| "a sealed value does not open: changed, or moved to another place".into())
    }
}

fn label(what: &str, seq: u64, hash: &[u8]) -> Vec<u8> {
    let mut aad = format!("kernable.v1.sealed.{what}").into_bytes();
    aad.extend_from_slice(&seq.to_be_bytes());
    aad.extend_from_slice(hash);
    aad
}

fn call_label(copy: &str, id: u64, issued_at: i64) -> Vec<u8> {
    let mut aad = label("call", id, copy.as_bytes());
    aad.extend_from_slice(&issued_at.to_be_bytes());
    aad
}

/// A write for the writer, sealed by a reader of `copy` at `issued_at` (ms).
pub(crate) fn seal_call(
    key: &CopyKey,
    copy: &str,
    id: u64,
    issued_at: i64,
    call: &ForwardedCall,
) -> Vec<u8> {
    key.seal(&call_label(copy, id, issued_at), &call.encode_to_vec())
}

/// Opened by the pusher.
pub(crate) fn open_call(
    key: &CopyKey,
    copy: &str,
    id: u64,
    issued_at: i64,
    sealed: &[u8],
) -> Result<ForwardedCall, BoxError> {
    let plain = key.open(&call_label(copy, id, issued_at), sealed)?;
    Ok(ForwardedCall::decode(plain.as_slice())?)
}

/// The writer's answer to call `id`, sealed by the pusher.
pub(crate) fn seal_reply(key: &CopyKey, copy: &str, id: u64, reply: &ForwardedReply) -> Vec<u8> {
    key.seal(&label("reply", id, copy.as_bytes()), &reply.encode_to_vec())
}

/// Opened by the reader that sent call `id`.
pub(crate) fn open_reply(
    key: &CopyKey,
    copy: &str,
    id: u64,
    sealed: &[u8],
) -> Result<ForwardedReply, BoxError> {
    let plain = key.open(&label("reply", id, copy.as_bytes()), sealed)?;
    Ok(ForwardedReply::decode(plain.as_slice())?)
}

/// Seals what the writer sent a pusher, for the relay. `images` counts the batches of the image
/// under way.
pub(crate) fn seal(key: &CopyKey, msg: &mut follow_response::Msg, images: &mut u64) {
    match msg {
        follow_response::Msg::Image(batch) => {
            let plain = std::mem::take(batch).encode_to_vec();
            batch.sealed = key.seal(&label("image", *images, &[]), &plain);
            *images += 1;
        }
        follow_response::Msg::ImageEnd(end) => {
            end.sealed = key.seal(
                &label("image-end", end.seq, &end.hash),
                &images.to_be_bytes(),
            );
            *images = 0;
        }
        follow_response::Msg::Records(batch) => {
            for r in &mut batch.records {
                if let Some(record) = r.record.take() {
                    r.sealed = key.seal(&label("record", r.seq, &r.hash), &record.encode_to_vec());
                }
            }
            if let Some(last) = batch.records.last() {
                let attachments = SealedAttachments {
                    vault: std::mem::take(&mut batch.vault),
                    files: std::mem::take(&mut batch.files),
                };
                batch.sealed = key.seal(
                    &label("attachments", last.seq, &last.hash),
                    &attachments.encode_to_vec(),
                );
            }
        }
        _ => {}
    }
}

/// Opens what a reader got from a relay, in place. `images` counts the batches of the image
/// under way.
pub(crate) fn open(
    key: &CopyKey,
    msg: &mut follow_response::Msg,
    images: &mut u64,
) -> Result<(), BoxError> {
    match msg {
        follow_response::Msg::Image(batch) => {
            let plain = key.open(&label("image", *images, &[]), &batch.sealed)?;
            *batch = ImageBatch::decode(plain.as_slice())?;
            *images += 1;
        }
        follow_response::Msg::ImageEnd(ImageEnd { seq, hash, sealed }) => {
            let plain = key.open(&label("image-end", *seq, hash), sealed)?;
            let count = u64::from_be_bytes(
                plain
                    .as_slice()
                    .try_into()
                    .map_err(|_| "an image's end holds no count")?,
            );
            if count != *images {
                return Err(
                    format!("the image has {count} batches, the relay sent {images}").into(),
                );
            }
            sealed.clear();
            *images = 0;
        }
        follow_response::Msg::Records(batch) => open_records(key, batch)?,
        _ => {}
    }
    Ok(())
}

fn open_records(key: &CopyKey, batch: &mut RecordBatch) -> Result<(), BoxError> {
    for r in &mut batch.records {
        if r.record.is_some() || r.sealed.is_empty() {
            return Err(format!("record {} is not sealed", r.seq).into());
        }
        let plain = key.open(&label("record", r.seq, &r.hash), &r.sealed)?;
        r.record = Some(LogRecord::decode(plain.as_slice())?);
        r.sealed.clear();
    }
    if let Some(last) = batch.records.last() {
        if !batch.vault.is_empty() || !batch.files.is_empty() {
            return Err("a batch from a relay carries unsealed values".into());
        }
        let plain = key.open(&label("attachments", last.seq, &last.hash), &batch.sealed)?;
        let attachments = SealedAttachments::decode(plain.as_slice())?;
        (batch.vault, batch.files) = (attachments.vault, attachments.files);
        batch.sealed.clear();
    }
    Ok(())
}

/// The copy key sealed for the reader whose Ed25519 key is `reader` (as X25519): a fresh
/// key pair's public half, then the key under XChaCha20-Poly1305 keyed by the SHA-256 of the
/// label, both public halves and the shared secret.
pub(crate) fn seal_key_for(
    reader: &ed25519_dalek::VerifyingKey,
    copy: &str,
    key: &[u8; 32],
) -> Vec<u8> {
    use curve25519_dalek::montgomery::MontgomeryPoint;
    let mut ephemeral = Zeroizing::new([0u8; 32]);
    getrandom::fill(ephemeral.as_mut()).expect("randomness");
    let public = MontgomeryPoint::mul_base_clamped(*ephemeral);
    let theirs = reader.to_montgomery();
    let shared = theirs.mul_clamped(*ephemeral);
    let cipher = key_cipher(copy, &public, &theirs, &shared);
    let mut nonce = [0u8; NONCE];
    getrandom::fill(&mut nonce).expect("randomness");
    let sealed = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: key,
                aad: copy.as_bytes(),
            },
        )
        .expect("sealing does not fail");
    let mut out = public.to_bytes().to_vec();
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&sealed);
    out
}

/// Opened with the reader's own key.
pub(crate) fn open_key(
    reader: &ed25519_dalek::SigningKey,
    copy: &str,
    sealed: &[u8],
) -> Result<Zeroizing<[u8; 32]>, BoxError> {
    use curve25519_dalek::montgomery::MontgomeryPoint;
    if sealed.len() < 32 + NONCE {
        return Err("the sealed copy key is cut off".into());
    }
    let public = MontgomeryPoint(sealed[..32].try_into().expect("32 bytes"));
    let ours = reader.verifying_key().to_montgomery();
    let shared = public.mul_clamped(reader.to_scalar_bytes());
    let cipher = key_cipher(copy, &public, &ours, &shared);
    let plain = Zeroizing::new(
        cipher
            .decrypt(
                XNonce::from_slice(&sealed[32..32 + NONCE]),
                Payload {
                    msg: &sealed[32 + NONCE..],
                    aad: copy.as_bytes(),
                },
            )
            .map_err(|_| "the copy key is not sealed for this key")?,
    );
    let mut key = Zeroizing::new([0u8; 32]);
    key.copy_from_slice(plain.get(..32).ok_or("the copy key is too short")?);
    Ok(key)
}

fn key_cipher(
    copy: &str,
    ephemeral: &curve25519_dalek::montgomery::MontgomeryPoint,
    reader: &curve25519_dalek::montgomery::MontgomeryPoint,
    shared: &curve25519_dalek::montgomery::MontgomeryPoint,
) -> XChaCha20Poly1305 {
    let mut h = Sha256::new();
    h.update(b"kernable.v1.copy-key-for");
    h.update(copy.as_bytes());
    h.update(ephemeral.as_bytes());
    h.update(reader.as_bytes());
    h.update(shared.as_bytes());
    let key = Zeroizing::new(<[u8; 32]>::from(h.finalize()));
    XChaCha20Poly1305::new((&*key).into())
}

impl CopyKey {
    /// Writes `key` to `path`, readable by its owner only; never overwrites one.
    pub fn write(path: &Path, key: &[u8; 32]) -> Result<(), BoxError> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options
            .open(path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        std::io::Write::write_all(&mut file, format!("{}\n", hex::encode(key)).as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernable_core::proto::{ReplicatedRecord, VaultRow, log_record::Record};

    fn batch() -> RecordBatch {
        RecordBatch {
            records: (1..=3)
                .map(|seq| ReplicatedRecord {
                    seq,
                    hash: vec![seq as u8; 32],
                    record: Some(LogRecord {
                        record: Some(Record::Revocation(Default::default())),
                    }),
                    sealed: Vec::new(),
                })
                .collect(),
            vault: vec![VaultRow {
                r#ref: "r1".into(),
                ..Default::default()
            }],
            head: 3,
            ..Default::default()
        }
    }

    #[test]
    fn what_is_sealed_opens_with_the_key_only_and_only_where_it_belongs() {
        let key = CopyKey::new(&[7; 32]);
        let mut msg = follow_response::Msg::Records(batch());
        seal(&key, &mut msg, &mut 0);
        let follow_response::Msg::Records(sealed) = &msg else {
            unreachable!()
        };
        assert!(sealed.records.iter().all(|r| r.record.is_none()));
        assert!(sealed.vault.is_empty());

        // Another key does not open it.
        let mut copy = msg.clone();
        let other = CopyKey::new(&[8; 32]);
        assert!(open(&other, &mut copy, &mut 0).is_err());

        // A record moved to another place does not open.
        let mut moved = msg.clone();
        if let follow_response::Msg::Records(b) = &mut moved {
            b.records.swap(0, 1);
            (b.records[0].seq, b.records[1].seq) = (1, 2);
        }
        assert!(open(&key, &mut moved, &mut 0).is_err());

        // Only its end, as a relay sends a reader that holds the first record.
        let mut end = msg.clone();
        if let follow_response::Msg::Records(b) = &mut end {
            b.records.remove(0);
        }
        open(&key, &mut end, &mut 0).unwrap();

        open(&key, &mut msg, &mut 0).unwrap();
        assert_eq!(msg, follow_response::Msg::Records(batch()));
    }

    #[test]
    fn a_copy_key_sealed_for_a_reader_opens_with_its_key_only() {
        let reader = kernable_core::signing::generate_key();
        let other = kernable_core::signing::generate_key();
        let sealed = seal_key_for(&reader.verifying_key(), "shop", &[9; 32]);
        assert_eq!(*open_key(&reader, "shop", &sealed).unwrap(), [9; 32]);
        assert!(open_key(&other, "shop", &sealed).is_err());
        assert!(
            open_key(&reader, "other", &sealed).is_err(),
            "bound to its copy"
        );
    }

    #[test]
    fn an_answer_opens_only_for_its_call() {
        let key = CopyKey::new(&[7; 32]);
        let reply = ForwardedReply {
            status: 200,
            body: b"answer".to_vec(),
            ..Default::default()
        };
        let sealed = seal_reply(&key, "shop", 1, &reply);
        assert_eq!(open_reply(&key, "shop", 1, &sealed).unwrap(), reply);
        assert!(open_reply(&key, "shop", 2, &sealed).is_err());
        assert!(open_reply(&key, "other", 1, &sealed).is_err());
        assert!(
            open_call(&key, "shop", 1, 0, &sealed).is_err(),
            "nor as a call"
        );
    }

    #[test]
    fn an_image_with_a_batch_left_out_does_not_open() {
        let key = CopyKey::new(&[7; 32]);
        let mut images = 0;
        let mut sealed: Vec<_> = (0..3)
            .map(|_| {
                let mut m = follow_response::Msg::Image(ImageBatch::default());
                seal(&key, &mut m, &mut images);
                m
            })
            .collect();
        let mut end = follow_response::Msg::ImageEnd(ImageEnd {
            seq: 9,
            hash: vec![9; 32],
            sealed: Vec::new(),
        });
        seal(&key, &mut end, &mut images);

        // Batch 1 left out: batch 2 does not open in its place.
        let mut opened = 0;
        open(&key, &mut sealed[0].clone(), &mut opened).unwrap();
        assert!(open(&key, &mut sealed[2].clone(), &mut opened).is_err());
        // The end after two of the three.
        let mut opened = 2;
        assert!(open(&key, &mut end.clone(), &mut opened).is_err());
        // All of them, then the end.
        let mut opened = 0;
        for m in &mut sealed {
            open(&key, m, &mut opened).unwrap();
        }
        open(&key, &mut end, &mut opened).unwrap();
    }
}
