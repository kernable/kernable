//! Every backup object is encrypted with the backup key, so a bucket that leaks reveals
//! nothing; vault values inside are sealed with the vault key besides.
//!
//! An object is `KNBLBAK\n`, the key's ID (the first 8 bytes of the SHA-256 of the key), 8
//! random bytes, then chunks of at most 1 MiB of plaintext, each its ciphertext's length (4
//! bytes, big-endian) and the ciphertext with its tag (AES-256-GCM). Chunk `i`'s nonce is the 8
//! random bytes and `i`; its associated data is the header, `i` and whether it is the last
//! chunk, so chunks cannot be reordered, dropped or cut off unnoticed.

use std::path::Path;

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::BoxError;

/// The first bytes of every backup object.
const MAGIC: &[u8; 8] = b"KNBLBAK\n";
/// Plaintext bytes per chunk at most.
const CHUNK: usize = 1 << 20;
const HEADER: usize = 8 + 8 + 8;
const TAG: usize = 16;

/// The key that encrypts a backup.
pub struct BackupKey {
    cipher: Aes256Gcm,
    id: [u8; 8],
}

impl BackupKey {
    pub fn new(key: &[u8; 32]) -> Self {
        let digest = Sha256::digest(key);
        let mut id = [0u8; 8];
        id.copy_from_slice(&digest[..8]);
        BackupKey {
            cipher: Aes256Gcm::new(key.into()),
            id,
        }
    }

    /// Reads a key file: 32 bytes in hex, as a vault key.
    pub fn read(path: &Path) -> Result<Self, BoxError> {
        let key = kernable_core::signing::read_secret(path)
            .map_err(|e| format!("--backup-key {}: {e}", path.display()))?;
        Ok(BackupKey::new(&key))
    }

    /// Writes a new key file, readable by its owner only; never overwrites one.
    pub fn create(path: &Path) -> Result<(), BoxError> {
        let mut key = Zeroizing::new([0u8; 32]);
        getrandom::fill(key.as_mut()).map_err(|_| "no randomness")?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options
            .open(path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        std::io::Write::write_all(&mut file, format!("{}\n", hex::encode(*key)).as_bytes())?;
        Ok(())
    }

    /// The key's ID, in hex: which key a backup needs.
    pub fn id(&self) -> String {
        hex::encode(self.id)
    }

    /// `plain`, encrypted.
    pub fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, BoxError> {
        let mut header = [0u8; HEADER];
        header[..8].copy_from_slice(MAGIC);
        header[8..16].copy_from_slice(&self.id);
        getrandom::fill(&mut header[16..]).map_err(|_| "no randomness")?;
        let chunks: Vec<&[u8]> = if plain.is_empty() {
            vec![&[]]
        } else {
            plain.chunks(CHUNK).collect()
        };
        let mut out = Vec::with_capacity(HEADER + plain.len() + chunks.len() * (4 + TAG));
        out.extend_from_slice(&header);
        let last = chunks.len() - 1;
        for (i, chunk) in chunks.into_iter().enumerate() {
            let (nonce, aad) = nonce_and_aad(&header, i, i == last)?;
            let sealed = self
                .cipher
                .encrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: chunk,
                        aad: &aad,
                    },
                )
                .map_err(|_| "encrypting a backup object failed")?;
            out.extend_from_slice(&(sealed.len() as u32).to_be_bytes());
            out.extend_from_slice(&sealed);
        }
        Ok(out)
    }

    /// The plaintext of `sealed`; an error if another key sealed it, or it was changed or cut.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, BoxError> {
        if sealed.len() < HEADER || &sealed[..8] != MAGIC {
            return Err("not a kernable backup object".into());
        }
        if sealed[8..16] != self.id {
            return Err(format!(
                "the backup was encrypted with another key (ID {}, this one is {})",
                hex::encode(&sealed[8..16]),
                self.id()
            )
            .into());
        }
        let header: [u8; HEADER] = sealed[..HEADER].try_into().expect("checked above");
        let mut rest = &sealed[HEADER..];
        let mut plain = Vec::with_capacity(rest.len());
        let mut i = 0;
        loop {
            if rest.len() < 4 {
                return Err("the backup object is damaged: it is cut short".into());
            }
            let len = u32::from_be_bytes(rest[..4].try_into().expect("4 bytes")) as usize;
            if !(TAG..=CHUNK + TAG).contains(&len) || rest.len() < 4 + len {
                return Err("the backup object is damaged: a chunk length is invalid".into());
            }
            let last = rest.len() == 4 + len;
            let (nonce, aad) = nonce_and_aad(&header, i, last)?;
            let chunk = self
                .cipher
                .decrypt(
                    Nonce::from_slice(&nonce),
                    Payload {
                        msg: &rest[4..4 + len],
                        aad: &aad,
                    },
                )
                .map_err(|_| "the backup object is damaged or was changed")?;
            plain.extend_from_slice(&chunk);
            rest = &rest[4 + len..];
            if last {
                return Ok(plain);
            }
            i += 1;
        }
    }
}

fn nonce_and_aad(
    header: &[u8; HEADER],
    i: usize,
    last: bool,
) -> Result<([u8; 12], Vec<u8>), BoxError> {
    let i = u32::try_from(i).map_err(|_| "a backup object is too large")?;
    let mut nonce = [0u8; 12];
    nonce[..8].copy_from_slice(&header[16..]);
    nonce[8..].copy_from_slice(&i.to_be_bytes());
    let mut aad = header.to_vec();
    aad.extend_from_slice(&i.to_be_bytes());
    aad.push(u8::from(last));
    Ok((nonce, aad))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> BackupKey {
        BackupKey::new(&[b; 32])
    }

    #[test]
    fn what_is_sealed_opens_with_the_same_key_only() {
        for len in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK + 17] {
            let plain: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let sealed = key(1).seal(&plain).unwrap();
            assert_eq!(key(1).open(&sealed).unwrap(), plain, "{len} bytes");
            let other = key(2).open(&sealed).unwrap_err().to_string();
            assert!(other.contains("another key"), "{other}");
        }
    }

    #[test]
    fn a_changed_reordered_or_cut_object_is_refused() {
        let plain: Vec<u8> = (0..3 * CHUNK).map(|i| (i % 7) as u8).collect();
        let sealed = key(1).seal(&plain).unwrap();
        let mut changed = sealed.clone();
        changed[HEADER + 100] ^= 1;
        assert!(key(1).open(&changed).is_err());
        // Without its last chunk, the one before is not marked last: refused.
        let chunk = 4 + CHUNK + TAG;
        let cut = &sealed[..HEADER + 2 * chunk];
        assert!(key(1).open(cut).is_err());
        // Two chunks swapped.
        let mut swapped = sealed[..HEADER].to_vec();
        swapped.extend_from_slice(&sealed[HEADER + chunk..HEADER + 2 * chunk]);
        swapped.extend_from_slice(&sealed[HEADER..HEADER + chunk]);
        swapped.extend_from_slice(&sealed[HEADER + 2 * chunk..]);
        assert!(key(1).open(&swapped).is_err());
        assert!(key(1).open(b"KNBLBAK\nshort").is_err());
        assert!(key(1).open(b"something else entirely").is_err());
    }

    #[test]
    fn the_same_plaintext_seals_differently_each_time() {
        assert_ne!(key(1).seal(b"x").unwrap(), key(1).seal(b"x").unwrap());
    }
}
