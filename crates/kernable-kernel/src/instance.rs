//! One key per instance (design/instance-keys.md): the file `--instance-key` names is the
//! instance's Ed25519 seed, so its ID is the `.pub` beside it, and every other key the instance
//! needs is derived from the same 32 bytes, each for one purpose only.

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

/// The vault key of the instance whose seed this is.
pub fn vault_key(seed: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    derive(seed, b"kernable.v1.instance.vault")
}

fn derive(seed: &[u8; 32], info: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut out = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(None, seed)
        .expand(info, out.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 length");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_keys_are_stable_and_apart_from_the_seed() {
        let seed = [7u8; 32];
        let vault = vault_key(&seed);
        assert_eq!(
            *vault,
            *vault_key(&seed),
            "the same seed gives the same vault key"
        );
        assert_ne!(*vault, seed, "the vault key is not the signing seed");
        assert_ne!(
            *vault,
            *vault_key(&[8u8; 32]),
            "another instance, another vault key"
        );
        // Fixed, so a change of the derivation (which would lock every vault) fails here.
        assert_eq!(
            hex::encode(*vault),
            "6447972c32a3e4b75fa3078335e9ef8f38e60facfac049f275dc3ec29e578c6d"
        );
    }
}
