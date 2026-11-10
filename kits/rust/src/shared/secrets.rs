//! Tokens, hashes, passwords and TOTP: the same as the other languages' kits.

use argon2::{
    Algorithm, Argon2, Params, PasswordHash, PasswordHasher, PasswordVerifier, Version,
    password_hash::SaltString,
};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD, HEXLOWER};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// `size` random bytes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut bytes = vec![0; size];
    getrandom::fill(&mut bytes).expect("the system's random source");
    bytes
}

/// A new random secret: `size` bytes, base64url without padding.
pub fn random_secret(size: usize) -> String {
    BASE64URL_NOPAD.encode(&random_bytes(size))
}

/// A new random ID in the UUID v4 form, e.g. for an aggregate a reactor creates.
pub fn new_id() -> String {
    let mut b = random_bytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = HEXLOWER.encode(&b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// SHA-256 in hex: how high-entropy tokens are stored (refresh tokens, API keys, recovery codes).
pub fn sha256(value: &str) -> String {
    HEXLOWER.encode(&Sha256::digest(value.as_bytes()))
}

/// Compares two secrets in constant time (for equal lengths).
pub fn equal_secrets(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// `<aggregateId>.<tokenId>.<secret>` (emailed tokens), `<sessionId>.<generation>.<secret>` (refresh tokens).
pub fn token(a: &str, b: impl std::fmt::Display, c: &str) -> String {
    format!("{a}.{b}.{c}")
}

/// The three parts of a token, or `None` if it has another shape.
pub fn parse_token(value: &str) -> Option<(String, String, String)> {
    let parts: Vec<&str> = value.split('.').collect();
    (parts.len() == 3 && parts.iter().all(|p| !p.is_empty())).then(|| {
        (
            parts[0].to_owned(),
            parts[1].to_owned(),
            parts[2].to_owned(),
        )
    })
}

/// An invitation's token names its tenant too: four parts.
pub fn invitation_token(tenant: &str, invitation_id: &str, token_id: &str, secret: &str) -> String {
    [tenant, invitation_id, token_id, secret].join(".")
}

/// The parts of an invitation token.
pub struct InvitationToken {
    pub tenant: String,
    pub invitation_id: String,
    pub token_id: String,
    pub secret: String,
}

pub fn parse_invitation_token(value: &str) -> Option<InvitationToken> {
    let p: Vec<&str> = value.split('.').collect();
    (p.len() == 4 && p.iter().all(|x| !x.is_empty())).then(|| InvitationToken {
        tenant: p[0].to_owned(),
        invitation_id: p[1].to_owned(),
        token_id: p[2].to_owned(),
        secret: p[3].to_owned(),
    })
}

/// argon2id with OWASP's parameters (19 MiB, 2 iterations, 1 lane): hashes the other kits check, and back.
fn hasher() -> Argon2<'static> {
    Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(19 * 1024, 2, 1, None).expect("valid parameters"),
    )
}

pub fn hash_password(password: &str) -> String {
    let salt = SaltString::encode_b64(&random_bytes(16)).expect("16 bytes are a valid salt");
    hasher()
        .hash_password(password.as_bytes(), &salt)
        .expect("argon2 hashes")
        .to_string()
}

pub fn check_password(stored: Option<&str>, password: &str) -> bool {
    let Some(hash) = stored.and_then(|s| PasswordHash::new(s).ok()) else {
        return false;
    };
    hasher().verify_password(password.as_bytes(), &hash).is_ok()
}

/// One of the characters the kits take for space: controls, spaces, line and paragraph separators
/// and the byte order mark. One explicit set, the same in every language's kit (each language's own
/// idea of whitespace differs).
pub fn is_space(c: char) -> bool {
    matches!(
        c,
        '\u{00}'..='\u{20}'
            | '\u{7f}'
            | '\u{85}'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

/// `value` without the kits' space characters at either end.
pub fn trim_space(value: &str) -> &str {
    value.trim_matches(is_space)
}

/// Lowercased and trimmed: how emails are compared and held as unique keys.
pub fn normal_email(email: &str) -> String {
    trim_space(email).to_lowercase()
}

/// The domain of an address, lowercased: what follows its `@`.
pub fn email_domain(email: &str) -> String {
    let e = normal_email(email);
    match e.rfind('@') {
        Some(at) => e[at + 1..].to_owned(),
        None => e,
    }
}

/// An address: one `@`, a local part, a domain of at least two labels, none of them empty, none of
/// the kits' space characters (see [`is_space`]), at most 254 bytes in UTF-8. The same rule in every
/// language's kit.
pub fn valid_email(email: &str) -> bool {
    let Some((user, host)) = email.split_once('@') else {
        return false;
    };
    let labels: Vec<&str> = host.split('.').collect();
    email.len() <= 254
        && !user.is_empty()
        && !host.contains('@')
        && !email.contains(is_space)
        && labels.len() >= 2
        && labels.iter().all(|l| !l.is_empty())
}

pub fn valid_password(password: &str) -> bool {
    (10..=1024).contains(&password.chars().count())
}

// --- TOTP (RFC 6238): HMAC-SHA1, 30 s steps, 6 digits, base32 secrets ------------------------------

/// A new TOTP secret: 20 random bytes in base32, as authenticator apps take it.
pub fn totp_secret() -> String {
    BASE32_NOPAD.encode(&random_bytes(20))
}

/// The 6-digit code of `secret` for the 30-second step `step`.
pub fn totp_code(secret: &str, step: i64) -> String {
    let key = BASE32_NOPAD
        .decode(secret.to_uppercase().trim_end_matches('=').as_bytes())
        .unwrap_or_default();
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(&key).expect("HMAC takes any key");
    mac.update(&step.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[19] & 0x0f) as usize;
    let number = u32::from_be_bytes([
        digest[offset],
        digest[offset + 1],
        digest[offset + 2],
        digest[offset + 3],
    ]) & 0x7fff_ffff;
    format!("{:06}", number % 1_000_000)
}

/// The step `code` belongs to, at `now` (ms) give or take one step (clock drift), or `None`.
pub fn totp_step(secret: &str, code: &str, now: i64) -> Option<i64> {
    if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let step = now.div_euclid(30_000);
    [step, step - 1, step + 1]
        .into_iter()
        .find(|s| equal_secrets(&totp_code(secret, *s), code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totp_matches_rfc_6238() {
        // RFC 6238, appendix B: the SHA-1 secret "12345678901234567890" at 59 s is 94287082 (8 digits).
        let secret = BASE32_NOPAD.encode(b"12345678901234567890");
        assert_eq!(totp_code(&secret, 59 / 30), "287082");
        assert_eq!(totp_step(&secret, "287082", 59_000), Some(1));
    }

    #[test]
    fn passwords_round_trip_and_check_another_kit_s_hash() {
        let hash = hash_password("correct horse battery");
        assert!(hash.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
        assert!(check_password(Some(&hash), "correct horse battery"));
        assert!(!check_password(Some(&hash), "wrong"));
        assert!(!check_password(None, "x"));
    }
}
