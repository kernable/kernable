//! Access tokens: short-lived, signed with Ed25519 (JWT, EdDSA), checked by the API
//! itself without asking the kernel. The middleware still refuses an ended session at once. The
//! same bytes as the other kits' tokens, so either kit checks the other's.

use data_encoding::BASE64URL_NOPAD;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use kernable::Timestamp;
use serde::{Deserialize, Serialize};

use crate::shared::constants::ACCESS_TOKEN_MINUTES;
use crate::shared::secrets::random_bytes;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// The account.
    pub sub: String,
    /// The session.
    pub sid: String,
    /// The factors the session had when the token was issued.
    pub fac: Vec<String>,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Clone)]
pub struct SigningKeys {
    pub signing: SigningKey,
}

impl SigningKeys {
    pub fn verifying(&self) -> VerifyingKey {
        self.signing.verifying_key()
    }
}

/// New keys, e.g. for development and tests.
pub fn new_signing_keys() -> SigningKeys {
    let bytes: [u8; 32] = random_bytes(32).try_into().expect("32 bytes");
    SigningKeys {
        signing: SigningKey::from_bytes(&bytes),
    }
}

/// Keys from a PEM file holding the private key (PKCS #8).
pub fn signing_keys_from(path: &str) -> Result<SigningKeys, String> {
    let pem = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    Ok(SigningKeys {
        signing: SigningKey::from_pkcs8_pem(&pem).map_err(|e| format!("{path}: {e}"))?,
    })
}

/// `{"alg":"EdDSA","typ":"JWT"}`, as the other kits write it.
const HEADER: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9";

pub fn issue_access_token(
    keys: &SigningKeys,
    sub: &str,
    sid: &str,
    fac: &[String],
    now_ms: Option<i64>,
) -> String {
    let iat = now_ms
        .unwrap_or_else(|| Timestamp::now().millis())
        .div_euclid(1000);
    let claims = Claims {
        sub: sub.to_owned(),
        sid: sid.to_owned(),
        fac: fac.to_vec(),
        iat,
        exp: iat + ACCESS_TOKEN_MINUTES * 60,
    };
    let body = BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims).expect("claims are JSON"));
    let signature = keys.signing.sign(format!("{HEADER}.{body}").as_bytes());
    format!(
        "{HEADER}.{body}.{}",
        BASE64URL_NOPAD.encode(&signature.to_bytes())
    )
}

/// The claims of a valid, unexpired token, else `None`.
pub fn check_access_token(keys: &SigningKeys, token: &str, now_ms: Option<i64>) -> Option<Claims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 || parts[0] != HEADER {
        return None;
    }
    let signature =
        Signature::from_slice(&BASE64URL_NOPAD.decode(parts[2].as_bytes()).ok()?).ok()?;
    keys.verifying()
        .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .ok()?;
    let claims: Claims =
        serde_json::from_slice(&BASE64URL_NOPAD.decode(parts[1].as_bytes()).ok()?).ok()?;
    let now = now_ms.unwrap_or_else(|| Timestamp::now().millis());
    (claims.exp * 1000 > now).then_some(claims)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_refusals() {
        let keys = new_signing_keys();
        let fac = vec!["password".to_owned()];
        let token = issue_access_token(&keys, "a1", "s1", &fac, None);
        let claims = check_access_token(&keys, &token, None).unwrap();
        assert_eq!(
            (claims.sub.as_str(), claims.sid.as_str(), claims.fac),
            ("a1", "s1", fac.clone())
        );
        assert!(
            check_access_token(&new_signing_keys(), &token, None).is_none(),
            "another key"
        );
        assert!(
            check_access_token(
                &keys,
                &issue_access_token(&keys, "a1", "s1", &fac, Some(0)),
                None
            )
            .is_none(),
            "expired"
        );
        assert!(check_access_token(&keys, "nonsense", None).is_none());
        // The header is the other kits' bytes.
        assert_eq!(
            BASE64URL_NOPAD.decode(HEADER.as_bytes()).unwrap(),
            br#"{"alg":"EdDSA","typ":"JWT"}"#
        );
    }
}
