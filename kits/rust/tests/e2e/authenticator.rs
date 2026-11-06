//! A software authenticator for tests: it answers WebAuthn the way a browser with a platform
//! authenticator does (P-256, attestation "none", user verified), without a browser.

use ciborium::Value as Cbor;
use data_encoding::BASE64URL_NOPAD;
use p256::ecdsa::{DerSignature, SigningKey, signature::Signer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub struct Authenticator {
    rp_id: String,
    origin: String,
    user_id: String,
    credential_id: Vec<u8>,
    key: SigningKey,
    counter: u32,
}

fn b64(raw: &[u8]) -> String {
    BASE64URL_NOPAD.encode(raw)
}

fn cbor(value: &Cbor) -> Vec<u8> {
    let mut out = vec![];
    ciborium::into_writer(value, &mut out).unwrap();
    out
}

impl Authenticator {
    pub fn new(rp_id: &str, origin: &str, user_id: &str) -> Authenticator {
        Authenticator {
            rp_id: rp_id.into(),
            origin: origin.into(),
            user_id: user_id.into(),
            credential_id: kit::shared::secrets::random_bytes(16),
            key: SigningKey::from_slice(&kit::shared::secrets::random_bytes(32)).unwrap(),
            counter: 0,
        }
    }

    fn auth_data(&self, flags: u8, attested: &[u8]) -> Vec<u8> {
        let mut data = Sha256::digest(self.rp_id.as_bytes()).to_vec();
        data.push(flags);
        data.extend(self.counter.to_be_bytes());
        data.extend(attested);
        data
    }

    fn client_data(&self, r#type: &str, challenge: &str, origin: Option<&str>) -> Vec<u8> {
        serde_json::to_vec(&json!({ "type": r#type, "challenge": challenge, "origin": origin.unwrap_or(&self.origin), "crossOrigin": false })).unwrap()
    }

    /// The browser's answer to registration options with this challenge.
    pub fn register(&self, challenge: &str) -> Value {
        let point = self.key.verifying_key().to_encoded_point(false);
        // COSE_Key: kty EC2 (2), alg ES256 (-7), crv P-256 (1), x, y.
        let cose = cbor(&Cbor::Map(vec![
            (Cbor::Integer(1.into()), Cbor::Integer(2.into())),
            (Cbor::Integer(3.into()), Cbor::Integer((-7).into())),
            (Cbor::Integer((-1).into()), Cbor::Integer(1.into())),
            (
                Cbor::Integer((-2).into()),
                Cbor::Bytes(point.x().unwrap().to_vec()),
            ),
            (
                Cbor::Integer((-3).into()),
                Cbor::Bytes(point.y().unwrap().to_vec()),
            ),
        ]));
        let mut attested = vec![0u8; 16];
        attested.extend((self.credential_id.len() as u16).to_be_bytes());
        attested.extend(&self.credential_id);
        attested.extend(cose);
        // Flags: user present (0x01), user verified (0x04), attested credential data (0x40).
        let attestation = cbor(&Cbor::Map(vec![
            (Cbor::Text("fmt".into()), Cbor::Text("none".into())),
            (Cbor::Text("attStmt".into()), Cbor::Map(vec![])),
            (
                Cbor::Text("authData".into()),
                Cbor::Bytes(self.auth_data(0x45, &attested)),
            ),
        ]));
        json!({
            "id": b64(&self.credential_id), "rawId": b64(&self.credential_id), "type": "public-key", "clientExtensionResults": {},
            "response": { "clientDataJSON": b64(&self.client_data("webauthn.create", challenge, None)), "attestationObject": b64(&attestation), "transports": ["internal"] },
        })
    }

    /// The browser's answer to sign-in options with this challenge, from `origin` (default the right one).
    pub fn assert(&mut self, challenge: &str, origin: Option<&str>) -> Value {
        self.counter += 1;
        let auth_data = self.auth_data(0x05, &[]);
        let client_data = self.client_data("webauthn.get", challenge, origin);
        let mut signed = auth_data.clone();
        signed.extend(Sha256::digest(&client_data));
        let signature: DerSignature = self.key.sign(&signed);
        json!({
            "id": b64(&self.credential_id), "rawId": b64(&self.credential_id), "type": "public-key", "clientExtensionResults": {},
            "response": {
                "clientDataJSON": b64(&client_data), "authenticatorData": b64(&auth_data), "signature": b64(signature.as_bytes()),
                "userHandle": b64(self.user_id.as_bytes()),
            },
        })
    }
}
