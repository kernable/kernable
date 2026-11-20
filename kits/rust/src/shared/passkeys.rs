//! Passkeys (WebAuthn), verified here: a browser's answer to registration options with
//! attestation `none`, and to sign-in options, with the algorithms the options offer, as the
//! other kits: EdDSA (Ed25519), ES256 (P-256) and RS256 (RSA, which Windows Hello may use). The
//! account keeps the COSE key as base64url, the same as the other kits, so every kit verifies
//! every kit's passkeys.

use ciborium::Value as Cbor;
use data_encoding::BASE64URL_NOPAD;
use p256::ecdsa::{DerSignature, signature::Verifier};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::constants::passkeys;

/// A registered passkey: its ID, its COSE public key (both base64url) and its counter.
pub struct Registered {
    pub credential_id: String,
    pub public_key: String,
    pub counter: u64,
}

fn b64(value: &Value) -> Result<Vec<u8>, String> {
    let text = value.as_str().ok_or("a base64url field is missing")?;
    BASE64URL_NOPAD
        .decode(text.trim_end_matches('=').as_bytes())
        .map_err(|e| format!("base64url: {e}"))
}

/// Checks `clientDataJSON`: its type, the challenge the API issued, and one of our origins.
fn client_data(response: &Value, kind: &str, challenge: &str) -> Result<Vec<u8>, String> {
    let raw = b64(&response["response"]["clientDataJSON"])?;
    let data: Value = serde_json::from_slice(&raw).map_err(|e| format!("clientDataJSON: {e}"))?;
    if data["type"] != kind {
        return Err(format!("not a {kind} answer"));
    }
    if data["challenge"].as_str() != Some(challenge.trim_end_matches('=')) {
        return Err("another challenge".into());
    }
    if !data["origin"]
        .as_str()
        .is_some_and(|o| passkeys().origins.iter().any(|x| x == o))
    {
        return Err("another origin".into());
    }
    Ok(raw)
}

/// The authenticator data's flags and counter, after checking the relying party and that the
/// user was present and verified.
fn authenticator_data(data: &[u8]) -> Result<(u8, u64), String> {
    if data.len() < 37 {
        return Err("short authenticator data".into());
    }
    if data[..32] != Sha256::digest(passkeys().rp_id.as_bytes())[..] {
        return Err("another relying party".into());
    }
    let flags = data[32];
    if flags & 0x01 == 0 || flags & 0x04 == 0 {
        return Err("the user was not present and verified".into());
    }
    Ok((
        flags,
        u64::from(u32::from_be_bytes([data[33], data[34], data[35], data[36]])),
    ))
}

fn map_get<'a>(map: &'a [(Cbor, Cbor)], key: &Cbor) -> Option<&'a Cbor> {
    map.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// A passkey's public key, of one of the algorithms the registration options offer.
enum PublicKey {
    /// COSE alg -8 (EdDSA), crv 6 (Ed25519).
    Ed25519(ed25519_dalek::VerifyingKey),
    /// COSE alg -7 (ES256), crv 1 (P-256).
    Es256(p256::ecdsa::VerifyingKey),
    /// COSE alg -257 (RS256): RSASSA-PKCS1-v1_5 with SHA-256.
    Rs256(rsa::pkcs1v15::VerifyingKey<Sha256>),
}

/// The COSE algorithms passkeys may use, in the order the registration options offer them.
pub const ALGORITHMS: [i64; 3] = [-8, -7, -257];

/// The public key of a COSE key: OKP Ed25519 (EdDSA), EC2 P-256 (ES256) or RSA (RS256).
fn public_key(cose: &[u8]) -> Result<PublicKey, String> {
    let value: Cbor = ciborium::from_reader(cose).map_err(|e| format!("COSE key: {e}"))?;
    let map = value.as_map().ok_or("COSE key is no map")?;
    let int = |k: i64| map_get(map, &Cbor::Integer(k.into())).cloned();
    let is = |k: i64, v: i64| int(k) == Some(Cbor::Integer(v.into()));
    let bytes = |k: i64| int(k).and_then(|v| v.into_bytes().ok());
    if is(1, 1) && is(3, -8) && is(-1, 6) {
        let x: [u8; 32] = bytes(-2)
            .and_then(|b| b.try_into().ok())
            .ok_or("an Ed25519 key")?;
        return ed25519_dalek::VerifyingKey::from_bytes(&x)
            .map(PublicKey::Ed25519)
            .map_err(|e| format!("Ed25519 key: {e}"));
    }
    if is(1, 2) && is(3, -7) && is(-1, 1) {
        let coordinate = |k: i64| {
            bytes(k)
                .filter(|b| b.len() == 32)
                .ok_or("a P-256 coordinate")
        };
        let mut point = vec![0x04];
        point.extend(coordinate(-2)?);
        point.extend(coordinate(-3)?);
        return p256::ecdsa::VerifyingKey::from_sec1_bytes(&point)
            .map(PublicKey::Es256)
            .map_err(|e| format!("P-256 key: {e}"));
    }
    if is(1, 3) && is(3, -257) {
        let n = bytes(-1).ok_or("an RSA modulus")?;
        let e = bytes(-2).ok_or("an RSA exponent")?;
        let key = rsa::RsaPublicKey::new(
            rsa::BigUint::from_bytes_be(&n),
            rsa::BigUint::from_bytes_be(&e),
        )
        .map_err(|e| format!("RSA key: {e}"))?;
        return Ok(PublicKey::Rs256(rsa::pkcs1v15::VerifyingKey::new(key)));
    }
    Err("only EdDSA (Ed25519), ES256 (P-256) and RS256 passkeys".into())
}

impl PublicKey {
    fn verify(&self, signed: &[u8], signature: &[u8]) -> Result<(), String> {
        let wrong = |_| "the signature does not match".to_owned();
        match self {
            PublicKey::Ed25519(key) => {
                let signature = ed25519_dalek::Signature::from_slice(signature)
                    .map_err(|e| format!("signature: {e}"))?;
                key.verify(signed, &signature).map_err(wrong)
            }
            PublicKey::Es256(key) => {
                let signature =
                    DerSignature::from_bytes(signature).map_err(|e| format!("signature: {e}"))?;
                key.verify(signed, &signature).map_err(wrong)
            }
            PublicKey::Rs256(key) => {
                let signature = rsa::pkcs1v15::Signature::try_from(signature)
                    .map_err(|e| format!("signature: {e}"))?;
                key.verify(signed, &signature).map_err(wrong)
            }
        }
    }
}

/// Verifies a registration answer (`navigator.credentials.create`) for `challenge`.
pub fn verify_registration(response: &Value, challenge: &str) -> Result<Registered, String> {
    client_data(response, "webauthn.create", challenge)?;
    let attestation: Cbor =
        ciborium::from_reader(&b64(&response["response"]["attestationObject"])?[..])
            .map_err(|e| format!("attestation: {e}"))?;
    let map = attestation.as_map().ok_or("attestation is no map")?;
    let text = |k: &str| map_get(map, &Cbor::Text(k.into()));
    if text("fmt") != Some(&Cbor::Text("none".into()))
        || !text("attStmt")
            .and_then(Cbor::as_map)
            .is_some_and(|m| m.is_empty())
    {
        return Err("only attestation none".into());
    }
    let data = text("authData")
        .and_then(Cbor::as_bytes)
        .ok_or("no authenticator data")?;
    let (flags, counter) = authenticator_data(data)?;
    if flags & 0x40 == 0 || data.len() < 55 {
        return Err("no credential in the answer".into());
    }
    let id_length = usize::from(u16::from_be_bytes([data[53], data[54]]));
    let id = data.get(55..55 + id_length).ok_or("short credential ID")?;
    let cose = data.get(55 + id_length..).ok_or("no public key")?;
    // The key ends the data unless extensions follow: take exactly one CBOR item.
    let mut reader = cose;
    let key: Cbor = ciborium::from_reader(&mut reader).map_err(|e| format!("COSE key: {e}"))?;
    let cose = &cose[..cose.len() - reader.len()];
    drop(key);
    public_key(cose)?;
    Ok(Registered {
        credential_id: BASE64URL_NOPAD.encode(id),
        public_key: BASE64URL_NOPAD.encode(cose),
        counter,
    })
}

/// Verifies a sign-in answer (`navigator.credentials.get`) for `challenge` with a stored passkey;
/// the new counter, which must grow unless the authenticator keeps none (0).
pub fn verify_authentication(
    response: &Value,
    challenge: &str,
    public_key: &str,
    counter: u64,
) -> Result<u64, String> {
    let client = client_data(response, "webauthn.get", challenge)?;
    let data = b64(&response["response"]["authenticatorData"])?;
    let (_, new_counter) = authenticator_data(&data)?;
    let key = self::public_key(
        &BASE64URL_NOPAD
            .decode(public_key.as_bytes())
            .map_err(|e| format!("stored key: {e}"))?,
    )?;
    let mut signed = data.clone();
    signed.extend(Sha256::digest(&client));
    key.verify(&signed, &b64(&response["response"]["signature"])?)?;
    if (new_counter > 0 || counter > 0) && new_counter <= counter {
        return Err("the counter did not grow: a cloned passkey?".into());
    }
    Ok(new_counter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::Value as Cbor;
    use rsa::signature::{SignatureEncoding, Signer};
    use serde_json::json;

    const ORIGIN: &str = "http://localhost:3000";

    fn cbor(value: &Cbor) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::into_writer(value, &mut out).unwrap();
        out
    }

    fn int(i: i64) -> Cbor {
        Cbor::Integer(i.into())
    }

    fn client(kind: &str, challenge: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({ "type": kind, "challenge": challenge, "origin": ORIGIN }))
            .unwrap()
    }

    fn rp_hash() -> Vec<u8> {
        Sha256::digest(passkeys().rp_id.as_bytes()).to_vec()
    }

    /// A registration answer for `cose`, and a sign-in answer signed by `sign` (over the
    /// authenticator data and the client data's hash, as WebAuthn signs).
    fn round_trip(cose: Cbor, sign: impl Fn(&[u8]) -> Vec<u8>) -> Result<(), String> {
        let id = [9u8; 16];
        let mut auth = rp_hash();
        auth.push(0x45); // user present, verified, credential data
        auth.extend(1u32.to_be_bytes());
        auth.extend([0u8; 16]);
        auth.extend((id.len() as u16).to_be_bytes());
        auth.extend(id);
        auth.extend(cbor(&cose));
        let attestation = Cbor::Map(vec![
            (Cbor::Text("fmt".into()), Cbor::Text("none".into())),
            (Cbor::Text("attStmt".into()), Cbor::Map(vec![])),
            (Cbor::Text("authData".into()), Cbor::Bytes(auth)),
        ]);
        let registration = json!({ "response": {
            "clientDataJSON": BASE64URL_NOPAD.encode(&client("webauthn.create", "c1")),
            "attestationObject": BASE64URL_NOPAD.encode(&cbor(&attestation)),
        }});
        let registered = verify_registration(&registration, "c1")?;
        assert_eq!(registered.credential_id, BASE64URL_NOPAD.encode(&id));
        assert_eq!(registered.counter, 1);

        let mut data = rp_hash();
        data.push(0x05);
        data.extend(2u32.to_be_bytes());
        let client_data = client("webauthn.get", "c2");
        let mut signed = data.clone();
        signed.extend(Sha256::digest(&client_data));
        let answer = |signature: Vec<u8>| {
            json!({ "response": {
                "clientDataJSON": BASE64URL_NOPAD.encode(&client_data),
                "authenticatorData": BASE64URL_NOPAD.encode(&data),
                "signature": BASE64URL_NOPAD.encode(&signature),
            }})
        };
        assert_eq!(
            verify_authentication(&answer(sign(&signed)), "c2", &registered.public_key, 1)?,
            2
        );
        let mut forged = sign(&signed);
        forged[0] ^= 1;
        assert!(verify_authentication(&answer(forged), "c2", &registered.public_key, 1).is_err());
        Ok(())
    }

    #[test]
    fn an_ed25519_passkey_registers_and_signs_in() {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let cose = Cbor::Map(vec![
            (int(1), int(1)),
            (int(3), int(-8)),
            (int(-1), int(6)),
            (
                int(-2),
                Cbor::Bytes(key.verifying_key().to_bytes().to_vec()),
            ),
        ]);
        round_trip(cose, |m| {
            ed25519_dalek::Signer::sign(&key, m).to_bytes().to_vec()
        })
        .unwrap();
    }

    #[test]
    fn an_rs256_passkey_registers_and_signs_in() {
        let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 1024).unwrap();
        let public = rsa::traits::PublicKeyParts::n(&key).to_bytes_be();
        let exponent = rsa::traits::PublicKeyParts::e(&key).to_bytes_be();
        let cose = Cbor::Map(vec![
            (int(1), int(3)),
            (int(3), int(-257)),
            (int(-1), Cbor::Bytes(public)),
            (int(-2), Cbor::Bytes(exponent)),
        ]);
        let signer = rsa::pkcs1v15::SigningKey::<Sha256>::new(key);
        round_trip(cose, |m| signer.sign(m).to_vec()).unwrap();
    }

    #[test]
    fn other_keys_are_refused() {
        let cose = Cbor::Map(vec![
            (int(1), int(2)),
            (int(3), int(-35)),
            (int(-1), int(2)),
        ]);
        assert!(round_trip(cose, |_| vec![0; 64]).is_err());
    }
}
