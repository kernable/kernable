//! Passkeys (WebAuthn), verified here: a browser's answer to registration options with
//! attestation `none`, and to sign-in options, with ES256 keys (P-256), as platform
//! authenticators and password managers make them. The account keeps the COSE key as base64url,
//! the same as the other kits, so every kit verifies every kit's passkeys.

use ciborium::Value as Cbor;
use data_encoding::BASE64URL_NOPAD;
use p256::ecdsa::{DerSignature, VerifyingKey, signature::Verifier};
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

/// The P-256 key of a COSE key (kty EC2, alg ES256, crv P-256).
fn es256_key(cose: &[u8]) -> Result<VerifyingKey, String> {
    let value: Cbor = ciborium::from_reader(cose).map_err(|e| format!("COSE key: {e}"))?;
    let map = value.as_map().ok_or("COSE key is no map")?;
    let int = |k: i64| map_get(map, &Cbor::Integer(k.into())).cloned();
    if int(1) != Some(Cbor::Integer(2.into()))
        || int(3) != Some(Cbor::Integer((-7).into()))
        || int(-1) != Some(Cbor::Integer(1.into()))
    {
        return Err("only ES256 (P-256) passkeys".into());
    }
    let coordinate = |k: i64| {
        int(k)
            .and_then(|v| v.into_bytes().ok())
            .filter(|b| b.len() == 32)
            .ok_or("a P-256 coordinate")
    };
    let mut point = vec![0x04];
    point.extend(coordinate(-2)?);
    point.extend(coordinate(-3)?);
    VerifyingKey::from_sec1_bytes(&point).map_err(|e| format!("P-256 key: {e}"))
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
    es256_key(cose)?;
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
    let key = es256_key(
        &BASE64URL_NOPAD
            .decode(public_key.as_bytes())
            .map_err(|e| format!("stored key: {e}"))?,
    )?;
    let signature = DerSignature::from_bytes(&b64(&response["response"]["signature"])?)
        .map_err(|e| format!("signature: {e}"))?;
    let mut signed = data.clone();
    signed.extend(Sha256::digest(&client));
    key.verify(&signed, &signature)
        .map_err(|_| "the signature does not match".to_owned())?;
    if (new_counter > 0 || counter > 0) && new_counter <= counter {
        return Err("the counter did not grow: a cloned passkey?".into());
    }
    Ok(new_counter)
}
