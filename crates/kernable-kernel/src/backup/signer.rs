//! A reference backup signer (`kernable backup signer`): answers the signer protocol
//! ([`super::target::Signer`]) with requests presigned with credentials it holds, for one bucket
//! and prefix, to the keys it allows. For tests, for a trusted network, and as a model of a
//! signing service: a real one keeps the credentials away from the machines it signs for, and
//! knows which prefix each key may use.
//!
//! It serves plain HTTP: `POST` (any path) with a [`SignRequest`] as JSON, answered with `200`
//! and a [`Presigned`], or `400` (a malformed request), `401` (a bad signature, or too old) or
//! `403` (a key or place it does not sign for) with `{"error": "…"}`.

use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::target::{Keys, Op, Presigned, SignRequest, s3_location};
use crate::{BoxError, now_ms};

/// The most a request's head and body may take.
const HEAD_BYTES: usize = 16 << 10;
const BODY_BYTES: usize = 64 << 10;
/// How long a connection may take to send its request.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// What the reference signer signs, and for whom.
pub struct Reference {
    keys: Keys,
    bucket: String,
    prefix: String,
    /// The key IDs it signs for.
    allowed: HashSet<String>,
    expires: Duration,
}

impl Reference {
    /// Signs with `keys` for `to` (`s3://<bucket>/<prefix>`: that prefix and the ones below
    /// it), to the keys `allowed` (key IDs), each request valid for `expires`.
    pub fn new(
        keys: Keys,
        to: &str,
        allowed: impl IntoIterator<Item = String>,
        expires: Duration,
    ) -> Result<Self, BoxError> {
        let (bucket, prefix) = s3_location(to)?;
        let allowed: HashSet<String> = allowed.into_iter().collect();
        if allowed.is_empty() {
            return Err("the signer allows no key".into());
        }
        Ok(Reference {
            keys,
            bucket,
            prefix,
            allowed,
            expires,
        })
    }

    /// The answer to a request's body at `now_ms`: a presigned request, or the HTTP status and
    /// why not.
    pub fn answer(&self, body: &[u8], now_ms: i64) -> Result<Presigned, (u16, String)> {
        let r: SignRequest =
            serde_json::from_slice(body).map_err(|e| (400, format!("not a sign request: {e}")))?;
        let op = r.verify(now_ms).map_err(|e| (401, e))?;
        if !self.allowed.contains(&r.key_id) {
            return Err((403, format!("key {} may not back up here", r.key_id)));
        }
        let under = self.prefix.is_empty()
            || r.prefix == self.prefix
            || r.prefix.starts_with(&format!("{}/", self.prefix));
        if r.bucket != self.bucket || !under || !valid_path(&r.prefix, true) {
            return Err((
                403,
                format!(
                    "s3://{}/{} is not s3://{}/{} or below it",
                    r.bucket, r.prefix, self.bucket, self.prefix
                ),
            ));
        }
        let name_ok = match op {
            // A listing's prefix may be empty, or end with a slash.
            Op::List => r.name.is_empty() || valid_path(r.name.trim_end_matches('/'), false),
            _ => valid_path(&r.name, false),
        };
        if !name_ok || !r.continuation.chars().all(|c| !c.is_control()) {
            return Err((400, format!("not an object name: {:?}", r.name)));
        }
        let key = match r.prefix.as_str() {
            "" => r.name.clone(),
            p => format!("{p}/{}", r.name),
        };
        Ok(self.keys.presign(
            op,
            &self.bucket,
            &key,
            &r.continuation,
            self.expires,
            now_ms,
        ))
    }

    /// Answers requests on `listener` until it fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<(), BoxError> {
        loop {
            let (conn, peer) = listener.accept().await?;
            let me = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = me.connection(conn).await {
                    tracing::debug!(%peer, error = %e, "a signer connection failed");
                }
            });
        }
    }

    async fn connection(&self, mut conn: TcpStream) -> Result<(), BoxError> {
        let body = match tokio::time::timeout(READ_TIMEOUT, read_request(&mut conn)).await {
            Ok(Ok(body)) => Ok(body),
            Ok(Err(why)) => Err(why),
            Err(_) => Err((408, "the request took too long".to_owned())),
        };
        let (status, json) = match body.and_then(|b| self.answer(&b, now_ms())) {
            Ok(presigned) => (200, serde_json::to_vec(&presigned)?),
            Err((status, why)) => {
                tracing::warn!(status, error = %why, "refused to sign");
                (
                    status,
                    serde_json::to_vec(&serde_json::json!({ "error": why }))?,
                )
            }
        };
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            _ => "Error",
        };
        let head = format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            json.len()
        );
        conn.write_all(head.as_bytes()).await?;
        conn.write_all(&json).await?;
        conn.shutdown().await?;
        Ok(())
    }
}

/// `path` as an object name or prefix: segments that are not empty, `.` or `..`, no control
/// characters; empty only if `may_be_empty`.
fn valid_path(path: &str, may_be_empty: bool) -> bool {
    if path.is_empty() {
        return may_be_empty;
    }
    path.split('/')
        .all(|p| !p.is_empty() && p != "." && p != "..")
        && !path.chars().any(char::is_control)
}

/// The body of a `POST` on `conn`, or the status and why not.
async fn read_request(conn: &mut TcpStream) -> Result<Vec<u8>, (u16, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > HEAD_BYTES {
            return Err((400, "the request's head is too long".into()));
        }
        let n = conn
            .read(&mut chunk)
            .await
            .map_err(|e| (400, e.to_string()))?;
        if n == 0 {
            return Err((400, "the connection ended in the request".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut lines = head.split("\r\n");
    if !lines.next().unwrap_or_default().starts_with("POST ") {
        return Err((405, "POST a sign request".into()));
    }
    let length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .ok_or((400, "no content-length".to_owned()))?;
    if length > BODY_BYTES {
        return Err((400, "the request is too long".into()));
    }
    let mut body = buf.split_off(end + 4);
    while body.len() < length {
        let n = conn
            .read(&mut chunk)
            .await
            .map_err(|e| (400, e.to_string()))?;
        if n == 0 {
            return Err((400, "the connection ended in the request".into()));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(length);
    Ok(body)
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use kernable_core::signing;

    use super::*;

    fn reference(key: &SigningKey) -> Reference {
        let keys = Keys::new("https://fsn1.example.com", "eu", "AK", "SK", None, true).unwrap();
        Reference::new(
            keys,
            "s3://backups/shop",
            [signing::key_id(&key.verifying_key())],
            Duration::from_secs(300),
        )
        .unwrap()
    }

    fn ask(key: &SigningKey, op: Op, prefix: &str, name: &str, now: i64) -> Vec<u8> {
        let r = SignRequest::new(key, op, "backups", prefix, name, "", b"", now);
        serde_json::to_vec(&r).unwrap()
    }

    #[test]
    fn it_signs_its_prefix_and_below_for_the_keys_it_allows() {
        let key = SigningKey::from_bytes(&[1; 32]);
        let signer = reference(&key);
        let now = 1_700_000_000_000;
        let put = signer
            .answer(&ask(&key, Op::Put, "shop", "g1/seg-1", now), now)
            .unwrap();
        assert!(
            put.url
                .starts_with("https://fsn1.example.com/backups/shop/g1/seg-1?")
        );
        assert_eq!(
            (put.method.as_str(), put.expires_at),
            ("PUT", now + 300_000)
        );
        let list = signer
            .answer(&ask(&key, Op::List, "shop/a", "", now), now)
            .unwrap();
        assert!(list.url.contains("prefix=shop%2Fa%2F"), "{}", list.url);
        signer
            .answer(&ask(&key, Op::List, "shop", "g1/", now), now)
            .unwrap();

        let refused = |body: Vec<u8>| signer.answer(&body, now).unwrap_err().0;
        // Another place.
        assert_eq!(refused(ask(&key, Op::Get, "shopx", "g1/seg-1", now)), 403);
        assert_eq!(refused(ask(&key, Op::Get, "", "shop/g1", now)), 403);
        assert_eq!(refused(ask(&key, Op::Get, "shop/../x", "a", now)), 403);
        let other_bucket = SignRequest::new(&key, Op::Get, "other", "shop", "a", "", b"", now);
        assert_eq!(refused(serde_json::to_vec(&other_bucket).unwrap()), 403);
        // Another key, an old request, a bad name, a tampered one.
        let stranger = SigningKey::from_bytes(&[2; 32]);
        assert_eq!(refused(ask(&stranger, Op::Get, "shop", "a", now)), 403);
        assert_eq!(refused(ask(&key, Op::Get, "shop", "a", now - 120_000)), 401);
        assert_eq!(refused(ask(&key, Op::Get, "shop", "a/../../b", now)), 400);
        assert_eq!(refused(ask(&key, Op::Get, "shop", "", now)), 400);
        let mut tampered = SignRequest::new(&key, Op::Get, "backups", "shop", "a", "", b"", now);
        tampered.prefix = "shop/b".into();
        assert_eq!(refused(serde_json::to_vec(&tampered).unwrap()), 401);
        assert_eq!(refused(b"{}".to_vec()), 400);
    }
}
