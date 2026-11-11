//! Where a backup goes: a directory (`file:<dir>`), or S3-compatible object storage
//! (`s3://<bucket>/<prefix>`: AWS, Hetzner Object Storage, MinIO and others).
//!
//! An S3 request is signed in one of two ways:
//!
//! - **With credentials** ([`Keys`]) from the environment: `KERNABLE_S3_ENDPOINT` (default
//!   `https://s3.<region>.amazonaws.com`), `KERNABLE_S3_REGION` or `AWS_REGION` (default
//!   `us-east-1`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` (if
//!   any). Buckets are addressed as `<bucket>.<endpoint host>`, or as `<endpoint>/<bucket>` with
//!   `KERNABLE_S3_PATH_STYLE=1` (MinIO). Requests are signed with AWS Signature Version 4.
//! - **By a signer** ([`Signer`]): for every request, it asks a signing endpoint for a presigned
//!   request ([`SignRequest`], signed with its own key as `kernable.v1.backup-object`), and sends
//!   that to the storage. The machine holds no storage credentials. It keeps no presigned
//!   request: each is asked for right before it is sent. [`super::signer`] is a reference signer.
//!
//! The objects are small enough for one request each (a base part at most 64 MiB, sealed): the
//! targets put, get, list and delete whole objects, never in parts or ranges.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use ed25519_dalek::SigningKey;
use hmac::{Hmac, Mac};
use kernable_core::{proto::Signature, signing};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BoxError, now_ms};

/// A place for backup objects, by name (`<generation>/<object>`).
pub trait Target: Send + Sync {
    /// Stores `bytes` under `name`, whole or not at all.
    fn put(&self, name: &str, bytes: &[u8]) -> Result<(), BoxError>;
    fn get(&self, name: &str) -> Result<Vec<u8>, BoxError>;
    /// The names under `prefix`, sorted.
    fn list(&self, prefix: &str) -> Result<Vec<String>, BoxError>;
    fn delete(&self, name: &str) -> Result<(), BoxError>;
    /// Where it is, for log lines.
    fn describe(&self) -> String;
}

/// The target `url` names: `file:<dir>` or `s3://<bucket>[/<prefix>]`. With a `signer`, an S3
/// target has every request presigned by it instead of signing with credentials.
pub fn open(url: &str, signer: Option<Signer>) -> Result<Box<dyn Target>, BoxError> {
    if let Some(dir) = url.strip_prefix("file:") {
        if signer.is_some() {
            return Err("a signer presigns requests to s3:// targets, not to file:".into());
        }
        if dir.is_empty() {
            return Err("--to file: needs a directory".into());
        }
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        return Ok(Box::new(Dir(PathBuf::from(dir))));
    }
    if url.starts_with("s3://") {
        let (bucket, prefix) = s3_location(url)?;
        let auth = match signer {
            Some(s) => Auth::Signer(s),
            None => Auth::Keys(Keys::from_env()?),
        };
        return Ok(Box::new(S3 {
            agent: agent(Duration::from_secs(300), None),
            bucket,
            prefix,
            auth,
        }));
    }
    Err(format!("--to {url}: neither file:<dir> nor s3://<bucket>/<prefix>").into())
}

/// The bucket and prefix (without slashes at its ends) of `s3://<bucket>[/<prefix>]`.
pub fn s3_location(url: &str) -> Result<(String, String), BoxError> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| format!("{url}: not s3://<bucket>/<prefix>"))?;
    let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err("--to s3:// needs a bucket: s3://<bucket>/<prefix>".into());
    }
    Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
}

/// A directory: each object a file, written next to its place and renamed.
struct Dir(PathBuf);

impl Dir {
    fn path(&self, name: &str) -> Result<PathBuf, BoxError> {
        if name
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return Err(format!("not an object name: {name}").into());
        }
        Ok(self.0.join(name))
    }
}

impl Target for Dir {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
        let path = self.path(name)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("partial");
        {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        // The rename on the disk too, or a crash could lose an object already acknowledged.
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>, BoxError> {
        let path = self.path(name)?;
        std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()).into())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, BoxError> {
        let mut names = Vec::new();
        walk(&self.0, &self.0, &mut names)?;
        names.retain(|n| n.starts_with(prefix));
        names.sort();
        Ok(names)
    }

    fn delete(&self, name: &str) -> Result<(), BoxError> {
        let path = self.path(name)?;
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        // An emptied generation leaves no directory behind.
        if let Some(parent) = path.parent().filter(|p| *p != self.0) {
            let _ = std::fs::remove_dir(parent);
        }
        Ok(())
    }

    fn describe(&self) -> String {
        format!("file:{}", self.0.display())
    }
}

fn walk(root: &Path, dir: &Path, names: &mut Vec<String>) -> Result<(), BoxError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            walk(root, &path, names)?;
        } else if path.extension().is_none_or(|e| e != "partial") {
            let rel = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            names.push(rel);
        }
    }
    Ok(())
}

/// A request to object storage, or to its signer, that failed: the backup cannot be written or
/// read for now. `signer`: the signer failed, or refused.
#[derive(Debug)]
pub struct StorageError {
    pub signer: bool,
    message: String,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StorageError {}

fn storage_error(signer: bool, message: String) -> BoxError {
    Box::new(StorageError { signer, message })
}

/// What a request to object storage does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Stores an object (`PUT`).
    Put,
    /// Reads an object (`GET`).
    Get,
    /// Lists the objects whose names start with a prefix (`GET` of the bucket, ListObjectsV2).
    List,
    /// Deletes an object (`DELETE`).
    Delete,
}

impl Op {
    /// Its name in a [`SignRequest`].
    pub fn name(self) -> &'static str {
        match self {
            Op::Put => "put",
            Op::Get => "get",
            Op::List => "list",
            Op::Delete => "delete",
        }
    }

    pub fn parse(name: &str) -> Option<Op> {
        [Op::Put, Op::Get, Op::List, Op::Delete]
            .into_iter()
            .find(|op| op.name() == name)
    }

    /// The HTTP method its request has.
    pub fn method(self) -> &'static str {
        match self {
            Op::Put => "PUT",
            Op::Get | Op::List => "GET",
            Op::Delete => "DELETE",
        }
    }
}

/// A signed request to object storage: send `method` to `url` with `headers` (and the body, for
/// a put) before `expires_at` (ms since epoch). The signer's answer, as JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presigned {
    pub method: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    pub expires_at: i64,
}

/// What asks a signer for a [`Presigned`] request, as JSON: `op` (`put`, `get`, `list`,
/// `delete`) on the object `name` under `prefix` in `bucket` (for `list`, the objects whose
/// names start with `name`, from `continuation` on), with the SHA-256 (hex) and length of the
/// body a put sends; signed by `key_id` (`sig`, hex) as `kernable.v1.backup-object`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignRequest {
    pub op: String,
    pub bucket: String,
    pub prefix: String,
    pub name: String,
    #[serde(default)]
    pub continuation: String,
    pub sha256: String,
    pub length: u64,
    pub issued_at: i64,
    pub key_id: String,
    pub sig: String,
}

/// How far a [`SignRequest`]'s `issued_at` may be from the signer's clock.
pub const SIGN_WINDOW_MS: i64 = 60_000;

impl SignRequest {
    /// `op` on `name`, signed with `key` at `issued_at`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: &SigningKey,
        op: Op,
        bucket: &str,
        prefix: &str,
        name: &str,
        continuation: &str,
        body: &[u8],
        issued_at: i64,
    ) -> Self {
        let sha256 = Sha256::digest(body);
        let length = body.len() as u64;
        let message = signing::backup_object_message(
            op.name(),
            bucket,
            prefix,
            name,
            continuation,
            &sha256,
            length,
            issued_at,
        );
        let sig = signing::sign(key, &message);
        SignRequest {
            op: op.name().to_owned(),
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            name: name.to_owned(),
            continuation: continuation.to_owned(),
            sha256: hex::encode(sha256),
            length,
            issued_at,
            key_id: sig.key_id,
            sig: hex::encode(sig.sig),
        }
    }

    /// Its operation, if its signature is `key_id`'s and it was issued within
    /// [`SIGN_WINDOW_MS`] of `now_ms`. Whether that key may ask for it is the signer's to say.
    pub fn verify(&self, now_ms: i64) -> Result<Op, String> {
        let op = Op::parse(&self.op).ok_or_else(|| format!("unknown op {:?}", self.op))?;
        if (now_ms - self.issued_at).abs() > SIGN_WINDOW_MS {
            return Err("issued_at is more than 60 s from the signer's clock".into());
        }
        let sha256 = hex::decode(&self.sha256)
            .ok()
            .filter(|h| h.len() == 32)
            .ok_or("sha256 is not 32 bytes in hex")?;
        let sig = Signature {
            key_id: self.key_id.clone(),
            sig: hex::decode(&self.sig).map_err(|_| "sig is not hex")?,
        };
        let message = signing::backup_object_message(
            &self.op,
            &self.bucket,
            &self.prefix,
            &self.name,
            &self.continuation,
            &sha256,
            self.length,
            self.issued_at,
        );
        signing::verify(&sig, &message).map_err(|e| format!("signature: {e}"))?;
        Ok(op)
    }
}

/// A signing endpoint that presigns every request (see the module's docs): it takes a `POST`
/// of a [`SignRequest`] and answers `200` with a [`Presigned`], or another status with
/// `{"error": "…"}`.
pub struct Signer {
    url: String,
    key: SigningKey,
    agent: ureq::Agent,
}

/// The most a signer's answer may take.
const SIGNER_ANSWER_BYTES: u64 = 64 << 10;

impl Signer {
    /// The signer at `url` (`https://…`, or `http://` on a network you trust), asked with
    /// `key`. Its certificate is checked against the public roots, or only against the CA
    /// certificates in the PEM file `KERNABLE_S3_SIGNER_CA` names.
    pub fn new(url: &str, key: SigningKey) -> Result<Self, BoxError> {
        if !(url.starts_with("https://") || url.starts_with("http://")) {
            return Err(format!("--signer {url}: not an https:// URL").into());
        }
        let ca = match std::env::var("KERNABLE_S3_SIGNER_CA") {
            Ok(path) if !path.is_empty() => {
                let pem = std::fs::read(&path)
                    .map_err(|e| format!("KERNABLE_S3_SIGNER_CA {path}: {e}"))?;
                let certs: Vec<_> = ureq::tls::parse_pem(&pem)
                    .filter_map(|item| match item {
                        Ok(ureq::tls::PemItem::Certificate(c)) => Some(c),
                        _ => None,
                    })
                    .collect();
                if certs.is_empty() {
                    return Err(format!("KERNABLE_S3_SIGNER_CA {path}: no certificate").into());
                }
                Some(ureq::tls::RootCerts::from(certs))
            }
            _ => None,
        };
        Ok(Signer {
            url: url.to_owned(),
            key,
            agent: agent(Duration::from_secs(15), ca),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// A presigned request for `op` on `name` under `prefix` in `bucket`, asked for now. Every
    /// failure is a [`StorageError`] of the signer.
    pub fn ask(
        &self,
        op: Op,
        bucket: &str,
        prefix: &str,
        name: &str,
        continuation: &str,
        body: &[u8],
    ) -> Result<Presigned, BoxError> {
        let fail = |m: String| storage_error(true, format!("the backup signer {}: {m}", self.url));
        let what = format!("{} {}", op.name(), if name.is_empty() { "/" } else { name });
        let request = SignRequest::new(
            &self.key,
            op,
            bucket,
            prefix,
            name,
            continuation,
            body,
            now_ms(),
        );
        let mut response = self
            .agent
            .post(&self.url)
            .header("content-type", "application/json")
            .send(serde_json::to_vec(&request)?)
            .map_err(|e| fail(format!("cannot be reached ({e})")))?;
        let status = response.status().as_u16();
        let answer = response
            .body_mut()
            .with_config()
            .limit(SIGNER_ANSWER_BYTES)
            .read_to_vec()
            .map_err(|e| fail(format!("{what}: {e}")))?;
        if !(200..300).contains(&status) {
            let why = serde_json::from_slice::<serde_json::Value>(&answer)
                .ok()
                .and_then(|v| v.get("error")?.as_str().map(str::to_owned))
                .unwrap_or_else(|| String::from_utf8_lossy(&answer).chars().take(200).collect());
            let verb = if status < 500 { "refused" } else { "failed" };
            return Err(fail(format!("{verb} {what}: HTTP {status} {why}")));
        }
        let presigned: Presigned = serde_json::from_slice(&answer)
            .map_err(|e| fail(format!("{what}: not a presigned request ({e})")))?;
        if presigned.method != op.method() {
            return Err(fail(format!(
                "{what}: a {} request, not {}",
                presigned.method,
                op.method()
            )));
        }
        if !(presigned.url.starts_with("https://") || presigned.url.starts_with("http://")) {
            return Err(fail(format!("{what}: not an http(s) URL")));
        }
        if presigned.expires_at <= now_ms() {
            return Err(fail(format!(
                "{what}: the request expired already (at {})",
                crate::config::utc(presigned.expires_at)
            )));
        }
        Ok(presigned)
    }
}

fn agent(timeout: Duration, roots: Option<ureq::tls::RootCerts>) -> ureq::Agent {
    let mut config = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false);
    if let Some(roots) = roots {
        config = config.tls_config(ureq::tls::TlsConfig::builder().root_certs(roots).build());
    }
    config.build().new_agent()
}

/// S3-compatible object storage.
struct S3 {
    agent: ureq::Agent,
    bucket: String,
    prefix: String,
    auth: Auth,
}

/// How an [`S3`] target's requests are signed.
enum Auth {
    Keys(Keys),
    Signer(Signer),
}

/// Storage credentials and the endpoint they are for: what signs requests itself.
#[derive(Clone)]
pub struct Keys {
    scheme: String,
    /// The endpoint's host (with a port other than the default) and path (`""` or `/s3`).
    host: String,
    path: String,
    path_style: bool,
    creds: Credentials,
}

#[derive(Clone)]
struct Credentials {
    region: String,
    access_key: String,
    secret_key: String,
    token: Option<String>,
}

impl Keys {
    /// The endpoint and credentials of the environment (see the module's docs).
    pub fn from_env() -> Result<Self, BoxError> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let region = var("KERNABLE_S3_REGION")
            .or_else(|| var("AWS_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint = var("KERNABLE_S3_ENDPOINT")
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        Keys::new(
            &endpoint,
            &region,
            &var("AWS_ACCESS_KEY_ID").ok_or(
                "AWS_ACCESS_KEY_ID is not set (or give a signer: --signer, KERNABLE_S3_SIGNER)",
            )?,
            &var("AWS_SECRET_ACCESS_KEY").ok_or("AWS_SECRET_ACCESS_KEY is not set")?,
            var("AWS_SESSION_TOKEN"),
            var("KERNABLE_S3_PATH_STYLE").is_some_and(|v| v == "1"),
        )
    }

    /// Credentials for the storage at `endpoint` (`https://host[:port][/path]`), buckets in
    /// the path with `path_style`, else in the host.
    pub fn new(
        endpoint: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        token: Option<String>,
        path_style: bool,
    ) -> Result<Self, BoxError> {
        let (scheme, host, path) = endpoint_parts(endpoint)?;
        Ok(Keys {
            scheme,
            host,
            path,
            path_style,
            creds: Credentials {
                region: region.to_owned(),
                access_key: access_key.to_owned(),
                secret_key: secret_key.to_owned(),
                token,
            },
        })
    }

    /// The host, path and query of `op` on the object `key` in `bucket` (for a listing: the
    /// objects whose keys start with `key`, from `continuation` on).
    fn shape(
        &self,
        op: Op,
        bucket: &str,
        key: &str,
        continuation: &str,
    ) -> (String, String, Vec<(String, String)>) {
        let object = if op == Op::List { "" } else { key };
        let (host, path) = match self.path_style {
            true => (
                self.host.clone(),
                format!("{}/{bucket}/{object}", self.path),
            ),
            false => (
                format!("{bucket}.{}", self.host),
                format!("{}/{object}", self.path),
            ),
        };
        let mut query = Vec::new();
        if op == Op::List {
            query.push(("list-type".to_owned(), "2".to_owned()));
            query.push(("prefix".to_owned(), key.to_owned()));
            if !continuation.is_empty() {
                query.push(("continuation-token".to_owned(), continuation.to_owned()));
            }
        }
        (host, path, query)
    }

    fn url(&self, host: &str, path: &str, query: &str) -> String {
        let mut url = format!("{}://{host}{}", self.scheme, uri_encode(path, false));
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        url
    }

    /// `op` signed in its headers at `now_ms`, for `body`.
    fn sign(
        &self,
        op: Op,
        bucket: &str,
        key: &str,
        continuation: &str,
        body: &[u8],
        now_ms: i64,
    ) -> Presigned {
        let (host, path, query) = self.shape(op, bucket, key, continuation);
        let headers = vec![("host".to_owned(), host.clone())];
        let signed = sign(
            &self.creds,
            op.method(),
            &path,
            &query,
            headers,
            body,
            now_ms,
        );
        Presigned {
            method: op.method().to_owned(),
            url: self.url(&host, &path, &canonical_query(&query)),
            headers: signed.into_iter().filter(|(k, _)| k != "host").collect(),
            // S3 takes a request signed in its headers within 15 minutes.
            expires_at: now_ms + 15 * 60_000,
        }
    }

    /// `op` presigned in its URL at `now_ms`, valid for `expires` (at most 7 days): whoever
    /// holds it may send it, with any body, until then. The body is not signed
    /// (`UNSIGNED-PAYLOAD`), only the host.
    pub fn presign(
        &self,
        op: Op,
        bucket: &str,
        key: &str,
        continuation: &str,
        expires: Duration,
        now_ms: i64,
    ) -> Presigned {
        let (host, path, mut query) = self.shape(op, bucket, key, continuation);
        let expires = expires.as_secs().clamp(1, 7 * 86_400);
        let stamp = amz_date(now_ms);
        let scope = format!("{}/{}/s3/aws4_request", &stamp[..8], self.creds.region);
        query.extend(
            [
                ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
                (
                    "X-Amz-Credential",
                    format!("{}/{scope}", self.creds.access_key),
                ),
                ("X-Amz-Date", stamp.clone()),
                ("X-Amz-Expires", expires.to_string()),
                ("X-Amz-SignedHeaders", "host".to_owned()),
            ]
            .map(|(k, v)| (k.to_owned(), v)),
        );
        if let Some(token) = &self.creds.token {
            query.push(("X-Amz-Security-Token".to_owned(), token.clone()));
        }
        let canonical_query = canonical_query(&query);
        let canonical = format!(
            "{}\n{}\n{canonical_query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD",
            op.method(),
            uri_encode(&path, false)
        );
        let signature = signature(&self.creds, &stamp, &scope, &canonical);
        Presigned {
            method: op.method().to_owned(),
            url: self.url(
                &host,
                &path,
                &format!("{canonical_query}&X-Amz-Signature={signature}"),
            ),
            headers: BTreeMap::new(),
            expires_at: now_ms + expires as i64 * 1000,
        }
    }
}

impl S3 {
    /// The object's key: the prefix, then `name`.
    fn key(&self, name: &str) -> String {
        match self.prefix.as_str() {
            "" => name.to_owned(),
            p => format!("{p}/{name}"),
        }
    }

    /// Sends `op` on `name`, signed or presigned; the answer's body if it succeeded. Every
    /// failure is a [`StorageError`].
    fn call(
        &self,
        op: Op,
        name: &str,
        continuation: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let request = match &self.auth {
            Auth::Keys(keys) => keys.sign(
                op,
                &self.bucket,
                &self.key(name),
                continuation,
                body,
                now_ms(),
            ),
            Auth::Signer(signer) => {
                signer.ask(op, &self.bucket, &self.prefix, name, continuation, body)?
            }
        };
        let what = match op {
            Op::List => "list".to_owned(),
            _ => format!("{} {name}", op.method()),
        };
        let (status, answer) = self
            .send(&request, body)
            .map_err(|e| storage_error(false, format!("S3 {what}: {e}")))?;
        if (200..300).contains(&status) {
            return Ok(answer);
        }
        let text = String::from_utf8_lossy(&answer);
        let code = between(&text, "<Code>", "</Code>").unwrap_or_default();
        Err(storage_error(
            false,
            format!("S3 {what}: HTTP {status} {code}"),
        ))
    }

    /// Sends a signed request; the status and body.
    fn send(&self, request: &Presigned, body: &[u8]) -> Result<(u16, Vec<u8>), BoxError> {
        let mut builder = ureq::http::Request::builder()
            .method(request.method.as_str())
            .uri(&request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        let mut response = self.agent.run(builder.body(body.to_vec())?)?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_vec()?;
        Ok((status, body))
    }
}

impl Target for S3 {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
        self.call(Op::Put, name, "", bytes).map(|_| ())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>, BoxError> {
        self.call(Op::Get, name, "", &[])
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, BoxError> {
        let strip = match self.prefix.as_str() {
            "" => String::new(),
            p => format!("{p}/"),
        };
        let mut names = Vec::new();
        let mut token = String::new();
        loop {
            let body = self.call(Op::List, prefix, &token, &[])?;
            let text = String::from_utf8_lossy(&body).into_owned();
            let mut rest = text.as_str();
            while let Some(start) = rest.find("<Key>") {
                rest = &rest[start + 5..];
                let end = rest.find("</Key>").ok_or("S3 list: a malformed answer")?;
                let key = xml_unescape(&rest[..end]);
                match key.strip_prefix(&strip) {
                    Some(name) if !name.is_empty() => names.push(name.to_owned()),
                    _ => {}
                }
                rest = &rest[end..];
            }
            token = match between(&text, "<IsTruncated>", "</IsTruncated>") {
                Some("true") => {
                    between(&text, "<NextContinuationToken>", "</NextContinuationToken>")
                        .map(xml_unescape)
                        .filter(|t| !t.is_empty())
                        .ok_or("S3 list: truncated without a continuation token")?
                }
                _ => break,
            };
        }
        names.sort();
        Ok(names)
    }

    fn delete(&self, name: &str) -> Result<(), BoxError> {
        self.call(Op::Delete, name, "", &[]).map(|_| ())
    }

    fn describe(&self) -> String {
        let at = format!("s3://{}/{}", self.bucket, self.prefix);
        match &self.auth {
            Auth::Keys(_) => at,
            Auth::Signer(s) => format!("{at} (signed by {})", s.url()),
        }
    }
}

/// The scheme, host and path of an endpoint URL: `https://host[:port][/path]`. A default port
/// (443 for https, 80 for http) is left out, as the client leaves it out of the `Host` header the
/// signature covers.
fn endpoint_parts(endpoint: &str) -> Result<(String, String, String), BoxError> {
    let (scheme, rest) = endpoint
        .trim_end_matches('/')
        .split_once("://")
        .filter(|(scheme, _)| matches!(*scheme, "http" | "https"))
        .ok_or("KERNABLE_S3_ENDPOINT: not a URL like https://fsn1.your-objectstorage.com")?;
    let (host, path) = match rest.split_once('/') {
        Some((host, path)) => (host, format!("/{}", path.trim_matches('/'))),
        None => (rest, String::new()),
    };
    let host = match (scheme, host.rsplit_once(':')) {
        ("https", Some((h, "443"))) | ("http", Some((h, "80"))) => h,
        _ => host,
    };
    if host.is_empty() {
        return Err("KERNABLE_S3_ENDPOINT: no host".into());
    }
    Ok((scheme.to_owned(), host.to_owned(), path))
}

fn between<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = text.find(open)? + open.len();
    let end = text[start..].find(close)? + start;
    Some(&text[start..end])
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// `s` percent-encoded as Signature Version 4 wants it: everything but unreserved characters,
/// and `/` too unless `slash` is false (a path).
fn uri_encode(s: &str, slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query<K: AsRef<str>, V: AsRef<str>>(query: &[(K, V)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k.as_ref(), true), uri_encode(v.as_ref(), true)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

/// `now_ms` as Signature Version 4 writes it: `20130524T000000Z`.
fn amz_date(now_ms: i64) -> String {
    crate::config::utc(now_ms).replace(['-', ':'], "")
}

/// The signature of `canonical` (a canonical request) at `stamp` in `scope`.
fn signature(creds: &Credentials, stamp: &str, scope: &str, canonical: &str) -> String {
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = [&stamp[..8], &creds.region, "s3", "aws4_request"]
        .iter()
        .fold(
            format!("AWS4{}", creds.secret_key).into_bytes(),
            |k, part| hmac(&k, part.as_bytes()),
        );
    hex::encode(hmac(&key, to_sign.as_bytes()))
}

/// The headers of a request signed with Signature Version 4 at `now_ms`: `headers` (with `host`)
/// and `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token` (with a session token) and
/// `authorization`, lowercase names.
fn sign(
    creds: &Credentials,
    method: &str,
    path: &str,
    query: &[(String, String)],
    mut headers: Vec<(String, String)>,
    body: &[u8],
    now_ms: i64,
) -> Vec<(String, String)> {
    let stamp = amz_date(now_ms);
    let payload = hex::encode(Sha256::digest(body));
    headers.push(("x-amz-content-sha256".into(), payload.clone()));
    headers.push(("x-amz-date".into(), stamp.clone()));
    if let Some(token) = &creds.token {
        headers.push(("x-amz-security-token".into(), token.clone()));
    }
    headers.sort();
    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload}",
        uri_encode(path, false),
        canonical_query(query)
    );
    let scope = format!("{}/{}/s3/aws4_request", &stamp[..8], creds.region);
    let signature = signature(creds, &stamp, &scope, &canonical);
    headers.push((
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            creds.access_key
        ),
    ));
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example credentials of the AWS documentation ("Signature Calculations for the
    /// Authorization Header", Amazon S3 API reference), on 2013-05-24.
    fn example() -> (Credentials, i64) {
        let creds = Credentials {
            region: "us-east-1".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            token: None,
        };
        (
            creds,
            crate::config::utc_ms("2013-05-24T00:00:00Z").unwrap(),
        )
    }

    fn signature(headers: &[(String, String)]) -> String {
        let auth = &headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .unwrap()
            .1;
        auth.rsplit("Signature=").next().unwrap().to_owned()
    }

    fn host() -> Vec<(String, String)> {
        vec![("host".into(), "examplebucket.s3.amazonaws.com".into())]
    }

    #[test]
    fn get_object_matches_the_aws_example() {
        let (creds, now) = example();
        let mut headers = host();
        headers.push(("range".into(), "bytes=0-9".into()));
        let signed = sign(&creds, "GET", "/test.txt", &[], headers, b"", now);
        assert_eq!(
            signature(&signed),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn list_objects_matches_the_aws_example() {
        let (creds, now) = example();
        let query = [("max-keys", "2"), ("prefix", "J")].map(|(k, v)| (k.into(), v.into()));
        let signed = sign(&creds, "GET", "/", &query, host(), b"", now);
        assert_eq!(
            signature(&signed),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn names_and_queries_are_encoded_as_signature_version_4_wants() {
        assert_eq!(uri_encode("/a b/c~d", false), "/a%20b/c~d");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(
            canonical_query(&[("prefix", "g/"), ("list-type", "2")]),
            "list-type=2&prefix=g%2F"
        );
        assert_eq!(xml_unescape("a&amp;b&lt;"), "a&b<");
    }

    #[test]
    fn endpoints_keep_their_path_and_drop_a_default_port() {
        let parts = |e: &str| endpoint_parts(e).unwrap();
        assert_eq!(
            parts("https://fsn1.your-objectstorage.com"),
            (
                "https".into(),
                "fsn1.your-objectstorage.com".into(),
                String::new()
            )
        );
        assert_eq!(
            parts("https://s3.example.com:443/storage/"),
            ("https".into(), "s3.example.com".into(), "/storage".into())
        );
        assert_eq!(
            parts("http://127.0.0.1:9000"),
            ("http".into(), "127.0.0.1:9000".into(), String::new())
        );
        assert!(endpoint_parts("s3.example.com").is_err());
        assert!(endpoint_parts("ftp://x").is_err());
    }

    #[test]
    fn a_directory_holds_objects_by_name() {
        let dir = std::env::temp_dir().join(format!("kernable-target-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let t = open(&format!("file:{}", dir.display()), None).unwrap();
        t.put("g1/base-000001", b"one").unwrap();
        t.put("g1/seg-1", b"two").unwrap();
        t.put("g2/base-000001", b"three").unwrap();
        assert_eq!(t.list("g1/").unwrap(), ["g1/base-000001", "g1/seg-1"]);
        assert_eq!(t.get("g1/seg-1").unwrap(), b"two");
        t.delete("g1/seg-1").unwrap();
        t.delete("g1/base-000001").unwrap();
        assert_eq!(t.list("").unwrap(), ["g2/base-000001"]);
        assert!(t.put("../escape", b"x").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_presigned_get_matches_the_aws_example() {
        // "Authenticating Requests: Using Query Parameters", Amazon S3 API reference.
        let (creds, now) = example();
        let keys = Keys::new(
            "https://s3.amazonaws.com",
            &creds.region,
            &creds.access_key,
            &creds.secret_key,
            None,
            false,
        )
        .unwrap();
        let p = keys.presign(
            Op::Get,
            "examplebucket",
            "test.txt",
            "",
            Duration::from_secs(86_400),
            now,
        );
        assert_eq!(p.method, "GET");
        assert!(p.headers.is_empty());
        assert_eq!(p.expires_at, now + 86_400_000);
        assert_eq!(
            p.url,
            "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn a_presigned_listing_in_path_style_names_the_bucket_in_the_path() {
        let (creds, now) = example();
        let keys = Keys::new(
            "http://127.0.0.1:9000",
            &creds.region,
            &creds.access_key,
            &creds.secret_key,
            Some("tok/en".into()),
            true,
        )
        .unwrap();
        let p = keys.presign(
            Op::List,
            "backups",
            "shop/g1/",
            "next+1",
            Duration::from_secs(300),
            now,
        );
        assert_eq!(p.method, "GET");
        assert!(
            p.url
                .starts_with("http://127.0.0.1:9000/backups/?X-Amz-Algorithm=AWS4-HMAC-SHA256&"),
            "{}",
            p.url
        );
        for part in [
            "&X-Amz-Expires=300&",
            "&X-Amz-Security-Token=tok%2Fen&",
            "&continuation-token=next%2B1&list-type=2&prefix=shop%2Fg1%2F&X-Amz-Signature=",
        ] {
            assert!(p.url.contains(part), "{part} in {}", p.url);
        }
        // A put presigned in its URL: the object's path, no query of its own.
        let put = keys.presign(
            Op::Put,
            "backups",
            "shop/g1/seg 1",
            "",
            Duration::from_secs(60),
            now,
        );
        assert!(
            put.url
                .starts_with("http://127.0.0.1:9000/backups/shop/g1/seg%201?X-Amz-")
        );
        assert_eq!(put.method, "PUT");
    }

    #[test]
    fn a_sign_request_holds_its_signature_and_its_time() {
        let key = SigningKey::from_bytes(&[5; 32]);
        let now = 1_700_000_000_000;
        let r = SignRequest::new(&key, Op::Put, "b", "shop", "g1/seg-1", "", b"sealed", now);
        assert_eq!(r.length, 6);
        assert_eq!(r.verify(now + 59_000), Ok(Op::Put));
        assert!(r.verify(now + 61_000).unwrap_err().contains("60 s"));
        let mut other = r.clone();
        other.name = "g1/seg-2".into();
        assert!(other.verify(now).unwrap_err().contains("signature"));
        let mut other = r.clone();
        other.op = "get".into();
        assert!(other.verify(now).is_err(), "a signed put is no get");
        let mut other = r.clone();
        other.op = "copy".into();
        assert!(other.verify(now).unwrap_err().contains("unknown op"));
        // As JSON, the way a signer reads it.
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<SignRequest>(&json).unwrap(), r);
    }

    #[test]
    fn a_signer_is_for_s3_targets() {
        let key = SigningKey::from_bytes(&[5; 32]);
        let signer = Signer::new("http://127.0.0.1:1", key.clone()).unwrap();
        assert!(open("file:/tmp/x", Some(signer)).is_err());
        assert!(Signer::new("ftp://x", key).is_err());
        assert_eq!(
            s3_location("s3://b/shop/a/").unwrap(),
            ("b".to_owned(), "shop/a".to_owned())
        );
        assert!(s3_location("s3:///x").is_err());
    }
}
