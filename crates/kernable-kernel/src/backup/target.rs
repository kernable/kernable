//! Where a backup goes: a directory (`file:<dir>`), or S3-compatible object storage
//! (`s3://<bucket>/<prefix>`: AWS, Hetzner Object Storage, MinIO and others).
//!
//! S3 takes its endpoint and credentials from the environment: `KERNABLE_S3_ENDPOINT` (default
//! `https://s3.<region>.amazonaws.com`), `KERNABLE_S3_REGION` or `AWS_REGION` (default
//! `us-east-1`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` (if any).
//! Buckets are addressed as `<bucket>.<endpoint host>`, or as `<endpoint>/<bucket>` with
//! `KERNABLE_S3_PATH_STYLE=1` (MinIO). Requests are signed with AWS Signature Version 4.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::BoxError;

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

/// The target `url` names: `file:<dir>` or `s3://<bucket>[/<prefix>]`.
pub fn open(url: &str) -> Result<Box<dyn Target>, BoxError> {
    if let Some(dir) = url.strip_prefix("file:") {
        if dir.is_empty() {
            return Err("--to file: needs a directory".into());
        }
        std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
        return Ok(Box::new(Dir(PathBuf::from(dir))));
    }
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err("--to s3:// needs a bucket: s3://<bucket>/<prefix>".into());
        }
        return Ok(Box::new(S3::from_env(bucket, prefix.trim_matches('/'))?));
    }
    Err(format!("--to {url}: neither file:<dir> nor s3://<bucket>/<prefix>").into())
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

/// S3-compatible object storage.
struct S3 {
    agent: ureq::Agent,
    /// `https://<host>` (with the bucket in the host, unless path style).
    base: String,
    host: String,
    /// The path before an object's key: `/` or `/<bucket>/`.
    root: String,
    bucket: String,
    prefix: String,
    creds: Credentials,
}

#[derive(Clone)]
struct Credentials {
    region: String,
    access_key: String,
    secret_key: String,
    token: Option<String>,
}

impl S3 {
    fn from_env(bucket: &str, prefix: &str) -> Result<Self, BoxError> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let region = var("KERNABLE_S3_REGION")
            .or_else(|| var("AWS_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        let endpoint = var("KERNABLE_S3_ENDPOINT")
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let (scheme, host, base) = endpoint_parts(&endpoint)?;
        let path_style = var("KERNABLE_S3_PATH_STYLE").is_some_and(|v| v == "1");
        let (host, root) = if path_style {
            (host, format!("{base}/{bucket}/"))
        } else {
            (format!("{bucket}.{host}"), format!("{base}/"))
        };
        let creds = Credentials {
            region,
            access_key: var("AWS_ACCESS_KEY_ID").ok_or("AWS_ACCESS_KEY_ID is not set")?,
            secret_key: var("AWS_SECRET_ACCESS_KEY").ok_or("AWS_SECRET_ACCESS_KEY is not set")?,
            token: var("AWS_SESSION_TOKEN"),
        };
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(300)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Ok(S3 {
            agent,
            base: format!("{scheme}://{host}"),
            host,
            root,
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            creds,
        })
    }

    fn key(&self, name: &str) -> String {
        match self.prefix.as_str() {
            "" => name.to_owned(),
            p => format!("{p}/{name}"),
        }
    }

    /// Sends a signed request; the status and body.
    fn send(
        &self,
        method: &str,
        path: &str,
        query: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(u16, Vec<u8>), BoxError> {
        let now = crate::now_ms();
        let headers = vec![("host".to_owned(), self.host.clone())];
        let signed = sign(&self.creds, method, path, query, headers, body, now);
        let mut url = format!("{}{}", self.base, uri_encode(path, false));
        if !query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query(query));
        }
        let mut request = ureq::http::Request::builder().method(method).uri(&url);
        for (name, value) in &signed {
            if name != "host" {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let request = request.body(body.to_vec())?;
        let mut response = self
            .agent
            .run(request)
            .map_err(|e| format!("S3 {method} {path}: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_vec()
            .map_err(|e| format!("S3 {method} {path}: {e}"))?;
        Ok((status, body))
    }

    fn checked(&self, what: &str, (status, body): (u16, Vec<u8>)) -> Result<Vec<u8>, BoxError> {
        if (200..300).contains(&status) {
            return Ok(body);
        }
        let text = String::from_utf8_lossy(&body);
        let code = between(&text, "<Code>", "</Code>").unwrap_or_default();
        Err(format!("S3 {what}: HTTP {status} {code}").into())
    }
}

impl Target for S3 {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<(), BoxError> {
        let path = format!("{}{}", self.root, self.key(name));
        let reply = self.send("PUT", &path, &[], bytes)?;
        self.checked(&format!("PUT {name}"), reply).map(|_| ())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>, BoxError> {
        let path = format!("{}{}", self.root, self.key(name));
        let reply = self.send("GET", &path, &[], &[])?;
        self.checked(&format!("GET {name}"), reply)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, BoxError> {
        let full = self.key(prefix);
        let strip = match self.prefix.as_str() {
            "" => 0,
            p => p.len() + 1,
        };
        let mut names = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![("list-type", "2"), ("prefix", full.as_str())];
            if let Some(t) = &token {
                query.push(("continuation-token", t.as_str()));
            }
            let reply = self.send("GET", &self.root, &query, &[])?;
            let body = self.checked("list", reply)?;
            let text = String::from_utf8_lossy(&body).into_owned();
            let mut rest = text.as_str();
            while let Some(start) = rest.find("<Key>") {
                rest = &rest[start + 5..];
                let end = rest.find("</Key>").ok_or("S3 list: a malformed answer")?;
                let key = xml_unescape(&rest[..end]);
                if key.len() > strip {
                    names.push(key[strip..].to_owned());
                }
                rest = &rest[end..];
            }
            token = match between(&text, "<IsTruncated>", "</IsTruncated>") {
                Some("true") => Some(
                    between(&text, "<NextContinuationToken>", "</NextContinuationToken>")
                        .map(xml_unescape)
                        .ok_or("S3 list: truncated without a continuation token")?,
                ),
                _ => None,
            };
            if token.is_none() {
                break;
            }
        }
        names.sort();
        Ok(names)
    }

    fn delete(&self, name: &str) -> Result<(), BoxError> {
        let path = format!("{}{}", self.root, self.key(name));
        let reply = self.send("DELETE", &path, &[], &[])?;
        self.checked(&format!("DELETE {name}"), reply).map(|_| ())
    }

    fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
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

fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
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

/// The headers of a request signed with Signature Version 4 at `now_ms`: `headers` (with `host`)
/// and `x-amz-date`, `x-amz-content-sha256`, `x-amz-security-token` (with a session token) and
/// `authorization`, lowercase names.
fn sign(
    creds: &Credentials,
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    mut headers: Vec<(String, String)>,
    body: &[u8],
    now_ms: i64,
) -> Vec<(String, String)> {
    let stamp = crate::config::utc(now_ms).replace(['-', ':'], "");
    let date = &stamp[..8];
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
    let scope = format!("{date}/{}/s3/aws4_request", creds.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let key = [date, &creds.region, "s3", "aws4_request"].iter().fold(
        format!("AWS4{}", creds.secret_key).into_bytes(),
        |k, part| hmac(&k, part.as_bytes()),
    );
    let signature = hex::encode(hmac(&key, to_sign.as_bytes()));
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
        let query = [("max-keys", "2"), ("prefix", "J")];
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
        let t = open(&format!("file:{}", dir.display())).unwrap();
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
}
