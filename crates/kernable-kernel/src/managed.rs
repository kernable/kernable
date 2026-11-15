//! A managed node (docs/reference/managed-node.md): `kernable node` linked to a control point
//! that tells it a few things about its kernel, and that it tells how it stands.
//!
//! `kernable node join <url> <token>` makes the node's key (`node.key`), joins with a one-time
//! token and writes `managed.toml` beside the project file: the control point's address, the
//! node's ID, its key, and the control point's settings key, pinned. From then on the node pulls
//! its settings every five seconds, signed with its key, and takes them only if the pinned key
//! signed them, they are for this node, and their version is newer than the one it holds
//! (`managed-settings.json`); every thirty seconds it reports its role, lag and version.
//!
//! Settings name only a fixed list, and the node refuses settings with anything else: its role
//! and whom it follows (writer, standby, reader of a writer or of a copy at a relay), the relay
//! and copy a writer pushes to, the kernel version (a binary the customer put at
//! `versions/<version>/kernable`), a reader's maximum lag, and the witness of a pair. Keys,
//! grants, secrets and the customer's services stay in the customer's own file; settings never
//! touch them.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use ed25519_dalek::{Signer as _, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BoxError;

/// `managed.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Link {
    pub url: String,
    pub node: String,
    pub key: PathBuf,
    pub settings_key: String,
    /// A control point with a certificate of its own CA (one the node does not trust otherwise):
    /// that CA's PEM file (made absolute at the join).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca: Option<PathBuf>,
}

/// What a control point may set, and nothing else.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub node: String,
    pub version: u64,
    pub issued_at: i64,
    /// `writer`, `standby` or `reader`.
    pub role: String,
    #[serde(default)]
    pub follow: String,
    #[serde(default)]
    pub copy: String,
    #[serde(default)]
    pub push: Option<Push>,
    #[serde(default)]
    pub kernel_version: String,
    #[serde(default)]
    pub max_lag_ms: Option<u64>,
    #[serde(default)]
    pub witness: String,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Push {
    pub to: String,
    pub copy: String,
}

/// `managed-settings.json`: the settings taken, as signed.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Held {
    settings: String,
    sig: String,
}

const LINK: &str = "managed.toml";
const HELD: &str = "managed-settings.json";

/// What the settings key signs.
fn settings_message(json: &str) -> Vec<u8> {
    let mut m = b"kernable.v1.node-settings\n".to_vec();
    m.extend_from_slice(json.as_bytes());
    m
}

/// What a request's signature covers: method, path, time (s) and the body's SHA-256.
fn request_message(method: &str, path: &str, time: i64, body: &[u8]) -> Vec<u8> {
    format!(
        "kernable-state-request\n{method}\n{path}\n{time}\n{}",
        hex::encode(Sha256::digest(body))
    )
    .into_bytes()
}

fn now_s() -> i64 {
    crate::now_ms() / 1000
}

/// An HTTP client; with `ca`, trusting that CA only.
fn agent(ca: Option<&Path>) -> Result<ureq::Agent, BoxError> {
    let mut config = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(30)));
    if let Some(ca) = ca {
        let pem = std::fs::read(ca).map_err(|e| format!("{}: {e}", ca.display()))?;
        let certs = ureq::tls::parse_pem(&pem)
            .filter_map(|item| match item {
                Ok(ureq::tls::PemItem::Certificate(c)) => Some(c),
                _ => None,
            })
            .collect::<Vec<_>>();
        if certs.is_empty() {
            return Err(format!("{}: no certificate", ca.display()).into());
        }
        config = config.tls_config(
            ureq::tls::TlsConfig::builder()
                .root_certs(ureq::tls::RootCerts::Specific(std::sync::Arc::new(certs)))
                .build(),
        );
    }
    Ok(config.build().into())
}

/// `kernable node join <url> <token>`: the node's key in `dir` (a new one unless there is
/// one), joined, `managed.toml` written. Returns the node's ID.
pub fn join(url: &str, token: &str, dir: &Path, ca: Option<&Path>) -> Result<String, BoxError> {
    let key_path = dir.join("node.key");
    let key = if key_path.exists() {
        kernable_core::signing::read_signing_key(&key_path)?
    } else {
        let key = kernable_core::signing::generate_key();
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        std::io::Write::write_all(
            &mut options.open(&key_path)?,
            format!("{}\n", hex::encode(key.to_bytes())).as_bytes(),
        )?;
        key
    };
    let url = url.trim_end_matches('/');
    let body = serde_json::json!({
        "token": token,
        "key": hex::encode(key.verifying_key().as_bytes()),
    })
    .to_string();
    let ca = ca
        .map(|c| std::fs::canonicalize(c).map_err(|e| format!("{}: {e}", c.display())))
        .transpose()?;
    let mut response = agent(ca.as_deref())?
        .post(&format!("{url}/v1/nodes/join"))
        .header("content-type", "application/json")
        .send(body.as_bytes())
        .map_err(|e| format!("{url}: {e}"))?;
    let text = response.body_mut().read_to_string().unwrap_or_default();
    if response.status().as_u16() != 200 {
        return Err(format!("{url}: joining refused: {} {text}", response.status()).into());
    }
    #[derive(Deserialize)]
    struct Joined {
        node: String,
        settings_key: String,
    }
    let joined: Joined = serde_json::from_str(&text)?;
    kernable_core::signing::verifying_key_from_hex(&joined.settings_key)?;
    let link = Link {
        url: url.to_owned(),
        node: joined.node.clone(),
        key: PathBuf::from("node.key"),
        settings_key: joined.settings_key,
        ca,
    };
    std::fs::write(dir.join(LINK), toml::to_string(&link)?)?;
    Ok(joined.node)
}

/// The link in `dir`, if the node is managed.
pub fn link(dir: &Path) -> Result<Option<Link>, BoxError> {
    let path = dir.join(LINK);
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path)?;
    Ok(Some(
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?,
    ))
}

/// `json`, if `pinned` signed it, it is for `node` and names only what settings may.
fn open(json: &str, sig: &str, pinned: &VerifyingKey, node: &str) -> Result<Settings, BoxError> {
    let sig = ed25519_dalek::Signature::from_slice(&hex::decode(sig)?)?;
    pinned
        .verify_strict(&settings_message(json), &sig)
        .map_err(|_| "the settings are not signed by the control point's settings key")?;
    let settings: Settings = serde_json::from_str(json)
        .map_err(|e| format!("settings with more than a node may take: {e}"))?;
    if settings.node != node {
        return Err(format!("settings for another node ({})", settings.node).into());
    }
    Ok(settings)
}

/// The settings this node took, checked again.
pub fn held(dir: &Path) -> Result<Option<Settings>, BoxError> {
    let (Some(link), Ok(text)) = (link(dir)?, std::fs::read_to_string(dir.join(HELD))) else {
        return Ok(None);
    };
    let held: Held = serde_json::from_str(&text)?;
    let pinned = kernable_core::signing::verifying_key_from_hex(&link.settings_key)?;
    Ok(Some(open(&held.settings, &held.sig, &pinned, &link.node)?))
}

/// A signed request to the control point; its answer's status and body.
fn call(
    link: &Link,
    key: &SigningKey,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<(u16, String), BoxError> {
    let time = now_s();
    let sig = hex::encode(
        key.sign(&request_message(method, path, time, body))
            .to_bytes(),
    );
    let url = format!("{}{path}", link.url);
    let request = ureq::http::Request::builder()
        .method(method)
        .uri(&url)
        .header(
            "x-kernable-key",
            hex::encode(key.verifying_key().as_bytes()),
        )
        .header("x-kernable-time", time.to_string())
        .header("x-kernable-signature", sig)
        .header("content-type", "application/json")
        .body(body.to_vec())?;
    let mut response = agent(link.ca.as_deref())?
        .run(request)
        .map_err(|e| format!("{url}: {e}"))?;
    let text = response.body_mut().read_to_string().unwrap_or_default();
    Ok((response.status().as_u16(), text))
}

/// Pulls the settings once; takes newer ones. Whether it took any.
pub fn pull(dir: &Path) -> Result<bool, BoxError> {
    let Some(link) = link(dir)? else {
        return Ok(false);
    };
    let key = kernable_core::signing::read_signing_key(dir.join(&link.key))?;
    let (status, text) = call(
        &link,
        &key,
        "GET",
        &format!("/v1/nodes/{}/settings", link.node),
        b"",
    )?;
    match status {
        200 => {}
        404 => return Ok(false),
        _ => return Err(format!("the control point answered {status}: {text}").into()),
    }
    let held: Held = serde_json::from_str(&text)?;
    let pinned = kernable_core::signing::verifying_key_from_hex(&link.settings_key)?;
    let settings = open(&held.settings, &held.sig, &pinned, &link.node)?;
    check(&settings)?;
    let current = self::held(dir).ok().flatten().map_or(0, |s| s.version);
    if settings.version <= current {
        // Never back to older settings, even signed ones.
        return Ok(false);
    }
    let tmp = dir.join(format!("{HELD}.new"));
    std::fs::write(&tmp, serde_json::to_vec(&held)?)?;
    std::fs::rename(&tmp, dir.join(HELD))?;
    tracing::info!(version = settings.version, role = %settings.role, "[node] new settings from the control point");
    Ok(true)
}

/// Settings that make sense together.
fn check(s: &Settings) -> Result<(), BoxError> {
    match s.role.as_str() {
        "writer" if s.follow.is_empty() && s.copy.is_empty() => Ok(()),
        "writer" => Err("a writer follows nobody".into()),
        "standby" | "reader" if s.follow.is_empty() => {
            Err(format!("a {} needs follow", s.role).into())
        }
        "standby" if s.copy.is_empty() && s.push.is_none() => Ok(()),
        "standby" => Err("a standby neither reads a copy nor pushes".into()),
        "reader" if s.push.is_none() => Ok(()),
        "reader" => Err("a reader pushes nothing".into()),
        other => Err(format!("role {other:?}: writer, standby or reader").into()),
    }
}

/// The kernel's flags the settings decide; the customer's own values for them give way.
const DECIDED: &[&str] = &["follow", "reader", "reader-copy", "reader-max-lag-ms"];

/// What settings change in a node's file: the kernel's role flags, the reader's maximum lag,
/// the push's relay and copy, the witness, the kernel binary. `push` and `pair` come as the
/// customer wrote them (keys and all) and only get their address and copy set.
pub(crate) struct Overlay<'a> {
    pub flags: &'a mut BTreeMap<String, toml::Value>,
    pub binary: &'a mut Option<PathBuf>,
    pub push: Option<(&'a mut String, &'a mut String)>,
    pub new_push: &'a mut Option<(String, String)>,
    pub witness: Option<&'a mut String>,
}

/// Applies `settings` to a node's file in `dir`; what it could not apply, said.
pub(crate) fn apply(settings: &Settings, dir: &Path, o: Overlay<'_>) -> Vec<String> {
    let mut problems = Vec::new();
    for flag in DECIDED {
        o.flags.remove(*flag);
    }
    let text = |s: &str| toml::Value::String(s.to_owned());
    match settings.role.as_str() {
        "standby" => {
            o.flags.insert("follow".into(), text(&settings.follow));
        }
        "reader" => {
            o.flags.insert("reader".into(), text(&settings.follow));
            if !settings.copy.is_empty() {
                o.flags.insert("reader-copy".into(), text(&settings.copy));
                o.flags
                    .entry("reader-copy-key".into())
                    .or_insert_with(|| text("copy.key"));
            }
            if let Some(ms) = settings.max_lag_ms {
                o.flags.insert(
                    "reader-max-lag-ms".into(),
                    toml::Value::Integer(ms.min(i64::MAX as u64) as i64),
                );
            }
        }
        _ => {}
    }
    if let Some(push) = &settings.push {
        match o.push {
            Some((to, copy)) => {
                *to = push.to.clone();
                *copy = push.copy.clone();
            }
            None => *o.new_push = Some((push.to.clone(), push.copy.clone())),
        }
    }
    if !settings.witness.is_empty() {
        match o.witness {
            Some(w) => *w = settings.witness.clone(),
            None => problems.push(format!(
                "witness {}: this node's file has no [pair] section to set it in",
                settings.witness
            )),
        }
    }
    if !settings.kernel_version.is_empty() {
        let binary = PathBuf::from("versions")
            .join(&settings.kernel_version)
            .join("kernable");
        if dir.join(&binary).is_file() {
            *o.binary = Some(binary);
        } else {
            problems.push(format!(
                "kernel version {}: no {} on this machine; the kernel runs as it is",
                settings.kernel_version,
                binary.display()
            ));
        }
    }
    problems
}

/// What the node reports: the settings it runs, what it could not apply, its kernel's role.
async fn report(dir: &Path, admin: &Path, problems: &[String]) -> Result<(), BoxError> {
    let Some(link) = link(dir)? else {
        return Ok(());
    };
    let key = kernable_core::signing::read_signing_key(dir.join(&link.key))?;
    let settings = held(dir).ok().flatten();
    let role = role(admin)
        .await
        .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() }));
    let body = serde_json::json!({
        "version": settings.as_ref().map_or(0, |s| s.version),
        "binary_version": env!("CARGO_PKG_VERSION"),
        "kernel_version": settings.map(|s| s.kernel_version).unwrap_or_default(),
        "role": role,
        "problems": problems,
        "at": crate::now_ms(),
    })
    .to_string();
    let path = format!("/v1/nodes/{}/report", link.node);
    let (status, text) =
        tokio::task::spawn_blocking(move || call(&link, &key, "POST", &path, body.as_bytes()))
            .await??;
    if status != 200 {
        return Err(format!("the control point answered {status}: {text}").into());
    }
    Ok(())
}

/// The kernel's `GET /api/role` on its local socket.
async fn role(admin: &Path) -> Result<serde_json::Value, BoxError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::UnixStream::connect(admin).await?;
    stream
        .write_all(b"GET /api/role HTTP/1.0\r\nHost: node\r\n\r\n")
        .await?;
    let mut text = String::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut text)).await??;
    let body = text.split("\r\n\r\n").nth(1).ok_or("no answer")?;
    Ok(serde_json::from_str(body)?)
}

/// Pulls every five seconds and reports every thirty, until the node stops. `problems`: what
/// the newest plan could not apply.
pub(crate) async fn run(
    dir: PathBuf,
    admin: PathBuf,
    problems: tokio::sync::watch::Receiver<Vec<String>>,
) {
    let mut pulls = tokio::time::interval(Duration::from_secs(5));
    let mut reported_at = tokio::time::Instant::now() - Duration::from_secs(60);
    loop {
        pulls.tick().await;
        let d = dir.clone();
        match tokio::task::spawn_blocking(move || pull(&d)).await {
            // New settings: reported once the node planned with them (it looks twice a second).
            Ok(Ok(true)) => {
                reported_at =
                    tokio::time::Instant::now() - Duration::from_secs(30) + Duration::from_secs(4)
            }
            Ok(Ok(false)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "[node] pulling the settings failed"),
            Err(e) => tracing::warn!(error = %e, "[node] pulling the settings failed"),
        }
        if reported_at.elapsed() >= Duration::from_secs(30) {
            reported_at = tokio::time::Instant::now();
            let problems = problems.borrow().clone();
            if let Err(e) = report(&dir, &admin, &problems).await {
                tracing::warn!(error = %e, "[node] reporting failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed(key: &SigningKey, json: &str) -> String {
        hex::encode(key.sign(&settings_message(json)).to_bytes())
    }

    #[test]
    fn settings_count_only_signed_for_this_node_and_within_the_list() {
        let key = kernable_core::signing::generate_key();
        let pinned = key.verifying_key();
        let ok = r#"{"node":"n-1","version":2,"issued_at":1,"role":"reader","follow":"r:7400","copy":"shop.eu"}"#;
        assert_eq!(
            open(ok, &signed(&key, ok), &pinned, "n-1").unwrap().copy,
            "shop.eu"
        );
        let other = kernable_core::signing::generate_key();
        assert!(
            open(ok, &signed(&other, ok), &pinned, "n-1").is_err(),
            "another key"
        );
        assert!(
            open(ok, &signed(&key, ok), &pinned, "n-2").is_err(),
            "another node"
        );
        let more = r#"{"node":"n-1","version":2,"issued_at":1,"role":"reader","follow":"r","env":{"X":"1"}}"#;
        assert!(
            open(more, &signed(&key, more), &pinned, "n-1").is_err(),
            "beyond the list"
        );
    }

    #[test]
    fn settings_set_the_role_flags_and_say_what_they_could_not() {
        let settings = Settings {
            role: "reader".into(),
            follow: "relay:7400".into(),
            copy: "shop.eu".into(),
            max_lag_ms: Some(10_000),
            kernel_version: "9.9.9".into(),
            witness: "w:7300".into(),
            ..Default::default()
        };
        let mut flags = BTreeMap::from([
            ("follow".to_owned(), toml::Value::String("old".into())),
            (
                "follow-key".to_owned(),
                toml::Value::String("reader.key".into()),
            ),
        ]);
        let (mut binary, mut new_push) = (None, None);
        let problems = apply(
            &settings,
            Path::new("/nonexistent"),
            Overlay {
                flags: &mut flags,
                binary: &mut binary,
                push: None,
                new_push: &mut new_push,
                witness: None,
            },
        );
        assert!(!flags.contains_key("follow"));
        assert_eq!(flags["reader"].as_str(), Some("relay:7400"));
        assert_eq!(flags["reader-copy"].as_str(), Some("shop.eu"));
        assert_eq!(flags["reader-copy-key"].as_str(), Some("copy.key"));
        assert_eq!(flags["reader-max-lag-ms"].as_integer(), Some(10_000));
        assert_eq!(
            flags["follow-key"].as_str(),
            Some("reader.key"),
            "keys stay the customer's"
        );
        assert_eq!(binary, None);
        assert_eq!(problems.len(), 2, "{problems:?}");
    }
}
