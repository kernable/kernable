//! A managed node (docs/reference/managed-node.md): `kernable node` linked to a control point
//! that tells it a few things about its kernel, and that it tells how it stands.
//!
//! `kernable node join <url> <token> --root <root>` makes the kernel's one key (`instance.key`,
//! unless there is one), joins with it and a one-time token, and writes `managed.toml` beside the
//! project file: the control point's address, the node's ID, its key, the control point's
//! settings key, pinned, and the project's root key, pinned too. From then on the node pulls
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
//!
//! What is signed by the project's root key comes from the control point too, checked against
//! the pinned root before it is used (design/instance-keys.md): the grant for the node's key,
//! written beside it, and the writer approvals of the project, written to `approvals/`. The
//! control point carries them and can make none.

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
    /// The project's root keys (key IDs), pinned at the join: grants and writer approvals from
    /// the control point count only signed by one of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roots: Vec<String>,
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

/// `kernable node join <url> <token> --root <root>`: the kernel's key in `dir` (`instance.key`,
/// a new one unless there is one), joined, `managed.toml` written with `roots` pinned. Returns
/// the node's ID and its key's ID.
pub fn join(
    url: &str,
    token: &str,
    dir: &Path,
    ca: Option<&Path>,
    roots: &[String],
) -> Result<(String, String), BoxError> {
    if roots.is_empty() {
        return Err(
            "give the project's root key with --root (its .pub or ID): the grants and \
                    approvals the control point brings count only signed by it"
                .into(),
        );
    }
    let key_path = dir.join(INSTANCE_KEY);
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
        std::fs::write(
            key_path.with_extension("pub"),
            format!("{}\n", kernable_core::signing::key_id(&key.verifying_key())),
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
        key: PathBuf::from(INSTANCE_KEY),
        settings_key: joined.settings_key,
        roots: roots.to_vec(),
        ca,
    };
    std::fs::write(dir.join(LINK), toml::to_string(&link)?)?;
    Ok((
        joined.node,
        kernable_core::signing::key_id(&key.verifying_key()),
    ))
}

/// The kernel's one key a joined node makes, and runs its kernel with unless its file names
/// another `instance-key`.
pub const INSTANCE_KEY: &str = "instance.key";

/// Where a joined node keeps the project's writer approvals (`<copy>.approval`), for its kernel's
/// `--approvals`.
pub const APPROVALS: &str = "approvals";

/// What the control point carries for a node: what the project's root signed.
#[derive(Debug, Default, Deserialize)]
struct Signed {
    /// Grants for the node's key (hex of the encoded `Grant`).
    #[serde(default)]
    grants: Vec<String>,
    /// The project's writer approvals (hex of the encoded `WriterApproval`).
    #[serde(default)]
    approvals: Vec<String>,
}

/// Takes what the control point carries once it checked out against the pinned roots: the
/// newest grant for the node's key, written beside it (`<key>.grant`) when newer than the one
/// there, and the approvals, written to `approvals/`. Whether anything changed.
fn take_signed(dir: &Path, link: &Link, key_id: &str, signed: &Signed) -> Result<bool, BoxError> {
    use prost::Message;
    let mut changed = false;
    let newest = signed
        .grants
        .iter()
        .filter_map(|g| match hex::decode(g.trim()).map(|b| kernable_core::proto::Grant::decode(b.as_slice())) {
            Ok(Ok(grant)) => Some(grant),
            _ => {
                tracing::warn!("[node] a grant from the control point does not read: left out");
                None
            }
        })
        .filter(|g| {
            let ok = g.key_id == key_id && crate::grants::chain(g, &link.roots, None).is_ok();
            if !ok {
                tracing::warn!(key = %g.key_id, "[node] a grant from the control point is not this node's, or not signed by the pinned root: left out");
            }
            ok
        })
        .max_by_key(|g| g.issued_at);
    if let Some(grant) = newest {
        let path = dir.join(&link.key).with_extension("grant");
        let held = std::fs::read_to_string(&path)
            .ok()
            .and_then(|t| hex::decode(t.trim()).ok())
            .and_then(|b| kernable_core::proto::Grant::decode(b.as_slice()).ok());
        if held.is_none_or(|h| h.issued_at < grant.issued_at) {
            std::fs::write(&path, format!("{}\n", hex::encode(grant.encode_to_vec())))?;
            tracing::info!(file = %path.display(), "[node] a new grant for this node's key, signed by the root");
            changed = true;
        }
    }
    let mut wanted = std::collections::BTreeMap::new();
    for text in &signed.approvals {
        let approval = match hex::decode(text.trim())
            .map(|b| kernable_core::proto::WriterApproval::decode(b.as_slice()))
        {
            Ok(Ok(a)) => a,
            _ => {
                tracing::warn!("[node] an approval from the control point does not read: left out");
                continue;
            }
        };
        let signed_by_root = approval.root_sig.as_ref().is_some_and(|sig| {
            link.roots.contains(&sig.key_id)
                && kernable_core::signing::verify(
                    sig,
                    &kernable_core::signing::writer_approval_message(
                        &approval.copy,
                        &approval.kernel_key_id,
                        approval.issued_at,
                    ),
                )
                .is_ok()
        });
        if !signed_by_root || !crate::relay::valid_name(&approval.copy) {
            tracing::warn!(copy = %approval.copy, "[node] an approval from the control point is not signed by the pinned root: left out");
            continue;
        }
        // The newest approval per copy.
        let newer =
            wanted
                .get(&approval.copy)
                .is_none_or(|a: &kernable_core::proto::WriterApproval| {
                    a.issued_at < approval.issued_at
                });
        if newer {
            wanted.insert(approval.copy.clone(), approval);
        }
    }
    let approvals = dir.join(APPROVALS);
    std::fs::create_dir_all(&approvals)?;
    for (copy, approval) in &wanted {
        let path = approvals.join(format!("{copy}.approval"));
        let text = format!("{}\n", hex::encode(approval.encode_to_vec()));
        if std::fs::read_to_string(&path).ok().as_deref() != Some(text.as_str()) {
            std::fs::write(&path, text)?;
            tracing::info!(%copy, writer = %approval.kernel_key_id, "[node] a writer approval, signed by the root");
            changed = true;
        }
    }
    // An approval the control point no longer carries (another writer approved since) goes.
    for entry in std::fs::read_dir(&approvals)? {
        let path = entry?.path();
        let copy = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".approval"))
            .map(str::to_owned);
        if copy.is_some_and(|c| !wanted.contains_key(&c)) {
            std::fs::remove_file(&path)?;
            changed = true;
        }
    }
    Ok(changed)
}

/// Pulls what the project's root signed for this node once (`GET /v1/nodes/<id>/signed`).
pub fn pull_signed(dir: &Path) -> Result<bool, BoxError> {
    let Some(link) = link(dir)? else {
        return Ok(false);
    };
    if link.roots.is_empty() {
        return Ok(false);
    }
    let key = kernable_core::signing::read_signing_key(dir.join(&link.key))?;
    let (status, text) = call(
        &link,
        &key,
        "GET",
        &format!("/v1/nodes/{}/signed", link.node),
        b"",
    )?;
    match status {
        200 => {}
        404 => return Ok(false),
        _ => return Err(format!("the control point answered {status}: {text}").into()),
    }
    let signed: Signed = serde_json::from_str(&text)?;
    take_signed(
        dir,
        &link,
        &kernable_core::signing::key_id(&key.verifying_key()),
        &signed,
    )
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
    // A version names a directory under versions/, nothing outside it.
    if !s.kernel_version.is_empty()
        && (s.kernel_version.starts_with('.')
            || !s
                .kernel_version
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ".-+".contains(c)))
    {
        return Err(format!(
            "kernel version {:?}: letters, digits, '.', '-' and '+' only",
            s.kernel_version
        )
        .into());
    }
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
    if !settings.kernel_version.is_empty() && check(settings).is_err() {
        problems.push(format!(
            "kernel version {:?}: not a version",
            settings.kernel_version
        ));
    } else if !settings.kernel_version.is_empty() {
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

/// The IDs of the copy keys the node holds, by copy (`<copy>` → the key's ID, the first 8
/// bytes of its SHA-256, hex): the reader's (`reader-copy-key`) and the pusher's (`[push]
/// copy-key`), as its project file and settings name them. An ID names a key without revealing
/// it; the control point compares them, so a reader that lacks the pusher's key shows.
fn copy_keys(dir: &Path, project: &Path) -> BTreeMap<String, String> {
    let mut held = BTreeMap::new();
    let file: toml::Value = std::fs::read_to_string(project)
        .ok()
        .and_then(|t| toml::from_str(&t).ok())
        .unwrap_or(toml::Value::Table(Default::default()));
    let settings = held_settings(dir);
    let flags = &file["kernel"]["flags"];
    let reader_copy = settings
        .as_ref()
        .map(|s| s.copy.clone())
        .filter(|c| !c.is_empty())
        .or_else(|| {
            flags
                .get("reader-copy")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });
    let push = file.get("push");
    let push_copy = settings
        .as_ref()
        .and_then(|s| s.push.as_ref().map(|p| p.copy.clone()))
        .or_else(|| {
            push.and_then(|p| p.get("copy"))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        });
    let entries = [
        (
            reader_copy,
            flags.get("reader-copy-key").and_then(|v| v.as_str()),
        ),
        (
            push_copy,
            push.and_then(|p| p.get("copy-key"))
                .and_then(|v| v.as_str()),
        ),
    ];
    for (copy, file) in entries {
        let Some(copy) = copy else { continue };
        let path = dir.join(file.unwrap_or("copy.key"));
        if let Ok(key) = crate::seal::CopyKey::read(&path) {
            held.insert(copy, key.id());
        }
    }
    held
}

fn held_settings(dir: &Path) -> Option<Settings> {
    held(dir).ok().flatten()
}

/// What the node reports: the settings it runs, what it could not apply, its kernel's role.
async fn report(
    dir: &Path,
    project: &Path,
    admin: &Path,
    problems: &[String],
) -> Result<(), BoxError> {
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
        "copy_keys": copy_keys(dir, project),
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
    project: PathBuf,
    admin: PathBuf,
    problems: tokio::sync::watch::Receiver<Vec<String>>,
) {
    let mut pulls = tokio::time::interval(Duration::from_secs(5));
    let mut reported_at = tokio::time::Instant::now() - Duration::from_secs(60);
    loop {
        pulls.tick().await;
        let d = dir.clone();
        let s = dir.clone();
        match tokio::task::spawn_blocking(move || pull_signed(&s)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "[node] pulling what the root signed failed"),
            Err(e) => tracing::warn!(error = %e, "[node] pulling what the root signed failed"),
        }
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
            if let Err(e) = report(&dir, &project, &admin, &problems).await {
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
    fn a_node_reports_the_ids_of_the_copy_keys_it_holds() {
        let dir = std::env::temp_dir().join(format!("kernable-copy-ids-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let project = dir.join("project.toml");
        std::fs::write(
            &project,
            "[kernel]\nflags = { reader-copy = \"acme.shop.us\", reader-copy-key = \"us.key\" }\n\n[push]\nto = \"r\"\ncopy = \"acme.shop.eu\"\ncopy-key = \"copy.key\"\n",
        )
        .unwrap();
        crate::seal::CopyKey::create(&dir.join("copy.key")).unwrap();
        let ids = copy_keys(&dir, &project);
        assert_eq!(
            ids.get("acme.shop.eu"),
            Some(
                &crate::seal::CopyKey::read(&dir.join("copy.key"))
                    .unwrap()
                    .id()
            )
        );
        assert!(
            !ids.contains_key("acme.shop.us"),
            "no us.key on this machine"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_node_takes_only_what_its_pinned_root_signed_for_it() {
        use prost::Message;
        let dir = std::env::temp_dir().join(format!("kernable-signed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (root, stranger, own) = (
            kernable_core::signing::generate_key(),
            kernable_core::signing::generate_key(),
            kernable_core::signing::generate_key(),
        );
        let id = |k: &SigningKey| kernable_core::signing::key_id(&k.verifying_key());
        let link = Link {
            url: "https://control.example".into(),
            node: "n-1".into(),
            key: PathBuf::from(INSTANCE_KEY),
            settings_key: id(&root),
            roots: vec![id(&root)],
            ca: None,
        };
        let perms = || {
            vec![kernable_core::proto::Permission {
                action: "read".into(),
                types: vec!["*".into()],
            }]
        };
        let grant = |by: &SigningKey, to: &str, at: i64| {
            hex::encode(kernable_core::signing::grant(by, to, perms(), "", at).encode_to_vec())
        };
        let approval = |by: &SigningKey, copy: &str, writer: &str, at: i64| {
            hex::encode(
                kernable_core::proto::WriterApproval {
                    copy: copy.into(),
                    kernel_key_id: writer.into(),
                    issued_at: at,
                    root_sig: Some(kernable_core::signing::sign(
                        by,
                        &kernable_core::signing::writer_approval_message(copy, writer, at),
                    )),
                }
                .encode_to_vec(),
            )
        };
        let writer = id(&stranger);
        let signed = Signed {
            grants: vec![
                grant(&root, &id(&own), 5),
                grant(&stranger, &id(&own), 9), // not the root's: left out
                grant(&root, &writer, 9),       // another key's: left out
            ],
            approvals: vec![
                approval(&root, "acme.shop.eu", &writer, 3),
                approval(&stranger, "acme.shop.us", &writer, 3), // not the root's
            ],
        };
        assert!(take_signed(&dir, &link, &id(&own), &signed).unwrap());
        let held = std::fs::read_to_string(dir.join("instance.grant")).unwrap();
        let held =
            kernable_core::proto::Grant::decode(hex::decode(held.trim()).unwrap().as_slice())
                .unwrap();
        assert_eq!((held.key_id, held.issued_at), (id(&own), 5));
        assert!(dir.join("approvals/acme.shop.eu.approval").exists());
        assert!(!dir.join("approvals/acme.shop.us.approval").exists());
        // The same again changes nothing; an approval no longer carried goes.
        assert!(!take_signed(&dir, &link, &id(&own), &signed).unwrap());
        let fewer = Signed {
            grants: Vec::new(),
            approvals: Vec::new(),
        };
        assert!(take_signed(&dir, &link, &id(&own), &fewer).unwrap());
        assert!(!dir.join("approvals/acme.shop.eu.approval").exists());
        assert!(
            dir.join("instance.grant").exists(),
            "a grant is never taken back"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
    fn a_kernel_version_names_nothing_outside_versions() {
        let s = |v: &str| Settings {
            role: "writer".into(),
            kernel_version: v.into(),
            ..Default::default()
        };
        assert!(check(&s("1.2.0-rc.1+build")).is_ok());
        for bad in ["../../usr/bin", "..", "a/b", ".hidden"] {
            assert!(check(&s(bad)).is_err(), "{bad}");
        }
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
                "instance-key".to_owned(),
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
            flags["instance-key"].as_str(),
            Some("reader.key"),
            "keys stay the customer's"
        );
        assert_eq!(binary, None);
        assert_eq!(problems.len(), 2, "{problems:?}");
    }
}
