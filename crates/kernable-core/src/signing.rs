//! Signed messages and the event hash chain, as specified in `proto/SIGNING.md`.
//!
//! A key ID is the hex-encoded Ed25519 public key, so any signature can be checked
//! without a key registry; *authorization* of a key is up to the kernel.

use std::{fmt, fs, io, path::Path};

use ed25519_dalek::{Signature as Ed25519Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::proto::{
    CacheRequest, Command, DataRequest, Event, Grant, Membership, NewEvent, Permission,
    QueryRequest, Rejected, RequestContext, Requirement, Signature, UniqueKey,
};

/// Hash that precedes the first event of the chain.
pub const GENESIS: [u8; 32] = [0; 32];

#[derive(Debug, PartialEq)]
pub enum SignError {
    InvalidKey,
    InvalidSignature,
}

impl fmt::Display for SignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignError::InvalidKey => write!(f, "invalid key"),
            SignError::InvalidSignature => write!(f, "invalid signature"),
        }
    }
}

impl std::error::Error for SignError {}

/// Length-prefixed message builder: every field is `u32 BE length || bytes`, integers are 8 bytes BE.
struct Msg(Vec<u8>);

impl Msg {
    fn new(tag: &str) -> Self {
        Msg(Vec::new()).str(tag)
    }

    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend((b.len() as u32).to_be_bytes());
        self.0.extend(b);
        self
    }

    fn str(self, s: &str) -> Self {
        self.bytes(s.as_bytes())
    }

    fn u64(mut self, v: u64) -> Self {
        self.0.extend(v.to_be_bytes());
        self
    }

    fn i64(mut self, v: i64) -> Self {
        self.0.extend(v.to_be_bytes());
        self
    }

    /// A request context; an absent one encodes like an empty one.
    fn context(self, c: Option<&RequestContext>) -> Self {
        let default = RequestContext::default();
        let c = c.unwrap_or(&default);
        let mut m = self
            .str(&c.sender_tenant)
            .str(&c.sender_identity)
            .str(&c.sender_account)
            .str(&c.sender_session)
            .str(&c.on_behalf_of);
        m = match c.expected_version {
            Some(v) => m.u64(1).u64(v),
            None => m.u64(0),
        };
        m = m
            .str(&c.correlation_id)
            .str(&c.causation_id)
            .str(&c.trace_id)
            .u64(c.bypass_authorization as u64)
            .u64(c.attributes.len() as u64);
        for a in &c.attributes {
            m = m.str(&a.key).str(&a.value);
        }
        m
    }

    /// Vault references, appended only when there are any, so that signatures and hashes
    /// from before the vault stay valid.
    fn refs(self, refs: &[String]) -> Self {
        if refs.is_empty() {
            return self;
        }
        let mut m = self.u64(refs.len() as u64);
        for r in refs {
            m = m.str(r);
        }
        m
    }
}

/// The reference of a value kept in the vault: it commits to the value, its subject and kind,
/// and a random salt, so it reveals nothing about an erased value.
pub fn vault_ref(salt: &[u8], subject: &str, secret: bool, value: &[u8]) -> String {
    let msg = Msg::new("kernable.v1.vault")
        .bytes(salt)
        .str(subject)
        .u64(secret as u64)
        .bytes(value);
    hex::encode(Sha256::digest(msg.0))
}

/// What a client signs when submitting a command.
pub fn command_message(cmd: &Command) -> Vec<u8> {
    Msg::new("kernable.v1.command")
        .str(&cmd.command_id)
        .str(&cmd.tenant_id)
        .str(&cmd.domain)
        .str(&cmd.aggregate_type)
        .str(&cmd.aggregate_id)
        .str(&cmd.command_type)
        .bytes(&cmd.payload)
        .context(cmd.context.as_ref())
        .refs(&cmd.vault_refs)
        .0
}

/// What a client signs when asking a query.
pub fn query_message(q: &QueryRequest) -> Vec<u8> {
    Msg::new("kernable.v1.query")
        .str(&q.query_id)
        .str(&q.tenant_id)
        .str(&q.domain)
        .str(&q.query_type)
        .bytes(&q.payload)
        .context(q.context.as_ref())
        .u64(q.min_position)
        .i64(q.issued_at)
        .0
}

/// What a worker signs when returning events for `cmd` based on `expected_version`.
pub fn result_message(cmd: &Command, expected_version: u64, events: &[NewEvent]) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.result")
        .str(&cmd.command_id)
        .str(&cmd.tenant_id)
        .str(&cmd.domain)
        .str(&cmd.aggregate_type)
        .str(&cmd.aggregate_id)
        .u64(expected_version)
        .u64(events.len() as u64);
    for e in events {
        m = m.str(&e.event_type).u64(e.schema.into()).bytes(&e.payload);
    }
    // Vault fields, appended only when an event has any (see `Msg::refs`).
    if events
        .iter()
        .any(|e| !e.vault_refs.is_empty() || !e.erases.is_empty())
    {
        for e in events {
            m = m.u64(e.vault_refs.len() as u64);
            for r in &e.vault_refs {
                m = m.str(r);
            }
            m = m.str(&e.erases);
        }
    }
    m.0
}

/// What a worker signs for the unique keys, facts and requirements of its result for `cmd`
/// based on `expected_version`. A message of its own, not part of [`result_message`]: every
/// event stores the result signature, and `kernable verify` checks it against the stored events
/// alone, which never hold value hashes or requirements.
pub fn index_message(
    cmd: &Command,
    expected_version: u64,
    keys: &[UniqueKey],
    facts: &[String],
    requires: &[Requirement],
) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.index")
        .str(&cmd.command_id)
        .str(&cmd.tenant_id)
        .str(&cmd.domain)
        .str(&cmd.aggregate_type)
        .str(&cmd.aggregate_id)
        .u64(expected_version)
        .u64(keys.len() as u64);
    for k in keys {
        m = m
            .str(&k.name)
            .u64(k.global as u64)
            .bytes(&k.value_hash)
            .str(&k.subject);
    }
    m = m.u64(facts.len() as u64);
    for f in facts {
        m = m.str(f);
    }
    m = m.u64(requires.len() as u64);
    for r in requires {
        m = m
            .str(&r.domain)
            .str(&r.aggregate_type)
            .str(&r.aggregate_id)
            .str(&r.fact)
            .str(&r.key)
            .bytes(&r.value_hash)
            .u64(u64::from(r.absent));
    }
    m.0
}

/// The identity of a unique key in the kernel's index. The kernel computes it from the
/// command's own tenant (none for a global key), domain and aggregate type, so a worker cannot
/// place a key in another domain's namespace.
pub fn unique_key_id(cmd: &Command, key: &UniqueKey) -> [u8; 32] {
    let tenant = if key.global { "" } else { &cmd.tenant_id };
    unique_key_id_of(
        tenant,
        &cmd.domain,
        &cmd.aggregate_type,
        &key.name,
        &key.value_hash,
    )
}

/// The identity of the unique key `name` with the value hash `value_hash` of aggregates of
/// `domain` and `aggregate_type`, in `tenant` (empty for a global key); see [`unique_key_id`].
pub fn unique_key_id_of(
    tenant: &str,
    domain: &str,
    aggregate_type: &str,
    name: &str,
    value_hash: &[u8],
) -> [u8; 32] {
    let msg = Msg::new("kernable.v1.key")
        .str(tenant)
        .str(domain)
        .str(aggregate_type)
        .str(name)
        .bytes(value_hash);
    Sha256::digest(msg.0).into()
}

/// What a worker signs for a snapshot of `stream`'s aggregate (the stream fields of a command:
/// tenant, domain, aggregate type and ID).
pub fn snapshot_message(stream: &Command, version: u64, schema: &str, state: &[u8]) -> Vec<u8> {
    Msg::new("kernable.v1.snapshot")
        .str(&stream.tenant_id)
        .str(&stream.domain)
        .str(&stream.aggregate_type)
        .str(&stream.aggregate_id)
        .u64(version)
        .str(schema)
        .bytes(state)
        .0
}

/// What a worker signs when rejecting a command.
pub fn rejection_message(command_id: &str, reason: &Rejected) -> Vec<u8> {
    Msg::new("kernable.v1.rejection")
        .str(command_id)
        .str(&reason.code)
        .str(&reason.message)
        .0
}

/// What a worker signs to prove it holds its key when registering.
pub fn challenge_message(nonce: &[u8]) -> Vec<u8> {
    Msg::new("kernable.v1.challenge").bytes(nonce).0
}

/// What a subscriber signs to open an event stream.
pub fn subscribe_message(after_position: u64, event_types: &[String], issued_at: i64) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.subscribe")
        .u64(after_position)
        .i64(issued_at)
        .u64(event_types.len() as u64);
    for t in event_types {
        m = m.str(t);
    }
    m.0
}

/// What a client signs to put, get or delete a file (`op`: `put`, `get`, `delete`). The data is
/// signed by its SHA-256 hash; get and delete sign the hash of nothing.
pub fn data_message(op: &str, r: &DataRequest) -> Vec<u8> {
    Msg::new(&format!("kernable.v1.data.{op}"))
        .str(&r.tenant_id)
        .str(&r.domain)
        .str(&r.key)
        .str(&r.content_type)
        .bytes(&Sha256::digest(&r.data))
        .i64(r.issued_at)
        .context(r.context.as_ref())
        .0
}

/// What a client signs to set, get or delete a cache value (`op`: `set`, `get`, `delete`), with
/// the value's SHA-256 hash as for data.
pub fn cache_message(op: &str, r: &CacheRequest) -> Vec<u8> {
    Msg::new(&format!("kernable.v1.cache.{op}"))
        .str(&r.tenant_id)
        .str(&r.domain)
        .str(&r.key)
        .bytes(&Sha256::digest(&r.value))
        .u64(r.ttl_ms)
        .i64(r.issued_at)
        .context(r.context.as_ref())
        .0
}

/// What the kernel signs to revoke `key_id` from `since` on (both ms since epoch).
pub fn revocation_message(key_id: &str, since: i64, created_at: i64) -> Vec<u8> {
    Msg::new("kernable.v1.revocation")
        .str(key_id)
        .i64(since)
        .i64(created_at)
        .0
}

/// What a root key signs to grant `grant.key_id` its permissions.
pub fn grant_message(grant: &Grant) -> Vec<u8> {
    let mut m = Msg::new("kernable.v1.grant")
        .str(&grant.key_id)
        .str(&grant.succeeds)
        .i64(grant.issued_at)
        .u64(grant.permissions.len() as u64);
    for p in &grant.permissions {
        m = m.str(&p.action).u64(p.types.len() as u64);
        for t in &p.types {
            m = m.str(t);
        }
    }
    m.0
}

/// A grant for `key_id`, signed by `root`.
pub fn grant(
    root: &SigningKey,
    key_id: &str,
    permissions: Vec<Permission>,
    succeeds: &str,
    issued_at: i64,
) -> Grant {
    let mut grant = Grant {
        key_id: key_id.to_owned(),
        permissions,
        succeeds: succeeds.to_owned(),
        issued_at,
        root_sig: None,
    };
    grant.root_sig = Some(sign(root, &grant_message(&grant)));
    grant
}

/// What a key granted `manage` signs to apply a service's membership.
pub fn membership_message(m: &Membership) -> Vec<u8> {
    let mut msg = Msg::new("kernable.v1.membership").str(&m.service);
    for list in [&m.workers, &m.listeners, &m.middlewares] {
        msg = msg.u64(list.len() as u64);
        for entry in list {
            msg = msg.str(entry);
        }
    }
    msg = msg.u64(m.metadata.len() as u64);
    for a in &m.metadata {
        msg = msg.str(&a.key).str(&a.value);
    }
    msg.i64(m.issued_at).0
}

/// What a key granted `manage` signs to deliver a listener's parked events again, or dismiss
/// them; no `positions` means all of them.
pub fn retry_message(listener: &str, positions: &[u64], dismiss: bool, issued_at: i64) -> Vec<u8> {
    let mut msg = Msg::new("kernable.v1.retry")
        .str(listener)
        .u64(positions.len() as u64);
    for p in positions {
        msg = msg.u64(*p);
    }
    msg.u64(u64::from(dismiss)).i64(issued_at).0
}

/// What a root key signs to revoke `key_id` from `since` on (both ms since epoch).
pub fn revoke_message(key_id: &str, since: i64, issued_at: i64) -> Vec<u8> {
    Msg::new("kernable.v1.revoke")
        .str(key_id)
        .i64(since)
        .i64(issued_at)
        .0
}

/// What an operator's key signs to promote a standby to the primary (`kernable promote`).
pub fn promote_message(issued_at: i64) -> Vec<u8> {
    Msg::new("kernable.v1.promote").i64(issued_at).0
}

/// What a kernel of a pair signs to ask the witness (`WitnessRequest`).
pub fn witness_message(
    op: i32,
    term: u64,
    address: &str,
    standby: &str,
    issued_at: i64,
) -> Vec<u8> {
    Msg::new("kernable.v1.witness")
        .u64(op as u64)
        .u64(term)
        .str(address)
        .str(standby)
        .i64(issued_at)
        .0
}

/// What the kernel signs to anchor the chain at `global_position`.
pub fn checkpoint_message(global_position: u64, chain_hash: &[u8], created_at: i64) -> Vec<u8> {
    Msg::new("kernable.v1.checkpoint")
        .u64(global_position)
        .bytes(chain_hash)
        .i64(created_at)
        .0
}

/// The `n`-th ID a command handler derives while handling `command_id` (`ctx.id()` in the
/// SDKs): a UUID (version 8) from SHA-256 over a tagged message, so it is the same in every
/// language and every time the command runs again.
pub fn derived_id(command_id: &str, n: u64) -> String {
    let digest = Sha256::digest(Msg::new("kernable.v1.id").str(command_id).u64(n).0);
    let mut b: [u8; 16] = digest[..16].try_into().expect("32-byte digest");
    b[6] = (b[6] & 0x0f) | 0x80; // version 8
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 9562 variant
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// Chain hash of `event`, linked to the hash of the previous event (`GENESIS` for the first).
pub fn event_hash(prev: &[u8], event: &Event) -> [u8; 32] {
    let sig = event.producer_sig.clone().unwrap_or_default();
    let msg = Msg::new("kernable.v1.event")
        .bytes(prev)
        .u64(event.global_position)
        .str(&event.tenant_id)
        .str(&event.domain)
        .str(&event.aggregate_type)
        .str(&event.aggregate_id)
        .u64(event.version)
        .str(&event.event_type)
        .u64(event.schema.into())
        .bytes(&event.payload)
        .str(&event.command_id)
        .i64(event.created_at)
        .str(&sig.key_id)
        .bytes(&sig.sig)
        .context(event.context.as_ref());
    // Vault fields, appended only when set (see `Msg::refs`).
    let msg = if event.vault_refs.is_empty() && event.erases.is_empty() {
        msg
    } else {
        let mut m = msg.u64(event.vault_refs.len() as u64);
        for r in &event.vault_refs {
            m = m.str(r);
        }
        m.str(&event.erases)
    };
    Sha256::digest(msg.0).into()
}

pub fn key_id(key: &VerifyingKey) -> String {
    hex::encode(key.as_bytes())
}

pub fn generate_key() -> SigningKey {
    let mut seed = Zeroizing::new([0u8; 32]);
    getrandom::fill(seed.as_mut()).expect("OS randomness unavailable");
    SigningKey::from_bytes(&seed)
}

pub fn sign(key: &SigningKey, msg: &[u8]) -> Signature {
    Signature {
        key_id: key_id(&key.verifying_key()),
        sig: key.sign(msg).to_bytes().to_vec(),
    }
}

pub fn verify(sig: &Signature, msg: &[u8]) -> Result<(), SignError> {
    let key = verifying_key_from_hex(&sig.key_id)?;
    let sig = Ed25519Signature::from_slice(&sig.sig).map_err(|_| SignError::InvalidSignature)?;
    key.verify_strict(msg, &sig)
        .map_err(|_| SignError::InvalidSignature)
}

pub fn verifying_key_from_hex(s: &str) -> Result<VerifyingKey, SignError> {
    let bytes: [u8; 32] = hex::decode(s.trim())
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or(SignError::InvalidKey)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| SignError::InvalidKey)
}

pub fn signing_key_from_hex(s: &str) -> Result<SigningKey, SignError> {
    let seed = secret_from_hex(s)?;
    Ok(SigningKey::from_bytes(&seed))
}

/// 32 secret bytes from hex, decoded in place: no copy of the secret is left behind.
fn secret_from_hex(s: &str) -> Result<Zeroizing<[u8; 32]>, SignError> {
    let mut secret = Zeroizing::new([0u8; 32]);
    hex::decode_to_slice(s.trim(), secret.as_mut()).map_err(|_| SignError::InvalidKey)?;
    Ok(secret)
}

/// Reads a file of 32 secret bytes in hex, such as a `.key` file or a vault key. The secret
/// and the text it was read from are wiped from memory when dropped.
pub fn read_secret(path: impl AsRef<Path>) -> io::Result<Zeroizing<[u8; 32]>> {
    secret_from_hex(&Zeroizing::new(fs::read_to_string(path)?)).map_err(invalid_data)
}

/// Reads a `.key` file (hex-encoded 32-byte seed).
pub fn read_signing_key(path: impl AsRef<Path>) -> io::Result<SigningKey> {
    let seed = read_secret(path)?;
    Ok(SigningKey::from_bytes(&seed))
}

/// Reads a `.pub` file (hex-encoded public key, i.e. the key ID).
pub fn read_verifying_key(path: impl AsRef<Path>) -> io::Result<VerifyingKey> {
    verifying_key_from_hex(&fs::read_to_string(path)?).map_err(invalid_data)
}

fn invalid_data(e: SignError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_keys_are_wiped_when_dropped() {
        fn wiped<T: zeroize::ZeroizeOnDrop>() {}
        wiped::<SigningKey>();
        wiped::<Zeroizing<[u8; 32]>>();
    }

    #[test]
    fn key_files_read_back_as_written() {
        let key = generate_key();
        let dir = std::env::temp_dir().join(format!("kernable-keys-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("k.key"), hex::encode(key.as_bytes())).unwrap();
        fs::write(
            dir.join("k.pub"),
            format!("{}\n", key_id(&key.verifying_key())),
        )
        .unwrap();
        assert_eq!(
            read_signing_key(dir.join("k.key")).unwrap().as_bytes(),
            key.as_bytes()
        );
        assert_eq!(
            read_verifying_key(dir.join("k.pub")).unwrap(),
            key.verifying_key()
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_secret_key_is_exactly_32_bytes_in_hex() {
        let hex = "11".repeat(32);
        let key = signing_key_from_hex(&format!("  {hex}\n")).unwrap();
        assert_eq!(key.as_bytes(), &[0x11; 32]);
        for bad in [
            &hex[..62],
            &format!("{hex}11"),
            &format!("{}x", &hex[..63]),
            "",
        ] {
            assert_eq!(
                signing_key_from_hex(bad).err(),
                Some(SignError::InvalidKey),
                "{bad}"
            );
        }
    }
}
