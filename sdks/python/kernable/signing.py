"""Signed messages, as specified in proto/SIGNING.md."""

import hashlib
import struct

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from kernable.v1 import kernable_pb2 as pb


class Msg:
    """Length-prefixed fields: `u32 BE length || bytes`; integers are 8 bytes BE."""

    def __init__(self, tag: str):
        self.buf = bytearray()
        self.str(tag)

    def bytes(self, b: bytes) -> "Msg":
        self.buf += struct.pack(">I", len(b)) + b
        return self

    def str(self, s: str) -> "Msg":
        return self.bytes(s.encode())

    def u64(self, n: int) -> "Msg":
        self.buf += struct.pack(">Q", n)
        return self

    def i64(self, n: int) -> "Msg":
        self.buf += struct.pack(">q", n)
        return self

    def context(self, c) -> "Msg":
        """A RequestContext message; an unset one encodes like an empty one."""
        self.str(c.sender_tenant).str(c.sender_identity).str(c.sender_account).str(c.sender_session)
        self.str(c.on_behalf_of)
        if c.HasField("expected_version"):
            self.u64(1).u64(c.expected_version)
        else:
            self.u64(0)
        self.str(c.correlation_id).str(c.causation_id).str(c.trace_id)
        self.u64(1 if c.bypass_authorization else 0).u64(len(c.attributes))
        for a in c.attributes:
            self.str(a.key).str(a.value)
        return self

    def refs(self, refs) -> "Msg":
        """Vault references, appended only when there are any (see SIGNING.md)."""
        if refs:
            self.u64(len(refs))
            for r in refs:
                self.str(r)
        return self


def _stream(m: Msg, c) -> Msg:
    return m.str(c.command_id).str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id)


def command_message(c) -> bytes:
    m = _stream(Msg("kernable.v1.command"), c).str(c.command_type).bytes(c.payload).context(c.context)
    return bytes(m.refs(c.vault_refs).buf)


def vault_ref(salt: bytes, subject: str, secret: bool, value: bytes) -> str:
    """The reference of a value kept in the vault: it commits to salt, subject, kind and value."""
    m = Msg("kernable.v1.vault").bytes(salt).str(subject).u64(1 if secret else 0).bytes(value)
    return hashlib.sha256(bytes(m.buf)).hexdigest()


def result_message(c, expected_version: int, events) -> bytes:
    m = _stream(Msg("kernable.v1.result"), c).u64(expected_version).u64(len(events))
    for e in events:
        m.str(e.event_type).u64(e.schema).bytes(e.payload)
    if any(e.vault_refs or e.erases for e in events):  # vault fields, appended only when set
        for e in events:
            m.u64(len(e.vault_refs))
            for r in e.vault_refs:
                m.str(r)
            m.str(e.erases)
    return bytes(m.buf)


def index_message(c, expected_version: int, keys, facts, requires) -> bytes:
    """What a worker signs for the unique keys, facts and requirements of its result
    (`Result.index_sig`): a message of its own, apart from `result_message`, which every event
    stores and `kernable verify` checks against the events alone."""
    m = _stream(Msg("kernable.v1.index"), c).u64(expected_version).u64(len(keys))
    for k in keys:
        # `global` is a Python keyword: the protobuf field is read with getattr.
        m.str(k.name).u64(1 if getattr(k, "global") else 0).bytes(k.value_hash).str(k.subject)
    m.u64(len(facts))
    for f in facts:
        m.str(f)
    m.u64(len(requires))
    for r in requires:
        m.str(r.domain).str(r.aggregate_type).str(r.aggregate_id).str(r.fact)
        m.str(r.key).bytes(r.value_hash).u64(1 if r.absent else 0)
    return bytes(m.buf)


def snapshot_message(c, version: int, schema: str, state: bytes) -> bytes:
    return bytes(Msg("kernable.v1.snapshot").str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id)
                 .u64(version).str(schema).bytes(state).buf)


def query_message(q) -> bytes:
    m = Msg("kernable.v1.query").str(q.query_id).str(q.tenant_id).str(q.domain).str(q.query_type).bytes(q.payload)
    m = m.context(q.context).u64(q.min_position).i64(q.issued_at)
    # Each only when set, so a query without them signs as before.
    if q.wait_only:
        m = m.str("wait_only")
    if q.HasField("target"):
        m = m.str("target").str(q.target.aggregate_type).str(q.target.aggregate_id)
    return bytes(m.buf)


def data_message(op: str, r) -> bytes:
    """What a client signs to put, get, delete or list files (`op`: `put`, `get`, `delete`,
    `list`); the data is signed as its SHA-256 hash, followed by the request context (an unset
    one encodes like an empty one), and for a listing its page (`after`)."""
    msg = (Msg(f"kernable.v1.data.{op}").str(r.tenant_id).str(r.domain).str(r.key).str(r.content_type)
           .bytes(hashlib.sha256(r.data).digest()).i64(r.issued_at).context(r.context))
    if op == "list":
        msg = msg.str(r.after)
    return bytes(msg.buf)


def cache_message(op: str, r) -> bytes:
    """What a client signs to set, get or delete a cache value (`op`: `set`, `get`, `delete`), with
    the value's SHA-256 hash and the request context as for data."""
    return bytes(Msg(f"kernable.v1.cache.{op}").str(r.tenant_id).str(r.domain).str(r.key)
                 .bytes(hashlib.sha256(r.value).digest()).u64(r.ttl_ms).i64(r.issued_at).context(r.context).buf)


def membership_message(m) -> bytes:
    """What a key granted `manage` signs to apply a service's membership (a `Membership`)."""
    msg = Msg("kernable.v1.membership").str(m.service)
    for entries in (m.workers, m.listeners, m.middlewares):
        msg.u64(len(entries))
        for e in entries:
            msg.str(e)
    msg.u64(len(m.metadata))
    for a in m.metadata:
        msg.str(a.key).str(a.value)
    return bytes(msg.i64(m.issued_at).buf)


def rejection_message(command_id: str, reason) -> bytes:
    return bytes(Msg("kernable.v1.rejection").str(command_id).str(reason.code).str(reason.message).buf)


def challenge_message(nonce: bytes) -> bytes:
    return bytes(Msg("kernable.v1.challenge").bytes(nonce).buf)


def derived_id(command_id: str, n: int) -> str:
    """The `n`-th ID derived for a command (`ctx.id()`): a version 8 UUID, see SIGNING.md."""
    b = bytearray(hashlib.sha256(bytes(Msg("kernable.v1.id").str(command_id).u64(n).buf)).digest()[:16])
    b[6] = (b[6] & 0x0F) | 0x80
    b[8] = (b[8] & 0x3F) | 0x80
    h = b.hex()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


class Key:
    """An Ed25519 key; its ID (`id`) is the hex-encoded public key. `Kernable` reads its own
    from the `.key` file (`kernable keys new`); you rarely need this class directly."""

    def __init__(self, seed_hex: str):
        """The key with this hex-encoded 32-byte seed, the content of a `.key` file."""
        self._key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex.strip()))
        self.id = self._key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()

    @staticmethod
    def from_file(path: str) -> "Key":
        """The key in the `.key` file at `path`."""
        with open(path) as f:
            return Key(f.read())

    def sign(self, message: bytes) -> pb.Signature:
        """Signs `message` (the signed bytes of a request, see Signatures) with this key."""
        return pb.Signature(key_id=self.id, sig=self._key.sign(message))
