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

    def context(self, c) -> "Msg":
        """A RequestContext message; an unset one encodes like an empty one."""
        self.str(c.sender_tenant).str(c.sender_identity).str(c.sender_account).str(c.sender_session)
        self.str(c.on_behalf_of).str(c.workspace)
        if c.HasField("expected_version"):
            self.u64(1).u64(c.expected_version)
        else:
            self.u64(0)
        self.str(c.correlation_id).str(c.causation_id).str(c.trace_id)
        self.u64(1 if c.bypass_authorization else 0).u64(len(c.attributes))
        for a in c.attributes:
            self.str(a.key).str(a.value)
        return self


def _stream(m: Msg, c) -> Msg:
    return m.str(c.command_id).str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id)


def command_message(c) -> bytes:
    return bytes(_stream(Msg("kernable.v1.command"), c).str(c.command_type).bytes(c.payload).context(c.context).buf)


def result_message(c, expected_version: int, events) -> bytes:
    m = _stream(Msg("kernable.v1.result"), c).u64(expected_version).u64(len(events))
    for e in events:
        m.str(e.event_type).u64(e.schema).bytes(e.payload)
    return bytes(m.buf)


def snapshot_message(c, version: int, schema: str, state: bytes) -> bytes:
    return bytes(Msg("kernable.v1.snapshot").str(c.tenant_id).str(c.domain).str(c.aggregate_type).str(c.aggregate_id)
                 .u64(version).str(schema).bytes(state).buf)


def query_message(q) -> bytes:
    m = Msg("kernable.v1.query").str(q.query_id).str(q.tenant_id).str(q.domain).str(q.query_type).bytes(q.payload)
    m.context(q.context).u64(q.min_position)
    m.buf += struct.pack(">q", q.issued_at)
    return bytes(m.buf)


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
    """An Ed25519 key; its ID is the hex-encoded public key."""

    def __init__(self, seed_hex: str):
        self._key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex.strip()))
        self.id = self._key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()

    @staticmethod
    def from_file(path: str) -> "Key":
        with open(path) as f:
            return Key(f.read())

    def sign(self, message: bytes) -> pb.Signature:
        return pb.Signature(key_id=self.id, sig=self._key.sign(message))
