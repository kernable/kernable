"""Signed messages, as specified in proto/SIGNING.md."""

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


def _stream(m: Msg, c) -> Msg:
    return m.str(c.command_id).str(c.tenant_id).str(c.aggregate_type).str(c.aggregate_id)


def command_message(c) -> bytes:
    return bytes(_stream(Msg("kernable.v1.command"), c).str(c.command_type).bytes(c.payload).buf)


def result_message(c, expected_version: int, events) -> bytes:
    m = _stream(Msg("kernable.v1.result"), c).u64(expected_version).u64(len(events))
    for e in events:
        m.str(e.event_type).bytes(e.payload)
    return bytes(m.buf)


def rejection_message(command_id: str, reason) -> bytes:
    return bytes(Msg("kernable.v1.rejection").str(command_id).str(reason.code).str(reason.message).buf)


def challenge_message(nonce: bytes) -> bytes:
    return bytes(Msg("kernable.v1.challenge").bytes(nonce).buf)


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
