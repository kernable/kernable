"""A software authenticator for tests: it answers WebAuthn the way a browser with a platform
authenticator does (P-256, attestation "none", user verified), without a browser."""

import base64
import hashlib
import json
import os
import struct

import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _sha256(raw: bytes) -> bytes:
    return hashlib.sha256(raw).digest()


class Authenticator:
    def __init__(self, rp_id: str, origin: str, user_id: str) -> None:
        self.rp_id, self.origin, self.user_id = rp_id, origin, user_id
        self.credential_id = os.urandom(16)
        self.counter = 0
        self.key = ec.generate_private_key(ec.SECP256R1())
        n = self.key.public_key().public_numbers()
        # COSE_Key: kty EC2 (2), alg ES256 (-7), crv P-256 (1), x, y.
        self.cose = cbor2.dumps({1: 2, 3: -7, -1: 1, -2: n.x.to_bytes(32, "big"), -3: n.y.to_bytes(32, "big")})

    def _auth_data(self, flags: int, attested: bytes = b"") -> bytes:
        return _sha256(self.rp_id.encode()) + bytes([flags]) + struct.pack(">I", self.counter) + attested

    def _client_data(self, type_: str, challenge: str, origin: str | None = None) -> bytes:
        return json.dumps({"type": type_, "challenge": challenge, "origin": origin or self.origin, "crossOrigin": False}).encode()

    def register(self, challenge: str) -> dict:
        """The browser's answer to registration options with this challenge."""
        attested = bytes(16) + struct.pack(">H", len(self.credential_id)) + self.credential_id + self.cose
        # Flags: user present (0x01), user verified (0x04), attested credential data (0x40).
        attestation = cbor2.dumps({"fmt": "none", "attStmt": {}, "authData": self._auth_data(0x45, attested)})
        return {"id": b64(self.credential_id), "rawId": b64(self.credential_id), "type": "public-key", "clientExtensionResults": {},
                "response": {"clientDataJSON": b64(self._client_data("webauthn.create", challenge)), "attestationObject": b64(attestation),
                             "transports": ["internal"]}}

    def assert_(self, challenge: str, origin: str | None = None) -> dict:
        """The browser's answer to sign-in options with this challenge, from `origin` (default the right one)."""
        self.counter += 1
        auth_data = self._auth_data(0x05)
        client_data = self._client_data("webauthn.get", challenge, origin)
        signature = self.key.sign(auth_data + _sha256(client_data), ec.ECDSA(hashes.SHA256()))
        return {"id": b64(self.credential_id), "rawId": b64(self.credential_id), "type": "public-key", "clientExtensionResults": {},
                "response": {"clientDataJSON": b64(client_data), "authenticatorData": b64(auth_data), "signature": b64(signature),
                             "userHandle": b64(self.user_id.encode())}}
