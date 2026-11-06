"""Access tokens: short-lived, signed with Ed25519 (JWT, EdDSA), checked by the API
itself without asking the kernel. The middleware still refuses an ended session at once."""

import base64
import json
import time
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from ..shared.constants import ACCESS_TOKEN_MINUTES


@dataclass
class Claims:
    #: The account.
    sub: str
    #: The session.
    sid: str
    #: The factors the session had when the token was issued.
    fac: list[str]
    iat: int = 0
    exp: int = 0


@dataclass
class SigningKeys:
    private_key: Ed25519PrivateKey | None
    public_key: Ed25519PublicKey


def new_signing_keys() -> SigningKeys:
    """New keys, e.g. for development and tests."""
    key = Ed25519PrivateKey.generate()
    return SigningKeys(key, key.public_key())


def signing_keys_from(path: str) -> SigningKeys:
    """Keys from a PEM file holding the private key (PKCS #8)."""
    key = load_pem_private_key(Path(path).read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError(f"{path} holds no Ed25519 key")
    return SigningKeys(key, key.public_key())


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


# The same bytes as the TypeScript kit's header, so either kit checks the other's tokens.
HEADER = _b64(json.dumps({"alg": "EdDSA", "typ": "JWT"}, separators=(",", ":")).encode())


def issue_access_token(keys: SigningKeys, sub: str, sid: str, fac: list[str], now_ms: int | None = None) -> str:
    iat = int((now_ms if now_ms is not None else time.time() * 1000) // 1000)
    body = _b64(json.dumps({"sub": sub, "sid": sid, "fac": list(fac), "iat": iat, "exp": iat + ACCESS_TOKEN_MINUTES * 60},
                           separators=(",", ":")).encode())
    signature = _b64(keys.private_key.sign(f"{HEADER}.{body}".encode()))
    return f"{HEADER}.{body}.{signature}"


def check_access_token(keys: SigningKeys, token: str, now_ms: int | None = None) -> Claims | None:
    """The claims of a valid, unexpired token, else None."""
    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 3 or parts[0] != HEADER:
        return None
    try:
        keys.public_key.verify(_unb64(parts[2]), f"{parts[0]}.{parts[1]}".encode())
        c = json.loads(_unb64(parts[1]))
    except (InvalidSignature, ValueError):
        return None
    now = now_ms if now_ms is not None else time.time() * 1000
    if not isinstance(c.get("sub"), str) or not isinstance(c.get("sid"), str) or not (c.get("exp", 0) * 1000 > now):
        return None
    return Claims(sub=c["sub"], sid=c["sid"], fac=list(c.get("fac") or []), iat=c.get("iat", 0), exp=c["exp"])
