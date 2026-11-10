"""Tokens, hashes, passwords and TOTP: the same as the TypeScript kit's."""

import base64
import hashlib
import hmac
import re
import secrets
import struct

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

# argon2id with OWASP's parameters (19 MiB, 2 iterations, 1 lane): hashes the TypeScript kit checks, and back.
_hasher = PasswordHasher(memory_cost=19 * 1024, time_cost=2, parallelism=1)


def random_secret(size: int = 32) -> str:
    """A new random secret: `size` bytes, base64url without padding."""
    return base64.urlsafe_b64encode(secrets.token_bytes(size)).rstrip(b"=").decode()


def sha256(value: str) -> str:
    """SHA-256 in hex: how high-entropy tokens are stored (refresh tokens, API keys, recovery codes)."""
    return hashlib.sha256(value.encode()).hexdigest()


def equal_secrets(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode(), b.encode())


def token(a: str, b: object, c: str) -> str:
    """`<aggregateId>.<tokenId>.<secret>` (emailed tokens), `<sessionId>.<generation>.<secret>` (refresh tokens)."""
    return f"{a}.{b}.{c}"


def parse_token(value: object) -> tuple[str, str, str] | None:
    if not isinstance(value, str):
        return None
    parts = value.split(".")
    return (parts[0], parts[1], parts[2]) if len(parts) == 3 and all(parts) else None


def invitation_token(tenant: str, invitation_id: str, token_id: str, secret: str) -> str:
    """An invitation's token names its tenant too: four parts."""
    return ".".join([tenant, invitation_id, token_id, secret])


def parse_invitation_token(value: object) -> dict | None:
    if not isinstance(value, str):
        return None
    p = value.split(".")
    return {"tenant": p[0], "invitation_id": p[1], "token_id": p[2], "secret": p[3]} if len(p) == 4 and all(p) else None


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def check_password(stored: str | None, password: object) -> bool:
    if not stored or not isinstance(password, str):
        return False
    try:
        return _hasher.verify(stored, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


# The characters the kits take for space: controls, spaces, line and paragraph separators and the
# byte order mark. One explicit set, the same in every language's kit (each language's own idea of
# whitespace differs).
_SPACE = "\\x00-\\x20\\x7f\\x85\\xa0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000\\ufeff"
_SPACE_ANY = re.compile(f"[{_SPACE}]")
_SPACE_EDGES = re.compile(f"^[{_SPACE}]+|[{_SPACE}]+$")
_SURROGATE = re.compile("[\\ud800-\\udfff]")


def trim_space(value: str) -> str:
    """`value` without the kits' space characters at either end."""
    return _SPACE_EDGES.sub("", value)


def normal_email(email: object) -> str:
    """Lowercased and trimmed: how emails are compared and held as unique keys."""
    return trim_space(email).lower() if isinstance(email, str) else ""


def email_domain(email: object) -> str:
    """The domain of an address, lowercased: what follows its `@`."""
    e = normal_email(email)
    return e[e.rfind("@") + 1:]


def valid_email(email: str) -> bool:
    """An address: one `@`, a local part, a domain of at least two labels, none of them empty, none
    of the kits' space characters (see `_SPACE`), no surrogates, at most 254 bytes in UTF-8. The
    same rule in every language's kit."""
    at = email.find("@")
    if at < 1 or "@" in email[at + 1:] or _SPACE_ANY.search(email) or _SURROGATE.search(email):
        return False
    if len(email.encode()) > 254:
        return False
    labels = email[at + 1:].split(".")
    return len(labels) >= 2 and all(labels)


def valid_password(password: object) -> bool:
    return isinstance(password, str) and 10 <= len(password) <= 1024


# --- TOTP (RFC 6238): HMAC-SHA1, 30 s steps, 6 digits, base32 secrets -----------------------------


def totp_secret() -> str:
    """A new TOTP secret: 20 random bytes in base32, as authenticator apps take it."""
    return base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")


def totp_code(secret: str, step: int) -> str:
    """The 6-digit code of `secret` for the 30-second step `step`."""
    key = base64.b32decode(secret.upper() + "=" * (-len(secret) % 8))
    mac = hmac.new(key, struct.pack(">Q", step), hashlib.sha1).digest()
    offset = mac[-1] & 0x0F
    return str((struct.unpack(">I", mac[offset:offset + 4])[0] & 0x7FFFFFFF) % 1_000_000).zfill(6)


def totp_step(secret: str, code: object, now: int) -> int | None:
    """The step `code` belongs to, at `now` (ms) give or take one step (clock drift), or None."""
    if not isinstance(code, str) or not re.fullmatch(r"\d{6}", code):
        return None
    step = now // 30_000
    for s in (step, step - 1, step + 1):
        if equal_secrets(totp_code(secret, s), code):
            return s
    return None
