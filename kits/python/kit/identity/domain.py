"""identity: a person or service in one tenant, holding its group memberships
(design/kits.md "identity"). `__init__.py` registers the parts."""

import re
from typing import Literal, TypedDict

from kernable import domain

from ..shared.secrets import sha256

identity_domain = domain("identity")

IdentityStatus = Literal["none", "active", "suspended", "deleted"]


class Scope(TypedDict):
    type: str
    id: str


def own(_d, identity_id: str) -> str:
    """The identity as the subject of its personal data."""
    return f"identity:{identity_id}"


def api_key(key_id: str, secret: str) -> str:
    """An API key as its holder sends it: `key_<keyId>_<secret>`."""
    return f"key_{key_id}_{secret}"


def parse_api_key(value: str) -> dict | None:
    """The key ID and the whole key's hash, or None if it has another shape."""
    m = re.fullmatch(r"key_([A-Za-z0-9]{8,32})_(.{32,})", value or "")
    return {"key_id": m.group(1), "hash": sha256(value)} if m else None
