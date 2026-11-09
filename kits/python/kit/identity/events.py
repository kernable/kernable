"""The identity events: what happened to identities, the vocabulary other domains read."""

from typing import Literal

from kernable import Event

from .domain import Scope, identity_domain, own


class IdentityCreated(Event):
    vault = {"personal": [{"fields": ["display_name", "title", "email"], "subject": own}]}
    kind: Literal["person", "service", "process"]
    account_id: str | None
    display_name: str | None
    title: str | None
    email: str | None


class ProfileUpdated(Event):
    vault = {"personal": [{"fields": ["display_name", "title", "email"], "subject": own}]}
    display_name: str | None
    title: str | None
    email: str | None


class MembershipGranted(Event):
    membership_id: str
    group: str
    #: None: the whole tenant; else one aggregate (or what has it as parent).
    scope: Scope | None
    expires_at: int | None


class MembershipSuspended(Event):
    membership_id: str


class MembershipResumed(Event):
    membership_id: str


class MembershipRevoked(Event):
    membership_id: str


class ApiKeyCreated(Event):
    key_id: str
    #: SHA-256 of the whole key: it has high entropy, so the hash needs no vault.
    hash: str
    label: str
    expires_at: int | None


class ApiKeyRevoked(Event):
    key_id: str


class IdentitySuspended(Event):
    reason: str


class IdentityReactivated(Event):
    pass


class IdentityDeleted(Event):
    """Erases the identity's personal data."""
    vault = {"erases": own}


# Declared on the domain, so a process that only reads them knows them too.
identity_domain.event(IdentityCreated, ProfileUpdated, MembershipGranted, MembershipSuspended, MembershipResumed, MembershipRevoked,
                      ApiKeyCreated, ApiKeyRevoked, IdentitySuspended, IdentityReactivated, IdentityDeleted)
