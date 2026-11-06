"""account: one Account per person and a Session per sign-in, in the platform tenant, and a
tenant's single sign-on (design/kits.md "account"). Vault fields are declared on the event
and command classes; `__init__.py` registers the parts."""

from typing import Literal

from kernable import domain

account_domain = domain("account")

AccountStatus = Literal["none", "pending", "active", "suspended", "deleted"]
AccountSuspensionReason = Literal["owner_requested", "abuse", "by_platform"]
ACCOUNT_SUSPENSION_REASONS = ["owner_requested", "abuse", "by_platform"]
TokenPurpose = Literal["verify", "reset", "email_change"]
SessionEnd = Literal["signed_out", "revoked", "account_suspended", "password_changed", "account_deleted", "reuse", "idle", "expired"]


def own(_d, aggregate_id: str) -> str:
    """The account itself as the subject of its personal data."""
    return aggregate_id
