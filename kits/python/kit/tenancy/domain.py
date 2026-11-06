"""tenancy: one Tenant per tenant, living under its own tenant ID (design/kits.md "tenancy").
The domain object and the types its parts share; `__init__.py` registers the parts."""

from typing import Literal, TypedDict

from kernable import domain

tenancy_domain = domain("tenancy")

TenantStatus = Literal["none", "pending", "active", "suspended", "deleted"]
SuspensionReason = Literal["owner_requested", "unpaid", "abuse", "by_platform", "rejected"]
SUSPENSION_REASONS = ["owner_requested", "unpaid", "abuse", "by_platform", "rejected"]


class SignUpPolicy(TypedDict):
    #: The status a self-service tenant starts with, or `off`.
    tenants: Literal["off", "pending", "active"]
    accounts: Literal["off", "open"]


class Owner(TypedDict, total=False):
    """`{"email": ...}` when the platform created it, `{"account_id": ...}` by self-service."""
    email: str
    account_id: str
