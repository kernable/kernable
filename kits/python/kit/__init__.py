"""The kit as a whole: its domains, ready for `kernable.app(domains=kit_domains())`."""

from kernable import Domain

from .account import account_domain
from .group import group_domain
from .identity import identity_domain
from .invitation import invitation_domain
from .tenancy import tenancy_domain


def kit_domains() -> list[Domain]:
    """Every domain of the kit, with the relations declared so far: declare yours first."""
    return [tenancy_domain, account_domain, identity_domain, group_domain, invitation_domain]
