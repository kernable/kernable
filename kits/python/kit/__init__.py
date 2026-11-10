"""The kit as a whole: its domains, ready for `kernable.app(domains=kit_domains())`."""

from kernable import Domain

from .account import account_domain
from .authorization.middleware import Explain, authorization_listener, install_relations, use_authorization_store
from .authorization.relations import parent, relation
from .authorization.visibility import visibility
from .group import group_domain
from .identity import identity_domain
from .invitation import invitation_domain
from .tenancy import tenancy_domain

__all__ = ["kit_domains", "kit_schemas", "use_passkeys", "relation", "parent", "visibility", "Explain", "authorization_listener",
           "use_authorization_store"]


def kit_domains() -> list[Domain]:
    """Every domain of the kit, with the relations declared so far: declare yours first."""
    install_relations()
    return [tenancy_domain, account_domain, identity_domain, group_domain, invitation_domain]


def kit_schemas() -> list[Domain]:
    """The kit's domains for `Kernable().use(...)`, so clients seal the vault fields before signing."""
    return [tenancy_domain, account_domain, identity_domain, group_domain, invitation_domain]


def use_passkeys(rp_id: str, rp_name: str, origins: list[str]) -> None:
    """The relying party passkeys are bound to: your domain (`example.com`), a name, the origins your
    pages are served from."""
    from .shared.constants import PASSKEYS

    PASSKEYS.rp_id, PASSKEYS.rp_name, PASSKEYS.origins = rp_id, rp_name, list(origins)
