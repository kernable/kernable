"""group: a named set of permissions per tenant (design/kits.md "group"). The `owners` group
is virtual: owners live on the Tenant. The authorization middleware is a listener of this domain too."""

from typing import TypedDict

from kernable import domain

group_domain = domain("group")


class Permission(TypedDict, total=False):
    """A type pattern in the grant syntax (`shop.PlaceOrder`, `shop.*`, `*`), optionally tied to a
    relation of the target aggregate (`when`), which may hold on its parent (`via_parent`)."""
    pattern: str
    when: str
    via_parent: bool
