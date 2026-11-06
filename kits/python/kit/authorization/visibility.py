"""Lists that show only what one may see: the middleware decides on single requests; a read
model answering a list filters its rows with the same decision. `visibility(listener)` teaches an
app's read model what the middleware knows and returns `visible`.

    orders = shop_domain.read_model("shop.orders")
    visible = visibility(orders)

    @orders.query(ListOrders)
    def _(d, qry, ctx):
        return [o for o in rows if visible(qry.tenant, ctx.request, "shop.GetOrder", {"type": "shop.order", "id": o["id"]})]
"""

import copy
import time
from dataclasses import replace

from kernable import Check

from .middleware import authorization_listener, install_relations, knowledge
from .policy import MemoryKeyValues, decide


class _Prefixed:
    """The listener's store under a prefix of its own, so the kit's namespaces never meet the app's."""

    def __init__(self, store) -> None:
        self.store = store

    def get(self, ns, key, default=None):
        return self.store.get(f"kit:{ns}", key, default)

    def set(self, ns, key, value):
        self.store.set(f"kit:{ns}", key, value)

    def remove(self, ns, key):
        self.store.remove(f"kit:{ns}", key)

    def all(self, ns):
        return self.store.all(f"kit:{ns}")


def visibility(listener):
    """Adds the middleware's event handlers to `listener` (after its own, for the same events) and
    returns `visible(tenant, request, type, target)`: whether the asker may `type` (a qualified type,
    e.g. `shop.GetOrder`) on the target aggregate (`{"type": "shop.order", "id": ...}`), as the
    middleware would decide. Declare the relations before calling it. Kept in the listener's store,
    if it has one, else in memory."""
    install_relations()
    memory = MemoryKeyValues()
    current = [memory]

    def kv(store):
        current[0] = _Prefixed(store) if store is not None else memory
        return current[0]

    for type_, kit_handler in list(authorization_listener._handlers.items()):
        own = listener._handlers.get(type_)

        def handler(data, evt=None, ctx=None, own=own, kit_handler=kit_handler):
            if own:
                own(data, evt, ctx)
            kit_handler(copy.deepcopy(data), evt, replace(ctx, store=kv(getattr(ctx, "store", None))))

        listener._handlers[type_] = handler
        listener._domains.update(authorization_listener._domains)

    def visible(tenant: str, request, type: str, target: dict) -> bool:
        domain, _, name = type.partition(".")
        check = Check(kind="command", id="", tenant=tenant, domain=domain, type=name, data={}, request=request,
                      aggregate_type=target["type"].split(".")[1], aggregate_id=target["id"])
        saved = knowledge.kv
        knowledge.kv = current[0]
        try:
            return decide(knowledge, check, int(time.time() * 1000)) is None
        finally:
            knowledge.kv = saved

    return visible
