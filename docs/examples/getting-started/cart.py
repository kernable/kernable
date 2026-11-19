# region file
from kernable import Kernable, Rejected, domain

# region aggregate
shop_domain = domain("shop")
cart_aggregate = shop_domain.aggregate("cart", {"items": 0, "checked_out": False})
# endregion aggregate


# region events
@cart_aggregate.apply("ItemAdded")
def _(state, data):
    state["items"] += data["qty"]


@cart_aggregate.apply("CheckedOut")
def _(state, data):
    state["checked_out"] = True
# endregion events


# region commands
@cart_aggregate.command("AddItem")
def _(state, data):
    if state["checked_out"]:
        raise Rejected("Cart is checked out")
    return [{"type": "ItemAdded", "data": data}]


@cart_aggregate.command("CheckOut")
def _(state, data, cmd, ctx):
    if state["items"] == 0:
        raise Rejected("Cart is empty")
    if state["checked_out"]:
        raise Rejected("Cart is checked out")
    # ctx.now() and ctx.id() stay the same if the command runs again.
    return [{"type": "CheckedOut", "data": {"at": ctx.now().isoformat(), "orderId": ctx.id()}}]
# endregion commands


# region worker
k = Kernable(key="app.key")
k.worker(cart_aggregate).run()
# endregion worker
# endregion file
