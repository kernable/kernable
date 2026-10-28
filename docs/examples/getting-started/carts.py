# region file
from kernable import Kernable, Rejected, domain

# region register
rows = {}
shop_domain = domain("shop")
carts_readmodel = shop_domain.read_model("carts")
# endregion register


# region events
@carts_readmodel.on("ItemAdded")
def _(data, evt):
    rows.setdefault(evt.aggregate_id, {"items": 0})["items"] += data["qty"]


@carts_readmodel.on("CheckedOut")
def _(data, evt):
    rows[evt.aggregate_id]["order"] = data["orderId"]
# endregion events


# region queries
@carts_readmodel.query("GetCart")
def _(data):
    if data["id"] not in rows:
        raise Rejected("no such cart", "not_found")
    return rows[data["id"]]
# endregion queries


# region run
k = Kernable(key="app.key")
k.listener(carts_readmodel).run()
# endregion run
# endregion file
