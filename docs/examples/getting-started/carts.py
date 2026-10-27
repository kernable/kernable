import os

from kernable import Kernable, Rejected

carts = {}
k = Kernable(f"unix:{os.getcwd()}/k.sock", "app.key")
model = k.read_model("carts")


@model.on("shop.ItemAdded")
def _(e):
    carts.setdefault(e.aggregate_id, {"items": 0})["items"] += e.data["qty"]


@model.on("shop.CheckedOut")
def _(e):
    carts[e.aggregate_id]["order"] = e.data["orderId"]


@model.query("shop.GetCart")
def _(data):
    if data["id"] not in carts:
        raise Rejected("no such cart", "not_found")
    return carts[data["id"]]


model.run()
