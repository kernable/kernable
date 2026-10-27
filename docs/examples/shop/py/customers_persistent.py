"""The customers read model with a store: it keeps its rows in a file and continues after a
restart where it left off, instead of reading every event again."""

from kernable import Rejected

from connect import connect

# region persistent
model = connect().read_model("customers", store="sqlite:customers.db", schema="1")


@model.on("shop.CustomerRegistered")
def _(e, ctx):
    ctx.store.set("customers", e.aggregate_id, {"id": e.aggregate_id, "email": e.data["email"], "name": e.data["name"]})


@model.on("shop.CustomerErased")
def _(e, ctx):
    ctx.store.remove("customers", e.aggregate_id)


@model.query("shop.GetCustomer")
def _(data, ctx):
    row = ctx.store.get("customers", data["id"])
    if row is None:
        raise Rejected(f"No customer {data['id']}", "not_found")
    return row


model.run()
# endregion persistent
