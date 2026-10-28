"""The customers read model with a store: it keeps its rows in a file and continues after a
restart where it left off, instead of reading every event again."""

from kernable import Kernable, Rejected

from domain import shop_domain

# region persistent
customers_readmodel = shop_domain.read_model("customers", store="sqlite:customers.db", schema="1")


@customers_readmodel.on("CustomerRegistered")
def _(data, evt, ctx):
    ctx.store.set("customers", evt.aggregate_id, {"id": evt.aggregate_id, "email": data["email"], "name": data["name"]})


@customers_readmodel.on("CustomerErased")
def _(data, evt, ctx):
    ctx.store.remove("customers", evt.aggregate_id)


@customers_readmodel.query("GetCustomer")
def _(data, qry, ctx):
    row = ctx.store.get("customers", data["id"])
    if row is None:
        raise Rejected(f"No customer {data['id']}", "not_found")
    return row


Kernable().listener(customers_readmodel).run()
# endregion persistent
