"""The read models: customers and orders, in memory, answering queries."""

import threading

from kernable import Kernable, Rejected

from domain import shop_domain

k = Kernable()

# region customers
customers: dict[str, dict] = {}
customers_readmodel = shop_domain.read_model("customers")


@customers_readmodel.on("CustomerRegistered")
def _(data, evt):
    customers[evt.aggregate_id] = {"id": evt.aggregate_id, "email": data["email"], "name": data["name"]}


@customers_readmodel.query("GetCustomer")
def _(data):
    if data["id"] not in customers:
        raise Rejected(f"No customer {data['id']}", "not_found")
    return customers[data["id"]]
# endregion customers


# region orders
orders: dict[str, dict] = {}
orders_readmodel = shop_domain.read_model("orders")


@orders_readmodel.on("OrderPlaced")
def _(data, evt):
    orders[evt.aggregate_id] = {"id": evt.aggregate_id, "customerId": data["customerId"], "status": "draft",
                              "items": {}}


@orders_readmodel.on("ItemAdded")
def _(data, evt):
    items = orders[evt.aggregate_id]["items"]
    items[data["sku"]] = items.get(data["sku"], 0) + data["qty"]


@orders_readmodel.on("OrderSubmitted")
def _(data, evt):
    orders[evt.aggregate_id]["status"] = "submitted"


@orders_readmodel.query("GetOrder")
def _(data):
    if data["id"] not in orders:
        raise Rejected(f"No order {data['id']}", "not_found")
    return orders[data["id"]]
# endregion orders


# region confirmed
@orders_readmodel.on("OrderConfirmed")
def _(data, evt):
    orders[evt.aggregate_id].update(status="confirmed", confirmedTo=data["email"])
# endregion confirmed


# region erased
# An erased customer's personal data must not outlive the erasure in a read model either.
@customers_readmodel.on("CustomerErased")
def _(data, evt):
    customers.pop(evt.aggregate_id, None)


@orders_readmodel.on("CustomerErased")
def _(data, evt):
    for row in orders.values():
        if row["customerId"] == evt.aggregate_id and row.get("confirmedTo"):
            row["confirmedTo"] = None
# endregion erased


# region fulfilled
@orders_readmodel.on("OrderPaid")
def _(data, evt):
    orders[evt.aggregate_id]["status"] = "paid"


@orders_readmodel.on("OrderShipped")
def _(data, evt):
    orders[evt.aggregate_id]["status"] = "shipped"


@orders_readmodel.on("OrderCancelled")
def _(data, evt):
    orders[evt.aggregate_id].update(status="cancelled", cancelled=data["reason"])
# endregion fulfilled


# region run
threading.Thread(target=k.listener(customers_readmodel).run, daemon=True).start()
k.listener(orders_readmodel).run()
# endregion run
