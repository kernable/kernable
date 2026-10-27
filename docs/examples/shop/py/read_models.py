"""The read models: customers and orders, in memory, answering queries."""

import threading

from kernable import Rejected

from connect import connect

k = connect()

# region customers
customers: dict[str, dict] = {}
customers_model = k.read_model("customers")


@customers_model.on("shop.CustomerRegistered")
def _(e):
    customers[e.aggregate_id] = {"id": e.aggregate_id, "email": e.data["email"], "name": e.data["name"]}


@customers_model.query("shop.GetCustomer")
def _(data):
    if data["id"] not in customers:
        raise Rejected(f"No customer {data['id']}", "not_found")
    return customers[data["id"]]
# endregion customers


# region orders
orders: dict[str, dict] = {}
orders_model = k.read_model("orders")


@orders_model.on("shop.OrderPlaced")
def _(e):
    orders[e.aggregate_id] = {"id": e.aggregate_id, "customerId": e.data["customerId"], "status": "draft",
                              "items": {}}


@orders_model.on("shop.ItemAdded")
def _(e):
    items = orders[e.aggregate_id]["items"]
    items[e.data["sku"]] = items.get(e.data["sku"], 0) + e.data["qty"]


@orders_model.on("shop.OrderSubmitted")
def _(e):
    orders[e.aggregate_id]["status"] = "submitted"


@orders_model.query("shop.GetOrder")
def _(data):
    if data["id"] not in orders:
        raise Rejected(f"No order {data['id']}", "not_found")
    return orders[data["id"]]
# endregion orders


# region confirmed
@orders_model.on("shop.OrderConfirmed")
def _(e):
    orders[e.aggregate_id].update(status="confirmed", confirmedTo=e.data["email"])
# endregion confirmed


# region erased
# An erased customer's personal data must not outlive the erasure in a read model either.
@customers_model.on("shop.CustomerErased")
def _(e):
    customers.pop(e.aggregate_id, None)


@orders_model.on("shop.CustomerErased")
def _(e):
    for row in orders.values():
        if row["customerId"] == e.aggregate_id and row.get("confirmedTo"):
            row["confirmedTo"] = None
# endregion erased


# region run
threading.Thread(target=customers_model.run, daemon=True).start()
orders_model.run()
# endregion run
