"""Part 4: the reactor confirms a submitted order."""

from connect import connect

k = connect()


def send(aggregate_type, aggregate_id, type, data=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data)


# region try
send("Customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"})
send("Order", "o1", "Place", {"customerId": "c1"})
send("Order", "o1", "AddItem", {"sku": "apple", "qty": 2})
send("Order", "o1", "Submit")
# The submit settled after the reactor, which confirmed the order before it returned.
print(k.query("shop", "GetOrder", {"id": "o1"}))
# endregion try
k.close()
