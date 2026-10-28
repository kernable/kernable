"""Part 3: read your own writes through queries."""

from kernable import Kernable, Rejected

k = Kernable()


def send(aggregate_type, aggregate_id, type, data=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data)


# region try
def get_order(id):
    return k.query("shop", "GetOrder", {"id": id})


send("customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"})
send("order", "o1", "Place", {"customerId": "c1"})
send("order", "o1", "AddItem", {"sku": "apple", "qty": 2})
# The submit returned after the read model processed the event: the query sees it.
print(get_order("o1"))
print(k.query("shop", "GetCustomer", {"id": "c1"}))
try:
    get_order("o9")
except Rejected as err:
    print(f"{err.code}: {err}")
# endregion try
k.close()
