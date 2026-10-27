"""Part 6: the guard decides who may do what."""

from kernable import Rejected, RequestContext

from connect import connect

k = connect()


def show(what, o):
    print(f"{what}: " + ("committed" if o.committed else f"{o.code} ({o.message})"))


# region try
# Who sends a request is part of its signed request context.
def send(who, aggregate_type, aggregate_id, type, data=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data, context=RequestContext(sender_identity=who))


show("c1 registers", send("c1", "Customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"}))
show("c2 registers", send("c2", "Customer", "c2", "Register", {"email": "bob@example.com", "name": "Bob"}))
show("c1 places o1", send("c1", "Order", "o1", "Place", {"customerId": "c1"}))
show("c2 adds to o1", send("c2", "Order", "o1", "AddItem", {"sku": "apple", "qty": 1}))
show("c2 places for c1", send("c2", "Order", "o2", "Place", {"customerId": "c1"}))


def get_order(who):
    return k.query("shop", "GetOrder", {"id": "o1"}, context=RequestContext(sender_identity=who))


print("c1 reads o1:", get_order("c1"))
try:
    get_order("c2")
except Rejected as err:
    print(f"c2 reads o1: {err.code} ({err})")
# endregion try
k.close()
