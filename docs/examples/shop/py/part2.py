"""Part 2: commands, rejections and idempotency."""

from connect import connect

k = connect()


def show(what, o):
    print(f"{what}: " + (f"committed, version {o.events[-1].version}" if o.committed else f"{o.code} ({o.message})"))


# region try
def send(aggregate_type, aggregate_id, type, data=None, id=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data, id=id)


show("register", send("Customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"}))
show("register again", send("Customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"}))
show("place", send("Order", "o1", "Place", {"customerId": "c1"}))
show("add apples", send("Order", "o1", "AddItem", {"sku": "apple", "qty": 2}, "add-1"))
show("same command again", send("Order", "o1", "AddItem", {"sku": "apple", "qty": 2}, "add-1"))
show("submit", send("Order", "o1", "Submit"))
show("add after submit", send("Order", "o1", "AddItem", {"sku": "pear", "qty": 1}))
# endregion try
k.close()
