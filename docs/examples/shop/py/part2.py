"""Part 2: commands, rejections and idempotency."""

from kernable import Kernable

k = Kernable()


def show(what, o):
    print(f"{what}: " + (f"committed, version {o.events[-1].version}" if o.committed else f"{o.code} ({o.message})"))


# region try
def send(aggregate_type, aggregate_id, type, data=None, id=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data, id=id)


show("register", send("customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"}))
show("register again", send("customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"}))
show("place", send("order", "o1", "Place", {"customerId": "c1"}))
show("add apples", send("order", "o1", "AddItem", {"sku": "apple", "qty": 2}, "add-1"))
show("same command again", send("order", "o1", "AddItem", {"sku": "apple", "qty": 2}, "add-1"))
show("submit", send("order", "o1", "Submit"))
show("add after submit", send("order", "o1", "AddItem", {"sku": "pear", "qty": 1}))
# endregion try
k.close()
