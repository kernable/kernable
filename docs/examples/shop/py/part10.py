"""Part 10: the fulfilment process ships a paid order and cancels an unpaid one."""

import time

from kernable import Kernable, RequestContext

from domain import shop_domain

k = Kernable(context=RequestContext(sender_identity="c1")).use(shop_domain)


def send(aggregate_type, aggregate_id, type, data=None):
    return k.submit("shop", aggregate_type, aggregate_id, type, data)


def until(id, status):
    """The order's status once it is `status`: the process moves it on its own, a little later."""
    while True:
        order = k.query("shop", "GetOrder", {"id": id})
        if order["status"] == status:
            return order
        time.sleep(0.1)


# region try
send("customer", "c1", "Register", {"email": "ann@example.com", "name": "Ann"})
for id in ("o1", "o2"):
    send("order", id, "Place", {"customerId": "c1"})
    send("order", id, "AddItem", {"sku": "apple", "qty": 1})
    send("order", id, "Submit")  # confirmed by the reactor: a case starts for the order
send("order", "o1", "Pay")
print("o1:", until("o1", "shipped")["status"])
# o2 is never paid: when its time is up, the process cancels it.
o2 = until("o2", "cancelled")
print("o2:", o2["status"], o2["cancelled"])
# endregion try
k.close()
