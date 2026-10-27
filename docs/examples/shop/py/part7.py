"""Part 7: personal data in the vault, passwords as secrets, erasure."""

from kernable import Rejected, RequestContext

from connect import connect
from domain import shop


def show(what, o):
    print(f"{what}: " + ("committed" if o.committed else f"{o.code} ({o.message})"))


# region try
# use(shop): the client moves the fields the domain declares personal or secret to the vault.
k = connect(context=RequestContext(sender_identity="c1")).use(shop)


def customer(type, data=None):
    return k.submit("shop", "Customer", "c1", type, data)


show("register", customer("Register", {"email": "ann@example.com", "name": "Ann"}))
show("set password", customer("SetPassword", {"password": "correct horse"}))
show("sign in, wrong password", customer("SignIn", {"password": "wrong horse"}))
show("sign in", customer("SignIn", {"password": "correct horse"}))
print(k.query("shop", "GetCustomer", {"id": "c1"}))
show("erase", customer("Erase"))
# Erased: the customer's data is gone, and with it the right to act.
try:
    k.query("shop", "GetCustomer", {"id": "c1"})
except Rejected as err:
    print(f"read: {err.code}")
# endregion try
k.close()
