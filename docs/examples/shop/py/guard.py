"""The guard: a middleware that lets customers act only on themselves and their own orders."""

from kernable import Check, Kernable, Rejected

from domain import shop_domain

# region guard
SERVICES = {"svc-confirmations"}
registered: set[str] = set()
owner_of: dict[str, str] = {}  # order -> customer

guard_readmodel = shop_domain.read_model("guard")


@guard_readmodel.on("CustomerRegistered")
def _(data, evt):
    registered.add(evt.aggregate_id)


@guard_readmodel.on("CustomerErased")
def _(data, evt):
    registered.discard(evt.aggregate_id)


@guard_readmodel.on("OrderPlaced")
def _(data, evt):
    owner_of[evt.aggregate_id] = data["customerId"]


@guard_readmodel.middleware(["shop.*"])
def _(check: Check):
    who = check.request.sender_identity
    if check.type == "Register":
        return  # anyone may sign up
    if check.kind == "query" and who in SERVICES:
        return  # services may read
    target = check.aggregate_id if check.kind == "command" else check.data.get("id")
    if check.type == "Place":
        owner = check.data.get("customerId")
    elif check.type == "GetCustomer" or check.aggregate_type == "customer":
        owner = target
    else:
        owner = owner_of.get(target)
    if who not in registered or owner != who:
        raise Rejected(f"{check.type} is not allowed for {who or 'anonymous'}", "forbidden")


Kernable().listener(guard_readmodel).run()
# endregion guard
