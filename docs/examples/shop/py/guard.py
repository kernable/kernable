"""The guard: a middleware that lets customers act only on themselves and their own orders."""

from kernable import Check, Rejected

from connect import connect

# region guard
SERVICES = {"svc-confirmations"}
registered: set[str] = set()
owner_of: dict[str, str] = {}  # order -> customer

k = connect()
guard = k.read_model("guard")


@guard.on("shop.CustomerRegistered")
def _(e):
    registered.add(e.aggregate_id)


@guard.on("shop.CustomerErased")
def _(e):
    registered.discard(e.aggregate_id)


@guard.on("shop.OrderPlaced")
def _(e):
    owner_of[e.aggregate_id] = e.data["customerId"]


@guard.middleware(["shop.*"])
def _(check: Check):
    who = check.request.sender_identity
    if check.type == "Register":
        return  # anyone may sign up
    if check.kind == "query" and who in SERVICES:
        return  # services may read
    target = check.aggregate_id if check.kind == "command" else check.data.get("id")
    if check.type == "Place":
        owner = check.data.get("customerId")
    elif check.type == "GetCustomer" or check.aggregate_type == "Customer":
        owner = target
    else:
        owner = owner_of.get(target)
    if who not in registered or owner != who:
        raise Rejected(f"{check.type} is not allowed for {who or 'anonymous'}", "forbidden")


guard.run()
# endregion guard
