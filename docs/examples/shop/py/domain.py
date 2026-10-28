"""The shop's domain: customers and their orders. Imported by every process of the shop."""

import hashlib
import re

from kernable import Rejected, domain

# region domain
shop_domain = domain("shop")
# endregion domain

# region customer
customer_aggregate = shop_domain.aggregate("customer", {"registered": False})


@customer_aggregate.apply("CustomerRegistered")
def _(state, data):
    state["registered"] = True


@customer_aggregate.command("Register")
def _(state, data):
    if state["registered"]:
        raise Rejected("Customer already registered", "exists")
    if not re.match(r"^[^@\s]+@[^@\s]+$", data.get("email") or ""):
        raise Rejected("Invalid email", "invalid")
    return [{"type": "CustomerRegistered", "data": {"email": data["email"], "name": data["name"]}}]
# endregion customer


# region order
order_aggregate = shop_domain.aggregate("order", {"status": "none", "items": 0})


@order_aggregate.apply("OrderPlaced")
def _(state, data):
    return {"status": "draft", "items": 0}


@order_aggregate.apply("ItemAdded")
def _(state, data):
    state["items"] += data["qty"]


@order_aggregate.apply("OrderSubmitted")
def _(state, data):
    state["status"] = "submitted"


@order_aggregate.apply("OrderConfirmed")
def _(state, data):
    state["status"] = "confirmed"


@order_aggregate.command("Place")
def _(state, data):
    if state["status"] != "none":
        raise Rejected("Order already exists", "exists")
    return [{"type": "OrderPlaced", "data": {"customerId": data["customerId"]}}]


@order_aggregate.command("AddItem")
def _(state, data):
    if state["status"] != "draft":
        raise Rejected(f"Cannot add items to a {state['status']} order", "invalid_state")
    if not data.get("qty", 0) > 0:
        raise Rejected("Quantity must be positive", "invalid")
    return [{"type": "ItemAdded", "data": {"sku": data["sku"], "qty": data["qty"]}}]


@order_aggregate.command("Submit")
def _(state, data):
    if state["status"] != "draft" or state["items"] == 0:
        raise Rejected(f"Cannot submit a {state['status']} order with {state['items']} items", "invalid_state")
    return [{"type": "OrderSubmitted"}]
# endregion order


# region confirm
@order_aggregate.command("Confirm")
def _(state, data):
    if state["status"] != "submitted":
        raise Rejected(f"Cannot confirm a {state['status']} order", "invalid_state")
    return [{"type": "OrderConfirmed", "data": {"customerId": data["customerId"], "email": data["email"]}}]
# endregion confirm


# region vault
# Personal data goes to the kernel's vault; the signed log keeps references only.
shop_domain.command("Register").personal(["email", "name"], lambda d, customer_id: customer_id)
shop_domain.event("CustomerRegistered", 1).personal(["email", "name"], lambda d, customer_id: customer_id)
shop_domain.command("Confirm").personal(["email"], lambda d: d["customerId"])
shop_domain.event("OrderConfirmed", 1).personal(["email"], lambda d: d["customerId"])
# Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
shop_domain.command("SetPassword").secret(["password"])
shop_domain.command("SignIn").secret(["password"])
shop_domain.event("PasswordSet", 1).personal(["hash", "salt"], lambda d, customer_id: customer_id)
# Erasing a customer deletes their personal data everywhere, in every stream.
shop_domain.event("CustomerErased", 1).erases(lambda d, customer_id: customer_id)


def password_hash(password: str, salt: str) -> str:
    return hashlib.scrypt(password.encode(), salt=salt.encode(), n=2**14, r=8, p=1, dklen=32).hex()


@customer_aggregate.apply("PasswordSet")
def _(state, data):
    state["password_hash"] = data["hash"]
    state["salt"] = data["salt"]


@customer_aggregate.apply("CustomerErased")
def _(state, data):
    state.pop("password_hash", None)
    state.pop("salt", None)
    state["registered"] = False


@customer_aggregate.command("SetPassword")
def _(state, data, cmd, ctx):
    if not state["registered"]:
        raise Rejected("No such customer", "not_found")
    if len(data.get("password") or "") < 8:
        raise Rejected("At least 8 characters", "invalid")
    salt = ctx.id()  # deterministic: the command may run again
    return [{"type": "PasswordSet", "data": {"hash": password_hash(data["password"], salt), "salt": salt}}]


@customer_aggregate.command("SignIn")
def _(state, data):
    if "password_hash" not in state or password_hash(data.get("password") or "", state["salt"]) != state["password_hash"]:
        raise Rejected("Wrong customer or password", "denied")
    return [{"type": "SignedIn"}]


@customer_aggregate.command("Erase")
def _(state, data):
    if not state["registered"]:
        raise Rejected("No such customer", "not_found")
    return [{"type": "CustomerErased"}]
# endregion vault
