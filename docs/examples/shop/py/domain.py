"""The shop's domain: customers and their orders. Imported by every process of the shop."""

import hashlib
import re

from kernable import Rejected, domain

# region domain
shop = domain("shop")
# endregion domain

# region customer
customer = shop.aggregate("Customer", {"registered": False})


@customer.apply("CustomerRegistered")
def _(state, e):
    return {**state, "registered": True}


@customer.command("Register")
def _(state, cmd):
    if state["registered"]:
        raise Rejected("Customer already registered", "exists")
    if not re.match(r"^[^@\s]+@[^@\s]+$", cmd.get("email") or ""):
        raise Rejected("Invalid email", "invalid")
    return [{"type": "CustomerRegistered", "data": {"email": cmd["email"], "name": cmd["name"]}}]
# endregion customer


# region order
order = shop.aggregate("Order", {"status": "none", "items": 0})


@order.apply("OrderPlaced")
def _(state, e):
    return {"status": "draft", "items": 0}


@order.apply("ItemAdded")
def _(state, e):
    return {**state, "items": state["items"] + e["qty"]}


@order.apply("OrderSubmitted")
def _(state, e):
    return {**state, "status": "submitted"}


@order.apply("OrderConfirmed")
def _(state, e):
    return {**state, "status": "confirmed"}


@order.command("Place")
def _(state, cmd):
    if state["status"] != "none":
        raise Rejected("Order already exists", "exists")
    return [{"type": "OrderPlaced", "data": {"customerId": cmd["customerId"]}}]


@order.command("AddItem")
def _(state, cmd):
    if state["status"] != "draft":
        raise Rejected(f"Cannot add items to a {state['status']} order", "invalid_state")
    if not cmd.get("qty", 0) > 0:
        raise Rejected("Quantity must be positive", "invalid")
    return [{"type": "ItemAdded", "data": {"sku": cmd["sku"], "qty": cmd["qty"]}}]


@order.command("Submit")
def _(state, cmd):
    if state["status"] != "draft" or state["items"] == 0:
        raise Rejected(f"Cannot submit a {state['status']} order with {state['items']} items", "invalid_state")
    return [{"type": "OrderSubmitted"}]
# endregion order


# region confirm
@order.command("Confirm")
def _(state, cmd):
    if state["status"] != "submitted":
        raise Rejected(f"Cannot confirm a {state['status']} order", "invalid_state")
    return [{"type": "OrderConfirmed", "data": {"customerId": cmd["customerId"], "email": cmd["email"]}}]
# endregion confirm


# region vault
# Personal data goes to the kernel's vault; the signed log keeps references only.
shop.command("Register").personal(["email", "name"], lambda d, customer_id: customer_id)
shop.event("CustomerRegistered", 1).personal(["email", "name"], lambda d, customer_id: customer_id)
shop.command("Confirm").personal(["email"], lambda d: d["customerId"])
shop.event("OrderConfirmed", 1).personal(["email"], lambda d: d["customerId"])
# Passwords are secrets: the handler sees them once; the vault deletes them when it is done.
shop.command("SetPassword").secret(["password"])
shop.command("SignIn").secret(["password"])
shop.event("PasswordSet", 1).personal(["hash", "salt"], lambda d, customer_id: customer_id)
# Erasing a customer deletes their personal data everywhere, in every stream.
shop.event("CustomerErased", 1).erases(lambda d, customer_id: customer_id)


def password_hash(password: str, salt: str) -> str:
    return hashlib.scrypt(password.encode(), salt=salt.encode(), n=2**14, r=8, p=1, dklen=32).hex()


@customer.apply("PasswordSet")
def _(state, e):
    return {**state, "password_hash": e["hash"], "salt": e["salt"]}


@customer.apply("CustomerErased")
def _(state, e):
    return {"registered": False}


@customer.command("SetPassword")
def _(state, cmd, ctx):
    if not state["registered"]:
        raise Rejected("No such customer", "not_found")
    if len(cmd.get("password") or "") < 8:
        raise Rejected("At least 8 characters", "invalid")
    salt = ctx.id()  # deterministic: the command may run again
    return [{"type": "PasswordSet", "data": {"hash": password_hash(cmd["password"], salt), "salt": salt}}]


@customer.command("SignIn")
def _(state, cmd):
    if "password_hash" not in state or password_hash(cmd.get("password") or "", state["salt"]) != state["password_hash"]:
        raise Rejected("Wrong customer or password", "denied")
    return [{"type": "SignedIn"}]


@customer.command("Erase")
def _(state, cmd):
    if not state["registered"]:
        raise Rejected("No such customer", "not_found")
    return [{"type": "CustomerErased"}]
# endregion vault
