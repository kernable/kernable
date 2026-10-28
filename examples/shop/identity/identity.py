"""Identity service (Python): the `account` aggregate, its worker, the `accounts` read model and
the `rbac` middleware. Other services read accounts only through the query
`identity.GetAccount`, via the kernel.

Personal data (email, name, the password hash) lives in the kernel's vault, never in the signed
log; the password itself is a secret that is gone once its command ran. `EraseAccount` erases an
account's personal data everywhere, also in the `orders` domain.

Usage: python identity.py <kernel-url> <key> <health-port> [<data-dir>]
Mutual TLS: set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY.
"""

import hashlib
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kernable import Check, Kernable, Listener, Rejected, Tls, domain

EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")

# ---- write side ----

identity_domain = domain("identity")
# The vault keeps these fields; the subject is the account (the aggregate's ID).
identity_domain.command("RegisterAccount").personal(["email", "name"], lambda d, account: account).secret(["password"])
identity_domain.command("ChangeEmail").personal(["email"], lambda d, account: account)
identity_domain.command("SignIn").secret(["password"])
identity_domain.event("AccountRegistered", 1).personal(["email", "name", "passwordHash"], lambda d, account: account)
identity_domain.event("EmailChanged", 1).personal(["email"], lambda d, account: account)
identity_domain.event("AccountErased", 1).erases(lambda d, account: account)

account_aggregate = identity_domain.aggregate("account", {"status": "none"})


def password_hash(password: str, salt: str) -> str:
    return hashlib.scrypt(password.encode(), salt=salt.encode(), n=2**14, r=8, p=1).hex()


@account_aggregate.apply("AccountRegistered")
def _(state, data):
    return {"status": "active", "email": data["email"], "name": data["name"], "passwordHash": data["passwordHash"],
            "salt": data["salt"]}


@account_aggregate.apply("EmailChanged")
def _(state, data):
    state["email"] = data["email"]


@account_aggregate.apply("AccountDeactivated")
def _(state, data):
    state["status"] = "deactivated"


@account_aggregate.apply("AccountErased")
def _(state, data):
    return {"status": "erased"}


@account_aggregate.command("RegisterAccount")
def _(state, data, cmd, ctx):
    if state["status"] != "none":
        raise Rejected("Account already exists", "exists")
    if not EMAIL.match(data.get("email", "")):
        raise Rejected("Invalid email", "invalid")
    if len(data.get("password") or "") < 8:
        raise Rejected("The password needs at least 8 characters", "invalid")
    salt = ctx.id()  # deterministic, so the command may run again
    return [{"type": "AccountRegistered", "data": {"email": data["email"], "name": data.get("name", ""),
                                                   "passwordHash": password_hash(data["password"], salt),
                                                   "salt": salt}}]


@account_aggregate.command("SignIn")
def _(state, data):
    if state["status"] != "active" or password_hash(data.get("password") or "", state["salt"]) != state["passwordHash"]:
        raise Rejected("Wrong account or password", "denied")
    return [{"type": "SignedIn"}]


@account_aggregate.command("EraseAccount")
def _(state, data):
    if state["status"] in ("none", "erased"):
        raise Rejected(f"Cannot erase a {state['status']} account", "invalid_state")
    return [{"type": "AccountErased"}]


@account_aggregate.command("ChangeEmail")
def _(state, data):
    if state["status"] != "active":
        raise Rejected(f"Cannot change the email of a {state['status']} account", "invalid_state")
    if not EMAIL.match(data.get("email", "")):
        raise Rejected("Invalid email", "invalid")
    return [{"type": "EmailChanged", "data": {"email": data["email"]}}]


@account_aggregate.command("DeactivateAccount")
def _(state, data):
    if state["status"] != "active":
        raise Rejected(f"Cannot deactivate a {state['status']} account", "invalid_state")
    return [{"type": "AccountDeactivated"}]


# ---- read side: kept in a store, so a restart continues where it left off ----


def accounts_read_model(store: str) -> Listener:
    """The `accounts` read model, keeping its rows in `store`."""
    accounts_readmodel = identity_domain.read_model("accounts", store=store, schema="1")

    @accounts_readmodel.on("AccountRegistered")
    def _(data, evt, ctx):
        ctx.store.set("accounts", evt.aggregate_id, {"id": evt.aggregate_id, "status": "active", "email": data["email"],
                                                     "name": data["name"]})

    def update(ctx, account: str, **fields):
        ctx.store.set("accounts", account, {**ctx.store.get("accounts", account), **fields})

    @accounts_readmodel.on("EmailChanged")
    def _(data, evt, ctx):
        update(ctx, evt.aggregate_id, email=data["email"])

    @accounts_readmodel.on("AccountDeactivated")
    def _(data, evt, ctx):
        update(ctx, evt.aggregate_id, status="deactivated")

    @accounts_readmodel.on("AccountErased")
    def _(data, evt, ctx):
        ctx.store.remove("accounts", evt.aggregate_id)  # personal data must not outlive the erasure

    @accounts_readmodel.query("GetAccount")
    def _(data, qry, ctx):
        found = ctx.store.get("accounts", data["id"])
        if found is None:
            raise Rejected(f"no account {data['id']}", "not_found")
        return found

    return accounts_readmodel


# ---- authorization: a middleware for both domains, from its own read model ----

#: Services that may read on behalf of others, e.g. the orders reactor looking up an email.
SERVICES = {"svc-orders"}


# Checks every command and query of both domains before the kernel accepts it. The sender is the
# request context's `sender_identity`; the API asserts it (signed with its key).
rbac_readmodel = identity_domain.read_model("rbac")
active: set[str] = set()
customer_of: dict[str, str] = {}  # order -> account


@rbac_readmodel.on("AccountRegistered")
def _(data, evt):
    active.add(evt.aggregate_id)


@rbac_readmodel.on("AccountDeactivated")
def _(data, evt):
    active.discard(evt.aggregate_id)


@rbac_readmodel.on("AccountErased")
def _(data, evt):
    active.discard(evt.aggregate_id)


@rbac_readmodel.on("orders.OrderPlaced")
def _(data, evt):
    customer_of[evt.aggregate_id] = data["customerId"]


@rbac_readmodel.middleware(["identity.*", "orders.*"])
def _(check: Check):
    who = check.request.sender_identity
    if check.type == "RegisterAccount" or (who in SERVICES and check.kind == "query"):
        return
    if who not in active:
        raise Rejected("sign in with an active account", "forbidden")
    if check.domain == "identity":
        owner = check.aggregate_id if check.kind == "command" else check.data.get("id")
    elif check.type == "PlaceOrder":
        owner = check.data.get("customerId")
    else:
        owner = customer_of.get(check.aggregate_id if check.kind == "command" else check.data.get("id"))
    if owner != who:
        raise Rejected(f"{check.domain}.{check.type} is not allowed for {who}", "forbidden")


def serve_health(port: int, streams) -> None:
    """Readiness only; the data is reachable through the kernel and nowhere else."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            ok = self.path == "/health" and all(s.ready.is_set() for s in streams)
            self.reply(200 if ok else 503, {"ok": ok})

        def reply(self, status: int, body) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    url, key, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
    data = sys.argv[4] if len(sys.argv) > 4 else "."
    os.makedirs(data, exist_ok=True)
    tls = None
    if os.environ.get("KERNABLE_TLS_CA"):
        tls = Tls(os.environ["KERNABLE_TLS_CA"], os.environ["KERNABLE_TLS_CERT"], os.environ["KERNABLE_TLS_KEY"])
    k = Kernable(url, key, tls=tls)
    accounts_readmodel = accounts_read_model(f"sqlite:{data}/accounts.db")
    streams = [k.worker(account_aggregate), k.listener(accounts_readmodel), k.listener(rbac_readmodel)]
    for s in streams:
        threading.Thread(target=s.run, daemon=True).start()
    serve_health(port, streams)
