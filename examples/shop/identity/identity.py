"""Identity service (Python): the Account aggregate, its worker, the `accounts` read model and
the `rbac` middleware. Other services read accounts only through the query
`identity.GetAccount`, via the kernel.

Personal data (email, name, the password hash) lives in the kernel's vault, never in the signed
log; the password itself is a secret that is gone once its command ran. `EraseAccount` erases an
account's personal data everywhere, also in the orders domain.

Usage: python identity.py <kernel-url> <key> <health-port>
Mutual TLS: set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY.
"""

import hashlib
import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kernable import Check, Kernable, Rejected, Tls, domain

EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")

# ---- write side ----

identity = domain("identity")
# The vault keeps these fields; the subject is the account (the aggregate's ID).
identity.command("RegisterAccount").personal(["email", "name"], lambda d, account: account).secret(["password"])
identity.command("ChangeEmail").personal(["email"], lambda d, account: account)
identity.command("SignIn").secret(["password"])
identity.event("AccountRegistered", 1).personal(["email", "name", "passwordHash"], lambda d, account: account)
identity.event("EmailChanged", 1).personal(["email"], lambda d, account: account)
identity.event("AccountErased", 1).erases(lambda d, account: account)

account = identity.aggregate("Account", {"status": "none"})


def password_hash(password: str, salt: str) -> str:
    return hashlib.scrypt(password.encode(), salt=salt.encode(), n=2**14, r=8, p=1).hex()


@account.apply("AccountRegistered")
def _(state, e):
    return {"status": "active", "email": e["email"], "name": e["name"], "passwordHash": e["passwordHash"],
            "salt": e["salt"]}


@account.apply("EmailChanged")
def _(state, e):
    return {**state, "email": e["email"]}


@account.apply("AccountDeactivated")
def _(state, e):
    return {**state, "status": "deactivated"}


@account.apply("AccountErased")
def _(state, e):
    return {"status": "erased"}


@account.command("RegisterAccount")
def _(state, cmd, ctx):
    if state["status"] != "none":
        raise Rejected("Account already exists", "exists")
    if not EMAIL.match(cmd.get("email", "")):
        raise Rejected("Invalid email", "invalid")
    if len(cmd.get("password") or "") < 8:
        raise Rejected("The password needs at least 8 characters", "invalid")
    salt = ctx.id()  # deterministic, so the command may run again
    return [{"type": "AccountRegistered", "data": {"email": cmd["email"], "name": cmd.get("name", ""),
                                                   "passwordHash": password_hash(cmd["password"], salt),
                                                   "salt": salt}}]


@account.command("SignIn")
def _(state, cmd):
    if state["status"] != "active" or password_hash(cmd.get("password") or "", state["salt"]) != state["passwordHash"]:
        raise Rejected("Wrong account or password", "denied")
    return [{"type": "SignedIn"}]


@account.command("EraseAccount")
def _(state, cmd):
    if state["status"] in ("none", "erased"):
        raise Rejected(f"Cannot erase a {state['status']} account", "invalid_state")
    return [{"type": "AccountErased"}]


@account.command("ChangeEmail")
def _(state, cmd):
    if state["status"] != "active":
        raise Rejected(f"Cannot change the email of a {state['status']} account", "invalid_state")
    if not EMAIL.match(cmd.get("email", "")):
        raise Rejected("Invalid email", "invalid")
    return [{"type": "EmailChanged", "data": {"email": cmd["email"]}}]


@account.command("DeactivateAccount")
def _(state, cmd):
    if state["status"] != "active":
        raise Rejected(f"Cannot deactivate a {state['status']} account", "invalid_state")
    return [{"type": "AccountDeactivated"}]


# ---- read side: in memory, rebuilt from the first event on every start ----


def accounts_model(k: Kernable):
    accounts: dict[str, dict] = {}
    model = k.read_model("accounts")

    @model.on("identity.AccountRegistered")
    def _(e):
        accounts[e.aggregate_id] = {"id": e.aggregate_id, "status": "active", "email": e.data["email"],
                                    "name": e.data["name"]}

    @model.on("identity.EmailChanged")
    def _(e):
        accounts[e.aggregate_id]["email"] = e.data["email"]

    @model.on("identity.AccountDeactivated")
    def _(e):
        accounts[e.aggregate_id]["status"] = "deactivated"

    @model.on("identity.AccountErased")
    def _(e):
        accounts.pop(e.aggregate_id, None)

    @model.query("identity.GetAccount")
    def _(data, ctx):
        if data["id"] not in accounts:
            raise Rejected(f"no account {data['id']}", "not_found")
        return accounts[data["id"]]

    return model


# ---- authorization: a middleware for both domains, from its own read model ----

#: Services that may read on behalf of others, e.g. the orders reactor looking up an email.
SERVICES = {"svc-orders"}


def rbac_middleware(k: Kernable):
    """Checks every command and query of both domains before the kernel accepts it. The sender
    is the request context's `sender_identity`; the API asserts it (signed with its key)."""
    active: set[str] = set()
    customer_of: dict[str, str] = {}  # order -> account
    model = k.read_model("rbac")

    @model.on("identity.AccountRegistered")
    def _(e):
        active.add(e.aggregate_id)

    @model.on("identity.AccountDeactivated")
    def _(e):
        active.discard(e.aggregate_id)

    @model.on("identity.AccountErased")
    def _(e):
        active.discard(e.aggregate_id)

    @model.on("orders.OrderPlaced")
    def _(e):
        customer_of[e.aggregate_id] = e.data["customerId"]

    @model.middleware(["identity.*", "orders.*"])
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

    return model


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
    tls = None
    if os.environ.get("KERNABLE_TLS_CA"):
        tls = Tls(os.environ["KERNABLE_TLS_CA"], os.environ["KERNABLE_TLS_CERT"], os.environ["KERNABLE_TLS_KEY"])
    k = Kernable(url, key, tls=tls)
    streams = [k.worker(account), accounts_model(k), rbac_middleware(k)]
    for s in streams:
        threading.Thread(target=s.run, daemon=True).start()
    serve_health(port, streams)
