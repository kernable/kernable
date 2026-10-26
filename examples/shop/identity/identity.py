"""Identity service (Python): the Account aggregate, its worker and the `accounts` read model.
Other services read accounts only through the query `identity.GetAccount`, via the kernel.

Usage: python identity.py <kernel-url> <key> <health-port>
Mutual TLS: set KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY.
"""

import json
import os
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kernable import Kernable, Rejected, Tls, domain

EMAIL = re.compile(r"^[^@\s]+@[^@\s]+$")

# ---- write side ----

account = domain("identity").aggregate("Account", {"status": "none"})


@account.apply("AccountRegistered")
def _(state, e):
    return {"status": "active", "email": e["email"], "name": e["name"]}


@account.apply("EmailChanged")
def _(state, e):
    return {**state, "email": e["email"]}


@account.apply("AccountDeactivated")
def _(state, e):
    return {**state, "status": "deactivated"}


@account.command("RegisterAccount")
def _(state, cmd):
    if state["status"] != "none":
        raise Rejected("Account already exists", "exists")
    if not EMAIL.match(cmd.get("email", "")):
        raise Rejected("Invalid email", "invalid")
    return [{"type": "AccountRegistered", "data": {"email": cmd["email"], "name": cmd.get("name", "")}}]


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
        accounts[e.aggregate_id] = {"id": e.aggregate_id, "status": "active", **e.data}

    @model.on("identity.EmailChanged")
    def _(e):
        accounts[e.aggregate_id]["email"] = e.data["email"]

    @model.on("identity.AccountDeactivated")
    def _(e):
        accounts[e.aggregate_id]["status"] = "deactivated"

    @model.query("identity.GetAccount")
    def _(data, ctx):
        if data["id"] not in accounts:
            raise Rejected(f"no account {data['id']}", "not_found")
        return accounts[data["id"]]

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
    streams = [k.worker(account), accounts_model(k)]
    for s in streams:
        threading.Thread(target=s.run, daemon=True).start()
    serve_health(port, streams)
