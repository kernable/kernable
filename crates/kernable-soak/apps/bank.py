"""The soak's Python app: domain bank_py, the same as bank.ts and the Rust app. Accounts take
deposits and withdrawals (never below zero); a read model keeps balances in a store; a reactor
flags every withdrawal of 50 or more, once. An account holds its number as a unique key and,
while open, the fact open, which a withdrawal sent to an account requires of that account. The
soak checks all three against the log."""

import os

from kernable import Kernable, Rejected, Requirement, domain

bank = domain("bank_py")
account = bank.aggregate("account", {"balance": 0, "open": False, "number": None}, snapshot_every=5)
account.unique("number", lambda s: s["number"])
account.fact("open", lambda s: s["open"])


@account.apply("Deposited")
def _(s, d):
    s["balance"] += d["amount"]


@account.apply("Withdrawn")
def _(s, d):
    s["balance"] -= d["amount"]


@account.apply("Opened")
def _(s, d):
    s["open"] = True


@account.apply("Closed")
def _(s, d):
    s["open"] = False


@account.apply("Numbered")
def _(s, d):
    s["number"] = d["number"]


@account.command("Deposit")
def _(s, d):
    if not d.get("amount", 0) > 0:
        raise Rejected("amount must be positive", "invalid")
    return [{"type": "Deposited", "data": {"amount": d["amount"]}}]


@account.command("Withdraw", requires=lambda d, s: [Requirement("bank_py", "account", d["to"], "open")]
                 if d.get("to") else [])
def _(s, d):
    if s["balance"] < d["amount"]:
        raise Rejected("insufficient funds", "insufficient")
    return [{"type": "Withdrawn", "data": {"amount": d["amount"], "to": d.get("to")}}]


@account.command("Open")
def _(s, d):
    return [{"type": "Opened", "data": {}}]


@account.command("Close")
def _(s, d):
    return [{"type": "Closed", "data": {}}]


@account.command("Number")
def _(s, d):
    return [{"type": "Numbered", "data": {"number": d["number"]}}]


@account.command("Flag")
def _(s, d):
    return [{"type": "Flagged", "data": {"position": d["position"]}}]


balances = bank.read_model("balances-py", store=f"sqlite:{os.environ['SOAK_STORE']}", schema="1")


def row(store, id):
    return store.get("accounts", id) or {"balance": 0, "flags": 0}


@balances.on("Deposited")
def _(d, evt, ctx):
    r = row(ctx.store, evt.aggregate_id)
    ctx.store.set("accounts", evt.aggregate_id, {**r, "balance": r["balance"] + d["amount"]})


@balances.on("Withdrawn")
def _(d, evt, ctx):
    r = row(ctx.store, evt.aggregate_id)
    ctx.store.set("accounts", evt.aggregate_id, {**r, "balance": r["balance"] - d["amount"]})


@balances.on("Flagged")
def _(d, evt, ctx):
    r = row(ctx.store, evt.aggregate_id)
    ctx.store.set("accounts", evt.aggregate_id, {**r, "flags": r["flags"] + 1})


@balances.query("GetAccount")
def _(d, qry, ctx):
    return row(ctx.store, d["id"])


k = Kernable()
flagger = bank.reactor("flagger-py")


@flagger.on("Withdrawn")
def _(d, evt, ctx):
    if d["amount"] < 50:
        return
    k.submit("bank_py", "account", evt.aggregate_id, "Flag", {"position": evt.position},
             id=f"flag-{evt.position}", wait="committed", context=ctx.follow())


k.app(service="bank-py", domains=[bank]).run()
