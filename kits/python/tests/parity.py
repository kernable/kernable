"""The kit parity runner for Python (see ../../parity/README.md). Run by ../../parity/run.ts:

    python -m tests.parity host     # the kit's domains and the parity shop, until SIGTERM
    python -m tests.parity driver   # runs the scenario through KitApi, writes the transcript

Both read PARITY_URL, PARITY_KEY, PARITY_DIR; the driver also PARITY_SCENARIO, PARITY_OUT,
PARITY_KERNABLE (the kernel binary) and PARITY_STORE. Results are written as the log holds
values (camelCase keys), so they compare with the other languages'.
"""

import dataclasses
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid

from kernable import Aggregate, Command, Event, Kernable, Query, Rejected, RequestContext, domain
from kernable.messages import snake, to_wire

sys.path.insert(0, os.path.dirname(__file__))
from authenticator import Authenticator  # noqa: E402
from oidc_provider import OidcProvider  # noqa: E402

from kit import kit_domains, kit_schemas, relation, use_authorization_store, visibility  # noqa: E402
from kit.api.api import KitApi  # noqa: E402
from kit.api.tokens import Claims, new_signing_keys  # noqa: E402
from kit.bootstrap import bootstrap  # noqa: E402
from kit.shared.mail import use_mailer  # noqa: E402
from kit.shared.secrets import totp_code  # noqa: E402

URL = os.environ["PARITY_URL"]
KEY = os.environ["PARITY_KEY"]
DIR = os.environ["PARITY_DIR"]

# --- The parity shop: the same in every language ---------------------------------------------------

shop = domain("shop")


class OrderPlaced(Event):
    customer: str


class OrderCancelled(Event):
    pass


class PlaceOrder(Command):
    pass


class CancelOrder(Command):
    pass


class Order(Aggregate, domain=shop):
    status: str = "none"
    customer: str | None = None


@Order.apply(OrderPlaced)
def _(s: Order, d: OrderPlaced):
    s.update(status="placed", customer=d.customer)


@Order.apply(OrderCancelled)
def _(s: Order):
    s.status = "cancelled"


@Order.command(PlaceOrder)
def _(s: Order, d, cmd, ctx):
    if s.status != "none":
        raise Rejected("The order exists", "exists")
    return [OrderPlaced(customer=ctx.request.sender_identity)]


@Order.command(CancelOrder)
def _(s: Order, d):
    if s.status != "placed":
        raise Rejected("Not placed", "invalid_state")
    return [OrderCancelled()]


relation("shop.order", "owner").set("shop.OrderPlaced", lambda e: e.data.get("customer"))


class ListOrders(Query[list]):
    pass


order_rows: dict[str, dict] = {}
orders_model = shop.read_model("shop.orders")


@orders_model.on(OrderPlaced)
def _(d, evt):
    order_rows[f"{evt.tenant}/{evt.aggregate_id}"] = {"tenant": evt.tenant, "id": evt.aggregate_id}


visible = visibility(orders_model)


@orders_model.query(ListOrders)
def _(d, qry, ctx):
    return sorted(o["id"] for o in order_rows.values()
                  if o["tenant"] == qry.tenant and visible(qry.tenant, ctx.request, "shop.GetOrder", {"type": "shop.order", "id": o["id"]}))


class GetOrder(Query[dict]):
    target = {"aggregate_type": "order", "field": "id"}
    id: str


customer_of: dict[str, str] = {}
order_details = shop.read_model("shop.order-details")


@order_details.on(OrderPlaced)
def _(d: OrderPlaced, evt):
    customer_of[f"{evt.tenant}/{evt.aggregate_id}"] = d.customer


@order_details.query(GetOrder)
def _(d: GetOrder, qry):
    customer = customer_of.get(f"{qry.tenant}/{d.id}")
    if not customer:
        raise Rejected("No such order", "not_found")
    return {"customer": customer}


class ParcelRequested(Event):
    pass


class ParcelShipped(Event):
    pass


class RequestParcel(Command):
    pass


class ShipParcel(Command):
    pass


class Parcel(Aggregate, domain=shop):
    requested: bool = False
    shipped: bool = False


@Parcel.apply(ParcelRequested)
def _(s: Parcel):
    s.requested = True


@Parcel.apply(ParcelShipped)
def _(s: Parcel):
    s.shipped = True


@Parcel.command(RequestParcel)
def _(s: Parcel):
    return [ParcelRequested()]


@Parcel.command(ShipParcel)
def _(s: Parcel):
    if s.shipped:
        raise Rejected("Shipped already", "invalid_state")
    return [ParcelShipped()]


(shop.process("dispatch")
 .start(ParcelRequested, key="aggregate_id", data={"parcel_id": "aggregate_id"})
 .command("ship", ShipParcel, aggregate="case.parcel_id")
 .end("done"))

# Mails go to a file both processes see: the host's reactors send, the driver reads.
MAILS = os.path.join(DIR, "mails.jsonl")
_mail_lock = threading.Lock()


class FileMailer:
    def send(self, mail) -> None:
        line = json.dumps({"to": mail.to, "subject": mail.subject, "text": mail.text, "purpose": mail.purpose, "token": mail.token})
        with _mail_lock, open(MAILS, "a") as f:
            f.write(line + "\n")


def read_mails() -> list[dict]:
    if not os.path.exists(MAILS):
        return []
    with open(MAILS) as f:
        return [json.loads(line) for line in f if line.strip()]


# --- Host ------------------------------------------------------------------------------------------

def host() -> None:
    use_mailer(FileMailer())
    use_authorization_store(f"sqlite:{os.path.join(DIR, 'authorization.db')}")
    app = Kernable(URL, KEY).app(service="kit", domains=[*kit_domains(), shop])
    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()
    if not app.ready.wait(60):
        raise SystemExit("the app did not connect")
    print("ready", flush=True)
    stopped = threading.Event()

    def stop(*_):
        app.stop()
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    stopped.wait()
    thread.join(5)
    os._exit(0)


# --- Driver ----------------------------------------------------------------------------------------

def context_json(c: RequestContext | None):
    if c is None:
        return None
    out = {}
    for k in ("sender_tenant", "sender_identity", "sender_account", "sender_session", "on_behalf_of"):
        v = getattr(c, k, "")
        if v:
            out[to_wire({k: 1}).popitem()[0]] = v
    if getattr(c, "bypass_authorization", False):
        out["bypassAuthorization"] = True
    return out


def to_context(j) -> RequestContext:
    return RequestContext(**{snake(k): v for k, v in (j or {}).items()})


def canonical(v):
    """A result as the log holds values: camelCase keys, dataclasses as dicts."""
    if isinstance(v, RequestContext):
        return context_json(v)
    if dataclasses.is_dataclass(v) and not isinstance(v, type):
        return canonical(dataclasses.asdict(v))
    if isinstance(v, dict):
        return _canon_dict(v)
    if isinstance(v, (list, tuple)):
        return [canonical(x) for x in v]
    return v


def _canon_dict(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        key = next(iter(to_wire({k: 0}))) if isinstance(k, str) else k
        out[key] = canonical(v)
    return out


def matches(expected, actual) -> bool:
    if not isinstance(expected, dict):
        return json.dumps(expected, sort_keys=True) == json.dumps(actual, sort_keys=True)
    if not isinstance(actual, dict):
        return False
    return all(matches(v, actual.get(k)) for k, v in expected.items())


def driver() -> None:
    with open(os.environ["PARITY_SCENARIO"]) as f:
        scenario = json.load(f)
    kernable_bin = os.environ["PARITY_KERNABLE"]
    store = os.environ["PARITY_STORE"]
    kit = Kernable(URL, KEY).use(*kit_schemas(), shop)
    vars_: dict = {}
    apis: dict[str, KitApi] = {}
    devices: dict[str, Authenticator] = {}
    providers: dict[str, OidcProvider] = {}
    txt: dict[str, str] = {}
    state = {"consumed": 0, "service": None}

    def lookup(path: str):
        head, *rest = path.split(".")
        if head not in vars_:
            raise KeyError(f"unknown variable {head}")
        v = vars_[head]
        for p in rest:
            if v is None:
                return None
            v = v[int(p)] if isinstance(v, list) else v.get(p)
        return v

    def resolve(v):
        if isinstance(v, str):
            return lookup(v[1:]) if v.startswith("$") else v
        if isinstance(v, list):
            return [resolve(x) for x in v]
        if isinstance(v, dict):
            if "concat" in v:
                return "".join(str(resolve(x)) for x in v["concat"])
            if "merge" in v:
                out = {}
                for x in v["merge"]:
                    out.update(resolve(x))
                return out
            if "tamper" in v:
                s = resolve(v["tamper"])
                if v["how"] == "lastChar":
                    return s[:-1] + ("B" if s.endswith("A") else "A")
                if v["how"] == "secret":
                    return s[:s.rfind(".")] + ".not-the-secret"
                raise ValueError(f"unknown tamper {v['how']}")
            if "totp" in v:
                return totp_code(resolve(v["totp"]), resolve(v["at"]) + v.get("plus", 0))
            return {k: resolve(x) for k, x in v.items()}
        return v

    def api(s) -> KitApi:
        name = s.get("api", "api")
        if name not in apis:
            raise KeyError(f"no KitApi {name}")
        return apis[name]

    def claims_of(s, ref):
        v = resolve(ref)
        if isinstance(v, str):
            return api(s).check(v)
        if v is None:
            return None
        return Claims(sub=v["sub"], sid=v["sid"], fac=v["fac"], iat=v.get("iat", 0), exp=v.get("exp", 0))

    def run(s):
        op = s["op"]

        def p(k):
            return resolve(s.get(k))

        if op == "uuid":
            return str(uuid.uuid4())
        if op == "value":
            return p("value")
        if op == "bootstrap":
            booted = bootstrap(kit, sign_up=p("signUp"), admin=p("admin"))
            state["service"] = booted["api"]
            apis["api"] = KitApi(kit, new_signing_keys(), booted["api"])
            return booted
        if op == "kitApi":
            config = {}
            for name, handle in (s.get("providers") or {}).items():
                pr = providers[handle]
                config[name] = {"issuer": pr.issuer, "client_id": pr.client_id, "client_secret": pr.client_secret}
            apis[s["as"]] = KitApi(kit, new_signing_keys(), state["service"], providers=config, redirect_uri=p("redirectUri"),
                                   insecure_issuers=True, resolve_txt=lambda name: [txt[name]] if name in txt else [])
            return None
        if op == "register":
            return api(s).register(p("email"), p("name"), p("password"))
        if op == "mail":
            to = p("to")
            for _ in range(400):
                mails = read_mails()
                found = max((i for i, m in enumerate(mails) if m["to"] == to), default=-1)
                if found >= 0 and (s.get("old") or found >= state["consumed"]):
                    state["consumed"] = max(state["consumed"], found + 1)
                    return mails[found]
                time.sleep(0.025)
            raise RuntimeError(f"no new mail to {to}")
        if op == "mailCount":
            time.sleep(s.get("settleMs", 300) / 1000)
            n = len(read_mails())
            state["consumed"] = max(state["consumed"], n)
            return n
        if op == "verifyEmail":
            return api(s).verify_email(p("token"))
        if op == "signIn":
            return api(s).sign_in(p("email"), p("password"), **({"ip": p("ip")} if "ip" in s else {}))
        if op == "check":
            return api(s).check(p("token"))
        if op == "contextFor":
            return context_json(api(s).context_for(claims_of(s, s["claims"]), p("tenant") if "tenant" in s else None))
        if op == "refresh":
            return api(s).refresh(p("token"))
        if op == "signOut":
            api(s).sign_out(claims_of(s, s["claims"]))
            return None
        if op == "startTotp":
            return api(s).start_totp(claims_of(s, s["claims"]), p("issuer"), p("label"))
        if op == "totpStep":
            return int(time.time() // 30)
        if op == "confirmTotp":
            return api(s).confirm_totp(claims_of(s, s["claims"]), p("code"))
        if op == "proveSecondFactor":
            return api(s).prove_second_factor(claims_of(s, s["claims"]), p("code"))
        if op == "createApiKey":
            return api(s).create_api_key(to_context(p("context")), p("tenant"), p("identityId"), p("label") or "")
        if op == "checkApiKey":
            return context_json(api(s).check_api_key(p("key")))
        if op == "changePassword":
            return api(s).change_password(claims_of(s, s["claims"]), p("current"), p("next"))
        if op == "requestPasswordReset":
            api(s).request_password_reset(p("email"))
            return None
        if op == "resetPassword":
            return api(s).reset_password(p("token"), p("next"))
        if op == "requestEmailChange":
            return api(s).request_email_change(claims_of(s, s["claims"]), p("email"))
        if op == "confirmEmailChange":
            return api(s).confirm_email_change(p("token"))
        if op == "peekInvitation":
            return api(s).peek_invitation(p("token"))
        if op == "acceptInvitation":
            if "claims" in s:
                return api(s).accept_invitation(p("token"), claims=claims_of(s, s["claims"]))
            return api(s).accept_invitation(p("token"), name=p("name"), password=p("password"))
        if op == "createInvitationLink":
            groups = [{snake(k): v for k, v in g.items()} for g in (p("groups") or [])]
            return api(s).create_invitation_link(to_context(p("context")), p("tenant"), groups, label=p("label"), max_uses=p("maxUses"))
        if op == "peekInvitationLink":
            return api(s).peek_invitation_link(p("token"))
        if op == "joinWithInvitationLink":
            return api(s).join_with_invitation_link(claims_of(s, s["claims"]), p("token"))
        if op == "verifySsoDomain":
            return api(s).verify_sso_domain(p("tenant"), p("ssoId"), p("domain"))
        if op == "serviceContext":
            return context_json(api(s).service_context)
        if op == "authenticator":
            devices[s["as"]] = Authenticator("localhost", "http://localhost:3000", p("userId"))
            return None
        if op == "passkeyRegistrationOptions":
            return api(s).passkey_registration_options(claims_of(s, s["claims"]), p("userName"))
        if op == "passkeyRegister":
            return devices[s["device"]].register(p("challenge"))
        if op == "passkeyAssert":
            return devices[s["device"]].assert_(p("challenge"), p("origin") if "origin" in s else None)
        if op == "addPasskey":
            return api(s).add_passkey(claims_of(s, s["claims"]), p("response"), p("label") if "label" in s else None)
        if op == "passkeySignInOptions":
            return api(s).passkey_sign_in_options()
        if op == "signInWithPasskey":
            return api(s).sign_in_with_passkey(p("response"))
        if op == "oidcProvider":
            pr = OidcProvider()
            pr.start() if hasattr(pr, "start") else None
            providers[s["as"]] = pr
            return None
        if op == "oidcIssuer":
            pr = providers[s["provider"]]
            return {"issuer": pr.issuer, "clientId": pr.client_id, "clientSecret": pr.client_secret}
        if op == "setTxt":
            txt[p("name")] = p("value")
            return None
        if op == "oidcStart":
            by = {snake(k): v for k, v in p("by").items()}
            return api(s).oidc_start(link=claims_of(s, s["link"]) if "link" in s else None, **by)
        if op == "oidcLogin":
            return providers[s["provider"]].login(p("url"), p("user"))
        if op == "oidcFinish":
            return api(s).oidc_finish(p("callback"))
        if op == "submit":
            d, aggregate_type, type_ = s["command"].split(".")
            out = kit.submit(d, aggregate_type, p("aggregateId"), type_, p("data") or {}, tenant=p("tenant"), context=to_context(p("context")))
            if out.committed:
                return {"committed": True, "events": [{"type": e.type, "data": to_wire(e.data)} for e in out.events]}
            return {"committed": False, "code": out.code, "message": out.message}
        if op == "query":
            d, type_ = s["query"].split(".")
            try:
                return {"ok": True, "value": kit.query(d, type_, p("data") or {}, tenant=p("tenant"), context=to_context(p("context")))}
            except Rejected as e:
                return {"ok": False, "code": e.code, "message": str(e)}
        if op == "processIdentityId":
            return f"process-{p('process')}"
        if op == "processAct":
            return {"changed": kit.process_act(p("process"), to_context(p("context")))}
        if op == "waitIdentityKey":
            for _ in range(400):
                try:
                    identity = kit.query("identity", "GetIdentity", {"identityId": p("identityId")}, tenant="platform",
                                         context=RequestContext(bypass_authorization=True))
                    keys = identity.get("api_keys") or identity.get("apiKeys") or []
                    if keys:
                        k = keys[0]
                        return k.get("key_id") or k.get("keyId")
                except Rejected:
                    pass
                time.sleep(0.025)
            raise RuntimeError("no key for the identity")
        if op == "waitCase":
            for _ in range(400):
                out = subprocess.run([kernable_bin, "processes", "cases", store, p("process"), "--state", p("state")],
                                     capture_output=True, text=True).stdout
                if any(f" {p('case')} " in line for line in out.splitlines()):
                    return {"state": p("state")}
                time.sleep(0.025)
            return {"state": "not reached"}
        if op == "retryCase":
            subprocess.run([kernable_bin, "processes", "retry", URL, KEY, p("process"), p("case"), "--tenant", p("tenant")],
                           capture_output=True, text=True, check=True)
            return None
        if op == "restartHost":
            flag = os.path.join(DIR, "restart")
            open(flag, "w").close()
            for _ in range(1200):
                if not os.path.exists(flag):
                    return None
                time.sleep(0.025)
            raise RuntimeError("the host did not restart")
        raise ValueError(f"unknown op {op}")

    transcript = []
    for i, s in enumerate(scenario["steps"]):
        try:
            result = canonical(run(s))
            result = json.loads(json.dumps(result))
        except Rejected as e:
            result = {"error": e.code}
        except Exception as e:  # noqa: BLE001
            result = {"error": f"{type(e).__name__}: {e}"}
        if s.get("as"):
            vars_[s["as"]] = result
        entry = {"step": i, "op": s["op"], **({"note": s["note"]} if s.get("note") else {}), "result": result}
        if "expect" in s:
            expected = resolve(s["expect"])
            if not matches(expected, result):
                entry["unexpected"] = expected
        transcript.append(entry)
    for pr in providers.values():
        pr.stop()
    with open(os.environ["PARITY_OUT"], "w") as f:
        json.dump(transcript, f, indent=1)
    kit.close() if hasattr(kit, "close") else None
    os._exit(0)


if __name__ == "__main__":
    {"host": host, "driver": driver}[sys.argv[1]]()
