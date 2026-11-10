"""An example API on http.server over the kit's API part: how a framework wires `KitApi` in.
Carry the routes over to Flask, Django or FastAPI (with `AsyncKitApi`); the kit's functions stay.

    KERNABLE_URL=... KERNABLE_KEY=kit.key python -m example.server      (port 3000, or PORT)

Requests carry `Authorization: Bearer <access token>` (or `ApiKey <key>`) and, inside a tenant,
`X-Tenant: <tenant ID>`; the server turns them into the request context the middleware decides on.
"""

import json
import os
import re
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kernable import Kernable, Rejected, RequestContext

from kit.api.api import KitApi
from kit.shared.constants import PLATFORM_TENANT

def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(w.title() for w in rest)


def camel(value):
    """The kit's answers (snake_case keys) as the JSON the example serves: camelCase, like the other kits' examples."""
    if isinstance(value, dict):
        return {_camel(k): camel(v) for k, v in value.items()}
    if isinstance(value, list):
        return [camel(v) for v in value]
    return value


def context_json(c: RequestContext) -> dict:
    """The request context as JSON: its fields that are set."""
    out = {name: value for name, value in [("senderAccount", c.sender_account), ("senderSession", c.sender_session),
                                            ("senderTenant", c.sender_tenant), ("senderIdentity", c.sender_identity),
                                            ("onBehalfOf", c.on_behalf_of)] if value}
    if c.bypass_authorization:
        out["bypassAuthorization"] = True
    if c.attributes:
        out["attributes"] = c.attributes
    return out


COMMAND = re.compile(r"^/commands/([\w-]+)/([\w-]+)/(\w+)$")
QUERY = re.compile(r"^/queries/([\w-]+)/(\w+)$")


def tenant_for(domain: str, context: RequestContext) -> str | None:
    """Where a request of `domain` goes: accounts and sessions live in the platform tenant, the rest in the caller's."""
    return PLATFORM_TENANT if domain == "account" else context.sender_tenant


def example_server(api: KitApi, kernable: Kernable, port: int = 3000) -> ThreadingHTTPServer:
    """The routes; `kernable` submits the app's own commands and asks its queries with the caller's context. Call
    `serve_forever()` on the result (it listens on 127.0.0.1:`port`; 0 picks a free port)."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # quiet; log the way your framework does
            pass

        def send(self, status: int, value=None) -> None:
            raw = b"" if value is None else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def body(self) -> dict:
            raw = self.rfile.read(int(self.headers.get("content-length") or 0))
            try:
                value = json.loads(raw) if raw else {}
            except ValueError:
                return {}
            return value if isinstance(value, dict) else {}

        def claims(self):
            auth = self.headers.get("authorization") or ""
            return api.check(auth[7:]) if auth.startswith("Bearer ") else None

        def caller(self) -> RequestContext | None:
            """The caller's request context, from the bearer token or API key and the tenant header; None if none."""
            auth = self.headers.get("authorization") or ""
            if auth.startswith("ApiKey "):
                return api.check_api_key(auth[7:])
            claims = self.claims()
            if not claims:
                return None
            try:
                return api.context_for(claims, self.headers.get("x-tenant"))
            except Rejected:
                return None  # no identity in that tenant

        def do_POST(self) -> None:  # noqa: N802: http.server's name
            self.handle_route("POST", self.body())

        def do_GET(self) -> None:  # noqa: N802: http.server's name
            self.handle_route("GET", {})

        def handle_route(self, method: str, b: dict) -> None:
            try:
                if method == "POST":
                    self.route(b)
                else:
                    self.in_tenant("GET", self.path.split("?")[0], b)
            except Exception as err:  # noqa: BLE001: an example; your framework answers 500 its way
                self.send(500, {"error": str(err)})

        def route(self, b: dict) -> None:
            path, ip = self.path.split("?")[0], self.client_address[0]
            if path == "/register":
                api.register(b.get("email", ""), b.get("name", ""), b.get("password"))
                return self.send(202, {"message": "Check your email"})  # the same whether or not the address was taken
            if path == "/verify":
                return self.send(200 if api.verify_email(b.get("token", "")) else 400, {})
            if path == "/sign-in":
                r = api.sign_in(b.get("email", ""), b.get("password", ""), ip=ip, user_agent=self.headers.get("user-agent"))
                if not r["ok"]:
                    return self.send(401, {"error": r["reason"]})
                return self.send(200, {"accessToken": r["access_token"], "refreshToken": r["refresh_token"], "secondFactor": r["second_factor"]})
            if path == "/refresh":
                r = api.refresh(b.get("refreshToken", ""), ip)
                return self.send(200 if r["ok"] else 401, camel(r))
            if path == "/password-reset":
                api.request_password_reset(b.get("email", ""))
                return self.send(202, {"message": "Check your email"})
            if path == "/password-reset/confirm":
                return self.send(200 if api.reset_password(b.get("token", ""), b.get("password", ""))["ok"] else 400, {})
            if path == "/invitations/accept":
                claims = self.claims()
                r = (api.accept_invitation(b.get("token", ""), claims=claims) if claims
                     else api.accept_invitation(b.get("token", ""), name=b.get("name"), password=b.get("password")))
                return self.send(200 if r["ok"] else 400, camel(r))
            # Everything below needs a signed-in caller.
            claims = self.claims()
            if path == "/second-factor":
                if not claims:
                    return self.send(401, {"error": "unauthenticated"})
                r = api.prove_second_factor(claims, b.get("code", ""))
                return self.send(200 if r["ok"] else 401, camel(r))
            if path == "/sign-out":
                if claims:
                    api.sign_out(claims)
                return self.send(204)
            if path == "/tenants":
                # Opens a tenant the caller owns (self-service, as the sign-up policy allows).
                if not claims:
                    return self.send(401, {"error": "unauthenticated"})
                tenant = str(uuid.uuid4())
                outcome = kernable.submit("tenancy", "tenant", tenant, "CreateTenant",
                                          {"slug": b.get("slug"), "name": b.get("name"), "owner": {"accountId": claims.sub}},
                                          tenant=tenant, context=api.context_for(claims))
                return self.send(201, {"tenant": tenant}) if outcome.committed else self.send(403, {"error": outcome.code})
            if path == "/invitation-links/join":
                if not claims:
                    return self.send(401, {"error": "unauthenticated"})
                r = api.join_with_invitation_link(claims, b.get("token", ""))
                return self.send(200 if r["ok"] else 400, camel(r))
            self.in_tenant("POST", path, b)

        def in_tenant(self, method: str, path: str, b: dict) -> None:
            """Below: as the caller, in the tenant of `X-Tenant`."""
            context = self.caller()
            if not context:
                return self.send(401, {"error": "unauthenticated"})
            route = f"{method} {path}"
            if route == "GET /me":
                return self.send(200, context_json(context))
            if route == "POST /invitation-links":
                r = api.create_invitation_link(context, context.sender_tenant, b.get("groups") or [], label=b.get("label"),
                                               expires_at=b.get("expiresAt"), max_uses=b.get("maxUses"))
                if not r["ok"]:
                    return self.send(403, {"error": r["reason"]})
                return self.send(201, {"token": r["token"], "linkId": r["link_id"]})
            # POST /commands/<domain>/<aggregate_type>/<Type>: the app's commands, as the caller; `account`
            # ones go to the platform tenant, where every account lives.
            m = COMMAND.match(path) if method == "POST" else None
            if m:
                outcome = kernable.submit(m.group(1), m.group(2), b.get("aggregateId") or str(uuid.uuid4()), m.group(3), b.get("data") or {},
                                          tenant=tenant_for(m.group(1), context), context=context)
                if outcome.committed:
                    return self.send(200, {"events": [e.type for e in outcome.events]})
                return self.send(403, {"error": outcome.code})
            # POST /queries/<domain>/<Type>: the app's queries, as the caller.
            m = QUERY.match(path) if method == "POST" else None
            if m:
                try:
                    return self.send(200, kernable.query(m.group(1), m.group(2), b.get("data") or {}, tenant=tenant_for(m.group(1), context), context=context))
                except Rejected as err:
                    return self.send(404 if err.code == "not_found" else 403, {"error": err.code})
            self.send(404, {"error": "not_found"})

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def main() -> None:
    """The kit as one service, the bootstrap (it may run again), and the server."""
    from kit import kit_domains, kit_schemas
    from kit.api.tokens import new_signing_keys, signing_keys_from
    from kit.bootstrap import bootstrap

    app = Kernable().app(service="kit", domains=kit_domains())
    threading.Thread(target=app.run, daemon=True).start()
    if not app.ready.wait(30):
        sys.exit("the kit's service did not connect")
    # The API's client knows the kit's schemas, so it seals the vault fields before signing.
    kernable = Kernable().use(*kit_schemas())
    service = bootstrap(kernable, sign_up={"tenants": "active", "accounts": "open"})["api"]
    # The access token key: a PEM file in production (KIT_SIGNING_KEY), new on every start otherwise.
    keys = signing_keys_from(os.environ["KIT_SIGNING_KEY"]) if os.environ.get("KIT_SIGNING_KEY") else new_signing_keys()
    port = int(os.environ.get("PORT", "3000"))
    server = example_server(KitApi(kernable, keys, service), kernable, port)
    print(f"example server on :{port}", file=sys.stderr)
    server.serve_forever()


if __name__ == "__main__":
    main()
