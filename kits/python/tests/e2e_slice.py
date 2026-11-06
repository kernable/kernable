"""The thin slice: the kit end to end against a real kernel (`cargo build` first). A shop app
declares a relation; people register, verify, sign in, open a tenant, form groups, and the
authorization middleware decides. The tests run in order and share the kernel.

    .venv/bin/python -m unittest tests.e2e_slice
"""

import json
import os
import sqlite3
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid

from kernable import Aggregate, Command, Event, Kernable, Query, Rejected, RequestContext, domain

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdks", "python", "tests"))
sys.path.insert(0, os.path.dirname(__file__))
from kernel import TestKernel  # noqa: E402

from authenticator import Authenticator  # noqa: E402
from example.server import example_server  # noqa: E402
from kit import Explain, kit_domains, relation, use_authorization_store, visibility  # noqa: E402
from kit.account.commands import AddSsoDomain, CreateSso, DeleteAccount, EnforceSso, SuspendAccount  # noqa: E402
from kit.account.queries import FindAccountByEmail, GetSso  # noqa: E402
from kit.api.api import KitApi  # noqa: E402
from kit.api.tokens import new_signing_keys  # noqa: E402
from kit.bootstrap import bootstrap  # noqa: E402
from kit.group.commands import CreateGroup  # noqa: E402
from kit.identity.commands import CreateServiceIdentity, DeleteIdentity, GrantMembership, RevokeApiKey  # noqa: E402
from kit.identity.queries import GetIdentity  # noqa: E402
from kit.invitation.commands import CreateInvitation, EraseInvitation  # noqa: E402
from kit.invitation.queries import ListInvitations  # noqa: E402
from kit.shared.mail import MemoryMailer, use_mailer  # noqa: E402
from kit.shared.secrets import totp_code  # noqa: E402
from kit.tenancy.commands import CreateTenant, DeleteTenant, SuspendTenant  # noqa: E402
from kit.tenancy.queries import GetTenant  # noqa: E402
from oidc_provider import OidcProvider  # noqa: E402

# --- A small app: orders that belong to whoever placed them ------------------------------------------

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


# The app's list of orders, showing each asker only what the middleware would let them get.
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


# --- Setup -------------------------------------------------------------------------------------------

mailer = MemoryMailer()
use_mailer(mailer)
PASSWORD = "correct horse battery"
S = {}  # what the tests share: kernel, kit, app, api, service, tenant, acme


def setUpModule():
    kernel = TestKernel(["tenancy.*", "account.*", "identity.*", "group.*", "invitation.*", "shop.*"])
    kernel.start()
    S["kernel"] = kernel
    for args in (["keys", "new", "kit"],
                 ["grant", "root.key", "kit.pub", "run=tenancy.*,account.*,identity.*,group.*,invitation.*,shop.*",
                  *(f"{a}=*" for a in ("submit", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage",
                                       "reserved_order", "cache")),
                  "privileged=bypass_authorization"]):
        assert kernel.cli(*args).returncode == 0, args
    S["key"] = os.path.join(kernel.dir, "kit.key")
    S["kit"] = Kernable(kernel.url, S["key"])
    use_authorization_store(f"sqlite:{os.path.join(kernel.dir, 'authorization.db')}")
    start_app()
    booted = bootstrap(S["kit"], sign_up={"tenants": "active", "accounts": "open"},
                       admin={"email": "admin@example.com", "name": "Admin", "password": PASSWORD})
    S["service"] = booted["api"]
    S["api"] = KitApi(S["kit"], new_signing_keys(), S["service"])


def tearDownModule():
    if "app" in S:
        stop_app()
    if "kernel" in S:
        S["kernel"].dispose()


def start_app():
    app = Kernable(S["kernel"].url, S["key"]).app(service="kit", domains=[*kit_domains(), shop])
    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()
    assert app.ready.wait(30), "the app did not connect"
    S["app"], S["thread"] = app, thread


def stop_app():
    S["app"].stop()
    S["thread"].join(10)


def person(email: str):
    api: KitApi = S["api"]
    api.register(email, email.partition("@")[0], PASSWORD)
    mail = mailer.last(email)
    assert mail, f"a verification mail to {email}"
    assert api.verify_email(mail.token)
    signed_in = api.sign_in(email, PASSWORD, ip="203.0.113.7")
    assert signed_in["ok"], signed_in
    return api.check(signed_in["access_token"]), signed_in["account_id"]


def submit(command, aggregate_id: str, tenant: str, context: RequestContext):
    return S["kit"].submit(command, aggregate_id=aggregate_id, tenant=tenant, context=context)


def new_id() -> str:
    return str(uuid.uuid4())


# --- The slice ---------------------------------------------------------------------------------------

class SliceTest(unittest.TestCase):
    maxDiff = None

    def rejected(self, outcome, code: str) -> None:
        self.assertFalse(outcome.committed, outcome)
        self.assertEqual(outcome.code, code)

    def committed(self, outcome) -> None:
        self.assertTrue(outcome.committed, outcome)

    def test_1_register_verify_sign_in_open_a_tenant_and_let_groups_and_relations_decide(self):
        api: KitApi = S["api"]
        # Signing in before verifying fails without saying more; an unknown address too.
        api.register("early@example.com", "Early", PASSWORD)
        self.assertEqual(api.sign_in("early@example.com", PASSWORD), {"ok": False, "reason": "not_verified"})
        self.assertEqual(api.sign_in("nobody@example.com", PASSWORD), {"ok": False, "reason": "wrong_password"})

        alice, alice_id = person("alice@example.com")
        bob, bob_id = person("bob@example.com")
        carol, _ = person("carol@example.com")

        # Alice opens a tenant herself (self-service sign-up is on); the kit makes her its owner.
        tenant = S["tenant"] = new_id()
        self.committed(submit(CreateTenant(slug="acme", name="Acme", owner={"account_id": alice_id}, status="active"), tenant, tenant,
                              api.context_for(alice)))
        as_alice = api.context_for(alice, tenant)
        self.assertTrue(as_alice.sender_identity)

        # A slug is taken across tenants.
        other = new_id()
        self.assertFalse(submit(CreateTenant(slug="acme", name="Acme 2", owner={"account_id": bob_id}, status="active"), other, other,
                                api.context_for(bob)).committed)

        # Alice invites Bob (signed in already), Carol, and Dana (new: she registers by accepting).
        def invite(email: str) -> str:
            self.committed(submit(CreateInvitation(email=email), new_id(), tenant, as_alice))
            return mailer.last(email).token

        bobs = invite("bob@example.com")
        # Carol may not use Bob's invitation: it is for another address.
        self.assertEqual(api.accept_invitation(bobs, claims=carol), {"ok": False, "reason": "wrong_email"})
        self.assertTrue(api.accept_invitation(bobs, claims=bob)["ok"])
        # A used invitation is gone.
        self.assertFalse(api.accept_invitation(bobs, claims=bob)["ok"])
        self.assertTrue(api.accept_invitation(invite("carol@example.com"), claims=carol)["ok"])
        dana = api.accept_invitation(invite("dana@example.com"), name="Dana", password=PASSWORD)
        self.assertTrue(dana["ok"], dana)
        dana_in = api.sign_in("dana@example.com", PASSWORD)
        self.assertTrue(dana_in["ok"], "Dana signs in without verifying: the invitation proved her email")
        self.assertTrue(api.context_for(api.check(dana_in["access_token"]), tenant).sender_identity)
        # Members may not hand out owner invitations.
        self.rejected(submit(CreateInvitation(email="eve@example.com", owner=True), new_id(), tenant, api.context_for(bob, tenant)), "forbidden")
        as_bob = api.context_for(bob, tenant)
        as_carol = api.context_for(carol, tenant)
        S["acme"] = {"alice": as_alice, "bob_identity": as_bob.sender_identity}

        # Without a group, Bob may do nothing in the shop.
        self.rejected(submit(PlaceOrder(), "o0", tenant, as_bob), "forbidden")

        # Alice (an owner) forms "customers": place orders, cancel only your own.
        customers = new_id()
        self.committed(submit(CreateGroup(name="Customers", permissions=[{"pattern": "shop.PlaceOrder"},
                                                                         {"pattern": "shop.CancelOrder", "when": "owner"}]),
                              customers, tenant, as_alice))
        for who in (as_bob, as_carol):
            self.committed(submit(GrantMembership(membership_id=new_id(), group=customers), who.sender_identity, tenant, as_alice))

        # Bob places an order; Carol may not cancel it, Bob may.
        self.committed(submit(PlaceOrder(), "o1", tenant, as_bob))
        self.rejected(submit(CancelOrder(), "o1", tenant, as_carol), "forbidden")
        self.committed(submit(CancelOrder(), "o1", tenant, as_bob))

        # No escalation: Bob may not grant memberships at all, nor hand out groups.
        self.rejected(submit(GrantMembership(membership_id=new_id(), group=customers), as_carol.sender_identity, tenant, as_bob), "forbidden")

        # Nobody signed in: refused; another tenant's identity: refused.
        self.rejected(submit(PlaceOrder(), "o2", tenant, RequestContext()), "unauthenticated")
        wrong = RequestContext(sender_account=as_bob.sender_account, sender_session=as_bob.sender_session, sender_tenant=other,
                               sender_identity=as_bob.sender_identity)
        self.rejected(submit(PlaceOrder(), "o2", tenant, wrong), "forbidden")

        # Signing out ends the session at once, although the access token is still valid.
        api.sign_out(bob)
        self.rejected(submit(PlaceOrder(), "o3", tenant, as_bob), "unauthenticated")

        # Lookups are the API's only: a signed-in person cannot ask for accounts by email.
        with self.assertRaises(Rejected) as caught:
            S["kit"].query(FindAccountByEmail(email="alice@example.com"), tenant="platform", context=as_alice)
        self.assertEqual(caught.exception.code, "forbidden")

    def test_2_suspended_and_deleted(self):
        """Sessions end at once, and a deleted tenant leaves no identity, invitation or slug behind."""
        api: KitApi = S["api"]
        kit: Kernable = S["kit"]
        tenant, acme = S["tenant"], S["acme"]
        admin = api.sign_in("admin@example.com", PASSWORD)
        self.assertTrue(admin["ok"], admin)
        as_admin = api.context_for(api.check(admin["access_token"]), "platform")

        # An account the platform suspends is signed out at once, and cannot sign in.
        eve, eve_id = person("eve@example.com")
        self.committed(submit(SuspendAccount(reason="abuse"), eve_id, "platform", as_admin))
        eves = new_id()
        self.rejected(submit(CreateTenant(slug="eves", name="Eve's", owner={"account_id": eve_id}), eves, eves, api.context_for(eve)), "unauthenticated")
        self.assertEqual(api.sign_in("eve@example.com", PASSWORD), {"ok": False, "reason": "not_active"})
        # Deleting it frees the address: Eve can register again.
        self.committed(submit(DeleteAccount(), eve_id, "platform", as_admin))
        person("eve@example.com")

        # Alice suspends Acme for herself, the platform deletes it.
        open_ = new_id()
        self.committed(submit(CreateInvitation(email="frank@example.com"), open_, tenant, acme["alice"]))
        self.committed(submit(SuspendTenant(reason="owner_requested"), tenant, tenant, acme["alice"]))
        self.committed(submit(DeleteTenant(), tenant, tenant, as_admin))

        # The cleanup erased Bob's identity and the open invitation (nothing left to delete or erase) ...
        internal = RequestContext(bypass_authorization=True)
        self.rejected(submit(DeleteIdentity(), acme["bob_identity"], tenant, internal), "not_found")
        self.rejected(submit(EraseInvitation(), open_, tenant, internal), "not_found")
        # ... the read side forgot the tenant, and its slug is free.
        with self.assertRaises(Rejected) as caught:
            kit.query(GetTenant(tenant_id=tenant), tenant=tenant, context=as_admin)
        self.assertEqual(caught.exception.code, "not_found")
        self.assertEqual(kit.query(ListInvitations(), tenant=tenant, context=internal), [])
        bob_again = api.sign_in("bob@example.com", PASSWORD)
        self.assertTrue(bob_again["ok"])
        reuse = new_id()
        self.committed(submit(CreateTenant(slug="acme", name="Acme again", owner={"account_id": bob_again["account_id"]}), reuse, reuse,
                              api.context_for(api.check(bob_again["access_token"]))))

    def test_3_the_middleware_resumes_from_its_store_after_a_restart(self):
        api: KitApi = S["api"]
        grace, grace_id = person("grace@example.com")
        graces = new_id()
        self.committed(submit(CreateTenant(slug="graces", name="Grace's", owner={"account_id": grace_id}), graces, graces, api.context_for(grace)))
        as_grace = api.context_for(grace, graces)
        self.committed(submit(PlaceOrder(), "g1", graces, as_grace))

        stop_app()
        # The knowledge is in the store file, with the position it continues from.
        db = sqlite3.connect(f"file:{os.path.join(S['kernel'].dir, 'authorization.db')}?mode=ro", uri=True)
        position = int(db.execute("SELECT value FROM meta WHERE key = 'position'").fetchone()[0])
        stored = db.execute("SELECT value FROM data WHERE ns = 'tenants' AND key = ?", (graces,)).fetchone()
        db.close()
        self.assertGreater(position, 0, "a stored position")
        self.assertEqual(len(json.loads(stored[0])["owners"]), 1, "Grace's tenant and its owner in the store")
        start_app()
        # The owner still may, an ended session still may not: both from the stored knowledge.
        self.committed(submit(PlaceOrder(), "g2", graces, as_grace))
        api.sign_out(grace)
        self.rejected(submit(PlaceOrder(), "g3", graces, as_grace), "unauthenticated")

    def test_4_account_flows(self):
        """Refresh and reuse, password change and reset, email change, a taken address."""
        api: KitApi = S["api"]
        email = "frank@example.com"
        person(email)

        def sign_in(address=email, password=PASSWORD):
            s = api.sign_in(address, password)
            self.assertTrue(s["ok"], s)
            return s

        one, two, three = sign_in(), sign_in(), sign_in()

        # A refresh rotates the token; the old one again is a reuse and ends the session.
        refreshed = api.refresh(one["refresh_token"])
        self.assertTrue(refreshed["ok"])
        self.assertEqual(api.check(refreshed["access_token"]).sid, one["session_id"])
        self.assertEqual(api.refresh(one["refresh_token"]), {"ok": False, "reason": "reuse"})
        self.assertFalse(api.refresh(refreshed["refresh_token"])["ok"], "the session ended with the reuse")

        # A password change keeps its own session and ends the others.
        claims_two = api.check(two["access_token"])
        self.assertEqual(api.change_password(claims_two, "wrong password!", "another horse battery"), {"ok": False, "reason": "wrong_password"})
        self.assertEqual(api.change_password(claims_two, PASSWORD, "another horse battery"), {"ok": True})
        two_again = api.refresh(two["refresh_token"])
        self.assertTrue(two_again["ok"], "the changing session stays")
        self.assertFalse(api.refresh(three["refresh_token"])["ok"], "the other session ended")
        sign_in(email, "another horse battery")

        # A reset by mail sets a new password and ends every session; asking for nobody sends nothing.
        before = len(mailer.sent)
        api.request_password_reset("nobody@example.com")
        self.assertEqual(len(mailer.sent), before)
        api.request_password_reset(email)
        reset = mailer.last(email)
        self.assertEqual(reset.purpose, "reset")
        self.assertEqual(api.reset_password(reset.token, "third horse battery"), {"ok": True})
        self.assertFalse(api.refresh(two_again["refresh_token"])["ok"], "every session ended")
        self.assertEqual(api.sign_in(email, "another horse battery"), {"ok": False, "reason": "wrong_password"})
        four = sign_in(email, "third horse battery")

        # A new address is confirmed from that address; the old one then signs in no more.
        self.assertEqual(api.request_email_change(api.check(four["access_token"]), "frank@new.example"), {"ok": True})
        change = mailer.last("frank@new.example")
        self.assertEqual(change.purpose, "email_change")
        self.assertEqual(api.confirm_email_change(change.token), {"ok": True})
        sign_in("frank@new.example", "third horse battery")
        self.assertEqual(api.sign_in(email, "third horse battery"), {"ok": False, "reason": "wrong_password"})

        # Registering with a taken address answers as usual; the account's owner gets a mail.
        self.assertTrue(api.register("frank@new.example", "Not Frank", PASSWORD)["ok"])
        self.assertEqual(mailer.last("frank@new.example").purpose, "account_exists")

    def test_5_a_second_factor_and_a_service_with_an_api_key(self):
        api: KitApi = S["api"]
        email = "henry@example.com"
        first, _ = person(email)
        totp = api.start_totp(first, "Kit", email)
        self.assertTrue(totp["ok"] and totp["uri"].startswith("otpauth://totp/"))
        step = int(time.time()) // 30
        confirmed = api.confirm_totp(first, totp_code(totp["secret"], step))
        self.assertTrue(confirmed["ok"])
        self.assertEqual(len(confirmed["recovery_codes"]), 10)

        # Signing in now needs the second factor before anything else.
        signed_in = api.sign_in(email, PASSWORD)
        self.assertTrue(signed_in["ok"] and signed_in["second_factor"])
        claims = api.check(signed_in["access_token"])
        henrys = new_id()

        def open_():
            return submit(CreateTenant(slug="henrys", name="Henry's", owner={"account_id": claims.sub}), henrys, henrys, api.context_for(claims))

        self.rejected(open_(), "mfa_required")
        # The confirming code counts once; the next step's code, or a recovery code, proves it.
        self.assertEqual(api.prove_second_factor(claims, totp_code(totp["secret"], step)), {"ok": False, "reason": "wrong_code"})
        self.assertTrue(api.prove_second_factor(claims, confirmed["recovery_codes"][0])["ok"])
        self.committed(open_())
        again = api.check(api.sign_in(email, PASSWORD)["access_token"])
        self.assertTrue(api.prove_second_factor(again, totp_code(totp["secret"], step + 1))["ok"])
        self.assertEqual(api.prove_second_factor(again, confirmed["recovery_codes"][0]), {"ok": False, "reason": "wrong_code"})

        # Henry, now an owner, gives a service an API key, and a group to act in.
        as_henry = api.context_for(claims, henrys)
        service = new_id()
        self.committed(submit(CreateServiceIdentity(display_name="Billing"), service, henrys, as_henry))
        orders = new_id()
        self.committed(submit(CreateGroup(name="Orders", permissions=[{"pattern": "shop.PlaceOrder"}]), orders, henrys, as_henry))
        self.committed(submit(GrantMembership(membership_id="svc", group=orders), service, henrys, as_henry))
        created = api.create_api_key(as_henry, henrys, service, "billing")
        self.assertTrue(created["ok"], created)
        as_service = api.check_api_key(created["key"])
        self.assertEqual(as_service, RequestContext(sender_tenant=henrys, sender_identity=service, sender_session=created["key_id"]))
        self.assertIsNone(api.check_api_key(created["key"] + "x"), "a wrong key")
        self.committed(submit(PlaceOrder(), "h1", henrys, as_service))

        # Revoked: refused at once, even with the API's cached context.
        self.committed(submit(RevokeApiKey(key_id=created["key_id"]), service, henrys, as_henry))
        self.rejected(submit(PlaceOrder(), "h2", henrys, as_service), "unauthenticated")

    def test_6_passkeys(self):
        """Added against an issued challenge, signing in without a password, a challenge counting once."""
        api: KitApi = S["api"]
        email = "ivy@example.com"
        ivy, ivy_id = person(email)
        device = Authenticator("localhost", "http://localhost:3000", ivy_id)
        creation = api.passkey_registration_options(ivy, email)
        self.assertEqual(api.add_passkey(ivy, device.register("bm90LWlzc3VlZA")), {"ok": False, "reason": "invalid_challenge"})
        self.assertEqual(api.add_passkey(ivy, device.register(creation["challenge"]), "Laptop"), {"ok": True})

        # Without a password: the passkey names the account and counts as two factors.
        answer = device.assert_(api.passkey_sign_in_options()["challenge"])
        signed_in = api.sign_in_with_passkey(answer)
        self.assertTrue(signed_in["ok"] and signed_in["account_id"] == ivy_id, signed_in)
        self.assertEqual(api.check(signed_in["access_token"]).fac, ["passkey"])
        # The same answer again: its challenge is used up.
        self.assertEqual(api.sign_in_with_passkey(answer), {"ok": False, "reason": "wrong_password"})
        # A passkey answer from another site's page fails.
        self.assertFalse(api.sign_in_with_passkey(device.assert_(api.passkey_sign_in_options()["challenge"], "http://evil.example"))["ok"])

        # The passkey session acts like any other.
        claims = api.check(signed_in["access_token"])
        ivys = new_id()
        self.committed(submit(CreateTenant(slug="ivys", name="Ivy's", owner={"account_id": claims.sub}), ivys, ivys, api.context_for(claims)))

    def test_7_oauth_and_sso(self):
        """A social login registers or signs in, links on request; a tenant's SSO verified by DNS and enforced."""
        api: KitApi = S["api"]
        kit: Kernable = S["kit"]
        social, company = OidcProvider(), OidcProvider()
        txt: dict[str, str] = {}
        sso = KitApi(kit, new_signing_keys(), S["service"],
                     providers={"social": {"issuer": social.issuer, "client_id": social.client_id, "client_secret": social.client_secret}},
                     redirect_uri="http://localhost:3000/callback", insecure_issuers=True,
                     resolve_txt=lambda name: [txt[name]] if name in txt else [])
        try:
            def start(link=None, **by):
                s = sso.oidc_start(link=link, **by)
                self.assertTrue(s["ok"], s)
                return s["url"]

            # A social login of a new person registers and links them; the second time signs them in.
            jack = {"sub": "s-jack", "email": "jack@example.com", "email_verified": True}
            first = sso.oidc_finish(social.login(start(provider="social"), jack))
            self.assertTrue(first["ok"] and "account_id" in first, first)
            second = sso.oidc_finish(social.login(start(provider="social"), jack))
            self.assertEqual(second.get("account_id"), first["account_id"])
            # A callback used once does not work twice.
            url = social.login(start(provider="social"), jack)
            self.assertTrue(sso.oidc_finish(url)["ok"])
            self.assertEqual(sso.oidc_finish(url), {"ok": False, "reason": "invalid_state"})

            # A social login whose email has an account is not linked by itself: sign in and link it.
            kate, kate_id = person("kate@example.com")
            kate_social = {"sub": "s-kate", "email": "kate@example.com", "email_verified": True}
            self.assertEqual(sso.oidc_finish(social.login(start(provider="social"), kate_social)), {"ok": False, "reason": "not_linked"})
            self.assertEqual(sso.oidc_finish(social.login(start(kate, provider="social"), kate_social)), {"ok": True, "linked": True})
            self.assertEqual(sso.oidc_finish(social.login(start(provider="social"), kate_social)).get("account_id"), kate_id)

            # Liam's tenant sets up SSO: the domain proven by DNS, then enforced.
            liam, liam_id = person("liam@lumen.example")
            lumen = new_id()
            self.committed(submit(CreateTenant(slug="lumen", name="Lumen", owner={"account_id": liam_id}), lumen, lumen, api.context_for(liam)))
            as_liam = api.context_for(liam, lumen)
            sso_id = new_id()
            self.committed(submit(CreateSso(issuer=company.issuer, client_id=company.client_id, client_secret=company.client_secret), sso_id, lumen, as_liam))
            self.committed(submit(AddSsoDomain(domain="lumen.example"), sso_id, lumen, as_liam))
            self.assertEqual(sso.verify_sso_domain(lumen, sso_id, "lumen.example"), {"ok": False, "reason": "not_verified"})
            config = kit.query(GetSso(sso_id=sso_id), tenant=lumen, context=as_liam)
            txt["_kernable-sso.lumen.example"] = config["domains"][0]["txt"]
            self.assertEqual(sso.verify_sso_domain(lumen, sso_id, "lumen.example"), {"ok": True})
            self.committed(submit(EnforceSso(enforced=True), sso_id, lumen, as_liam))

            # Mia of lumen.example joins with a password; in Lumen only an SSO session counts.
            mia, mia_id = person("mia@lumen.example")
            submit(CreateInvitation(email="mia@lumen.example"), new_id(), lumen, as_liam)
            self.assertTrue(api.accept_invitation(mailer.last("mia@lumen.example").token, claims=mia)["ok"])
            as_mia = api.context_for(mia, lumen)

            def own(context):
                return kit.query(GetIdentity(identity_id=context.sender_identity), tenant=lumen, context=context)

            with self.assertRaises(Rejected) as caught:
                own(as_mia)
            self.assertEqual(caught.exception.code, "sso_required")
            # Through the company's provider: linked to her account by the verified domain and email.
            mia_sso = sso.oidc_finish(company.login(start(email="mia@lumen.example"), {"sub": "c-mia", "email": "mia@lumen.example", "email_verified": True}))
            self.assertEqual(mia_sso.get("account_id"), mia_id, mia_sso)
            self.assertEqual(own(api.context_for(sso.check(mia_sso["access_token"]), lumen))["id"], as_mia.sender_identity)
            # The owner keeps a password session, so a broken SSO can be mended.
            self.assertTrue(own(as_liam)["id"])
        finally:
            social.stop()
            company.stop()

    def test_8_lists_and_explain(self):
        """Lists show what one may see; Explain says why a request is allowed or refused."""
        api: KitApi = S["api"]
        kit: Kernable = S["kit"]
        nora, nora_id = person("nora@example.com")
        t = new_id()
        self.committed(submit(CreateTenant(slug="noras", name="Nora's", owner={"account_id": nora_id}), t, t, api.context_for(nora)))
        as_nora = api.context_for(nora, t)
        buyers = new_id()
        self.committed(submit(CreateGroup(name="Buyers", permissions=[{"pattern": "shop.PlaceOrder"}, {"pattern": "shop.ListOrders"},
                                                                       {"pattern": "shop.GetOrder", "when": "owner"}]), buyers, t, as_nora))

        def join(email: str) -> RequestContext:
            who, _ = person(email)
            submit(CreateInvitation(email=email, groups=[{"group": buyers}]), new_id(), t, as_nora)
            self.assertTrue(api.accept_invitation(mailer.last(email).token, claims=who)["ok"])
            return api.context_for(who, t)

        as_oscar, as_pia = join("oscar@example.com"), join("pia@example.com")
        for id_, who in (("o-oscar", as_oscar), ("o-pia", as_pia)):
            self.committed(submit(PlaceOrder(), id_, t, who))

        def orders(context):
            return kit.query(ListOrders(), tenant=t, context=context)

        self.assertEqual(orders(as_oscar), ["o-oscar"])
        self.assertEqual(orders(as_pia), ["o-pia"])
        self.assertEqual(orders(as_nora), ["o-oscar", "o-pia"], "the owner sees all")

        # Explain, for oneself: with one's own session; the deciding membership named.
        def explain(identity, context, type_, aggregate_id=None):
            return kit.query(Explain(identity_id=identity, type=type_, aggregate_type="order" if aggregate_id else None, aggregate_id=aggregate_id),
                             tenant=t, context=context)

        mine = explain(as_oscar.sender_identity, as_oscar, "shop.GetOrder", "o-oscar")
        self.assertTrue(mine["allowed"])
        self.assertRegex(mine["steps"][-1], r"^allowed: membership .* of group .*: shop.GetOrder when owner")
        theirs = explain(as_oscar.sender_identity, as_oscar, "shop.GetOrder", "o-pia")
        self.assertEqual((theirs["allowed"], theirs["code"]), (False, "forbidden"))
        # For someone else: the owner may ask, a member may not.
        for_pia = explain(as_pia.sender_identity, as_nora, "shop.PlaceOrder")
        self.assertTrue(for_pia["allowed"] and for_pia["steps"][0].startswith("sessions not checked"), for_pia)
        with self.assertRaises(Rejected) as caught:
            explain(as_pia.sender_identity, as_oscar, "shop.PlaceOrder")
        self.assertEqual(caught.exception.code, "forbidden")

    def test_9_the_example_server(self):
        """Register, verify, sign in, and a command as the caller."""
        server = example_server(S["api"], S["kit"], port=0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_address[1]}"

        def post(path: str, body: dict, headers: dict | None = None):
            request = urllib.request.Request(f"{base}{path}", data=json.dumps(body).encode(), method="POST",
                                             headers={"content-type": "application/json", **(headers or {})})
            try:
                with urllib.request.urlopen(request) as res:
                    raw = res.read()
                    return res.status, json.loads(raw) if raw else None
            except urllib.error.HTTPError as err:
                raw = err.read()
                return err.code, json.loads(raw) if raw else None

        try:
            self.assertEqual(post("/register", {"email": "quinn@example.com", "name": "Quinn", "password": PASSWORD})[0], 202)
            self.assertEqual(post("/verify", {"token": mailer.last("quinn@example.com").token})[0], 200)
            status, tokens = post("/sign-in", {"email": "quinn@example.com", "password": PASSWORD})
            self.assertEqual(status, 200)
            bearer = {"authorization": f"Bearer {tokens['access_token']}"}
            # Outside a tenant, Quinn has no identity: the shop refuses.
            self.assertEqual(post("/commands/shop/order/PlaceOrder", {"aggregate_id": "q1"}, bearer), (403, {"error": "forbidden"}))
            self.assertEqual(post("/commands/shop/order/PlaceOrder", {"aggregate_id": "q1"})[0], 401)
            self.assertEqual(post("/sign-out", {}, bearer)[0], 204)
            self.assertEqual(post("/refresh", {"refresh_token": tokens["refresh_token"]})[0], 401, "signed out")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
