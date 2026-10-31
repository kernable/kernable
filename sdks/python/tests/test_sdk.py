"""Python SDK: signing vectors, and a counter domain end to end against the real kernel.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import contextlib
import hashlib
import io
import json
import os
import re
import sqlite3
import threading
import time
import unittest
from unittest import mock

import grpc

from kernable import Data, Kernable, NotSettled, Outcome, QueryOutcome, Rejected, RequestContext, _upcast, domain
from kernable.signing import (Key, cache_message, challenge_message, command_message, data_message, derived_id,
                              membership_message, query_message, rejection_message, result_message, snapshot_message, vault_ref)
from kernable.v1 import kernable_pb2 as pb
from tests.kernel import ROOT, TestKernel

with open(os.path.join(ROOT, "proto", "test-vectors", "signing.json")) as f:
    VECTORS = json.load(f)


def vector_context(c: dict) -> pb.RequestContext:
    context = dict(c)
    attributes = [pb.Attribute(**a) for a in context.pop("attributes")]
    return pb.RequestContext(**context, attributes=attributes)


def vector_command(c) -> pb.Command:
    fields = ("command_id", "tenant_id", "domain", "aggregate_type", "aggregate_id", "command_type")
    cmd = pb.Command(**{k: c[k] for k in fields}, payload=bytes.fromhex(c["payload_hex"]))
    if "context" in c:
        cmd.context.CopyFrom(vector_context(c["context"]))
    return cmd



class ApplyTest(unittest.TestCase):
    """apply: a handler changes a copy of the state in place, or returns a state that replaces it."""

    def test_apply_changes_a_copy_in_place_deletes_or_replaces(self):
        account = domain("crm").aggregate("Account", {"tags": [], "status": "new"})

        @account.apply("Opened")
        def _(state, data):
            state["email"] = data["email"]
            state["tags"].append("open")

        account.apply("Reset", lambda state, data: {"tags": [], "status": "reset"})

        @account.apply("Closed")
        def _(state, data):
            del state["email"]

        opened = account.fold([("Opened", 1, {"email": "a@example.com"})])
        self.assertEqual(opened, {"email": "a@example.com", "tags": ["open"], "status": "new"})
        self.assertEqual(account.initial, {"tags": [], "status": "new"})  # untouched
        closed = account.fold([("Closed", 1, {})], opened)
        self.assertEqual(closed, {"tags": ["open"], "status": "new"})
        self.assertIn("email", opened)  # the state before is untouched
        self.assertEqual(account.fold([("Opened", 1, {"email": "b@example.com"}), ("Reset", 1, {})]),
                         {"tags": [], "status": "reset"})

class ListenerDefinitionTest(unittest.TestCase):
    """Listeners are defined on a domain: unqualified types are the domain's own."""

    def test_unqualified_types_are_the_own_domains_and_qualified_ones_stay(self):
        shop, identity = domain("Shop"), domain("Identity")
        carts = shop.read_model("carts").use(identity)
        self.assertEqual((carts.name, carts.order, carts.start), ("carts", 0, 0))
        self.assertEqual((shop.reactor("mailer").order, shop.reactor("mailer").start), (10, None))
        carts.on("ItemAdded", lambda data: None)
        carts.on("Identity.AccountRegistered", lambda data: None)
        carts.on("Identity.*", lambda data: None)
        carts.query("GetCart", lambda data: None)
        carts.query("Identity.GetAccount", lambda data: None)
        carts.middleware(["*", "Checkout", "Identity.*"], fn=lambda check: None)
        self.assertEqual(list(carts._handlers), ["Shop.ItemAdded", "Identity.AccountRegistered", "Identity.*"])
        self.assertEqual(list(carts._queries), ["Shop.GetCart", "Identity.GetAccount"])
        self.assertEqual(list(carts._middlewares["before"][0].types), ["*", "Shop.Checkout", "Identity.*"])
        with self.assertRaises(ValueError):
            carts.query("Identity.*")
        with self.assertRaises(ValueError):
            carts.on("Identity.Account.Registered")

    def test_listener_types_are_checked_when_defined(self):
        # A bare `*` is not the own domain (that is `counters.*`), as in TypeScript and Rust.
        model = domain("counters").read_model("model")
        for event_type in ("Added", "crm.Created", "crm.*", "counters.*"):
            model.on(event_type, lambda data: None)
        self.assertEqual(list(model._handlers), ["counters.Added", "crm.Created", "crm.*", "counters.*"])
        for event_type in ("*", "", "crm.a.b", "Add*"):
            with self.assertRaises(ValueError, msg=event_type):
                model.on(event_type, lambda data: None)

    def test_the_own_domains_event_schemas_apply_without_use(self):
        shop = domain("Shop")
        carts = shop.read_model("carts")
        shop.event("ItemAdded", 2).upcast(1, lambda d: {"sku": d["id"]})  # declared after the listener
        self.assertEqual(_upcast(carts._domains.get("Shop"), "Shop", "ItemAdded", 1, {"id": "x"}),
                         (2, {"sku": "x"}))
        self.assertIsNone(carts._domains.get("Identity"))


class SigningVectors(unittest.TestCase):
    def check(self, name: str, message: bytes):
        key = Key(VECTORS["key"]["seed"])
        self.assertEqual(key.id, VECTORS["key"]["key_id"])
        self.assertEqual(message.hex(), VECTORS[name]["message"])
        self.assertEqual(key.sign(message).sig.hex(), VECTORS[name]["signature"])

    def test_messages_and_signatures_match(self):
        self.check("command", command_message(vector_command(VECTORS["command"]["input"])))
        r = VECTORS["result"]["input"]
        events = [pb.NewEvent(event_type=e["event_type"], schema=e["schema"], payload=bytes.fromhex(e["payload_hex"])) for e in r["events"]]
        self.check("result", result_message(vector_command(r["command"]), r["expected_version"], events))
        j = VECTORS["rejection"]["input"]
        self.check("rejection", rejection_message(j["command_id"], pb.Rejected(code=j["code"], message=j["message"])))
        self.check("challenge", challenge_message(bytes.fromhex(VECTORS["challenge"]["input"]["nonce_hex"])))
        n = VECTORS["snapshot"]["input"]
        stream = pb.Command(tenant_id=n["tenant_id"], domain=n["domain"], aggregate_type=n["aggregate_type"],
                            aggregate_id=n["aggregate_id"])
        self.check("snapshot", snapshot_message(stream, n["version"], n["schema"], bytes.fromhex(n["state_hex"])))
        q = dict(VECTORS["query"]["input"])
        context = vector_context(q.pop("context"))
        payload = bytes.fromhex(q.pop("payload_hex"))
        query = pb.QueryRequest(**q, payload=payload, context=context)
        self.check("query", query_message(query))
        refs = [vault_ref(bytes.fromhex(i["salt_hex"]), i["subject"], i["secret"], bytes.fromhex(i["value_hex"]))
                for i in VECTORS["vault_ref"]["input"]]
        self.assertEqual(refs, VECTORS["vault_ref"]["refs"])
        c = vector_command(VECTORS["command_vault"]["input"]["command"])
        c.vault_refs.extend(VECTORS["command_vault"]["input"]["vault_refs"])
        self.check("command_vault", command_message(c))
        v = VECTORS["result_vault"]["input"]
        for e, refs_, erases in zip(events, v["vault_refs"], v["erases"]):
            e.vault_refs.extend(refs_)
            e.erases = erases
        self.check("result_vault", result_message(vector_command(r["command"]), r["expected_version"], events))
        d = VECTORS["derived_id"]
        self.assertEqual([derived_id(d["input"]["command_id"], n) for n in d["input"]["n"]], d["ids"])
        p = dict(VECTORS["data_put"]["input"])
        op, data, context = p.pop("op"), bytes.fromhex(p.pop("data_hex")), vector_context(p.pop("context"))
        self.check("data_put", data_message(op, pb.DataRequest(**p, data=data, context=context)))
        s = dict(VECTORS["cache_set"]["input"])  # without a context: encoded like an empty one
        op, value = s.pop("op"), bytes.fromhex(s.pop("value_hex"))
        self.check("cache_set", cache_message(op, pb.CacheRequest(**s, value=value)))
        self.assertEqual(cache_message(op, pb.CacheRequest(**s, value=value, context=pb.RequestContext())).hex(),
                         VECTORS["cache_set"]["message"])
        m = dict(VECTORS["membership"]["input"])
        metadata = [pb.Attribute(**a) for a in m.pop("metadata")]
        self.check("membership", membership_message(pb.Membership(**m, metadata=metadata)))


counters_domain = domain("counters")
counter = counters_domain.aggregate("Counter", 0)


@counter.apply("Incremented")
def _(state, data):
    return state + data["by"]


@counter.command("Increment")
def _(state, data):
    if data["by"] <= 0:
        raise Rejected("must increase", "invalid")
    if state + data["by"] > 10:
        raise Rejected("limit reached", "limit")
    return [{"type": "Incremented", "data": {"by": data["by"]}}]


def background(target) -> threading.Thread:
    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t


class DataAndCache(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["counters.*"], grants=["shop.*"])
        self.kernel.start()
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"})  # quiet: no connection lines
        env.start()
        self.addCleanup(env.stop)
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")

    def tearDown(self):
        self.k.close()
        self.kernel.dispose()

    def test_files_are_put_read_and_deleted(self):
        self.k.data.put("shop", "notes/n-1.txt", "Grüße")
        self.assertEqual(self.k.data.get("shop", "notes/n-1.txt"), Data("Grüße".encode(), "text/plain; charset=utf-8"))
        self.k.data.put("shop", "invoices/o-1.pdf", b"%PDF-1.7", "application/pdf")
        self.assertEqual(self.k.data.get("shop", "invoices/o-1.pdf"), Data(b"%PDF-1.7", "application/pdf"))
        self.assertIsNone(self.k.data.get("shop", "invoices/o-1.pdf", tenant="t2"))
        large = os.urandom(5 * 1024 * 1024)
        self.k.data.put("shop", "exports/large.bin", large)
        self.assertEqual(self.k.data.get("shop", "exports/large.bin"), Data(large, "application/octet-stream"))

        self.assertTrue(self.k.data.delete("shop", "invoices/o-1.pdf"))
        self.assertIsNone(self.k.data.get("shop", "invoices/o-1.pdf"))
        self.assertFalse(self.k.data.delete("shop", "invoices/o-1.pdf"))
        self.assertIsNone(self.k.data.get("shop", "missing"))

    def test_cache_values_expire_and_are_deleted(self):
        self.k.cache.set("shop", "rate/c-1", "3", ttl_ms=200)
        self.k.cache.set("shop", "token/c-1", b"\x00\x01")
        self.assertEqual(self.k.cache.get("shop", "rate/c-1"), b"3")
        self.assertEqual(self.k.cache.get("shop", "token/c-1"), b"\x00\x01")
        time.sleep(0.3)
        self.assertIsNone(self.k.cache.get("shop", "rate/c-1"))
        self.assertFalse(self.k.cache.delete("shop", "rate/c-1"))
        self.assertTrue(self.k.cache.delete("shop", "token/c-1"))
        self.assertIsNone(self.k.cache.get("shop", "token/c-1"))

    def test_a_domain_without_grant_is_refused(self):
        for call in (lambda: self.k.data.put("billing", "x", b"1"), lambda: self.k.data.get("billing", "x"),
                     lambda: self.k.cache.set("billing", "x", b"1"), lambda: self.k.cache.get("billing", "x")):
            with self.assertRaises(grpc.RpcError) as refused:
                call()
            self.assertEqual(refused.exception.code(), grpc.StatusCode.PERMISSION_DENIED)

    def test_a_middleware_checks_file_and_cache_requests_by_their_context(self):
        checks = []
        guard = domain("shop").read_model("shop_guard")

        @guard.middleware(["shop.*"])
        def _(check):
            checks.append((check.kind, check.type, check.domain, check.tenant, check.key, check.data,
                           check.request.sender_identity))
            if check.request.sender_identity != "alice":
                raise Rejected("unknown sender", "forbidden")

        stream = self.k.listener(guard)
        background(stream.run)
        self.addCleanup(stream.stop)
        self.assertTrue(stream.ready.wait(5), "stream did not connect")

        for call in (lambda: self.k.data.put("shop", "notes/n-1.txt", "hi"),
                     lambda: self.k.cache.set("shop", "rate/c-1", "1")):
            with self.assertRaises(grpc.RpcError) as refused:
                call()
            self.assertEqual(refused.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
            self.assertEqual(refused.exception.details(), "forbidden: unknown sender")
        alice = RequestContext(sender_identity="alice")
        self.k.data.put("shop", "notes/n-1.txt", "hi", context=alice)
        self.k.cache.set("shop", "rate/c-1", "1", context=alice)
        as_alice = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1", context=alice)
        self.addCleanup(as_alice.close)
        self.assertEqual(as_alice.data.get("shop", "notes/n-1.txt").data, b"hi")  # the client's default context
        self.assertEqual(as_alice.cache.get("shop", "rate/c-1"), b"1")

        self.assertEqual(checks[0], ("data", "PutData", "shop", "t1", "notes/n-1.txt", None, ""))
        self.assertEqual(checks[1], ("cache", "SetCache", "shop", "t1", "rate/c-1", None, ""))
        self.assertEqual([(c[0], c[1], c[6]) for c in checks[2:]],
                         [("data", "PutData", "alice"), ("cache", "SetCache", "alice"),
                          ("data", "GetData", "alice"), ("cache", "GetCache", "alice")])
        stream.stop()  # before tearDown stops the kernel: no reconnect warning


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.kernel = TestKernel(["counters.*"], ["--settle-timeout-ms", "500"])
        self.kernel.start()
        # Quiet: no connection lines in the test output (see test_connections_are_logged_to_stderr).
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"})
        env.start()
        self.addCleanup(env.stop)
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")
        self.streams = []

    def tearDown(self):
        for s in self.streams:
            s.stop()
        self.k.close()
        self.kernel.dispose()

    def run_stream(self, stream):
        self.streams.append(stream)
        background(stream.run)
        self.assertTrue(stream.ready.wait(5), "stream did not connect")
        return stream

    def increment(self, by: int, **kw):
        return self.k.submit("counters", "Counter", "c1", "Increment", {"by": by}, **kw)

    def test_worker_read_model_and_reactor(self):
        self.run_stream(self.k.worker(counter))
        totals: dict[str, int] = {}
        seen_by_reactor: list[tuple[int, int]] = []

        model = counters_domain.read_model("totals")
        model.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @model.query("counters.GetTotal")
        def _(data, qry, ctx):
            assert (qry.type, qry.domain, ctx.store) == ("GetTotal", "counters", None)
            if data["id"] not in totals:
                raise Rejected(f"no counter {data['id']}", "not_found")
            return totals[data["id"]]

        def total(counter: str) -> int:
            return self.k.query("counters", "GetTotal", {"id": counter})
        reactor = counters_domain.reactor("audit")

        @reactor.on("counters.Incremented")
        def _(data, evt):
            seen_by_reactor.append((evt.position, total("c1")))  # the read model ran first

        self.run_stream(self.k.listener(model))
        reactor = self.run_stream(self.k.listener(reactor))

        first = self.increment(3)
        self.assertTrue(first.committed)
        self.assertEqual([e.version for e in first.events], [1])
        self.assertEqual(total("c1"), 3)  # read your writes through the kernel, no polling
        self.increment(4)
        self.assertEqual(total("c1"), 7)
        with self.assertRaises(Rejected) as missing:
            total("c9")
        self.assertEqual(missing.exception.code, "not_found")
        self.assertEqual(seen_by_reactor, [(1, 3), (2, 7)])

        limit = self.increment(5)
        self.assertEqual((limit.committed, limit.code), (False, "limit"))
        self.assertEqual(self.increment(0).code, "invalid")

        # Idempotent: the same ID returns the original outcome.
        once = self.increment(1, id="fixed")
        again = self.increment(1, id="fixed")
        self.assertEqual([e.position for e in once.events], [e.position for e in again.events])

        # A stopped listener still counts.
        reactor.stop()
        with self.assertRaises(NotSettled) as ctx:
            self.increment(1)
        self.assertTrue(ctx.exception.outcome.committed)
        self.assertTrue(self.increment(1, wait="committed").committed)
        command_id = self.k.enqueue("counters", "Counter", "c2", "Increment", {"by": 1})
        self.assertTrue(command_id)

        time.sleep(0.15)  # let the kernel write a checkpoint
        out = self.kernel.verify()
        self.assertEqual(out.returncode, 0, out.stdout)

    def test_listeners_a_membership_does_not_list_and_live_ones_never_count(self):
        self.k.close()  # this test needs a short settle timeout: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["counters.*"], ["--settle-timeout-ms", "300"])
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1", service="counters")
        self.run_stream(self.k.worker(counter))
        seen: list[str] = []

        def mails():
            return counters_domain.reactor("mails").on("Incremented", lambda data, evt: seen.append(evt.aggregate_id))

        audit = counters_domain.reactor("audit").on("Incremented", lambda data: None)
        v1 = [self.k.listener(mails()), self.k.listener(audit)]
        self.k.apply(workers=[counter], listeners=[h.listener for h in v1], metadata={"version": "1"})
        threads = []
        for handle in v1:
            self.streams.append(handle)
            threads.append(background(handle.run))
        self.assertTrue(all(h.ready.wait(5) for h in v1), "listeners did not connect")
        self.assertTrue(self.k.submit("counters", "Counter", "c1", "Increment", {"by": 1}).committed)
        for handle, thread in zip(v1, threads):
            handle.stop()
            thread.join(5)

        # The next version of the service has no audit any more: nobody waits for it.
        v2 = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1", service="counters")
        self.addCleanup(v2.close)
        mails2 = v2.listener(mails())
        v2.apply(workers=[counter], listeners=[mails2.listener], metadata={"version": "2"})
        # A live listener counts for nobody, even while it does not get through its events.
        stuck = threading.Event()
        push = v2.listener(counters_domain.reactor("push", live=True).on("Incremented", lambda data: stuck.wait(10)))
        try:
            self.run_stream(mails2)
            self.run_stream(push)
            self.assertTrue(self.k.submit("counters", "Counter", "c2", "Increment", {"by": 1}).committed,
                            "settled without audit and push")
            # mails resumed where it left off.
            self.assertEqual(seen, ["c1", "c2"])
        finally:
            stuck.set()  # released before tearDown stops it, so it cannot hang

        # A listener of the service the membership does not list counts for nobody, even running.
        blocked = threading.Event()
        extra = self.run_stream(v2.listener(counters_domain.reactor("extra").on("Incremented",
                                                                            lambda data: blocked.wait(10))))
        try:
            self.assertTrue(self.k.submit("counters", "Counter", "c3", "Increment", {"by": 1}).committed,
                            "settled without the unlisted listener")
        finally:
            blocked.set()
            extra.stop()

        with self.assertRaisesRegex(ValueError, "live"):
            counters_domain.reactor("x", live=True).middleware(["*"], fn=lambda check: None)
        with self.assertRaisesRegex(ValueError, "service"):
            Kernable(self.kernel.url, self.kernel.app_key).apply(workers=[counter])

    def test_commands_need_a_registered_or_listed_aggregate_and_an_apply_drops_the_unlisted(self):
        self.k.close()  # needs the domain shop: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["shop.*"])
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1", service="shop")
        shop = domain("shop")
        cart = shop.aggregate("Cart", 0)
        cart.apply("ItemAdded", lambda state, data: state + 1)
        cart.command("AddItem", lambda state, data: [{"type": "ItemAdded", "data": {}}])
        wishlist = shop.aggregate("Wishlist", 0)
        wishlist.apply("Wished", lambda state, data: state + 1)
        wishlist.command("Wish", lambda state, data: [{"type": "Wished", "data": {}}])

        def submit(client, aggregate_type, command_type):
            return client.submit("shop", aggregate_type, "a1", command_type, {}, wait="committed")

        def not_found(client, aggregate_type, command_type) -> grpc.RpcError | None:
            # Enqueued, not submitted: a listed aggregate without a worker would only be queued.
            try:
                client.enqueue("shop", aggregate_type, "a1", command_type, {})
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.NOT_FOUND:
                    return e
                raise
            return None

        # No worker ever registered the aggregate and no membership lists it: refused rather than queued.
        refused = not_found(self.k, "Cart", "AddItem")
        self.assertIsNotNone(refused)
        self.assertIn("no worker registered", refused.details())

        # v1 runs both aggregates in one worker, then stops.
        self.k.apply(workers=[cart, wishlist], metadata={"version": "1"})
        v1 = self.k.worker(cart, wishlist)
        thread = background(v1.run)
        self.streams.append(v1)
        self.assertTrue(v1.ready.wait(5), "worker did not connect")
        self.assertTrue(submit(self.k, "Cart", "AddItem").committed)
        self.assertTrue(submit(self.k, "Wishlist", "Wish").committed)

        # v2 of the service lists only the cart: once v1's worker has stopped, the wishlist is gone.
        v2 = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1", service="shop")
        self.addCleanup(v2.close)
        v2.apply(workers=[cart], metadata={"version": "2"})
        v1.stop()
        thread.join(5)
        self.run_stream(v2.worker(cart))
        deadline = time.monotonic() + 5
        while (refused := not_found(v2, "Wishlist", "Wish")) is None and time.monotonic() < deadline:
            time.sleep(0.05)  # the kernel notices the worker's disconnect asynchronously
        self.assertIsNotNone(refused, "the dropped aggregate still takes commands")
        self.assertIn("no worker registered", refused.details())
        self.assertTrue(submit(v2, "Cart", "AddItem").committed)

    def test_connections_are_logged_to_stderr(self):
        model = counters_domain.read_model("totals")
        model.on("counters.Incremented", lambda data: None)
        model.on("Reset", lambda data: None)
        model.query("GetTotal", lambda data: 0)

        def connect(level: str) -> str:
            with mock.patch.dict(os.environ, {"KERNABLE_LOG": level}):
                k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")
            out = io.StringIO()
            with contextlib.redirect_stderr(out):
                worker = self.run_stream(k.worker(counter))
                listener = self.run_stream(k.listener(model))
                self.assertTrue(self.increment(1).committed)
                self.assertEqual(k.query("counters", "GetTotal"), 0)
                worker.stop()
                listener.stop()
                time.sleep(0.1)
            k.close()
            # Without the time each line starts with (`13:12:10  `).
            return re.sub(r"(?m)^\d\d:\d\d:\d\d  ", "", out.getvalue())

        id12 = Key.from_file(self.kernel.app_key).id[:12]
        info = connect("info").splitlines()
        self.assertEqual(sorted(info), [
            f"listener totals  connected to {self.kernel.url} · key app ({id12}…) · from 0 · "
            "reads counters.Incremented, counters.Reset · answers counters.GetTotal",
            f"worker  connected to {self.kernel.url} · key app ({id12}…) · runs counters.Increment",
        ])
        debug = connect("debug")
        self.assertIn("worker  counters.Increment Counter/c1 committed 1 events\n", debug)
        self.assertRegex(debug, r"listener totals  counters\.Incremented at \d+\n")
        self.assertIn("listener totals  answered counters.GetTotal\n", debug)
        self.assertEqual(connect("warn"), "")

    def test_snapshots_shorten_replays_and_a_new_schema_rebuilds(self):
        applied = []

        def increment(total: int, data) -> list:
            if total + data["by"] > 20:
                raise Rejected("limit")
            return [{"type": "Incremented", "data": {"by": data["by"]}}]

        # v1 keeps the state as a dict, with what `apply` learns about the last event: every
        # increment is by 1, so the version equals n, whether folded from history or a snapshot.
        v1 = domain("counters").aggregate("Counter", {"n": 0, "version": 0, "id": "c1"}, snapshot_every=3)

        @v1.apply("Incremented")
        def _(state, data, evt):
            applied.append(data)
            return {"n": state["n"] + data["by"], "version": evt.version, "id": evt.aggregate_id}

        @v1.command("Increment")
        def _(state, data):
            if state["version"] != state["n"] or state["id"] != "c1":
                raise Rejected(f"event meta {state}")
            return increment(state["n"], data)
        worker = self.run_stream(self.k.worker(v1))
        for _ in range(10):
            self.assertTrue(self.increment(1).committed)
        self.assertLessEqual(len(applied), 25)  # without snapshots: 45 for the history alone
        worker.stop()
        time.sleep(0.1)

        # v2 keeps it as a number: the old snapshot does not fit.
        v2 = domain("counters").aggregate("Counter", 0, snapshot_every=3, schema="2")
        v2.apply("Incremented", lambda n, data: n + data["by"])
        v2.command("Increment", increment)
        self.run_stream(self.k.worker(v2))
        self.assertFalse(self.increment(11).committed)  # 10 + 11 > 20: rebuilt from the events
        self.assertTrue(self.increment(10).committed)

    def test_request_context_reaches_handlers_events_and_follow_up_commands(self):
        seen, applied = [], []
        ticket = domain("support").aggregate("Ticket", {"open": False})

        @ticket.apply("Opened")
        def _(state, data, evt, ctx):
            applied.append(ctx.request)
            return {"open": True}

        @ticket.command("Open")
        def _(state, data, cmd, ctx):
            seen.append(ctx.request)
            assert (cmd.command_id, cmd.type, cmd.aggregate_id) == ("open-t1", "Open", "t1")
            assert ctx.deadline > ctx.now()
            return [{"type": "Opened", "data": {"ids": [ctx.id(), ctx.id()]}}]

        @ticket.command("Note")
        def _(state, data, cmd, ctx):
            seen.append(ctx.request)
            return [{"type": "Noted"}]

        self.k.close()  # this test needs other command types: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["support.*"])
        self.kernel.start()
        service = RequestContext(sender_identity="svc-1", attributes={"channel": "api"})
        self.k = Kernable(self.kernel.url, self.kernel.app_key, context=service)
        self.run_stream(self.k.worker(ticket))
        reactor = domain("support").reactor("notes")

        @reactor.on("Opened")
        def _(data, evt, ctx):
            self.k.submit("support", "Ticket", evt.aggregate_id, "Note", id=f"note-{evt.aggregate_id}", context=ctx.follow(),
                          wait="committed")

        self.run_stream(self.k.listener(reactor))
        opened = self.k.submit("support", "Ticket", "t1", "Open", id="open-t1",
                               context=RequestContext(sender_identity="svc-1", sender_session="s-9"))
        request = RequestContext(sender_identity="svc-1", sender_session="s-9", correlation_id="open-t1")
        self.assertEqual(opened.events[0].request, request)
        self.assertEqual(opened.events[0].data["ids"], [derived_id("open-t1", 0), derived_id("open-t1", 1)])
        follow_up = RequestContext(sender_identity="svc-1", sender_session="s-9", correlation_id="open-t1",
                                   causation_id="open-t1")
        self.assertEqual(seen, [request, follow_up])
        self.assertEqual(applied, [request])  # folded for the follow-up command

        stale = self.k.submit("support", "Ticket", "t1", "Note", context=RequestContext(expected_version=1))
        self.assertEqual((stale.committed, stale.code), (False, "version_mismatch"))

        # A request's own context replaces the client's: an anonymous one does not act as the service.
        anonymous = self.k.submit("support", "Ticket", "t1", "Note", context=RequestContext())
        self.assertEqual(anonymous.events[0].request, RequestContext(correlation_id=anonymous.events[0].command_id))
        time.sleep(0.15)
        self.assertEqual(self.kernel.verify().returncode, 0)

    def test_middleware_checks_from_its_own_state_and_fails_closed(self):
        self.run_stream(self.k.worker(counter))
        totals: dict[str, int] = {}
        checks = []
        guard = counters_domain.read_model("guard")
        guard.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @guard.query("counters.GetTotal")
        def _(data):
            return totals.get(data["id"], 0)

        @guard.middleware(["counters.*"])
        def _(check):
            checks.append((check.kind, check.type, check.request.sender_identity))
            if check.request.sender_identity == "guest" and (check.kind == "query" or totals.get(check.aggregate_id, 0) >= 3):
                raise Rejected("guests stop at 3", "forbidden")

        guard = self.run_stream(self.k.listener(guard))
        guest = RequestContext(sender_identity="guest")
        self.assertTrue(self.increment(3, context=guest).committed)
        refused = self.increment(1, context=guest)  # the middleware already counted the first
        self.assertEqual((refused.committed, refused.code), (False, "forbidden"))
        self.assertTrue(self.increment(1).committed)
        with self.assertRaises(Rejected) as asked:
            self.k.query("counters", "GetTotal", {"id": "c1"}, context=guest)
        self.assertEqual(asked.exception.code, "forbidden")
        self.assertEqual(self.k.query("counters", "GetTotal", {"id": "c1"}), 4)
        self.assertEqual(checks[:2], [("command", "Increment", "guest"), ("command", "Increment", "guest")])

        guard.stop()
        time.sleep(0.2)
        with self.assertRaises(grpc.RpcError) as away:
            self.increment(1)
        self.assertEqual(away.exception.code(), grpc.StatusCode.UNAVAILABLE)

    def test_a_middleware_after_requests_observes_commands_and_may_refuse_a_querys_response(self):
        self.k.close()  # this test needs other command types: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["support.*"])
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key)

        support = domain("support")
        ticket = support.aggregate("Ticket", False)
        ticket.apply("Opened", lambda state, data: True)

        @ticket.command("Open")
        def _(state, data):
            if state:
                raise Rejected("already open", "invalid_state")
            return [{"type": "Opened"}]

        tickets: set[str] = set()
        model = support.read_model("tickets")
        model.on("Opened", lambda data, evt: tickets.add(evt.aggregate_id))
        model.query("ListTickets", lambda data: sorted(tickets))

        seen = []
        audit = support.read_model("audit")

        @audit.middleware(["*"])
        def _(check):
            if check.request.sender_identity == "mallory":
                raise Rejected("banned", "forbidden")

        @audit.middleware(["*"], when="after")
        def _(check, outcome):
            seen.append((check, outcome))
            # Only the response of a query can still be refused.
            if check.kind == "query" and outcome.answered and "secret" in outcome.data:
                raise Rejected("hidden", "hidden")
            if check.kind == "command":
                raise Rejected("ignored for commands")

        self.run_stream(self.k.worker(ticket))
        self.run_stream(self.k.listener(model))
        self.run_stream(self.k.listener(audit))

        def after(n: int):
            for _ in range(100):
                if len(seen) >= n:
                    break
                time.sleep(0.02)
            taken = [(c.kind, c.type, o) for c, o in seen]
            seen.clear()
            return taken

        def open_ticket(id: str, sender_identity: str = "alice"):
            return self.k.submit("support", "Ticket", id, "Open", context=RequestContext(sender_identity=sender_identity))

        self.assertTrue(open_ticket("t1").committed)
        [(kind, type, outcome)] = after(1)
        self.assertEqual((kind, type), ("command", "Open"))
        self.assertTrue(outcome.committed)
        self.assertEqual(outcome.events[0].type, "Opened")

        # Rejected by the aggregate or refused before: it still hears of it, and refuses nothing.
        self.assertFalse(open_ticket("t1").committed)
        self.assertFalse(open_ticket("t2", "mallory").committed)
        self.assertEqual([o for _, _, o in after(2)], [
            Outcome(committed=False, code="invalid_state", message="already open"),
            Outcome(committed=False, code="forbidden", message="banned"),
        ])

        # A query's response passes it, or not.
        self.assertEqual(self.k.query("support", "ListTickets"), ["t1"])
        self.assertEqual(after(1)[0][2], QueryOutcome(answered=True, data=["t1"]))
        self.assertTrue(open_ticket("secret").committed)
        after(1)
        with self.assertRaises(Rejected) as refused:
            self.k.query("support", "ListTickets")
        self.assertEqual(refused.exception.code, "hidden")
        self.assertEqual(after(1)[0][2], QueryOutcome(answered=True, data=["secret", "t1"]))

        with self.assertRaisesRegex(ValueError, "already"):
            support.read_model("x").middleware(["*"], fn=lambda check: None).middleware(["*"], fn=lambda check: None)

    def test_a_new_event_schema_is_upcast_and_outdated_processes_stop(self):
        # Version 1 stores the increment as `by`.
        v1 = domain("counters").aggregate("Counter", 0)
        v1.apply("Incremented", lambda n, data: n + data["by"])
        v1.command("Increment", lambda n, data: [{"type": "Incremented", "data": {"by": data["by"]}}])

        # Version 2 renames it to `amount` and caps a counter at 5.
        counters = domain("counters")
        incremented = counters.event("Incremented", 2)

        @incremented.upcast(1)
        def _(d):
            return {"amount": d["by"]}

        v2 = counters.aggregate("Counter", 0)
        v2.apply("Incremented", lambda n, data: n + data["amount"])

        @v2.command("Increment")
        def _(n, data):
            if n + data["by"] > 5:
                raise Rejected("at most 5", "limit")
            return [{"type": "Incremented", "data": {"amount": data["by"]}}]

        seen, archived = [], []
        current = counters.read_model("current")  # upcasts its own domain's events, no `use`
        current.on("Incremented", lambda data, evt: seen.append((evt.schema, data)))
        archive = counters.read_model("archive")
        archive.on("counters.*", lambda data, evt: archived.append((evt.schema, data)), raw=True)
        stale = domain("counters").read_model("stale")
        stale.on("Incremented", lambda data: None)
        current, archive, stale = (self.run_stream(self.k.listener(listener)) for listener in (current, archive, stale))

        old = self.run_stream(self.k.worker(v1))
        self.assertTrue(self.increment(3, wait="committed").committed)
        old.stop()
        time.sleep(0.1)
        self.run_stream(self.k.worker(v2))
        self.assertEqual(self.increment(3, wait="committed").code, "limit")  # the v1 event was upcast
        self.assertEqual(self.increment(2, wait="committed").events[0].schema, 2)

        # An outdated worker is refused: it would write version 1 next to version 2.
        outdated = self.k.worker(v1)
        self.streams.append(outdated)
        background(outdated.run)
        self.assertFalse(outdated.ready.wait(0.5))

        self.assertEqual(seen, [(2, {"amount": 3}), (2, {"amount": 2})])
        self.assertEqual(archived, [(1, {"by": 3}), (2, {"amount": 2})])
        self.assertEqual(stale.position, 1)  # stopped before the version 2 event

    def test_a_reactor_settles_on_the_read_models_before_it_and_never_waits_for_itself(self):
        self.run_stream(self.k.worker(counter))
        totals: dict[str, int] = {}
        model = counters_domain.read_model("totals")
        model.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @model.query("counters.GetTotal")
        def _(data):
            if data["id"] == "self":
                return self.k.query("counters", "GetTotal", {"id": "c1"})  # its own read model
            return totals.get(data["id"], 0)

        seen = []
        reactor = counters_domain.reactor("mirror")

        @reactor.on("counters.Incremented")
        def _(data, evt, ctx):
            if evt.aggregate_id != "c1":
                return
            started = time.monotonic()
            # Settles on the read model (order 0), not on this reactor (order 10).
            self.k.submit("counters", "Counter", "c2", "Increment", {"by": data["by"]}, id=f"mirror-{evt.position}",
                          context=ctx.follow())
            seen.append((self.k.query("counters", "GetTotal", {"id": "c2"}), time.monotonic() - started < 0.4))

        self.run_stream(self.k.listener(model))
        self.run_stream(self.k.listener(reactor))
        self.increment(2)
        self.assertEqual(seen, [(2, True)])
        with self.assertRaises(Rejected) as refused:
            self.k.query("counters", "GetTotal", {"id": "self"})
        self.assertIn("answered only by totals itself", str(refused.exception))

    def test_personal_fields_and_secrets_live_in_the_vault_and_an_erasing_event_deletes_them(self):
        sha = lambda s: hashlib.sha256(s.encode()).hexdigest()  # noqa: E731
        identity = domain("identity")
        identity.command("RegisterAccount").personal(["email"], lambda d: d["id"]).secret(["password"])
        identity.event("AccountRegistered", 1).personal(["email"], lambda d: d["accountId"])
        identity.event("AccountErased", 1).erases(lambda d: d["accountId"])
        account = identity.aggregate("Account", {})
        account.apply("AccountRegistered", lambda s, data: data)
        account.command("RegisterAccount", lambda s, data: [{"type": "AccountRegistered", "data": {
            "accountId": data["id"], "email": data["email"], "passwordHash": sha(data["password"])}}])
        account.command("EraseAccount", lambda s, data: [{"type": "AccountErased", "data": {"accountId": data["id"]}}])

        self.k.close()  # this test needs other command types: start a kernel of its own
        self.kernel.dispose()
        self.kernel = TestKernel(["identity.*"])
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key).use(identity)
        self.run_stream(self.k.worker(account))
        emails, replayed = {}, []
        model = identity.read_model("accounts")
        model.on("identity.AccountRegistered", lambda data, evt: emails.__setitem__(evt.aggregate_id, data["email"]))
        model.on("identity.AccountErased", lambda data, evt: emails.pop(evt.aggregate_id, None))
        self.run_stream(self.k.listener(model))

        def send(type, data):
            return self.k.submit("identity", "Account", "a1", type, data)

        registered = send("RegisterAccount", {"id": "a1", "email": "max@firma.de", "password": "correct horse"})
        self.assertEqual(registered.events[0].data,
                         {"accountId": "a1", "email": None, "passwordHash": sha("correct horse")})
        self.assertEqual(registered.events[0].redacted, ["email"])
        self.assertEqual(emails, {"a1": "max@firma.de"})
        for name in os.listdir(self.kernel.dir):
            path = os.path.join(self.kernel.dir, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    content = f.read()
                self.assertNotIn(b"max@firma.de", content, name)
                self.assertNotIn(b"correct horse", content, name)

        self.assertTrue(send("EraseAccount", {"id": "a1"}).committed)
        self.assertEqual(emails, {})
        rebuilt = domain("Reports").read_model("rebuilt").use(identity)
        rebuilt.on("identity.AccountRegistered", lambda data, evt: replayed.append((data["email"], evt.redacted)))
        self.run_stream(self.k.listener(rebuilt))
        time.sleep(0.2)
        self.assertEqual(replayed, [(None, ["email"])])

    def test_a_read_model_with_a_store_continues_and_rebuilds_on_a_new_schema(self):
        self.run_stream(self.k.worker(counter))
        url = f"sqlite:{self.kernel.dir}/totals.db"
        handled = []

        def totals(schema="1"):
            model = counters_domain.read_model("totals", store=url, schema=schema, reconnect_s=0.05)

            @model.on("counters.Incremented")
            def _(data, evt, ctx):
                handled.append(evt.position)
                ctx.store.set("totals", evt.aggregate_id, ctx.store.get("totals", evt.aggregate_id, 0) + data["by"])
                if fail and ctx.store.get("totals", evt.aggregate_id) == 9:
                    raise RuntimeError("boom")  # after writing: the write must not stick

            @model.query("counters.GetTotal")
            def _(data, qry, ctx):
                return ctx.store.get("totals", data["id"])

            return self.k.listener(model)

        def total(position):
            return self.k.query("counters", "GetTotal", {"id": "c1"}, min_position=position)

        fail = False
        model = self.run_stream(totals())
        for by in (1, 2, 3):
            self.increment(by, wait="committed")
        self.assertEqual(total(3), 6)
        model.stop()
        time.sleep(0.1)

        # A new process with the same store: nothing is replayed, the data is there.
        self.increment(1, wait="committed")
        handled.clear()
        model = self.run_stream(totals())
        self.assertEqual(total(4), 7)
        self.assertEqual(handled, [4])
        self.assertEqual(model.store.all("totals"), [("c1", 7)])

        # A failing handler rolls its batch back; delivered again, it counts once.
        fail = True
        self.increment(2, wait="committed")
        time.sleep(0.2)
        fail = False
        for _ in range(100):
            if model.position >= 5:
                break
            time.sleep(0.02)
        self.assertEqual(total(5), 9)
        model.stop()
        time.sleep(0.1)

        # A new schema clears the store and rebuilds from the first event.
        handled.clear()
        self.run_stream(totals("2"))
        self.assertEqual(total(5), 9)
        self.assertEqual(handled, [1, 2, 3, 4, 5])

    def test_a_read_model_whose_commit_fails_handles_the_batch_again(self):
        self.run_stream(self.k.worker(counter))
        model = counters_domain.read_model("sums", store=f"sqlite:{self.kernel.dir}/sums.db", reconnect_s=0.05)

        @model.on("counters.Incremented")
        def _(data, evt, ctx):
            ctx.store.set("sums", evt.aggregate_id, ctx.store.get("sums", evt.aggregate_id, 0) + data["by"])

        handle = self.run_stream(self.k.listener(model))

        class FullDisk:
            """The store's connection, failing the first position update as a full disk would."""

            def __init__(self, db):
                self.db, self.failed = db, False

            def execute(self, sql, *args):
                if sql.startswith("UPDATE meta") and not self.failed:
                    self.failed = True
                    raise sqlite3.OperationalError("database or disk is full")
                return self.db.execute(sql, *args)

            def __getattr__(self, name):
                return getattr(self.db, name)

        disk = FullDisk(handle.store._db)
        handle.store._db = disk
        self.increment(1, wait="committed")
        self.increment(2, wait="committed")
        for _ in range(150):
            if handle.store.committed >= 2:
                break
            time.sleep(0.02)
        self.assertTrue(disk.failed)
        self.assertEqual(handle.store.committed, 2)
        self.assertEqual(handle.store.get("sums", "c1"), 3, "the failed batch was handled again, once")

    def test_a_new_reactor_with_a_store_starts_at_the_newest_event(self):
        self.run_stream(self.k.worker(counter))
        for by in (1, 2):
            self.increment(by, wait="committed")
        seen = []
        mailer = counters_domain.reactor("stored-mailer", store=f"sqlite:{self.kernel.dir}/mailer.db")

        @mailer.on("counters.Incremented")
        def _(data, evt, ctx):
            seen.append(data["by"])

        self.run_stream(self.k.listener(mailer))
        self.increment(3, wait="committed")
        for _ in range(100):
            if seen:
                break
            time.sleep(0.02)
        time.sleep(0.1)
        self.assertEqual(seen, [3])

    def test_a_command_handler_that_changes_its_state_does_not_change_the_snapshot(self):
        box = counters_domain.aggregate("Box", {"items": []}, snapshot_every=1)

        @box.apply("Opened")
        def _(state, data):
            state["items"].append("opened")

        @box.command("Open")
        def _(state, data):
            state["items"].append("opened")  # changes its copy only
            return [{"type": "Opened"}]

        @box.command("Count")
        def _(state, data):
            raise Rejected(str(len(state["items"])), "count")

        self.run_stream(self.k.worker(box))
        self.assertTrue(self.k.submit("counters", "Box", "b1", "Open", {}, wait="committed").committed)
        counted = self.k.submit("counters", "Box", "b1", "Count", {}, wait="committed")
        self.assertEqual((counted.code, counted.message), ("count", "1"), "the snapshot equals a replay")

    def test_event_schemas_are_checked_when_defined(self):
        counters = domain("counters")
        with self.assertRaises(ValueError):
            counters.event("Incremented", 0)
        with self.assertRaises(ValueError):
            counters.event("counters.Incremented", 2)
        with self.assertRaises(ValueError):
            counters.event("Incremented", 2).upcast(2, lambda d: d)


if __name__ == "__main__":
    unittest.main()
