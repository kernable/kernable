"""Python SDK: a counter domain end to end against the real kernel.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import contextlib
import hashlib
import io
import os
import re
import sqlite3
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import grpc

from kernable import (ClientClosed, Data, Kernable, NotSettled, Outcome, Query, QueryOutcome, Rejected, RequestContext, Target,
                      domain)
from kernable.signing import Key, derived_id
from tests.kernel import TestKernel


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
        sha = lambda b: hashlib.sha256(b).hexdigest()
        self.assertEqual(self.k.data.put("shop", "notes/n-1.txt", "Grüße"), sha("Grüße".encode()))
        self.assertEqual(self.k.data.get("shop", "notes/n-1.txt"),
                         Data("Grüße".encode(), "text/plain; charset=utf-8", sha("Grüße".encode())))
        self.k.data.put("shop", "invoices/o-1.pdf", b"%PDF-1.7", "application/pdf")
        self.assertEqual(self.k.data.get("shop", "invoices/o-1.pdf"), Data(b"%PDF-1.7", "application/pdf", sha(b"%PDF-1.7")))
        self.assertIsNone(self.k.data.get("shop", "invoices/o-1.pdf", tenant="t2"))
        large = os.urandom(5 * 1024 * 1024)
        self.k.data.put("shop", "exports/large.bin", large)
        self.assertEqual(self.k.data.get("shop", "exports/large.bin"), Data(large, "application/octet-stream", sha(large)))

        # Listed by prefix, in key order, page after page.
        self.k.data.put("shop", "invoices/o-2.pdf", b"%PDF", "application/pdf")
        page = self.k.data.list("shop", "invoices/")
        self.assertEqual([(f.key, f.size) for f in page.files], [("invoices/o-1.pdf", 8), ("invoices/o-2.pdf", 4)])
        self.assertIsNone(page.next)
        self.assertEqual([f.key for f in self.k.data.list("shop", "invoices/", after="invoices/o-1.pdf").files],
                         ["invoices/o-2.pdf"])
        self.assertEqual(len(self.k.data.list("shop").files), 4)
        self.k.data.delete("shop", "invoices/o-2.pdf")

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
        self.assertEqual([f.key for f in as_alice.data.list("shop", "notes/").files], ["notes/n-1.txt"])

        self.assertEqual(checks[0], ("data", "PutData", "shop", "t1", "notes/n-1.txt", None, ""))
        self.assertEqual(checks[1], ("cache", "SetCache", "shop", "t1", "rate/c-1", None, ""))
        self.assertEqual([(c[0], c[1], c[6]) for c in checks[2:]],
                         [("data", "PutData", "alice"), ("cache", "SetCache", "alice"),
                          ("data", "GetData", "alice"), ("cache", "GetCache", "alice"),
                          ("data", "ListData", "alice")])
        stream.stop()  # before tearDown stops the kernel: no reconnect warning

        # While it is away a write is refused, and not asked again: it may not run twice.
        time.sleep(0.2)
        started = time.monotonic()
        with self.assertRaises(grpc.RpcError) as away:
            self.k.data.put("shop", "notes/n-2.txt", "hi", context=alice)
        self.assertEqual(away.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertLess(time.monotonic() - started, 2)


class Connection(unittest.TestCase):
    def test_plain_text_goes_only_to_a_kernel_on_this_machine_unless_allowed(self):
        kernel = TestKernel(["counters.*"])
        self.addCleanup(kernel.dispose)
        for url in ("unix:/tmp/k.sock", "http://127.0.0.1:7233", "localhost:7233", "[::1]:7233", "127.0.0.2:7233"):
            Kernable(url, kernel.app_key).close()
        for url in ("http://10.0.0.5:7233", "kernel.internal:7233", "http://127.0.0.1.example.com:7233"):
            with self.assertRaisesRegex(ValueError, "not on this machine", msg=url):
                Kernable(url, kernel.app_key)
        Kernable("http://10.0.0.5:7233", kernel.app_key, insecure=True).close()


class Reconnects(unittest.TestCase):
    def test_reconnects_back_off_and_a_refusal_is_logged_as_an_error(self):
        kernel = TestKernel(["counters.*"])
        self.addCleanup(kernel.dispose)
        stderr = io.StringIO()
        patched = mock.patch("sys.stderr", stderr)
        patched.start()
        self.addCleanup(patched.stop)

        def lines():
            return [l for l in stderr.getvalue().splitlines() if "reconnecting in" in l]

        def until(done):
            # Up to 30 s, done as soon as it is: under load a kernel's start takes its time.
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if done():
                    return
                time.sleep(0.02)

        # No kernel at all: every attempt fails, and each waits longer.
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"}):
            k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        watcher = k.listener(domain("counters").read_model("watcher", reconnect_s=0.05))
        threading.Thread(target=watcher.run, daemon=True).start()
        until(lambda: len(lines()) >= 4)
        watcher.stop()
        delays = [float(re.search(r"reconnecting in ([\d.]+) s", l).group(1)) for l in lines()[:4]]
        self.assertTrue(all(b > a * 1.3 for a, b in zip(delays, delays[1:])), delays)

        # A worker for an aggregate the key may not run: refused, an error (a client that logs
        # errors only still prints it).
        kernel.start()
        stderr.truncate(0)
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "error"}):
            strict = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(strict.close)
        other = domain("other").aggregate("Thing", 0)

        @other.command("Do")
        def _(state, data):
            return []

        refused = strict.worker(other)
        threading.Thread(target=refused.run, daemon=True).start()
        self.addCleanup(refused.stop)
        until(lambda: any("refused (" in l for l in lines()))
        self.assertTrue(any("refused (" in l for l in lines()), stderr.getvalue())


class Stopping(unittest.TestCase):
    def test_stop_ends_a_listener_at_once_also_while_it_waits_to_reconnect(self):
        kernel = TestKernel(["counters.*"])  # never started
        self.addCleanup(kernel.dispose)
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        handle = k.listener(domain("counters").read_model("waiting", reconnect_s=2))
        thread = threading.Thread(target=handle.run, daemon=True)
        thread.start()
        time.sleep(0.3)  # the first attempt failed; it waits 2 s
        started = time.monotonic()
        handle.stop()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertLess(time.monotonic() - started, 0.5)

    def test_a_closed_client_refuses_requests_and_ends_its_streams(self):
        kernel = TestKernel(["counters.*"])
        self.addCleanup(kernel.dispose)
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(kernel.url, kernel.app_key)
        k.close()  # grpcio would crash the process on a call over the closed channel
        with self.assertRaises(ClientClosed):
            k.submit("counters", "Counter", "c1", "Increment", {"by": 1})
        with self.assertRaises(ClientClosed):
            k.query("counters", "GetTotal", {"id": "c1"})
        handle = k.listener(domain("counters").read_model("closed", reconnect_s=0.05))
        errors = []
        thread = threading.Thread(target=lambda: _record(errors, handle.run), daemon=True)
        thread.start()
        thread.join(2)
        self.assertFalse(thread.is_alive(), "run() ends")
        self.assertEqual(errors, [])


class LongHistories(unittest.TestCase):
    def test_a_history_longer_than_one_message_arrives_in_parts(self):
        # Messages of at most 64 KiB, no snapshots: the history of twelve 20 KiB events comes in
        # parts, and the state folds all of it.
        kernel = TestKernel(["shop.*"], ["--max-message-bytes", "65536"])
        self.addCleanup(kernel.dispose)
        kernel.start()
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        order = domain("shop").aggregate("Order", 0, snapshot_every=0)
        order.apply("Added", lambda n, data: n + 1)
        order.command("Add", lambda n, data: [{"type": "Added", "data": {"note": data["note"], "seen": n}}])
        worker = k.worker(order)
        self.addCleanup(worker.stop)
        background(worker.run)
        self.assertTrue(worker.ready.wait(5), "worker did not connect")
        note = "x" * (20 << 10)
        for n in range(13):
            outcome = k.submit("shop", "Order", "o1", "Add", {"note": note if n < 12 else ""}, wait="committed")
            self.assertTrue(outcome.committed, f"{n}: {outcome}")
            self.assertEqual(outcome.events[0].data["seen"], n, f"the state before command {n}")


def _record(errors, run):
    try:
        run()
    except Exception as err:  # what would kill the thread
        errors.append(repr(err))


class Keys(unittest.TestCase):
    def test_data_that_is_not_json_is_refused_before_it_is_sent(self):
        kernel = TestKernel(["counters.*"])
        self.addCleanup(kernel.dispose)
        k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        for bad in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                k.enqueue("counters", "Counter", "c1", "Increment", {"by": bad})

    def test_a_key_file_others_can_read_is_warned_about(self):
        kernel = TestKernel(["counters.*"])
        self.addCleanup(kernel.dispose)
        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            Kernable(kernel.url, kernel.app_key).close()
            self.assertEqual(stderr.getvalue(), "", "mode 600: no warning")
            os.chmod(kernel.app_key, 0o644)
            Kernable(kernel.url, kernel.app_key).close()
        self.assertRegex(stderr.getvalue(), r"app\.key can be read by other users \(mode 644\): chmod 600")


class Hangs(unittest.TestCase):
    def test_a_handler_that_hangs_is_reported(self):
        kernel = TestKernel(["counters.*"], ["--lease-ttl-ms", "500", "--query-timeout-ms", "8000"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        stderr = io.StringIO()
        patched = mock.patch("sys.stderr", stderr)
        patched.start()
        self.addCleanup(patched.stop)
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "error"}):
            k = Kernable(kernel.url, kernel.app_key)
        self.addCleanup(k.close)
        release = threading.Event()
        self.addCleanup(release.set)
        slow = domain("counters").aggregate("Slow", 0)

        @slow.command("Do")
        def _(state, data):
            release.wait(10)
            return []

        answers = domain("counters").read_model("slow-answers")

        @answers.query("counters.Ask")
        def _(data, qry, ctx):
            release.wait(10)
            return 1

        worker, listener = k.worker(slow), k.listener(answers)
        for s in (worker, listener):
            threading.Thread(target=s.run, daemon=True).start()
            self.addCleanup(s.stop)
            self.assertTrue(s.ready.wait(5))
        k.enqueue("counters", "Slow", "s1", "Do")
        for _ in range(150):
            if "Slow/s1 still running after its lease ended" in stderr.getvalue():
                break
            time.sleep(0.02)
        self.assertIn("Slow/s1 still running after its lease ended", stderr.getvalue())
        threading.Thread(target=lambda: self._ask(k), daemon=True).start()
        time.sleep(5.5)
        self.assertIn("still handling the query counters.Ask after 5 s", stderr.getvalue())

    @staticmethod
    def _ask(k):
        try:
            k.query("counters", "Ask")
        except grpc.RpcError:
            pass


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

        model = domain("counters").read_model("totals")
        model.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @model.query("counters.GetTotal")
        def _(data, qry, ctx):
            assert (qry.type, qry.domain, ctx.store) == ("GetTotal", "counters", None)
            if data["id"] not in totals:
                raise Rejected(f"no counter {data['id']}", "not_found")
            return totals[data["id"]]

        def total(counter: str) -> int:
            return self.k.query("counters", "GetTotal", {"id": counter})
        reactor = domain("counters").reactor("audit")

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
            return domain("counters").reactor("mails").on("Incremented", lambda data, evt: seen.append(evt.aggregate_id))

        audit = domain("counters").reactor("audit").on("Incremented", lambda data: None)
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
        push = v2.listener(domain("counters").reactor("push", live=True).on("Incremented", lambda data: stuck.wait(10)))
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
        extra = self.run_stream(v2.listener(domain("counters").reactor("extra").on("Incremented",
                                                                            lambda data: blocked.wait(10))))
        try:
            self.assertTrue(self.k.submit("counters", "Counter", "c3", "Increment", {"by": 1}).committed,
                            "settled without the unlisted listener")
        finally:
            blocked.set()
            extra.stop()

        with self.assertRaisesRegex(ValueError, "live"):
            domain("counters").reactor("x", live=True).middleware(["*"], fn=lambda check: None)
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
        model = domain("counters").read_model("totals")
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
        guard = domain("counters").read_model("guard")
        guard.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @guard.query("counters.GetTotal")
        def _(data):
            return totals.get(data["id"], 0)

        @guard.middleware(["counters.*"])
        def _(check):
            checks.append((check.kind, check.type, check.request.sender_identity))
            if check.request.sender_identity == "guest" and (check.kind == "query" or totals.get(check.aggregate_id, 0) >= 3):
                raise Rejected("guests stop at 3", "forbidden")

        guard_def, guard = guard, self.run_stream(self.k.listener(guard))
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
        with self.assertRaises(grpc.RpcError) as away:  # asked again until its deadline
            self.increment(1, timeout_s=0.5)
        self.assertEqual(away.exception.code(), grpc.StatusCode.UNAVAILABLE)
        # Back while a submit waits: it goes through, at a single address too.
        back = self.k.listener(guard_def)
        self.streams.append(back)
        threading.Timer(0.3, back.run).start()
        started = time.monotonic()
        self.assertTrue(self.increment(1).committed)
        self.assertGreaterEqual(time.monotonic() - started, 0.25)

    def test_a_query_declares_the_aggregate_it_is_about_and_a_middleware_gets_it(self):
        class GetCount(Query[int]):
            target = {"aggregate_type": "Counter", "field": "counter_id"}
            counter_id: str

        counters = domain("counters")
        counters.query("GetTotal").target("other.Counter", "id")
        model = counters.read_model("totals")
        model.query(GetCount, lambda d: 0)
        model.query("GetTotal", lambda data: 0)
        model.query("GetAll", lambda data: 0)
        targets = []

        @model.middleware(["counters.*"])
        def _(check):
            targets.append((check.type, check.target))

        self.run_stream(self.k.worker(counter))
        self.run_stream(self.k.listener(model))
        self.k.use(counters)
        self.k.query(GetCount(counter_id="c1"))
        self.k.query("counters", "GetTotal", {"id": "c2"})
        self.k.query("counters", "GetTotal", {})  # no ID: no target
        self.k.query("counters", "GetAll", {"id": "c3"})  # declares none
        self.k.query(GetCount(counter_id="c4"), all_partitions=True)  # the wait-only query carries it, signed
        self.assertTrue(self.increment(1).committed)
        self.assertEqual(targets, [
            ("GetCount", Target("counters.Counter", "c1")),
            ("GetTotal", Target("other.Counter", "c2")),
            ("GetTotal", None),
            ("GetAll", None),
            ("GetCount", Target("counters.Counter", "c4")),
            ("Increment", Target("counters.Counter", "c1")),
        ])

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

    def test_a_new_event_schema_is_upcast_outdated_workers_stop_and_outdated_read_models_skip(self):
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
        stale_seen = []
        stale.on("Incremented", lambda data: stale_seen.append(data))
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
        # It never misreads the version 2 event: it skips it, counts it, and goes on.
        for _ in range(100):
            if stale.position == 2:
                break
            time.sleep(0.02)
        self.assertEqual((stale.position, stale.skipped, stale_seen), (2, 1, [{"by": 3}]))

    def test_a_reactor_settles_on_the_read_models_before_it_and_never_waits_for_itself(self):
        self.run_stream(self.k.worker(counter))
        totals: dict[str, int] = {}
        model = domain("counters").read_model("totals")
        model.on("counters.Incremented", lambda data, evt: totals.__setitem__(evt.aggregate_id, totals.get(evt.aggregate_id, 0) + data["by"]))

        @model.query("counters.GetTotal")
        def _(data):
            if data["id"] == "self":
                return self.k.query("counters", "GetTotal", {"id": "c1"})  # its own read model
            return totals.get(data["id"], 0)

        seen = []
        reactor = domain("counters").reactor("mirror")

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

        registered = send("RegisterAccount", {"id": "a1", "email": "max@example.com", "password": "correct horse"})
        self.assertEqual(registered.events[0].data,
                         {"accountId": "a1", "email": None, "passwordHash": sha("correct horse")})
        self.assertEqual(registered.events[0].redacted, ["email"])
        self.assertEqual(emails, {"a1": "max@example.com"})
        for name in os.listdir(self.kernel.dir):
            path = os.path.join(self.kernel.dir, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    content = f.read()
                self.assertNotIn(b"max@example.com", content, name)
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
            model = domain("counters").read_model("totals", store=url, schema=schema, reconnect_s=0.05)

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
        model = domain("counters").read_model("sums", store=f"sqlite:{self.kernel.dir}/sums.db", reconnect_s=0.05)

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
        mailer = domain("counters").reactor("stored-mailer", store=f"sqlite:{self.kernel.dir}/mailer.db")

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

    def test_a_store_that_cannot_be_opened_yet_is_tried_again(self):
        self.run_stream(self.k.worker(counter))
        # Its directory is missing at first (as a full disk fails an open).
        later = f"{self.kernel.dir}/later"
        seen = []
        model = domain("counters").read_model("late-model", store=f"sqlite:{later}/model.db", reconnect_s=0.05)

        @model.on("counters.Incremented")
        def _(data, evt, ctx):
            seen.append(data["by"])

        listener = self.k.listener(model)
        self.streams.append(listener)
        thread = background(listener.run)
        time.sleep(0.3)
        self.assertTrue(thread.is_alive(), "it gave up")
        os.mkdir(later)
        self.assertTrue(listener.ready.wait(10), "it never opened the store")
        self.increment(4, wait="committed")
        for _ in range(100):
            if seen:
                break
            time.sleep(0.02)
        self.assertEqual(seen, [4])

    def test_a_request_waits_for_the_kernel_to_come_back_until_its_deadline(self):
        self.run_stream(self.k.worker(counter))
        self.kernel.kill()
        # Until the client has seen the connection close, a request still goes out on it and
        # ends UNAVAILABLE (it may have reached the kernel); this test is about the time after.
        time.sleep(0.3)
        started = time.monotonic()
        with self.assertRaises(grpc.RpcError) as refused:
            self.increment(1, wait="committed", id="i0", timeout_s=0.3)
        self.assertEqual(refused.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertLess(time.monotonic() - started, 2)
        # Back while the request waits: it goes through.
        threading.Timer(0.3, self.kernel.start).start()
        self.assertTrue(self.increment(1, wait="committed", id="i1").committed)

    def test_an_unexpected_error_is_rejected_with_its_type_only(self):
        leaky = domain("counters").aggregate("Leaky", 0)

        @leaky.command("Do")
        def _(state, data):
            raise TypeError("password hunter2 rejected by db://admin@10.0.0.5")

        self.run_stream(self.k.worker(leaky))
        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            outcome = self.k.submit("counters", "Leaky", "l1", "Do", {}, wait="committed")
        self.assertEqual((outcome.committed, outcome.code, outcome.message), (False, "handler_error", "TypeError"))
        self.assertIn("hunter2", stderr.getvalue(), "the worker's log has the whole error")

    def test_a_middleware_that_returns_false_or_something_else_refuses(self):
        self.run_stream(self.k.worker(counter))
        verdicts = {"value": None}
        guard = domain("counters").read_model("guard")

        @guard.middleware(["counters.*"])
        def _(check, outcome=None):
            return verdicts["value"]

        self.run_stream(self.k.listener(guard))
        for value, code in ((None, ""), (True, ""), (False, "refused"), ("no", "middleware_error"), (0, "middleware_error")):
            verdicts["value"] = value
            outcome = self.k.submit("counters", "Counter", "m1", "Increment", {"by": 1}, wait="committed")
            self.assertEqual(outcome.code, code, repr(value))

    def test_a_command_handler_that_changes_its_state_does_not_change_the_snapshot(self):
        box = domain("counters").aggregate("Box", {"items": []}, snapshot_every=1)

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

    def test_a_query_answer_of_none_arrives_as_none(self):
        model = domain("counters").read_model("finder")
        model.query("Find", lambda data: None)
        model.query("FindEmpty", lambda data: {})
        self.run_stream(self.k.listener(model))
        self.assertIsNone(self.k.query("counters", "Find"))
        self.assertEqual(self.k.query("counters", "FindEmpty"), {})

    def test_a_listener_sees_when_an_event_was_committed(self):
        self.run_stream(self.k.worker(counter))
        seen = []
        model = domain("counters").read_model("stamps")
        model.on("Incremented", lambda data, evt: seen.append(evt.created_at))
        self.run_stream(self.k.listener(model))
        outcome = self.increment(1)
        self.assertEqual(len(seen), 1)
        self.assertLess(abs(datetime.now(timezone.utc) - seen[0]), timedelta(seconds=5))
        self.assertEqual(seen[0], outcome.events[0].created_at)

    def test_a_subscription_streams_backlog_and_live_events_and_gets_a_failed_one_again(self):
        self.run_stream(self.k.worker(counter))
        first = self.increment(1)
        seen: list[tuple[int, str, int]] = []
        failed = []

        def handle(event):
            if event.data["by"] == 2 and not failed:
                failed.append(event.position)
                raise RuntimeError("once")
            seen.append((event.position, event.type, event.data["by"]))

        sub = self.k.subscribe(handle, types=["counters.*"], reconnect_s=0.05)
        self.addCleanup(sub.stop)
        self.increment(2)
        self.increment(3)
        for _ in range(250):
            if len(seen) >= 3:
                break
            time.sleep(0.02)
        self.assertEqual([(t, by) for _, t, by in seen], [("Incremented", 1), ("Incremented", 2), ("Incremented", 3)])
        self.assertEqual(seen[0][0], first.events[0].position)
        self.assertEqual(failed, [seen[1][0]], "the failed event came again")
        self.assertEqual(sub.position, seen[2][0])
        sub.stop()
        self.assertTrue(sub.done.wait(5), "the subscription ended")

        # From a position on: only what came after it.
        later: list[int] = []
        resumed = self.k.subscribe(lambda e: later.append(e.data["by"]), after=seen[1][0])
        self.addCleanup(resumed.stop)
        for _ in range(250):
            if later:
                break
            time.sleep(0.02)
        time.sleep(0.1)
        self.assertEqual(later, [3])

    def test_a_listener_closes_its_store_when_it_stops_and_a_later_run_opens_it_again(self):
        self.run_stream(self.k.worker(counter))
        path = f"{self.kernel.dir}/closing.db"
        model = domain("counters").read_model("closing", store=f"sqlite:{path}", reconnect_s=0.05)

        @model.on("Incremented")
        def _(data, evt, ctx):
            ctx.store.set("sums", evt.aggregate_id, ctx.store.get("sums", evt.aggregate_id, 0) + data["by"])

        handle = self.k.listener(model)
        self.streams.append(handle)
        thread = background(handle.run)
        self.assertTrue(handle.ready.wait(5))
        self.increment(2)
        store = handle.store
        handle.stop()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(handle.store)
        with self.assertRaises(sqlite3.ProgrammingError):
            store.get("sums", "c1")
        self.assertFalse(os.path.exists(f"{path}-wal"), "the database is released")
        again = self.run_stream(self.k.listener(model))
        self.assertEqual(again.store.get("sums", "c1"), 2)

    def identity_kernel(self, *domains):
        """Replaces the counters kernel with one that runs the identity domain's commands."""
        self.k.close()
        self.kernel.dispose()
        self.kernel = TestKernel(["identity.*"])
        self.kernel.start()
        self.k = Kernable(self.kernel.url, self.kernel.app_key).use(*domains)

    def test_a_read_model_store_keeps_no_erased_value_on_disk(self):
        identity = domain("identity")
        identity.command("RegisterAccount").personal(["email"], lambda d: d["id"])
        identity.event("AccountRegistered", 1).personal(["email"], lambda d: d["id"])
        identity.event("AccountErased", 1).erases(lambda d: d["id"])
        account = identity.aggregate("Account", {})
        account.command("RegisterAccount", lambda s, data: [{"type": "AccountRegistered", "data": data}])
        account.command("EraseAccount", lambda s, data: [{"type": "AccountErased", "data": data}])
        self.identity_kernel(identity)
        self.run_stream(self.k.worker(account))
        directory = f"{self.kernel.dir}/store"
        os.mkdir(directory)
        model = identity.read_model("accounts", store=f"sqlite:{directory}/accounts.db")
        model.on("AccountRegistered", lambda data, evt, ctx: ctx.store.set("accounts", data["id"], data))
        model.on("AccountErased", lambda data, evt, ctx: ctx.store.remove("accounts", data["id"]))
        handle = self.run_stream(self.k.listener(model))

        def on_disk() -> list[str]:
            found = []
            for name in os.listdir(directory):
                with open(os.path.join(directory, name), "rb") as f:
                    if b"max@example.com" in f.read():
                        found.append(name)
            return found

        send = lambda type, data: self.k.submit("identity", "Account", "a1", type, data)  # noqa: E731
        self.assertTrue(send("RegisterAccount", {"id": "a1", "email": "max@example.com"}).committed)
        self.assertEqual(handle.store.get("accounts", "a1")["email"], "max@example.com")
        self.assertTrue(on_disk(), "the store holds the value")
        # Settled: the read model acknowledged the erasing event's batch.
        self.assertTrue(send("EraseAccount", {"id": "a1"}).committed)
        self.assertIsNone(handle.store.get("accounts", "a1"))
        self.assertEqual(on_disk(), [], "no file of the store keeps the erased value")

    def test_an_erasing_event_is_never_parked(self):
        identity = domain("identity")
        identity.command("RegisterAccount").personal(["email"], lambda d: d["id"])
        identity.event("AccountRegistered", 1).personal(["email"], lambda d: d["id"])
        identity.event("AccountErased", 1).erases(lambda d: d["id"])
        account = identity.aggregate("Account", {})
        account.command("RegisterAccount", lambda s, data: [{"type": "AccountRegistered", "data": data}])
        account.command("EraseAccount", lambda s, data: [{"type": "AccountErased", "data": data}])
        self.identity_kernel(identity)
        self.run_stream(self.k.worker(account))
        # Parks at the first failure, but not an erasure: that must get through, or the email
        # would stay in the store.
        model = identity.reactor("accounts", store="memory:", park_after=1, reconnect_s=0.02)
        failures = [0]
        model.on("AccountRegistered", lambda data, evt, ctx: ctx.store.set("accounts", data["id"], data))

        @model.on("AccountErased")
        def _(data, evt, ctx):
            failures[0] += 1
            if failures[0] <= 2:
                raise Rejected("not yet")
            ctx.store.remove("accounts", data["id"])

        handle = self.run_stream(self.k.listener(model))
        send = lambda type, data: self.k.submit("identity", "Account", "a1", type, data)  # noqa: E731
        self.assertTrue(send("RegisterAccount", {"id": "a1", "email": "max@example.com"}).committed)
        self.assertTrue(send("EraseAccount", {"id": "a1"}).committed)
        self.assertEqual(failures[0], 3, "tried until it got through")
        self.assertIsNone(handle.store.get("accounts", "a1"))
        parked = self.kernel.cli("listeners", "parked", self.kernel.store)
        self.assertEqual(parked.stdout, "", "nothing parked")

    def test_a_personal_field_an_event_does_not_declare_is_refused(self):
        identity = domain("identity")
        identity.command("RegisterAccount").personal(["email"], lambda d: d["id"])
        identity.command("NoteAccount").personal(["email"], lambda d: d["id"])
        identity.event("AccountRegistered", 1).personal(["email"], lambda d: d["id"])
        account = identity.aggregate("Account", {})
        account.command("RegisterAccount", lambda s, data: [{"type": "AccountRegistered", "data": data}])
        # Forgot `identity.event("AccountNoted", 1).personal(...)`: the email would be in clear.
        account.command("NoteAccount", lambda s, data: [{"type": "AccountNoted", "data": data}])
        self.identity_kernel(identity)
        self.run_stream(self.k.worker(account))

        def send(type: str, email: str | None = "max@example.com"):
            return self.k.submit("identity", "Account", "a1", type, {"id": "a1", "email": email})

        refused = send("NoteAccount")
        self.assertEqual((refused.committed, refused.code), (False, "personal_in_clear"))
        for name in ("NoteAccount", "AccountNoted", "email"):
            self.assertIn(name, refused.message)
        self.assertTrue(send("NoteAccount", None).committed, "a null value holds nothing personal")
        registered = send("RegisterAccount")
        self.assertTrue(registered.committed, "a field declared on both is kept in the vault")
        self.assertEqual(registered.events[0].redacted, ["email"])
        for name in os.listdir(self.kernel.dir):
            path = os.path.join(self.kernel.dir, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    self.assertNotIn(b"max@example.com", f.read(), name)


if __name__ == "__main__":
    unittest.main()
