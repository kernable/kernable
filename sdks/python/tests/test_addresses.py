"""A client given several kernels (a pair, see Run a standby) uses the one that answers.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import os
import socket
import threading
import time
import unittest
from unittest import mock

from kernable import Kernable
from tests.kernel import TestKernel
from tests.test_sdk import counter


class SeveralAddresses(unittest.TestCase):
    def test_a_kernel_that_cannot_be_reached_hands_over_to_the_next_address(self):
        kernel = TestKernel(["counters.*"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        gone = f"unix:{kernel.dir}/gone.sock"
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(f"{gone},{kernel.url}", kernel.app_key, timeout_s=10)
        self.addCleanup(k.close)
        worker = k.worker(counter)
        threading.Thread(target=worker.run, daemon=True).start()
        self.addCleanup(worker.stop)
        self.assertTrue(worker.ready.wait(10), "the worker did not reach the kernel")
        outcome = k.submit("counters", "Counter", "c1", "Increment", {"by": 2}, wait="committed")
        self.assertTrue(outcome.committed, outcome)
        self.assertEqual(len(outcome.events), 1)

    def test_a_pair_client_turns_to_the_other_kernel_within_seconds_when_one_is_cut_off(self):
        kernel = TestKernel(["counters.*"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        proxied, freeze = _freezable_proxy(kernel.url.removeprefix("unix:"))
        # The worker connects directly: what is measured is the client's turn (through the
        # proxy, the one kernel behind both addresses would lease the next command to the frozen
        # worker session until its lease ran out).
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            direct = Kernable(kernel.url, kernel.app_key, timeout_s=10)
            k = Kernable(f"{proxied},{kernel.url}", kernel.app_key, timeout_s=30)
        self.addCleanup(direct.close)
        self.addCleanup(k.close)
        worker = direct.worker(counter)
        threading.Thread(target=worker.run, daemon=True).start()
        self.addCleanup(worker.stop)
        self.assertTrue(worker.ready.wait(10), "the worker did not reach the kernel")
        first = k.submit("counters", "Counter", "c1", "Increment", {"by": 1}, wait="committed")
        self.assertTrue(first.committed, first)
        # The proxy freezes: the connection the client holds looks open and answers nothing.
        freeze.set()
        started = time.monotonic()
        second = k.submit("counters", "Counter", "c2", "Increment", {"by": 1}, wait="committed")
        self.assertTrue(second.committed, second)
        self.assertLess(time.monotonic() - started, 10, "the client turned to the other address too late")


class Connections(unittest.TestCase):
    def test_a_refused_grant_is_not_tried_again_on_the_same_connection(self):
        """HTTP/2 keeps headers in a table both sides share, so a header damaged on the way (the
        grant) stays wrong there for every later request: the next call opens a new connection."""
        kernel = TestKernel(["counters.*"])
        kernel.start()
        self.addCleanup(kernel.dispose)
        proxied, _, accepted = _counting_proxy(kernel.url.removeprefix("unix:"))
        # Hex, but no Grant: the kernel refuses it as unreadable.
        grant = os.path.join(kernel.dir, "damaged.grant")
        with open(grant, "w") as f:
            f.write("ff00ff00")
        with mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"}):
            k = Kernable(proxied, kernel.app_key, grant=grant, timeout_s=10)
        self.addCleanup(k.close)
        for i in range(3):
            with self.assertRaises(Exception) as refused:
                k.submit("counters", "Counter", f"c{i}", "Increment", {"by": 1})
            self.assertIn("grant", str(refused.exception))
            self.assertEqual(accepted(), i + 1, f"call {i}")


def _counting_proxy(to: str):
    """`_freezable_proxy` that also counts the connections it accepted."""
    count = [0]
    address, frozen = _freezable_proxy(to, lambda: count.__setitem__(0, count[0] + 1))
    return address, frozen, lambda: count[0]


def _freezable_proxy(to: str, on_accept=lambda: None) -> tuple[str, threading.Event]:
    """A TCP proxy to the kernel's socket at `to` that can be frozen, as when the network cuts a
    kernel off without a word (a partition): its address and the switch."""
    frozen = threading.Event()
    listener = socket.create_server(("127.0.0.1", 0))

    def pump(r: socket.socket, w: socket.socket) -> None:
        while True:
            try:
                data = r.recv(16 << 10)
            except OSError:
                return
            if not data:
                return
            if frozen.is_set():
                # Cut off: what comes in is dropped silently, nothing is closed.
                threading.Event().wait()
            try:
                w.sendall(data)
            except OSError:
                return

    def accept() -> None:
        while True:
            tcp, _ = listener.accept()
            on_accept()
            unix = socket.socket(socket.AF_UNIX)
            unix.connect(to)
            threading.Thread(target=pump, args=(tcp, unix), daemon=True).start()
            threading.Thread(target=pump, args=(unix, tcp), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    return f"127.0.0.1:{listener.getsockname()[1]}", frozen


if __name__ == "__main__":
    unittest.main()
