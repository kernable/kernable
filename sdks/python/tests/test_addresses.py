"""A client given several kernels (a pair, see Run a standby) uses the one that answers.

Run from sdks/python: .venv/bin/python -m unittest discover tests   (after `cargo build`)
"""

import os
import threading
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


if __name__ == "__main__":
    unittest.main()
