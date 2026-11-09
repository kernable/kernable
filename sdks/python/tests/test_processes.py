"""Business processes against the real kernel: registering a model versions it.

Run from sdks/python: .venv/bin/python -m unittest tests.test_processes   (after `cargo build`)
"""

import os
import subprocess
import threading
import time
import unittest
from unittest import mock

import grpc

from kernable import Kernable, RegisteredProcess, RequestContext, domain
from tests.kernel import TestKernel


class Processes(unittest.TestCase):
    def setUp(self):
        try:
            self.kernel = TestKernel(["shop.*"], grants=["process=shop.*"])
        except subprocess.CalledProcessError as err:
            self.skipTest(f"the kernel has no grant `process` yet: {err.stderr.strip()}")
        self.kernel.start()
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "warn"})  # quiet: no connection lines
        env.start()
        self.addCleanup(env.stop)
        self.k = Kernable(self.kernel.url, self.kernel.app_key, tenant="t1")
        self.worker = None

    def tearDown(self):
        if self.worker:
            self.worker.stop()
        self.k.close()
        self.kernel.dispose()

    def test_registering_a_model_versions_it_and_an_app_registers_its_domains_processes(self):
        shop = domain("shop")
        order = shop.aggregate("order", 0)
        order.command("Confirm")(lambda state, data: [])
        order.command("Cancel")(lambda state, data: [])
        # The kernel knows a command type once a worker registered it.
        self.worker = self.k.worker(order)
        threading.Thread(target=self.worker.run, daemon=True).start()
        self.assertTrue(self.worker.ready.wait(5))

        def model(command: str):
            return (domain("shop").process("fulfilment")
                    .start("shop.OrderSubmitted", key="aggregate_id", data={"order_id": "aggregate_id"})
                    .command("confirm", f"shop.order.{command}", aggregate="case.order_id")
                    .end("done"))

        try:
            first = self.k.register_process(model("Confirm"))
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.UNIMPLEMENTED:
                self.skipTest("the kernel does not serve ProcessService.Register yet")
            raise
        self.assertEqual(first, RegisteredProcess(name="shop.fulfilment", version=1, new=True))
        self.assertEqual(self.k.register_process(model("Confirm")), RegisteredProcess(name="shop.fulfilment", version=1, new=False))
        self.assertEqual(self.k.register_process(model("Cancel")), RegisteredProcess(name="shop.fulfilment", version=2, new=True))

        # As whom it acts: set once, the same again changes nothing.
        acting = RequestContext(sender_tenant="platform", sender_identity="process-shop.fulfilment", sender_session="k1")
        self.assertTrue(self.k.process_act("shop.fulfilment", acting))
        self.assertFalse(self.k.process_act("shop.fulfilment", acting))

        # The kernel checks what the builder cannot: a command type nobody runs.
        with self.assertRaises(grpc.RpcError) as refused:
            self.k.register_process(model("Unknown"))
        self.assertEqual(refused.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertIn("confirm", refused.exception.details())

        # An app registers its domains' processes once its worker is connected, and lists them.
        self.worker.stop()
        self.worker = None
        app_domain = domain("shop")
        app_order = app_domain.aggregate("order", 0)
        app_order.command("Confirm")(lambda state, data: [])
        app_order.command("Cancel")(lambda state, data: [])
        (app_domain.process("fulfilment")
         .start("shop.OrderSubmitted", key="aggregate_id", data={"order_id": "aggregate_id"})
         .command("confirm", "shop.order.Cancel", aggregate="case.order_id")
         .end("done"))
        app = self.k.app(service="shop", domains=[app_domain])
        self.assertEqual([p.qualified for p in app.manifest.processes], ["shop.fulfilment"])
        threading.Thread(target=app.run, daemon=True).start()
        try:
            self.assertTrue(app.ready.wait(5))
            for _ in range(250):
                if app.processes:
                    break
                time.sleep(0.02)
            self.assertEqual(app.processes, {"shop.fulfilment": RegisteredProcess(name="shop.fulfilment", version=2, new=False)})
        finally:
            app.stop()


if __name__ == "__main__":
    unittest.main()
