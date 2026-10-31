"""A kernel that misbehaves, played by a fake gRPC server: a listener must not handle an event
twice when batches repeat or go back, nor acknowledge what it did not handle."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent import futures
from unittest import mock

import grpc

from kernable import Kernable, domain
from kernable.v1 import kernable_pb2 as pb
from kernable.v1 import kernable_pb2_grpc as rpc
from tests.kernel import TestKernel


def event(position: int) -> pb.Event:
    return pb.Event(global_position=position, tenant_id="t1", domain="counters", aggregate_type="Counter",
                    aggregate_id="c1", event_type="Incremented", version=position, schema=1,
                    payload=json.dumps({"by": position}).encode())


class FakeKernel(rpc.ListenerServiceServicer):
    """Repeats a batch with a new event, then sends one that goes back."""

    def __init__(self):
        self.hellos: list[int | None] = []
        self.acks: list[int] = []

    def Listen(self, requests, context):
        hello = next(requests).hello
        self.hellos.append(hello.from_position if hello.HasField("from_position") else None)
        if len(self.hellos) > 1:
            return  # the reconnect: seen, that is all
        yield pb.ListenerDelivery(ready=pb.Ready(after_position=0, head_position=0))
        yield pb.ListenerDelivery(batch=pb.Batch(events=[event(1), event(2)], up_to=2))
        self.acks.append(next(requests).ack.position)
        yield pb.ListenerDelivery(batch=pb.Batch(events=[event(1), event(2), event(3)], up_to=3))
        self.acks.append(next(requests).ack.position)
        yield pb.ListenerDelivery(batch=pb.Batch(events=[event(2)], up_to=2))
        for m in requests:  # an ack here would be wrong
            if m.HasField("ack"):
                self.acks.append(m.ack.position)


class HostileKernel(unittest.TestCase):
    def test_a_listener_handles_each_event_once_when_the_kernel_repeats_batches_or_goes_back(self):
        keys = TestKernel(["counters.*"])  # only for its key files
        self.addCleanup(keys.dispose)
        directory = tempfile.mkdtemp(prefix="kn-fake-", dir="/tmp")
        self.addCleanup(shutil.rmtree, directory, True)
        fake = FakeKernel()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        rpc.add_ListenerServiceServicer_to_server(fake, server)
        url = f"unix:{directory}/fake.sock"
        server.add_insecure_port(url)
        server.start()
        self.addCleanup(server.stop, None)
        env = mock.patch.dict(os.environ, {"KERNABLE_LOG": "off"})
        env.start()
        self.addCleanup(env.stop)

        handled = []
        model = domain("counters").read_model("fake", reconnect_s=0.05)

        @model.on("counters.Incremented")
        def _(data, evt, ctx):
            handled.append(data["by"])

        k = Kernable(url, keys.app_key)
        self.addCleanup(k.close)
        handle = k.listener(model)
        threading.Thread(target=handle.run, daemon=True).start()
        self.addCleanup(handle.stop)
        for _ in range(250):
            if len(fake.hellos) >= 2:
                break
            time.sleep(0.02)
        self.assertEqual(handled, [1, 2, 3], "each event once")
        self.assertEqual(fake.acks, [2, 3], "no ack for the batch that went back")
        self.assertEqual(fake.hellos, [0, 3], "the reconnect resumes after what was handled")
