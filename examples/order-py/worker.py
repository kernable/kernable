"""Order worker in Python on raw generated gRPC stubs, without an SDK (PoC hypothesis H5).

Usage: python worker.py <kernel-url> <worker.key>
Stubs: python -m grpc_tools.protoc -I../../proto --python_out=gen --grpc_python_out=gen kernable/v1/kernable.proto
"""

import json
import os
import queue
import struct
import sys
import threading

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))
from kernable.v1 import kernable_pb2 as pb  # noqa: E402
from kernable.v1 import kernable_pb2_grpc as rpc  # noqa: E402

# ---- signing (proto/SIGNING.md) ----


class Msg:
    def __init__(self, tag: str):
        self.buf = bytearray()
        self.str(tag)

    def bytes(self, b: bytes) -> "Msg":
        self.buf += struct.pack(">I", len(b)) + b
        return self

    def str(self, s: str) -> "Msg":
        return self.bytes(s.encode())

    def u64(self, n: int) -> "Msg":
        self.buf += struct.pack(">Q", n)
        return self

    def i64(self, n: int) -> "Msg":
        self.buf += struct.pack(">q", n)
        return self


def stream(m: Msg, c) -> Msg:
    return m.str(c.command_id).str(c.tenant_id).str(c.aggregate_type).str(c.aggregate_id)


def command_message(c) -> bytes:
    return bytes(stream(Msg("kernable.v1.command"), c).str(c.command_type).bytes(c.payload).buf)


def result_message(c, expected_version: int, events) -> bytes:
    m = stream(Msg("kernable.v1.result"), c).u64(expected_version).u64(len(events))
    for e in events:
        m.str(e.event_type).bytes(e.payload)
    return bytes(m.buf)


def rejection_message(command_id: str, reason) -> bytes:
    return bytes(Msg("kernable.v1.rejection").str(command_id).str(reason.code).str(reason.message).buf)


def challenge_message(nonce: bytes) -> bytes:
    return bytes(Msg("kernable.v1.challenge").bytes(nonce).buf)


class Key:
    def __init__(self, seed_hex: str):
        self.key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex.strip()))
        self.id = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw).hex()

    def sign(self, message: bytes) -> pb.Signature:
        return pb.Signature(key_id=self.id, sig=self.key.sign(message))


# ---- the Order domain ----


class Rejected(Exception):
    pass


def apply(state: dict, event_type: str, data: dict) -> dict:
    if event_type == "OrderPlaced":
        return {**state, "status": "placed", "customer_id": data["customerId"]}
    if event_type == "ItemAdded":
        return {**state, "items": state["items"] + data["qty"]}
    if event_type == "OrderCancelled":
        return {**state, "status": "cancelled"}
    if event_type == "OrderShipped":
        return {**state, "status": "shipped"}
    return state


def handle(state: dict, command_type: str, data: dict) -> list:
    status = state["status"]
    if command_type == "PlaceOrder":
        if status != "none":
            raise Rejected("Order already exists")
        return [("OrderPlaced", {"customerId": data["customerId"]})]
    if status != "placed":
        raise Rejected(f"Cannot {command_type} on a {status} order")
    if command_type == "AddItem":
        if not data.get("qty", 0) > 0:
            raise Rejected("Quantity must be positive")
        return [("ItemAdded", data)]
    if command_type == "CancelOrder":
        return [("OrderCancelled", {})]
    if command_type == "ShipOrder":
        if state["items"] == 0:
            raise Rejected("Cannot ship an empty order")
        return [("OrderShipped", {})]
    raise Rejected(f"Unknown command {command_type}")


COMMANDS = ["PlaceOrder", "AddItem", "CancelOrder", "ShipOrder"]

# ---- the worker session ----


def execute(key: Key, x) -> pb.WorkerMessage:
    cmd = x.command
    state = {"status": "none", "items": 0}
    for e in x.history:
        state = apply(state, e.event_type, json.loads(e.payload or b"{}"))
    try:
        produced = handle(state, cmd.command_type, json.loads(cmd.payload or b"{}"))
    except Rejected as r:
        reason = pb.Rejected(code="rejected", message=str(r))
        sig = key.sign(rejection_message(cmd.command_id, reason))
        return pb.WorkerMessage(rejection=pb.Rejection(lease_id=x.lease_id, reason=reason, worker_sig=sig))
    events = [pb.NewEvent(event_type=t, payload=json.dumps(d).encode()) for t, d in produced]
    sig = key.sign(result_message(cmd, x.expected_version, events))
    return pb.WorkerMessage(result=pb.Result(lease_id=x.lease_id, events=events, worker_sig=sig))


def channel(url: str) -> grpc.Channel:
    # grpcio sends the socket path as :authority for unix: URLs, which the kernel's
    # HTTP/2 server rejects as invalid; any valid authority works.
    return grpc.insecure_channel(url, options=[("grpc.default_authority", "localhost")])


def run(url: str, key: Key, ready: threading.Event | None = None) -> None:
    """Runs one session until the kernel closes it."""
    outbox: queue.Queue = queue.Queue()
    outbox.put(pb.WorkerMessage(register=pb.Register(worker_id="py", key_id=key.id, command_types=COMMANDS, max_concurrency=1)))
    stub = rpc.WorkerGatewayStub(channel(url))
    for msg in stub.Session(iter(outbox.get, None)):
        kind = msg.WhichOneof("msg")
        if kind == "challenge":
            sig = key.sign(challenge_message(msg.challenge.nonce)).sig
            outbox.put(pb.WorkerMessage(challenge_response=pb.ChallengeResponse(sig=sig)))
        elif kind == "registered" and ready:
            ready.set()
        elif kind == "execute":
            outbox.put(execute(key, msg.execute))


if __name__ == "__main__":
    url, key_path = (sys.argv[1:] + ["unix:/tmp/kernable.sock", "worker.key"][len(sys.argv) - 1 :])[:2]
    with open(key_path) as f:
        run(url, Key(f.read()))
