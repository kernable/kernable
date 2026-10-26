"""kernable SDK for Python: write event-sourced business logic in Python; the kernel does the rest.

The API mirrors the TypeScript SDK:

    account = domain("identity").aggregate("Account", {"status": "none"})

    @account.apply("AccountRegistered")
    def _(state, data):
        return {**state, "status": "active", "email": data["email"]}

    @account.command("RegisterAccount")
    def _(state, data):
        if state["status"] != "none":
            raise Rejected("Account already exists")
        return [{"type": "AccountRegistered", "data": {"email": data["email"]}}]

    k = Kernable(url, key_path)
    k.worker(account).run()

Stubs: generate them first (`make py-stubs`), see sdks/python/README.md.
"""

import inspect
import json
import queue
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import grpc

from kernable.signing import (Key, challenge_message, command_message, derived_id, query_message,
                              rejection_message, result_message, snapshot_message)
from kernable.v1 import kernable_pb2 as pb
from kernable.v1 import kernable_pb2_grpc as rpc

__all__ = ["Aggregate", "CommandContext", "Domain", "Event", "Kernable", "Key", "Listener", "ListenerContext",
           "NotSettled", "Outcome", "QueryContext", "Rejected", "RequestContext", "Tls", "Worker", "domain"]


class Rejected(Exception):
    """Raise from a command handler to reject the command with a business reason."""

    def __init__(self, message: str, code: str = "rejected"):
        super().__init__(message)
        self.code = code


@dataclass
class RequestContext:
    """Who sends a request and in which context: signed with the command and stored with every
    event it causes. `attributes` are business data only, never security decisions."""

    sender_tenant: str = ""
    sender_identity: str = ""
    sender_account: str = ""
    sender_session: str = ""
    #: Authorize as this identity of the target tenant; the sender stays the real principal.
    on_behalf_of: str = ""
    workspace: str = ""
    #: Reject the command (`version_mismatch`) unless the aggregate is at this version.
    expected_version: int | None = None
    #: Shared by everything one original request causes; defaults to the command ID.
    correlation_id: str = ""
    #: The command that caused this one.
    causation_id: str = ""
    trace_id: str = ""
    #: Privileged: only keys in the kernel's `[privileged]` table may set it.
    bypass_authorization: bool = False
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass
class CommandContext:
    """What a command handler gets besides state and data (as an optional third parameter)."""

    command_id: str
    request: RequestContext
    #: When the kernel hands the command to another worker if this one has not answered.
    deadline: datetime
    received_at: datetime
    _ids: int = 0

    def now(self) -> datetime:
        """When the kernel received the command: the same every time the command runs.
        Use it instead of `datetime.now()`."""
        return self.received_at

    def id(self) -> str:
        """A new ID that is the same every time the command runs (1st call, 2nd call, ...).
        Use it instead of random IDs."""
        n, self._ids = self._ids, self._ids + 1
        return derived_id(self.command_id, n)


@dataclass
class QueryContext:
    """What a query handler gets besides the query's data (as an optional second parameter)."""

    query_id: str
    #: Who asks: check permissions here.
    request: RequestContext


@dataclass
class Event:
    """An event as stored by the kernel."""

    position: int
    tenant: str
    #: Always the domain of the command that caused the event.
    domain: str
    aggregate_type: str
    aggregate_id: str
    version: int
    type: str
    data: Any
    command_id: str
    #: The request context of the command that caused the event.
    request: RequestContext = field(default_factory=RequestContext)


@dataclass
class ListenerContext:
    """What a listener handler gets besides the event (as an optional second parameter)."""

    #: The event was committed before the listener connected: it is being replayed.
    replaying: bool
    event: Event

    def follow(self) -> RequestContext:
        """A request context for commands caused by this event: same sender and correlation."""
        r = self.event.request
        return RequestContext(r.sender_tenant, r.sender_identity, r.sender_account, r.sender_session,
                              r.on_behalf_of, r.workspace, None, r.correlation_id or self.event.command_id,
                              self.event.command_id, r.trace_id, False, dict(r.attributes))


@dataclass
class Outcome:
    committed: bool
    events: list[Event] = field(default_factory=list)
    code: str = ""
    message: str = ""


@dataclass
class Tls:
    """PEM files for mutual TLS: the CA of the kernel's certificate, and this process's own."""

    ca: str
    cert: str
    key: str


class NotSettled(Exception):
    """The command was committed, but not every listener processed its events in time.

    Read models may not show the change yet; `outcome` has the events.
    """

    def __init__(self, outcome: Outcome):
        super().__init__("command committed, but not all listeners have processed its events yet")
        self.outcome = outcome


_INITIAL = object()
_DOMAIN = re.compile(r"^[a-z][a-z0-9_-]*$")
_LISTENED = re.compile(r"^[a-z][a-z0-9_-]*\.([^.]+|\*)$")
_QUERY = re.compile(r"^[a-z][a-z0-9_-]*\.[^.*]+$")


class Domain:
    """A domain: a lowercase name (`orders`) that scopes its aggregates, commands and events.
    Types are unique within a domain; elsewhere they are named `domain.Type`."""

    def __init__(self, name: str):
        if not _DOMAIN.match(name):
            raise ValueError(f"invalid domain name {name!r}")
        self.name = name

    def aggregate(self, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1") -> "Aggregate":
        return Aggregate(self.name, name, initial, snapshot_every, schema)


def domain(name: str) -> Domain:
    return Domain(name)


class Aggregate:
    """How events change an aggregate's state (`apply`) and which events a command produces
    (`command`). Handlers must be deterministic and free of side effects; they may run again.

    Both work as decorators (`@agg.apply("Type")`) or directly (`agg.apply("Type", fn)`).
    A command handler returns a list of `{"type": ..., "data": ...}`.

    Every `snapshot_every` events (0 turns it off) the worker sends a snapshot of the state, so
    later commands fold only the events after it. The state must survive a JSON round trip.
    Change `schema` when `apply` produces differently shaped state: older snapshots are then
    ignored and the state is rebuilt from the events.
    """

    def __init__(self, domain: str, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1"):
        self.domain = domain
        self.name = name
        self.initial = initial
        self.snapshot_every = snapshot_every
        self.schema = schema
        self.appliers: dict[str, Callable] = {}
        self.handlers: dict[str, Callable] = {}

    def apply(self, event_type: str, fn: Callable | None = None):
        return _register(self, self.appliers, event_type, fn)

    def command(self, command_type: str, fn: Callable | None = None):
        return _register(self, self.handlers, command_type, fn)

    def fold(self, history: list[tuple[str, Any]], state: Any = _INITIAL) -> Any:
        """State after `history`, starting from `state` (default: `initial`);
        events without an `apply` are skipped."""
        if state is _INITIAL:
            state = self.initial
        for event_type, data in history:
            fn = self.appliers.get(event_type)
            if fn:
                state = fn(state, data)
        return state


def _register(owner, table: dict, name: str, fn: Callable | None):
    if fn is not None:
        table[name] = fn
        return owner

    def decorator(f: Callable) -> Callable:
        table[name] = f
        return f

    return decorator


class Kernable:
    def __init__(self, url: str, key: str, tenant: str = "default", tls: Tls | None = None,
                 context: RequestContext | None = None):
        """`url`: `unix:/path/to.sock` or `host:port`; `key`: path to this process's `.key` file;
        `tls`: mutual TLS for `host:port`; `context`: default request context for submits,
        e.g. this service's identity."""
        self._context = context or RequestContext()
        if tls:
            def read(path: str) -> bytes:
                with open(path, "rb") as f:
                    return f.read()

            creds = grpc.ssl_channel_credentials(read(tls.ca), read(tls.key), read(tls.cert))
            self._channel = grpc.secure_channel(url, creds)
        else:
            # grpcio sends the socket path as :authority for unix: URLs, which the kernel's
            # HTTP/2 server rejects; any valid authority works.
            self._channel = grpc.insecure_channel(url, options=[("grpc.default_authority", "localhost")])
        self._key = Key.from_file(key)
        self._tenant = tenant

    def worker(self, *aggregates: Aggregate) -> "Worker":
        return Worker(self._channel, self._key, aggregates)

    def submit(self, domain: str, aggregate_type: str, aggregate_id: str, type: str, data: Any = None, *,
               tenant: str | None = None, id: str | None = None, wait: str = "settled",
               context: RequestContext | None = None) -> Outcome:
        """Runs a command and returns its outcome.

        `wait="settled"` (default) responds once every listener has processed the events, so
        read models already show them, and raises `NotSettled` on timeout. `wait="committed"`
        responds once the events are stored; use it inside reactors, which would otherwise
        wait for themselves. `id` is an idempotency key. `context` replaces the client's default
        context; inside a listener, pass `ctx.follow()`.
        """
        mode = pb.WAIT_COMMITTED if wait == "committed" else pb.WAIT_SETTLED
        res = self._send(domain, aggregate_type, aggregate_id, type, data, tenant, id, mode, context)
        if res.WhichOneof("outcome") == "rejected":
            return Outcome(committed=False, code=res.rejected.code, message=res.rejected.message)
        outcome = Outcome(committed=True, events=[_to_event(e) for e in res.committed.events])
        if mode == pb.WAIT_SETTLED and not res.committed.settled:
            raise NotSettled(outcome)
        return outcome

    def enqueue(self, domain: str, aggregate_type: str, aggregate_id: str, type: str, data: Any = None, *,
                tenant: str | None = None, id: str | None = None, context: RequestContext | None = None) -> str:
        """Stores the command in the kernel's queue and returns its ID without waiting.
        Submit it again with the same `id` to get the outcome."""
        return self._send(domain, aggregate_type, aggregate_id, type, data, tenant, id, pb.WAIT_ACCEPTED,
                          context).command_id

    def query(self, domain: str, type: str, data: Any = None, *, tenant: str | None = None,
              context: RequestContext | None = None, min_position: int = 0) -> Any:
        """Asks the read model that answers `domain.type` and returns its answer. Queries always
        go through the kernel, also within one process. Raises `Rejected` if the read model
        rejects it. `min_position`: answer only after the read model has processed it."""
        query_id = str(uuid.uuid4())
        q = pb.QueryRequest(query_id=query_id, tenant_id=tenant or self._tenant, domain=domain, query_type=type,
                            payload=_encode(data), context=_to_wire_context(context or self._context, query_id),
                            min_position=min_position, issued_at=int(time.time() * 1000))
        q.client_sig.CopyFrom(self._key.sign(query_message(q)))
        res = rpc.QueryServiceStub(self._channel).Query(q)
        if res.WhichOneof("outcome") == "rejected":
            raise Rejected(res.rejected.message, res.rejected.code)
        return _decode(res.response)

    def read_model(self, name: str, *, order: int = 0, start: int | None = 0, reconnect_s: float = 1.0) -> "Listener":
        """A read model: kept in memory and rebuilt from the first event on every start (default)."""
        return Listener(self._channel, self._key, name, order, start, reconnect_s)

    def reactor(self, name: str, *, order: int = 10, start: int | None = None, reconnect_s: float = 1.0) -> "Listener":
        """A reactor: triggers side effects; resumes after the last acknowledged event (default).

        Inside a reactor, submit with `wait="committed"`: a settling submit would wait for the
        reactor itself and time out."""
        return Listener(self._channel, self._key, name, order, start, reconnect_s)

    def close(self) -> None:
        self._channel.close()

    def _send(self, domain, aggregate_type, aggregate_id, command_type, data, tenant, id, wait, context):
        command_id = id or str(uuid.uuid4())
        context = context or self._context
        cmd = pb.Command(command_id=command_id, tenant_id=tenant or self._tenant, domain=domain,
                         aggregate_type=aggregate_type, aggregate_id=aggregate_id,
                         command_type=command_type, payload=_encode(data),
                         context=_to_wire_context(context, command_id))
        cmd.client_sig.CopyFrom(self._key.sign(command_message(cmd)))
        return rpc.CommandServiceStub(self._channel).Submit(pb.SubmitRequest(command=cmd, wait=wait))


class _Stream:
    """A bidirectional stream that reconnects until stopped."""

    def __init__(self, what: str, reconnect_s: float):
        self.ready = threading.Event()
        self._what = what
        self._reconnect_s = reconnect_s
        self._stopped = False
        self._call = None
        self._outbox: queue.Queue | None = None

    def run(self) -> None:
        """Processes messages until `stop()`, reconnecting when the stream drops."""
        while not self._stopped:
            outbox: queue.Queue = queue.Queue()
            self._outbox = outbox
            try:
                self._call = self._open(iter(outbox.get, None))
                outbox.put(self._hello())
                for msg in self._call:
                    reply = self._on(msg)
                    if reply is not None:
                        outbox.put(reply)
            except grpc.RpcError as err:
                if not self._stopped:
                    print(f"kernable {self._what}: {err.details()}", file=sys.stderr)
            except Exception as err:  # a failing handler: reconnect and get the event again
                if not self._stopped:
                    print(f"kernable {self._what}: {err!r}", file=sys.stderr)
                self._call.cancel()
            finally:
                outbox.put(None)
            if not self._stopped:
                time.sleep(self._reconnect_s)

    def stop(self) -> None:
        self._stopped = True
        if self._call is not None:
            self._call.cancel()

    def _open(self, requests):
        raise NotImplementedError

    def _hello(self):
        raise NotImplementedError

    def _on(self, msg):
        raise NotImplementedError


class Worker(_Stream):
    """Executes commands for its aggregates, one at a time."""

    def __init__(self, channel: grpc.Channel, key: Key, aggregates):
        super().__init__("worker", 1.0)
        self._stub = rpc.WorkerGatewayStub(channel)
        self._key = key
        self._by_command = {f"{a.domain}.{t}": a for a in aggregates for t in a.handlers}

    def _open(self, requests):
        return self._stub.Session(requests)

    def _hello(self):
        register = pb.Register(worker_id=f"py-{uuid.uuid4().hex[:8]}", key_id=self._key.id,
                               command_types=list(self._by_command), max_concurrency=1)
        return pb.WorkerMessage(register=register)

    def _on(self, msg):
        kind = msg.WhichOneof("msg")
        if kind == "challenge":
            sig = self._key.sign(challenge_message(msg.challenge.nonce)).sig
            return pb.WorkerMessage(challenge_response=pb.ChallengeResponse(sig=sig))
        if kind == "registered":
            self.ready.set()
        elif kind == "execute":
            return self._execute(msg.execute)
        return None

    def _execute(self, x):
        cmd = x.command
        try:
            agg = self._by_command.get(f"{cmd.domain}.{cmd.command_type}")
            if agg is None or agg.name != cmd.aggregate_type:
                raise Rejected(f"no handler for {cmd.command_type} on {cmd.domain}.{cmd.aggregate_type}",
                               "unknown_command")
            if x.HasField("snapshot") and x.snapshot.schema != agg.schema:
                raise Rejected(f"snapshot schema {x.snapshot.schema} is not {agg.schema}", "snapshot_stale")
            history = [(e.event_type, _decode(e.payload)) for e in x.history]
            state = agg.fold(history, _decode(x.snapshot.state) if x.HasField("snapshot") else _INITIAL)
            ctx = CommandContext(cmd.command_id, _from_wire_context(cmd.context), _time(x.deadline), _time(x.received_at))
            produced = _call(agg.handlers[cmd.command_type], state, _decode(cmd.payload), ctx)
            events = [pb.NewEvent(event_type=e["type"], payload=_encode(e.get("data"))) for e in produced]
            snapshot = self._snapshot(agg, cmd, x.expected_version, state, events)
        except Exception as err:
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="handler_error", message=repr(err)))
            sig = self._key.sign(rejection_message(cmd.command_id, reason))
            return pb.WorkerMessage(rejection=pb.Rejection(lease_id=x.lease_id, reason=reason, worker_sig=sig))
        sig = self._key.sign(result_message(cmd, x.expected_version, events))
        return pb.WorkerMessage(result=pb.Result(lease_id=x.lease_id, events=events, worker_sig=sig, snapshot=snapshot))

    def _snapshot(self, agg: Aggregate, cmd, before: int, state, events):
        """A snapshot of the state after `events`, if they cross a multiple of `snapshot_every`."""
        version = before + len(events)
        every = agg.snapshot_every
        if every <= 0 or not events or version // every == before // every:
            return None
        # Apply the events as they will be read back, so the snapshot equals a replay.
        after = agg.fold([(e.event_type, _decode(e.payload)) for e in events], state)
        state_bytes = _encode(after)
        sig = self._key.sign(snapshot_message(cmd, version, agg.schema, state_bytes))
        return pb.Snapshot(version=version, schema=agg.schema, state=state_bytes, worker_sig=sig)


class Listener(_Stream):
    """A durable, named event listener. Events arrive in order, one at a time, after every
    listener with a lower order has processed them. Delivery is at least once: if a handler
    raises, the listener reconnects and the event is delivered again."""

    def __init__(self, channel: grpc.Channel, key: Key, name: str, order: int, start: int | None, reconnect_s: float):
        super().__init__(f"listener {name}", reconnect_s)
        self._stub = rpc.ListenerServiceStub(channel)
        self._key = key
        self._name = name
        self._order = order
        self._start = start
        self._handlers: dict[str, Callable] = {}
        self._queries: dict[str, Callable] = {}
        #: Global position of the last handled event (or skipped, for other event types).
        self.position = 0
        # Newest event when the kernel accepted the listener; events up to it are replayed.
        self._replay_up_to = 0

    def on(self, event_type: str, fn: Callable | None = None):
        """Registers a handler for a qualified type (`orders.OrderPlaced`) or a whole domain
        (`orders.*`); works as a decorator too."""
        if not _LISTENED.match(event_type):
            raise ValueError(f"listen to a qualified type (domain.Type) or a domain (domain.*), not {event_type!r}")
        return _register(self, self._handlers, event_type, fn)

    def query(self, query_type: str, fn: Callable | None = None):
        """Answers the qualified query type (`orders.GetOrder`) from this listener's state; works as
        a decorator too. The kernel delivers queries in order with the events, so an answer
        reflects every event handled before it. Raise `Rejected` to refuse. A handler must not
        query its own listener: the answer would wait behind itself."""
        if not _QUERY.match(query_type):
            raise ValueError(f"answer a qualified query type (domain.Query), not {query_type!r}")
        return _register(self, self._queries, query_type, fn)

    def _open(self, requests):
        return self._stub.Listen(requests)

    def _hello(self):
        hello = pb.Hello(name=self._name, key_id=self._key.id, order=self._order, event_types=list(self._handlers),
                         query_types=list(self._queries))
        # Reconnects resume after the last handled event.
        start = self.position if self.ready.is_set() else self._start
        if start is not None:
            hello.from_position = start
        return pb.ListenerMessage(hello=hello)

    def _on(self, msg):
        kind = msg.WhichOneof("msg")
        if kind == "challenge":
            sig = self._key.sign(challenge_message(msg.challenge.nonce)).sig
            return pb.ListenerMessage(challenge_response=pb.ChallengeResponse(sig=sig))
        if kind == "ready":
            self.position = msg.ready.after_position
            self._replay_up_to = msg.ready.head_position
            self.ready.set()
        elif kind == "batch":
            for e in msg.batch.events:
                handler = self._handlers.get(f"{e.domain}.{e.event_type}") or self._handlers.get(f"{e.domain}.*")
                if handler:
                    event = _to_event(e)
                    _call(handler, event, ListenerContext(e.global_position <= self._replay_up_to, event))
                self.position = e.global_position
            self.position = msg.batch.up_to
            return pb.ListenerMessage(ack=pb.Ack(position=msg.batch.up_to))
        elif kind == "query":
            return pb.ListenerMessage(answer=self._answer(msg.query.call_id, msg.query.query))
        return None

    def _answer(self, call_id: str, q) -> pb.QueryAnswer:
        try:
            handler = self._queries.get(f"{q.domain}.{q.query_type}")
            if handler is None:
                raise Rejected(f"no handler for {q.domain}.{q.query_type}", "unknown_query")
            result = _call(handler, _decode(q.payload), QueryContext(q.query_id, _from_wire_context(q.context)))
            return pb.QueryAnswer(call_id=call_id, response=_encode(result))
        except Exception as err:
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="handler_error", message=repr(err)))
            return pb.QueryAnswer(call_id=call_id, rejected=reason)


def _encode(data: Any) -> bytes:
    return json.dumps({} if data is None else data).encode()


def _decode(payload: bytes) -> Any:
    return json.loads(payload) if payload else {}


def _time(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _call(fn: Callable, *args):
    """Calls `fn` with as many of `args` as it takes, so the context parameter is optional."""
    params = inspect.signature(fn).parameters.values()
    if any(p.kind == p.VAR_POSITIONAL for p in params):
        return fn(*args)
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return fn(*args[:len(positional)])


def _to_wire_context(c: RequestContext, command_id: str) -> pb.RequestContext:
    wire = pb.RequestContext(
        sender_tenant=c.sender_tenant, sender_identity=c.sender_identity, sender_account=c.sender_account,
        sender_session=c.sender_session, on_behalf_of=c.on_behalf_of, workspace=c.workspace,
        correlation_id=c.correlation_id or command_id, causation_id=c.causation_id, trace_id=c.trace_id,
        bypass_authorization=c.bypass_authorization,
        attributes=[pb.Attribute(key=k, value=v) for k, v in c.attributes.items()])
    if c.expected_version is not None:
        wire.expected_version = c.expected_version
    return wire


def _from_wire_context(c) -> RequestContext:
    return RequestContext(
        c.sender_tenant, c.sender_identity, c.sender_account, c.sender_session, c.on_behalf_of, c.workspace,
        c.expected_version if c.HasField("expected_version") else None, c.correlation_id, c.causation_id,
        c.trace_id, c.bypass_authorization, {a.key: a.value for a in c.attributes})


def _to_event(e) -> Event:
    return Event(position=e.global_position, tenant=e.tenant_id, domain=e.domain, aggregate_type=e.aggregate_type,
                 aggregate_id=e.aggregate_id, version=e.version, type=e.event_type,
                 data=_decode(e.payload), command_id=e.command_id, request=_from_wire_context(e.context))
