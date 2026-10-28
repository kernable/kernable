"""kernable SDK for Python: write event-sourced business logic in Python; the kernel does the rest.

The API mirrors the TypeScript SDK:

    account_aggregate = domain("identity").aggregate("account", {"status": "none"})

    @account_aggregate.apply("AccountRegistered")
    def _(state, data):
        return {**state, "status": "active", "email": data["email"]}

    @account_aggregate.command("RegisterAccount")
    def _(state, data):
        if state["status"] != "none":
            raise Rejected("Account already exists")
        return [{"type": "AccountRegistered", "data": {"email": data["email"]}}]

    k = Kernable(url, key_path)
    k.worker(account_aggregate).run()

Listeners are defined on a domain too and run through the connection:

    accounts_readmodel = domain("identity").read_model("accounts")
    k.listener(accounts_readmodel).run()

Stubs: generate them first (`make py-stubs`), see sdks/python/README.md.
"""

import contextvars
import copy
import inspect
import json
import os
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

from kernable.signing import (Key, challenge_message, command_message, derived_id, query_message, vault_ref,
                              rejection_message, result_message, snapshot_message)
from kernable.store import Store
from kernable.v1 import kernable_pb2 as pb
from kernable.v1 import kernable_pb2_grpc as rpc

__all__ = ["PROTOCOL", "Aggregate", "ApplyContext", "Check", "CommandContext", "CommandMeta", "CommandSchema", "Domain",
           "Event", "EventMeta", "EventSchema", "Kernable", "Key", "Listener", "ListenerContext", "ListenerHandle", "NotSettled", "Outcome",
           "QueryContext", "QueryMeta", "Rejected", "RequestContext", "Store", "StoredEventMeta", "Tls", "Worker",
           "domain"]


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
    #: Privileged: only keys granted `privileged` for it may set it.
    bypass_authorization: bool = False
    attributes: dict[str, str] = field(default_factory=dict)


@dataclass
class CommandMeta:
    """What a command handler gets about the command besides state and data (as an optional third
    parameter)."""

    command_id: str
    #: The command type within the domain.
    type: str
    domain: str = ""
    aggregate_type: str = ""
    aggregate_id: str = ""
    tenant: str = ""


@dataclass
class CommandContext:
    """What a command handler gets besides state, data and the command (as an optional fourth
    parameter)."""

    #: Who sends the command: check permissions here.
    request: RequestContext
    #: When the kernel hands the command to another worker if this one has not answered.
    deadline: datetime
    received_at: datetime
    #: The command whose IDs `id()` derives.
    _command_id: str = field(default="", repr=False)
    _ids: int = field(default=0, repr=False)

    def now(self) -> datetime:
        """When the kernel received the command: the same every time the command runs.
        Use it instead of `datetime.now()`."""
        return self.received_at

    def id(self) -> str:
        """A new ID that is the same every time the command runs (1st call, 2nd call, ...).
        Use it instead of random IDs."""
        n, self._ids = self._ids, self._ids + 1
        return derived_id(self._command_id, n)


@dataclass
class EventMeta:
    """What `apply` gets besides state and data (as an optional third parameter): the event
    without its payload. Only fields known before the kernel stores the event, so a snapshot and
    a replay compute the same state; put a time into the data (`ctx.now()`) if the state needs one."""

    type: str
    #: The aggregate's version after this event: 1 for its first event.
    version: int = 0
    tenant: str = ""
    domain: str = ""
    aggregate_type: str = ""
    aggregate_id: str = ""
    command_id: str = ""


@dataclass
class ApplyContext:
    """What `apply` gets besides state, data and the event (as an optional fourth parameter).
    No clock and no IDs: `apply` must compute the same state on every replay."""

    #: The request context of the command that caused the event.
    request: RequestContext = field(default_factory=RequestContext)


@dataclass
class QueryMeta:
    """What a query handler gets about the query besides its data (as an optional second
    parameter)."""

    query_id: str
    #: The query type within the domain.
    type: str
    domain: str = ""
    tenant: str = ""


@dataclass
class QueryContext:
    """What a query handler gets besides the query's data and meta (as an optional third
    parameter)."""

    #: Who asks: check permissions here.
    request: RequestContext
    #: The listener's store, if it has one (`store=` of `Domain.read_model` and `Domain.reactor`).
    store: "Store | None" = None


@dataclass
class Check:
    """A command or query a middleware checks before the kernel accepts or routes it."""

    kind: str  # "command" or "query"
    #: The command or query ID.
    id: str
    tenant: str
    domain: str
    #: The command or query type within the domain.
    type: str
    data: Any
    #: Who sends it: check permissions here.
    request: RequestContext
    #: Commands only.
    aggregate_type: str = ""
    aggregate_id: str = ""


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
    #: Version of the payload format, as stored (see `Domain.event`).
    schema: int = 1
    #: Fields kept in the vault whose value is not here (None in `data`): erased, or not readable
    #: with this key (granted `vault_read`).
    redacted: list[str] = field(default_factory=list)


@dataclass
class StoredEventMeta(EventMeta):
    """What a listener handler gets about the event besides its data (as an optional second
    parameter): the stored event without its payload and request context."""

    position: int = 0
    #: Version of the payload format: the version the listener knows (see `Domain.event`), or the
    #: stored one for a `raw` handler.
    schema: int = 1
    #: Fields kept in the vault whose value is not in the data (None there): erased, or not
    #: readable with this key (granted `vault_read`).
    redacted: list[str] = field(default_factory=list)


@dataclass
class ListenerContext:
    """What a listener handler gets besides the event's data and meta (as an optional third
    parameter)."""

    #: The event was committed before the listener connected: it is being replayed.
    replaying: bool
    #: The request context of the command that caused the event.
    request: RequestContext = field(default_factory=RequestContext)
    #: The listener's store, if it has one (`store=` of `Domain.read_model` and `Domain.reactor`).
    store: "Store | None" = None
    #: The command that caused the event, for `follow()`.
    _command_id: str = field(default="", repr=False)

    def follow(self) -> RequestContext:
        """A request context for commands caused by this event: same sender and correlation."""
        r = self.request
        return RequestContext(r.sender_tenant, r.sender_identity, r.sender_account, r.sender_session,
                              r.on_behalf_of, r.workspace, None, r.correlation_id or self._command_id,
                              self._command_id, r.trace_id, False, dict(r.attributes))


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


#: The protocol version this SDK speaks; see "Protocol versions" in `kernable.proto`.
PROTOCOL = 1

#: The name of the listener whose handler is running, for requests sent from it.
_HANDLING: contextvars.ContextVar[str] = contextvars.ContextVar("kernable_listener", default="")

_INITIAL = object()
_DOMAIN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_UNQUALIFIED = re.compile(r"^[^.*]+$")
_LISTENED = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*)$")
_QUERY = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*\.[^.*]+$")
_CHECKED = re.compile(r"^(\*|[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*))$")


class EventSchema:
    """The payload format of one event type: its current version and the steps that bring older
    payloads up to it. Events are stored as written and never changed; readers upcast them."""

    def __init__(self, type: str, schema: int):
        if not re.match(r"^[^.*]+$", type):
            raise ValueError(f"event types are unqualified within their domain, not {type!r}")
        if not isinstance(schema, int) or schema < 1:
            raise ValueError(f"a schema is a version from 1, not {schema!r}")
        self.type = type
        self.schema = schema
        self.steps: dict[int, Callable] = {}
        #: The fields kept in the vault; see `personal`.
        self.vault = _VaultFields()
        #: The subject this event erases, if it is an erasing event; see `erases`.
        self.eraser: Callable | None = None

    def personal(self, fields: list[str], subject: Callable) -> "EventSchema":
        """Keeps these top-level fields in the vault, as personal data of the subject `subject`
        returns from the payload and, optionally, the aggregate's ID (e.g.
        `lambda d: d["customerId"]`, or `lambda d, id: id` for the aggregate itself). Events
        then hold only references; the kernel hands the values to workers and to listeners
        allowed to read them, until an event erases the subject."""
        self.vault.personal.append((fields, subject))
        return self

    def erases(self, subject: Callable) -> "EventSchema":
        """Makes this an erasing event: it deletes every vault value of the subject `subject`
        returns, in every stream, and the snapshots that may hold them. Read models should drop
        the subject's data when they see it; rebuilt, they read None."""
        self.eraser = subject
        return self

    def upcast(self, from_version: int, fn: Callable | None = None):
        """Turns a payload of version `from_version` into one of the next version; works as a
        decorator too."""
        if not isinstance(from_version, int) or not 1 <= from_version < self.schema:
            raise ValueError(f"upcast {self.type} from a version between 1 and {self.schema - 1}, not {from_version!r}")
        return _register(self, self.steps, from_version, fn)


class _VaultFields:
    """Payload fields kept in the vault instead of the signed log."""

    def __init__(self):
        self.personal: list[tuple[list[str], Callable]] = []
        self.secret: list[str] = []


class CommandSchema:
    """How a command's payload is kept: which fields go to the vault."""

    def __init__(self, type: str):
        if not re.match(r"^[^.*]+$", type):
            raise ValueError(f"command types are unqualified within their domain, not {type!r}")
        self.type = type
        self.vault = _VaultFields()

    def personal(self, fields: list[str], subject: Callable) -> "CommandSchema":
        """Keeps these top-level fields in the vault as personal data of `subject`; see
        `EventSchema.personal`."""
        self.vault.personal.append((fields, subject))
        return self

    def secret(self, fields: list[str]) -> "CommandSchema":
        """Keeps these top-level fields (passwords, one-time tokens) in the vault only until the
        command completed. The handler gets them in clear; the stored command keeps a reference."""
        self.vault.secret.extend(fields)
        return self


def _seal(data: Any, vault: _VaultFields | None, aggregate_id: str) -> tuple[bytes, list[str], list]:
    """Moves the vault fields of `data` into vault values; the payload keeps `{"$vault": ref}`.
    Absent and None fields stay as they are."""
    if vault is None or not (vault.personal or vault.secret) or not isinstance(data, dict):
        return _encode(data), [], []
    sealed = dict(data)
    values = []

    def keep(f: str, subject: str, secret: bool):
        if sealed.get(f) is None:
            return
        salt = os.urandom(16)
        value = json.dumps(sealed[f], separators=(",", ":")).encode()
        ref = vault_ref(salt, subject, secret, value)
        values.append(pb.VaultValue(ref=ref, subject=subject, secret=secret, salt=salt, value=value))
        sealed[f] = {"$vault": ref}

    for fields, subject in vault.personal:
        who = str(_call(subject, data, aggregate_id) or "")
        if not who:
            raise ValueError(f"no subject for the personal fields {', '.join(fields)}")
        for f in fields:
            keep(f, who, False)
    for f in vault.secret:
        keep(f, "", True)
    return _encode(sealed), [v.ref for v in values], values


def _unseal(data: Any, vault) -> tuple[Any, list[str]]:
    """Puts vault values back into `data`; references without a value become None."""
    values = {v.ref: v.value for v in vault or []}
    redacted: list[str] = []

    def walk(v, path: str):
        if isinstance(v, list):
            return [walk(x, f"{path}[{i}]") for i, x in enumerate(v)]
        if not isinstance(v, dict):
            return v
        if len(v) == 1 and isinstance(v.get("$vault"), str):
            if v["$vault"] not in values:
                redacted.append(path)
                return None
            return json.loads(values[v["$vault"]])
        return {k: walk(x, f"{path}.{k}" if path else k) for k, x in v.items()}

    return walk(data, ""), redacted


class SchemaError(Exception):
    """An event a process cannot read: its schema is newer than known, or a step is missing."""


def _upcast(events: dict | None, domain: str, type: str, schema: int, data: Any) -> tuple[int, Any]:
    """`data` of a `type` event stored at `schema`, upcast to the current version in `events`."""
    spec = (events or {}).get(type)
    current = spec.schema if spec else 1
    if schema > current:
        raise SchemaError(f"{domain}.{type} has schema {schema}, but this process knows it only up to {current}: "
                          "update it")
    for v in range(schema, current):
        if v not in spec.steps:
            raise SchemaError(f"no upcast for {domain}.{type} from schema {v}")
        data = spec.steps[v](data)
    return current, data


class Domain:
    """A domain: a name that scopes its aggregates, commands and events, lowercase by convention
    (`orders`) like aggregates, while message types are capitalized (`orders.PlaceOrder`). Both
    cases are valid; names are case-sensitive. Types are unique within a domain; elsewhere they
    are named `domain.Type`."""

    def __init__(self, name: str):
        if not _DOMAIN.match(name):
            raise ValueError(f"invalid domain name {name!r}")
        self.name = name
        #: Payload versions and upcasts per event type; types not listed are at version 1.
        self.events: dict[str, EventSchema] = {}
        #: Vault fields per command type; see `command`.
        self.commands: dict[str, CommandSchema] = {}

    def command(self, type: str) -> CommandSchema:
        """Declares which fields of `type` commands the vault keeps (`personal`, `secret`). Clients
        that submit them need the domain: `kernable.use(domain)`."""
        spec = CommandSchema(type)
        self.commands[type] = spec
        return spec

    def event(self, type: str, schema: int) -> EventSchema:
        """Declares that this domain writes `type` events at version `schema`, with an `upcast`
        per older version. The domain's aggregates and listeners use it; listeners of other
        domains get it with `use(domain)`."""
        spec = EventSchema(type, schema)
        self.events[type] = spec
        return spec

    def aggregate(self, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1") -> "Aggregate":
        """An aggregate of this domain; see `Aggregate`. Run it with `kernable.worker(aggregate)`."""
        return Aggregate(self.name, name, initial, snapshot_every, schema, self.events)

    def read_model(self, name: str, *, order: int = 0, start: int | None = 0, reconnect_s: float = 1.0,
                   store: str | None = None, schema: str = "1") -> "Listener":
        """A read model of this domain: kept in memory and rebuilt from the first event on every
        start (default). Run it with `kernable.listener(read_model)`.

        With `store` (`sqlite:<path>` or `memory:`), its state lives in the running listener's
        `store`: data and position are written together per batch, and a restart continues after
        the stored position. Change `schema` when the handlers store data differently: the store is
        cleared and the read model rebuilds from the first event."""
        return Listener(self, name, order, start, reconnect_s, store, schema)

    def reactor(self, name: str, *, order: int = 10, start: int | None = None, reconnect_s: float = 1.0,
                store: str | None = None, schema: str = "1") -> "Listener":
        """A reactor of this domain: triggers side effects; resumes after the last acknowledged
        event (default). Run it with `kernable.listener(reactor)`.

        A settling submit from its handler waits for the listeners before it (the read models),
        not for the reactor itself, which handles one event at a time. `store` and `schema`: see
        `read_model`."""
        return Listener(self, name, order, start, reconnect_s, store, schema)


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

    def __init__(self, domain: str, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1",
                 events: dict[str, EventSchema] | None = None):
        self.domain = domain
        #: The domain's event schemas.
        self.events = {} if events is None else events
        self.name = name
        self.initial = initial
        self.snapshot_every = snapshot_every
        self.schema = schema
        self.appliers: dict[str, Callable] = {}
        self.handlers: dict[str, Callable] = {}

    def apply(self, event_type: str, fn: Callable | None = None):
        """How an event of `event_type` changes the state: `fn(state, data, evt, ctx)` changes
        `state` in place and returns nothing, or returns a new state that replaces it. `state` is a
        copy, so changing it never touches the state it came from; `evt` (an `EventMeta`) and
        `ctx` (an `ApplyContext`) are optional."""
        return _register(self, self.appliers, event_type, fn)

    def command(self, command_type: str, fn: Callable | None = None):
        """Which events a `command_type` command produces: `fn(state, data, cmd, ctx)` returns
        them; `cmd` (a `CommandMeta`) and `ctx` (a `CommandContext`) are optional."""
        return _register(self, self.handlers, command_type, fn)

    def schema_of(self, event_type: str) -> int:
        """The payload version this aggregate writes `event_type` events at."""
        spec = self.events.get(event_type)
        return spec.schema if spec else 1

    def fold(self, history: list[tuple], state: Any = _INITIAL) -> Any:
        """State after `history` (type, schema, data, and optionally an `EventMeta` and an
        `ApplyContext`), starting from `state` (default: `initial`). Events are upcast first;
        events without an `apply` are skipped."""
        if state is _INITIAL:
            state = self.initial
        for event_type, schema, data, *rest in history:
            fn = self.appliers.get(event_type)
            if fn:
                meta = rest[0] if rest else EventMeta(event_type, domain=self.domain, aggregate_type=self.name)
                ctx = rest[1] if len(rest) > 1 else ApplyContext()
                draft = copy.deepcopy(state)
                result = _call(fn, draft, _upcast(self.events, self.domain, event_type, schema, data)[1], meta, ctx)
                state = draft if result is None else result
        return state


def _register(owner, table: dict, name: str, fn: Callable | None):
    if fn is not None:
        table[name] = fn
        return owner

    def decorator(f: Callable) -> Callable:
        table[name] = f
        return f

    return decorator


class _WithGrant(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
                 grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):
    """Sends the key's grant with every call, in the `kernable-grant` metadata entry."""

    def __init__(self, grant: str):
        self._grant = grant

    def _details(self, details):
        metadata = [*(details.metadata or []), ("kernable-grant", self._grant)]
        return details._replace(metadata=metadata)

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._details(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._details(details), request)

    def intercept_stream_unary(self, continuation, details, requests):
        return continuation(self._details(details), requests)

    def intercept_stream_stream(self, continuation, details, requests):
        return continuation(self._details(details), requests)


class Kernable:
    def __init__(self, url: str | None = None, key: str | None = None, tenant: str = "default",
                 tls: Tls | None = None, context: RequestContext | None = None,
                 grant: str | None = None):
        """`url`: `http://host:port` (`host:port` works too) or, on Unix, `unix:/path/to.sock`;
        default `KERNABLE_URL`, else `http://127.0.0.1:7233`, where `kernable start` listens;
        `key`: path to this process's `.key` file; default `KERNABLE_KEY`, else `app.key` in the
        current directory; `tls`: mutual TLS for `host:port`; default `KERNABLE_TLS_CA`,
        `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set; `context`: default request context for submits
        and queries, e.g. this service's identity, replaced by a request's own; `grant`: path to the key's grant (`kernable grant`), sent
        with every call; default `KERNABLE_GRANT`, else the `.grant` file next to the key file,
        if there is one."""
        key = key or os.environ.get("KERNABLE_KEY") or "app.key"
        tls = tls or _tls_from_env()
        self._context = context or RequestContext()
        url = url or os.environ.get("KERNABLE_URL") or "http://127.0.0.1:7233"
        url = re.sub(r"^https?://", "", url).rstrip("/")
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
        grant = grant or os.environ.get("KERNABLE_GRANT")
        if grant is None and os.path.exists(default := key.removesuffix(".key") + ".grant"):
            grant = default
        if grant is not None:
            with open(grant) as f:
                self._channel = grpc.intercept_channel(self._channel, _WithGrant(f.read().strip()))
        self._key = Key.from_file(key)
        self._tenant = tenant
        self._domains: dict[str, Domain] = {}

    def use(self, *domains: Domain) -> "Kernable":
        """Seals the vault fields of these domains' commands when submitting them; see
        `Domain.command`."""
        for d in domains:
            self._domains[d.name] = d
        return self

    def worker(self, *aggregates: Aggregate) -> "Worker":
        return Worker(self._channel, self._key, aggregates)

    def submit(self, domain: str, aggregate_type: str, aggregate_id: str, type: str, data: Any = None, *,
               tenant: str | None = None, id: str | None = None, wait: str = "settled",
               context: RequestContext | None = None) -> Outcome:
        """Runs a command and returns its outcome.

        `wait="settled"` (default) responds once every listener has processed the events, so
        read models already show them, and raises `NotSettled` on timeout; from a listener's
        handler, only the listeners with a lower order count. `wait="committed"` responds once
        the events are stored. `id` is an idempotency key. `context` replaces the client's default
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
                            min_position=min_position, issued_at=int(time.time() * 1000), protocol=PROTOCOL,
                            listener=_HANDLING.get())
        q.client_sig.CopyFrom(self._key.sign(query_message(q)))
        res = rpc.QueryServiceStub(self._channel).Query(q)
        if res.WhichOneof("outcome") == "rejected":
            raise Rejected(res.rejected.message, res.rejected.code)
        return _decode(res.response)

    def listener(self, listener: "Listener") -> "ListenerHandle":
        """Connects a listener defined with `Domain.read_model` or `Domain.reactor`; call `run()`
        on the result."""
        return ListenerHandle(self._channel, self._key, listener)

    def close(self) -> None:
        """Closes the connection now; only needed in a long-running process that discards the client."""
        self._channel.close()

    def _send(self, domain, aggregate_type, aggregate_id, command_type, data, tenant, id, wait, context):
        command_id = id or str(uuid.uuid4())
        context = context or self._context
        spec = self._domains[domain].commands.get(command_type) if domain in self._domains else None
        payload, refs, values = _seal(data, spec.vault if spec else None, aggregate_id)
        cmd = pb.Command(command_id=command_id, tenant_id=tenant or self._tenant, domain=domain,
                         aggregate_type=aggregate_type, aggregate_id=aggregate_id,
                         command_type=command_type, payload=payload,
                         context=_to_wire_context(context, command_id), vault_refs=refs)
        cmd.client_sig.CopyFrom(self._key.sign(command_message(cmd)))
        request = pb.SubmitRequest(command=cmd, wait=wait, protocol=PROTOCOL, listener=_HANDLING.get(),
                                   vault=values)
        return rpc.CommandServiceStub(self._channel).Submit(request)


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
        # Every event type the aggregates know: the kernel refuses an outdated worker.
        schemas = {f"{a.domain}.{t}": a.schema_of(t) for a in self._by_command.values()
                   for t in [*a.appliers, *a.events]}
        register = pb.Register(worker_id=f"py-{uuid.uuid4().hex[:8]}", key_id=self._key.id, protocol=PROTOCOL,
                               command_types=list(self._by_command), max_concurrency=1,
                               event_schemas=[pb.EventSchema(event_type=t, schema=s) for t, s in schemas.items()])
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
            history = [(e.event_type, e.schema, _unseal(_decode(e.payload), x.vault)[0],
                        _meta(e.event_type, e.version, cmd, e.command_id), ApplyContext(_from_wire_context(e.context)))
                       for e in x.history]
            state = agg.fold(history, _decode(x.snapshot.state) if x.HasField("snapshot") else _INITIAL)
            meta = CommandMeta(cmd.command_id, cmd.command_type, cmd.domain, cmd.aggregate_type, cmd.aggregate_id,
                               cmd.tenant_id)
            ctx = CommandContext(_from_wire_context(cmd.context), _time(x.deadline), _time(x.received_at), cmd.command_id)
            produced = _call(agg.handlers[cmd.command_type], state, _unseal(_decode(cmd.payload), x.vault)[0], meta, ctx)
            events, vault = [], []
            for e in produced:
                spec = agg.events.get(e["type"])
                payload, refs, values = _seal(e.get("data"), spec.vault if spec else None, cmd.aggregate_id)
                vault.extend(values)
                erases = str(_call(spec.eraser, e.get("data"), cmd.aggregate_id) or "") if spec and spec.eraser else ""
                events.append(pb.NewEvent(event_type=e["type"], schema=agg.schema_of(e["type"]), payload=payload,
                                          vault_refs=refs, erases=erases))
            snapshot = self._snapshot(agg, cmd, x.expected_version, state, produced)
        except SchemaError:
            raise  # not the command's fault: this worker cannot read the aggregate; stop until updated
        except Exception as err:
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="handler_error", message=repr(err)))
            sig = self._key.sign(rejection_message(cmd.command_id, reason))
            return pb.WorkerMessage(rejection=pb.Rejection(lease_id=x.lease_id, reason=reason, worker_sig=sig))
        sig = self._key.sign(result_message(cmd, x.expected_version, events))
        result = pb.Result(lease_id=x.lease_id, events=events, worker_sig=sig, snapshot=snapshot, vault=vault)
        return pb.WorkerMessage(result=result)

    def _snapshot(self, agg: Aggregate, cmd, before: int, state, events):
        """A snapshot of the state after the produced `events`, if they cross a multiple of
        `snapshot_every`."""
        version = before + len(events)
        every = agg.snapshot_every
        if every <= 0 or not events or version // every == before // every:
            return None
        # Apply the events as they will be read back (a JSON round trip), so the snapshot equals a replay.
        apply_ctx = ApplyContext(_from_wire_context(cmd.context))
        read = [(e["type"], agg.schema_of(e["type"]), _decode(_encode(e.get("data"))),
                 _meta(e["type"], before + i + 1, cmd, cmd.command_id), apply_ctx) for i, e in enumerate(events)]
        after = agg.fold(read, state)
        state_bytes = _encode(after)
        sig = self._key.sign(snapshot_message(cmd, version, agg.schema, state_bytes))
        return pb.Snapshot(version=version, schema=agg.schema, state=state_bytes, worker_sig=sig)


class Listener:
    """A listener definition: a durable, named event listener of a domain, made with
    `Domain.read_model` or `Domain.reactor` and run with `kernable.listener(listener)`, which
    returns a `ListenerHandle`. Events arrive in order, one at a time, after every listener with a
    lower order has processed them. Delivery is at least once: if a handler raises, the listener
    reconnects and the event is delivered again.

    Type names without a domain (`ItemAdded`, `GetCart`) are the listener's own domain's; types of
    other domains are qualified (`identity.AccountRegistered`, `identity.*`).

    Handlers get event payloads upcast to the versions of the listener's domain and of the domains
    it `use`s. An event with a newer schema than known stops the listener (it retries and logs)
    until it is updated, so it never misreads data."""

    def __init__(self, domain: Domain, name: str, order: int, start: int | None, reconnect_s: float,
                 store: str | None = None, schema: str = "1"):
        #: The domain the listener belongs to: unqualified type names are its types.
        self.domain = domain
        self.name = name
        self.order = order
        self.start = start
        self.reconnect_s = reconnect_s
        #: Where the running listener keeps its state (`sqlite:<path>` or `memory:`), if anywhere.
        self.store_url = store
        self.schema = schema
        self._handlers: dict[str, Callable] = {}
        self._raw: set[str] = set()
        self._domains: dict[str, dict[str, EventSchema]] = {domain.name: domain.events}
        self._queries: dict[str, Callable] = {}
        self._middleware: tuple[pb.Middleware, Callable] | None = None

    def _qualify(self, type: str) -> str:
        return type if "." in type else f"{self.domain.name}.{type}"

    def use(self, *domains: Domain) -> "Listener":
        """Upcasts the events of these other domains with their event schemas (see
        `Domain.event`); the listener's own domain's apply already."""
        for d in domains:
            self._domains[d.name] = d.events
        return self

    def on(self, event_type: str, fn: Callable | None = None, *, raw: bool = False):
        """Registers a handler for an event type (`ItemAdded`, or `orders.OrderPlaced` of another
        domain) or a whole domain (`orders.*`); works as a decorator too: `fn(data, evt, ctx)` gets
        the event's payload, a `StoredEventMeta` and a `ListenerContext` (both optional). A `raw`
        handler gets payloads as stored, without upcasting, whatever their schema (for archives
        and exports)."""
        # A bare `*` is not the own domain (that is `<domain>.*`): in middleware it means everything.
        if not _UNQUALIFIED.match(event_type) and not _LISTENED.match(event_type):
            raise ValueError(f"listen to a type (Type), a qualified type (domain.Type) or a domain (domain.*), not {event_type!r}")
        qualified = self._qualify(event_type)
        (self._raw.add if raw else self._raw.discard)(qualified)
        return _register(self, self._handlers, qualified, fn)

    def query(self, query_type: str, fn: Callable | None = None):
        """Answers a query type (`GetCart`, or `orders.GetOrder` of another domain) from this
        listener's state; works as a decorator too: `fn(data, qry, ctx)` gets the query's data, a
        `QueryMeta` and a `QueryContext` (both optional). The kernel delivers queries in order with
        the events, so an answer reflects every event handled before it. Raise `Rejected` to
        refuse. A handler cannot query its own listener (the answer would wait behind itself): the
        kernel refuses it."""
        qualified = self._qualify(query_type)
        if not _QUERY.match(qualified):
            raise ValueError(f"answer a query type (Query or domain.Query), not {query_type!r}")
        return _register(self, self._queries, qualified, fn)

    def middleware(self, types: list[str], order: int = 0, fn: Callable | None = None):
        """Makes this listener a middleware; works as a decorator too. The kernel asks `fn` about
        every command and query of `types` (`*` for all, `Type` of the own domain, `domain.*` or
        `domain.Type`) before accepting or routing it, in chain order (lower `order` first, then by
        listener name), and only once this listener has caught up with the events. `fn` gets a
        `Check`; raise `Rejected` to refuse; any other error refuses too. The registration is
        stored: while this listener is away, the kernel refuses those requests (`kernable listeners
        remove` unregisters it). `fn` cannot submit or query what it checks itself (it would wait
        behind itself): the kernel refuses it.
        Negative orders are reserved for kernable."""
        qualified = [t if t == "*" else self._qualify(t) for t in types]
        for t, q in zip(types, qualified):
            if not _CHECKED.match(q):
                raise ValueError(f"check `*`, a type (Type or domain.Type) or a domain (domain.*), not {t!r}")
        spec = pb.Middleware(order=order, types=qualified)
        if fn is not None:
            self._middleware = (spec, fn)
            return self

        def decorator(f: Callable) -> Callable:
            self._middleware = (spec, f)
            return f

        return decorator


class ListenerHandle(_Stream):
    """A running `Listener`, returned by `kernable.listener(listener)`: `run()` processes events,
    queries and checks until `stop()`; `ready` is set once the kernel accepted it."""

    def __init__(self, channel: grpc.Channel, key: Key, listener: Listener):
        super().__init__(f"listener {listener.name}", listener.reconnect_s)
        #: The listener definition this runs.
        self.listener = listener
        #: The listener's store, once `run()` opened it (with `store=`).
        self.store: Store | None = None
        self._stub = rpc.ListenerServiceStub(channel)
        self._key = key
        self._start = listener.start
        #: Global position of the last handled event (or skipped, for other event types).
        self.position = 0
        # Newest event when the kernel accepted the listener; events up to it are replayed.
        self._replay_up_to = 0

    def run(self) -> None:
        """Processes events, queries and checks until `stop()`, reconnecting when the stream
        drops; with a store, opens it first and continues after its position."""
        if self.listener.store_url and self.store is None:
            self.store = Store(self.listener.store_url, self.listener.schema)
            self.position = self._start = self.store.committed
        super().run()

    def _open(self, requests):
        return self._stub.Listen(requests)

    def _hello(self):
        d = self.listener
        hello = pb.Hello(name=d.name, key_id=self._key.id, protocol=PROTOCOL, order=d.order,
                         event_types=list(d._handlers),
                         query_types=list(d._queries))
        if d._middleware:
            hello.middleware.CopyFrom(d._middleware[0])
        # Reconnects resume after the last handled event.
        start = self.position if self.ready.is_set() else self._start
        if start is not None:
            hello.from_position = start
        return pb.ListenerMessage(hello=hello)

    def _on(self, msg):
        # Requests sent from the handlers name this listener, so the kernel never makes them
        # wait for it (it handles one message at a time).
        token = _HANDLING.set(self.listener.name)
        try:
            return self._handle(msg)
        finally:
            _HANDLING.reset(token)

    def _handle(self, msg):
        kind = msg.WhichOneof("msg")
        if kind == "challenge":
            sig = self._key.sign(challenge_message(msg.challenge.nonce)).sig
            return pb.ListenerMessage(challenge_response=pb.ChallengeResponse(sig=sig))
        if kind == "ready":
            self.position = msg.ready.after_position
            self._replay_up_to = msg.ready.head_position
            self.ready.set()
        elif kind == "batch":
            # With a store, the batch's data and position are committed together, before the ack.
            if self.store:
                self.store._begin()
            try:
                self._handle_batch(msg.batch)
                if self.store:
                    self.store._commit(msg.batch.up_to)
            except BaseException:
                if self.store:
                    self.store._rollback()
                    self.position = self.store.committed
                raise
            self.position = msg.batch.up_to
            return pb.ListenerMessage(ack=pb.Ack(position=msg.batch.up_to))
        elif kind == "query":
            return pb.ListenerMessage(answer=self._answer(msg.query.call_id, msg.query.query))
        elif kind == "check":
            return pb.ListenerMessage(verdict=self._verdict(msg.check))
        return None

    def _handle_batch(self, batch) -> None:
        d = self.listener
        for e in batch.events:
            name = f"{e.domain}.{e.event_type}"
            if name not in d._handlers:
                name = f"{e.domain}.*"
            handler = d._handlers.get(name)
            if handler:
                data, redacted = _unseal(_decode(e.payload), batch.vault)
                schema = e.schema
                if name not in d._raw:
                    schema, data = _upcast(d._domains.get(e.domain), e.domain, e.event_type, schema, data)
                evt = StoredEventMeta(e.event_type, e.version, e.tenant_id, e.domain, e.aggregate_type, e.aggregate_id,
                                      e.command_id, e.global_position, schema, redacted)
                ctx = ListenerContext(e.global_position <= self._replay_up_to, _from_wire_context(e.context),
                                      self.store, e.command_id)
                _call(handler, data, evt, ctx)
            self.position = e.global_position

    def _verdict(self, call) -> pb.Verdict:
        try:
            middleware = self.listener._middleware
            if middleware is None:
                raise Rejected("not a middleware", "not_a_middleware")
            middleware[1](_to_check(call))
            return pb.Verdict(call_id=call.call_id)
        except Exception as err:
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="middleware_error", message=repr(err)))
            return pb.Verdict(call_id=call.call_id, rejected=reason)

    def _answer(self, call_id: str, q) -> pb.QueryAnswer:
        try:
            handler = self.listener._queries.get(f"{q.domain}.{q.query_type}")
            if handler is None:
                raise Rejected(f"no handler for {q.domain}.{q.query_type}", "unknown_query")
            meta = QueryMeta(q.query_id, q.query_type, q.domain, q.tenant_id)
            result = _call(handler, _decode(q.payload), meta, QueryContext(_from_wire_context(q.context), self.store))
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


def _meta(event_type: str, version: int, cmd, command_id: str) -> EventMeta:
    """What `apply` gets about an event of `cmd`'s aggregate, caused by command `command_id`."""
    return EventMeta(event_type, version, cmd.tenant_id, cmd.domain, cmd.aggregate_type, cmd.aggregate_id,
                     command_id)


def _call(fn: Callable, *args):
    """Calls `fn` with as many of `args` as it takes, so the trailing parameters are optional."""
    params = inspect.signature(fn).parameters.values()
    if any(p.kind == p.VAR_POSITIONAL for p in params):
        return fn(*args)
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return fn(*args[:len(positional)])


def _tls_from_env() -> Tls | None:
    """Mutual TLS from `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`: all or none."""
    files = [os.environ.get(f"KERNABLE_TLS_{n}") for n in ("CA", "CERT", "KEY")]
    if not any(files):
        return None
    if not all(files):
        raise ValueError("set all of KERNABLE_TLS_CA, KERNABLE_TLS_CERT and KERNABLE_TLS_KEY, or none")
    return Tls(*files)


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


def _to_check(call) -> Check:
    if call.WhichOneof("request") == "command":
        c = call.command
        return Check("command", c.command_id, c.tenant_id, c.domain, c.command_type,
                     _unseal(_decode(c.payload), call.vault)[0],
                     _from_wire_context(c.context), c.aggregate_type, c.aggregate_id)
    q = call.query
    return Check("query", q.query_id, q.tenant_id, q.domain, q.query_type, _decode(q.payload),
                 _from_wire_context(q.context))


def _to_event(e, vault=None) -> Event:
    data, redacted = _unseal(_decode(e.payload), vault)
    return Event(position=e.global_position, tenant=e.tenant_id, domain=e.domain, aggregate_type=e.aggregate_type,
                 aggregate_id=e.aggregate_id, version=e.version, type=e.event_type,
                 data=data, command_id=e.command_id, request=_from_wire_context(e.context),
                 schema=e.schema, redacted=redacted)
