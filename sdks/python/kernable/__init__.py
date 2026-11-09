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

import concurrent.futures
import contextvars
import copy
import dataclasses
import hashlib
import inspect
import json
import os
import queue
import random
import re
import sqlite3
import sys
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Any, Callable, Iterable

import grpc

from kernable.signing import (Key, cache_message, challenge_message, command_message, data_message, derived_id,
                              index_message, membership_message, query_message, vault_ref, rejection_message, result_message,
                              snapshot_message)
from kernable.store import Store
from kernable.v1 import kernable_pb2 as pb
from kernable.v1 import kernable_pb2_grpc as rpc

from .messages import (  # noqa: E402
    Command, Event, Query, bind as _bind, binding as _binding, declare_command as _declare_command,
    declare_event as _declare_event, from_wire as _from_wire, is_message_class as _is_message_class,
    query_target as _query_target, to_wire as _to_wire,
)

__all__ = ["PROTOCOL", "Event", "Command", "Query", "StoredEvent", "Aggregate", "AggregateType", "MiddlewareContext", "App", "ApplyContext", "CacheClient", "Check", "ClientClosed", "CommandContext", "CommandMeta",
           "CommandSchema", "QuerySchema", "Target",
           "Data", "DataClient", "Domain", "Listed", "Listing",
           "EventMeta", "EventSchema", "HeldKey", "Index", "Kernable", "Key", "Listener", "ListenerContext", "ListenerHandle", "Manifest", "NotSettled", "Outcome",
           "QueryContext", "QueryMeta", "QueryOutcome", "Rejected", "RequestContext", "Requirement", "Store", "StoredEventMeta", "Tls", "Worker",
           "domain", "reindex_type"]

#: Time zero (UTC): the default of times not known.
_EPOCH = datetime.fromtimestamp(0, timezone.utc)


class Rejected(Exception):
    """A business reason to refuse a command or query: raise it from a command handler, a query
    handler or a middleware, e.g. `raise Rejected("Cart is checked out", "checked_out")`.

    `code` (default `rejected`) is what callers act on, the message (`str(err)`) what they show.
    `Kernable.query` raises it when the read model or a middleware refuses the query; a rejected
    command does not raise: `Kernable.submit` returns an `Outcome` with `committed=False`. Any
    other exception from a command or query handler rejects too, with the code `handler_error`
    (`middleware_error` from a middleware)."""

    def __init__(self, message: str, code: str = "rejected"):
        super().__init__(message)
        self.code = code
        #: For a rejected follow-up command (`ListenerContext.submit`): the key the kernel named
        #: (`unique_taken`).
        self.key = ""
        #: For a rejected follow-up command: the requirement the kernel named (`requirement_missing`).
        self.requirement: "Requirement | None" = None


@dataclass
class RequestContext:
    """Who sends a request and in which context: signed with the command and stored with every
    event it causes. `attributes` are business data only, never security decisions.

    Set a default for a client with `Kernable(context=...)`, or one per request with `context=`
    of `submit`, `enqueue` and `query`. Inside a listener's handler, submit follow-up commands
    with `ctx.follow()`. Example: `RequestContext(sender_identity="customer-7",
    expected_version=3)`."""

    #: The tenant the sender belongs to.
    sender_tenant: str = ""
    #: Who sends the request, e.g. a user or service ID; middlewares check permissions on it.
    sender_identity: str = ""
    sender_account: str = ""
    sender_session: str = ""
    #: Authorize as this identity of the target tenant; the sender stays the real principal.
    on_behalf_of: str = ""
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

    #: The command's ID: given as `id` to `submit`, else a new UUID.
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
    #: When the kernel received the command (UTC); `now()` returns it.
    received_at: datetime
    #: The command whose IDs `id()` derives.
    _command_id: str = field(default="", repr=False)
    _ids: int = field(default=0, repr=False)

    @classmethod
    def new(cls, command_id: str, request: RequestContext | None = None,
            received_at: datetime = _EPOCH) -> "CommandContext":
        """A context for calling a command handler directly, e.g. in a unit test with
        `Aggregate.handle`: `id()` derives from `command_id` as in a real command, `now()` returns
        `received_at` (default: the epoch), and its deadline is 30 s after that."""
        return cls(request or RequestContext(), received_at + timedelta(seconds=30), received_at, command_id)

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
class MiddlewareContext:
    """What a middleware handler gets besides the check (an optional third parameter)."""

    #: The listener's store, if it has one.
    store: "Store | None" = None


@dataclass(frozen=True)
class Target:
    """The aggregate a request is about, as `Check.target` holds it."""

    #: Qualified: `shop.order`.
    type: str
    id: str


@dataclass
class Check:
    """A command, query, file or cache request a middleware checks, before the kernel accepts,
    routes or runs it (or, for commands and queries, after): the first parameter of a
    `Listener.middleware` handler."""

    #: `"command"`, `"query"`, `"data"` (a file request of `k.data`) or `"cache"` (a cache
    #: request of `k.cache`).
    kind: str
    #: The command or query ID; empty for a file or cache request.
    id: str
    tenant: str
    domain: str
    #: The type within the domain: the command or query type, or for a file request `PutData`,
    #: `GetData` or `DeleteData`, for a cache request `SetCache`, `GetCache` or `DeleteCache`.
    type: str
    #: The request's payload; vault fields are `None` unless the key is granted `vault_read`.
    #: Always `None` for a file or cache request: the middleware decides on who asks for which
    #: key, never sees the file or value.
    data: Any
    #: Who sends it: check permissions here.
    request: RequestContext
    #: Commands only; empty otherwise.
    aggregate_type: str = ""
    #: Commands only; empty otherwise.
    aggregate_id: str = ""
    #: File and cache requests only: the file or cache key; empty otherwise.
    key: str = ""
    #: The aggregate the request is about: a command's aggregate (`Target("shop.order", "o-17")`),
    #: or the one a query declares (`Query.target`, `QuerySchema.target`). None for queries that
    #: declare none, and for file and cache requests.
    target: "Target | None" = None


@dataclass
class StoredEvent:
    """An event as stored by the kernel, as `Outcome.events` holds it."""

    #: Global position of the event in the kernel's log.
    position: int
    tenant: str
    #: Always the domain of the command that caused the event.
    domain: str
    aggregate_type: str
    aggregate_id: str
    #: The aggregate's version after this event: 1 for its first event.
    version: int
    #: The event type within the domain, e.g. `ItemAdded`.
    type: str
    #: The payload.
    data: Any
    #: The command that caused the event.
    command_id: str
    #: The request context of the command that caused the event.
    request: RequestContext = field(default_factory=RequestContext)
    #: Version of the payload format, as stored (see `Domain.event`).
    schema: int = 1
    #: Fields kept in the vault whose value is not here (None in `data`): erased, or not readable
    #: with this key (granted `vault_read`).
    redacted: list[str] = field(default_factory=list)
    #: When the kernel committed the event (UTC).
    created_at: datetime = _EPOCH


@dataclass
class StoredEventMeta(EventMeta):
    """What a listener handler gets about the event besides its data (as an optional second
    parameter): the stored event without its payload and request context. It has every field
    of `EventMeta` (`type`, `version`, `tenant`, `domain`, `aggregate_type`, `aggregate_id`,
    `command_id`), and these. Unlike `EventMeta`, it says when the event was committed: a read
    model may show that time, `apply` may not depend on it."""

    #: Global position of the event in the kernel's log.
    position: int = 0
    #: Version of the payload format: the version the listener knows (see `Domain.event`), or the
    #: stored one for a `raw` handler.
    schema: int = 1
    #: Fields kept in the vault whose value is not in the data (None there): erased, or not
    #: readable with this key (granted `vault_read`).
    redacted: list[str] = field(default_factory=list)
    #: When the kernel committed the event (UTC).
    created_at: datetime = _EPOCH


@dataclass
class ListenerContext:
    """What a listener handler gets besides the event's data and meta (as an optional third
    parameter)."""

    #: The event was committed before this connection began: a read model rebuilding from the
    #: start, and equally the backlog a reactor catches up on after a restart or a lost
    #: connection. A reactor must not skip side effects because of it (those events are new to
    #: it); submitting with fixed command IDs makes them safe to repeat instead. Use it to skip
    #: work that only matters live, e.g. a notification about something long past, in a listener
    #: that starts at 0.
    replaying: bool
    #: The request context of the command that caused the event.
    request: RequestContext = field(default_factory=RequestContext)
    #: The listener's store, if it has one (`store=` of `Domain.read_model` and `Domain.reactor`).
    store: "Store | None" = None
    #: The command that caused the event, for `follow()`.
    _command_id: str = field(default="", repr=False)
    #: The event's tenant: the default for follow-up commands and queries.
    _tenant: str = field(default="", repr=False)
    #: The listener's domain: the default for follow-up commands and queries.
    _domain: str = field(default="", repr=False)
    #: The listener's client, for follow-up commands and queries.
    _kernable: "Kernable | None" = field(default=None, repr=False)

    def submit(self, aggregate_type, aggregate_id: str | None = None, type: str | None = None, data: Any = None, *, id: str,
               domain: str | None = None, tenant: str | None = None, context: "RequestContext | None" = None,
               wait: str = "settled", timeout_s: float | None = None) -> "list":
        """Submits a command this event causes, on the listener's client, and returns its events.
        It needs a fixed `id` (`f"confirm-{evt.aggregate_id}"`), so a redelivered event runs it
        once. `domain` defaults to the listener's, `context` to `follow()`, `tenant` to the
        event's; the rest as for `Kernable.submit`.

        Raises `Rejected` (with the kernel's `key` or `requirement`, if any) if the command is
        rejected: a reactor then parks the event at once (see `park_after` of
        `Domain.reactor`); catch it to accept a rejection. Raises like `Kernable.submit` otherwise.
        Example: `ctx.submit("order", evt.aggregate_id, "Confirm", id=f"confirm-{evt.aggregate_id}")`."""
        if not id:
            raise ValueError(f"{type or aggregate_type}: a follow-up command needs a fixed id, so a redelivered event runs it once")
        assert self._kernable is not None, "only a running listener's handlers submit follow-ups"
        if isinstance(aggregate_type, Command):
            domain, aggregate_type, type, data = self._kernable._command_of(aggregate_type)
        outcome = self._kernable.submit(domain or self._domain, aggregate_type, aggregate_id, type, data,
                                        tenant=tenant or self._tenant, id=id, wait=wait,
                                        context=context or self.follow(), timeout_s=timeout_s)
        if outcome.committed:
            return outcome.events
        rejected = Rejected(outcome.message, outcome.code)
        rejected.key = outcome.key
        rejected.requirement = outcome.requirement
        raise rejected

    def query(self, type, data: Any = None, *, tenant: str | None = None,
              context: "RequestContext | None" = None, min_position: int = 0,
              timeout_s: float | None = None, all_partitions: bool = False) -> Any:
        """Asks a query on the listener's client, like `Kernable.query`: `type` is of the
        listener's domain (`GetOrder`) or qualified (`crm.GetCustomer`). `tenant` defaults to the
        event's. It is the listener's own read: it keeps the client's context, not the event's
        sender."""
        assert self._kernable is not None, "only a running listener's handlers ask queries"
        if isinstance(type, Query):
            return self._kernable.query(type, tenant=tenant or self._tenant, context=context, min_position=min_position,
                                        timeout_s=timeout_s, all_partitions=all_partitions)
        domain, _, name = type.rpartition(".")
        return self._kernable.query(domain or self._domain, name, data, tenant=tenant or self._tenant,
                                    context=context, min_position=min_position,
                                    timeout_s=timeout_s)

    def follow(self) -> RequestContext:
        """A request context for commands caused by this event: same sender and correlation."""
        r = self.request
        return RequestContext(r.sender_tenant, r.sender_identity, r.sender_account, r.sender_session,
                              r.on_behalf_of, None, r.correlation_id or self._command_id,
                              self._command_id, r.trace_id, False, dict(r.attributes))


@dataclass
class Outcome:
    """What became of a command, returned by `Kernable.submit`: committed with its events, or
    rejected with a code and a message. Example: `Outcome(committed=False, events=[],
    code='rejected', message='Cart is checked out')`."""

    #: Whether the command's events were stored; `False` if it was rejected.
    committed: bool
    #: The committed events, in order; empty if the command was rejected. Vault fields in them
    #: are `None` (see `StoredEvent.redacted`).
    events: list[StoredEvent] = field(default_factory=list)
    #: The rejection code (see `Rejected`), if rejected.
    code: str = ""
    #: The rejection message, if rejected.
    message: str = ""
    #: For `unique_taken`: the name of the key another aggregate holds (never its value). Set by
    #: the kernel only, and stored with the rejection.
    key: str = ""
    #: For `requirement_missing`: the first requirement that does not hold. Set by the kernel only.
    requirement: "Requirement | None" = None


@dataclass
class Requirement:
    """A fact another aggregate of the command's tenant must hold (or, `absent`, must not hold)
    when the command commits, as `requires=` of `Aggregate.command` returns them:
    `Requirement("funds", "vehicle", data["vehicleId"], "open")`. The kernel checks it atomically
    with the commit; if it does not hold, the command is rejected with `requirement_missing`.

    The aggregate is named by its ID, or by a unique key of its type (`Requirement.by_key`, not a
    personal key), which the kernel resolves at commit: no aggregate holds the key, none holds
    the fact; an aggregate without events holds none either. An empty `domain` is the command's
    own (`Requirement.of`)."""

    #: Empty: the domain of the command's aggregate.
    domain: str
    #: Unqualified, e.g. `vehicle`.
    aggregate_type: str
    #: Empty when the aggregate is named by `key`.
    aggregate_id: str
    #: A fact the aggregate declares with `Aggregate.fact`.
    fact: str
    #: A unique key of the aggregate type that names the aggregate instead of its ID.
    key: str = ""
    #: The key's value, normalised as the aggregate type's key function does: the kernel gets only
    #: its SHA-256. Empty in a rejection, which never carries the value.
    value: str = ""
    #: The aggregate must not hold the fact.
    absent: bool = False

    @classmethod
    def of(cls, aggregate_type: str, aggregate_id: str, fact: str, *, absent: bool = False) -> "Requirement":
        """A requirement of an aggregate type of the command's own domain:
        `Requirement.of("vehicle", data["vehicleId"], "woundUp", absent=True)`."""
        return cls("", aggregate_type, aggregate_id, fact, absent=absent)

    @classmethod
    def by_key(cls, aggregate_type: str, key: str, value: str, fact: str, *, domain: str = "",
               absent: bool = False) -> "Requirement":
        """The fact of the aggregate that holds the unique key `key` with `value`, normalised as
        its key function does: `Requirement.by_key("vehicle", "pmdxId", data["vehicle"], "open")`."""
        return cls(domain, aggregate_type, "", fact, key=key, value=value, absent=absent)


@dataclass
class HeldKey:
    """A unique key an aggregate holds for a state, as `Aggregate.index` returns it: the value in
    clear (the worker sends only its SHA-256 hash)."""

    #: The name declared with `Aggregate.unique`.
    name: str
    value: str
    #: `"tenant"` or `"global"`; see `Aggregate.unique`.
    scope: str = "tenant"
    #: The person the value belongs to, for a personal key; empty otherwise.
    subject: str = ""


@dataclass
class Index:
    """The unique keys and facts an aggregate holds for a state, returned by `Aggregate.index`."""

    keys: list[HeldKey] = field(default_factory=list)
    #: The names of the facts that are true, in the order they were declared.
    facts: list[str] = field(default_factory=list)


@dataclass
class QueryOutcome:
    """What came of a query, as a middleware that runs after requests (`when="after"`) gets it:
    its response data or why it was rejected."""

    #: Whether the read model answered; `False` if the query was rejected.
    answered: bool
    #: The answer, if answered.
    data: Any = None
    #: The rejection code, if rejected.
    code: str = ""
    #: The rejection message, if rejected.
    message: str = ""


@dataclass
class Tls:
    """Paths of PEM files for mutual TLS with a kernel at `host:port`: the CA that signed the
    kernel's certificate, and this process's own certificate and key. Pass it as
    `Kernable(tls=Tls(ca="ca.pem", cert="app.pem", key="app-key.pem"))`, or set
    `KERNABLE_TLS_CA`, `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`."""

    ca: str
    cert: str
    key: str


@dataclass
class Data:
    """A file as stored by the kernel, returned by `k.data.get`."""

    data: bytes
    #: The media type given when the file was put, e.g. `image/png`.
    content_type: str
    #: The SHA-256 of `data`, hex, as the kernel computed it.
    sha256: str = ""


@dataclass
class Listed:
    """A file as `k.data.list` names it."""

    key: str
    content_type: str
    #: The file's length in bytes.
    size: int


@dataclass
class Listing:
    """One page of `k.data.list`: up to 1,000 files, and `next` (to pass as `after=`) if there
    may be more."""

    files: list[Listed]
    next: str | None


class ClientClosed(RuntimeError):
    """Raised by a request on a client after `Kernable.close()`. Workers and listeners of a
    closed client end their `run()`."""

    def __init__(self):
        super().__init__("the kernable client is closed")


class NotSettled(Exception):
    """Raised by a settling `Kernable.submit` (the default `wait="settled"`): the command was
    committed, but not every listener that counts processed its events within the kernel's
    `--settle-timeout-ms`.

    Read models may not show the change yet; `outcome` (an `Outcome`) has the committed events,
    e.g. `except NotSettled as err: events = err.outcome.events`. Submitting again with the
    same `id` returns the same outcome, without running the command again.
    """

    def __init__(self, outcome: Outcome):
        super().__init__("command committed, but not all listeners have processed its events yet")
        self.outcome = outcome


#: The protocol version this SDK speaks; see "Protocol versions" in `kernable.proto`.
PROTOCOL = 1

#: Attempts before a reactor parks an event its handler keeps failing on.
_PARK_AFTER = 5

#: gRPC codes of a kernel that is away or busy.
_BUSY = frozenset({grpc.StatusCode.CANCELLED, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED,
                   grpc.StatusCode.ABORTED, grpc.StatusCode.INTERNAL, grpc.StatusCode.UNAVAILABLE})

#: The name of the listener whose handler is running, for requests sent from it.
_HANDLING: contextvars.ContextVar[str] = contextvars.ContextVar("kernable_listener", default="")

_INITIAL = object()
#: Room for the largest file a kernel takes (`--max-data-bytes`, at most 128 MiB) and the rest of
#: its message; grpcio receives 4 MiB by default.
_MAX_MESSAGE = 129 * 1024 * 1024
#: Pings on every connection, also idle ones: a connection the network dropped without a word (a
#: dead host, a NAT that forgot it) ends within 40 s, and the workers and listeners on it reconnect.
_CHANNEL_OPTIONS = [("grpc.max_send_message_length", _MAX_MESSAGE),
                    ("grpc.max_receive_message_length", _MAX_MESSAGE),
                    ("grpc.keepalive_time_ms", 30_000),
                    ("grpc.keepalive_timeout_ms", 10_000),
                    ("grpc.keepalive_permit_without_calls", 1)]
#: gRPC codes that a retry does not change.
_REFUSALS = {grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.FAILED_PRECONDITION,
             grpc.StatusCode.UNIMPLEMENTED, grpc.StatusCode.UNAUTHENTICATED}
#: How long a request waits, for the connection and the answer, unless told otherwise: above the
#: kernel's own submit and settle timeouts.
DEFAULT_TIMEOUT_S = 60.0
_DOMAIN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_UNQUALIFIED = re.compile(r"^[^.*]+$")
_LISTENED = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*)$")
_QUERY = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*\.[^.*]+$")
_CHECKED = re.compile(r"^(\*|[A-Za-z][A-Za-z0-9_-]*\.([^.]+|\*))$")
def reindex_type(aggregate_type: str) -> str:
    """The type of the built-in command an aggregate with unique keys or facts runs without
    events, `_reindex_<aggregate_type>` (see `Aggregate.unique` and `Kernable.reindex`): one per
    aggregate, as command types are unique within a domain. `reindex_type("fund")` is
    `"_reindex_fund"`."""
    return f"_reindex_{aggregate_type}"


class EventSchema:
    """The payload format of one event type: its current version and the steps that bring older
    payloads up to it. Events are stored as written and never changed; readers upcast them.
    Made with `Domain.event`; its methods return it, so they chain:
    `shop_domain.event("CustomerRegistered", 1).personal(["email"], lambda d, id: id)`."""

    def __init__(self, type: str, schema: int):
        """Use `Domain.event`. Raises `ValueError` if `type` is qualified or `schema` is below 1."""
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
        """Turns a payload of version `from_version` into one of the next version: `fn(data)`
        returns the new payload. Works as a decorator too: `@item_added.upcast(1)` over
        `def _(data): return {**data, "qty": data.get("qty", 1)}`. Declare one step per older
        version. Raises `ValueError` if `from_version` is not between 1 and the current version
        minus 1."""
        if not isinstance(from_version, int) or not 1 <= from_version < self.schema:
            raise ValueError(f"upcast {self.type} from a version between 1 and {self.schema - 1}, not {from_version!r}")
        return _register(self, self.steps, from_version, fn)


class _VaultFields:
    """Payload fields kept in the vault instead of the signed log."""

    def __init__(self):
        self.personal: list[tuple[list[str], Callable]] = []
        self.secret: list[str] = []


class CommandSchema:
    """How a command's payload is kept: which fields go to the vault. Made with
    `Domain.command`; its methods return it, so they chain:
    `shop_domain.command("Register").personal(["email"], lambda d, id: id).secret(["password"])`.
    Clients that submit these commands need the domain: `k.use(shop_domain)`."""

    def __init__(self, type: str):
        """Use `Domain.command`. Raises `ValueError` if `type` is qualified."""
        if not re.match(r"^[^.*]+$", type):
            raise ValueError(f"command types are unqualified within their domain, not {type!r}")
        self.type = type
        self.vault = _VaultFields()

    def personal(self, fields: list[str], subject: Callable) -> "CommandSchema":
        """Keeps these top-level fields in the vault as personal data of `subject`; see
        `EventSchema.personal`. Declare them on the events that carry them too: a worker rejects
        the command (`personal_in_clear`) if one of its events holds such a field, under the same
        name, without declaring it."""
        self.vault.personal.append((fields, subject))
        return self

    def secret(self, fields: list[str]) -> "CommandSchema":
        """Keeps these top-level fields (passwords, one-time tokens) in the vault only until the
        command completed. The handler gets them in clear; the stored command keeps a reference."""
        self.vault.secret.extend(fields)
        return self


class QuerySchema:
    """What a domain declares about a query type given by name: the aggregate it is about. Made
    with `Domain.query`; query classes declare it as `target` instead (see `Query`):
    `shop_domain.query("GetOrder").target("order", "id")` (`GetOrder {"id": "o-17"}` is about
    order o-17)."""

    def __init__(self, type: str):
        """Use `Domain.query`. Raises `ValueError` if `type` is qualified."""
        if not re.match(r"^[^.*]+$", type):
            raise ValueError(f"query types are unqualified within their domain, not {type!r}")
        self.type = type
        #: The aggregate the query is about, `(aggregate_type, field)`; see `target`.
        self.target_of: tuple[str, str] | None = None

    def target(self, aggregate_type: str, field: str) -> "QuerySchema":
        """The query is about one aggregate of `aggregate_type` (`order`, or qualified
        `shop.order`), whose ID is in the data's `field` (as sent: `orderId`). The SDK sends it
        with the query, signed, and a middleware decides on it as on a command's aggregate.
        Clients that ask it need the domain: `kernable.use(domain)`."""
        self.target_of = (aggregate_type, field)
        return self


def _query_target_of(declared: tuple[str, str] | None, domain: str, data: Any) -> "pb.Target | None":
    """The wire target of a query: the declared aggregate type, qualified, and the ID from `data`."""
    if declared is None:
        return None
    aggregate_type, field_ = declared
    id = data.get(field_) if isinstance(data, dict) else None
    if not isinstance(id, str) or not id:
        return None  # no ID: no target, so a rule that needs one refuses
    return pb.Target(aggregate_type=aggregate_type if "." in aggregate_type else f"{domain}.{aggregate_type}",
                     aggregate_id=id)


_issued_lock = threading.Lock()
_issued_last = 0


def _issued_now() -> int:
    """A request's issued_at: now, but later than every one this process issued before. Two calls in the
    same millisecond would otherwise carry the same signature, and the kernel refuses the second as a replay."""
    global _issued_last
    with _issued_lock:
        _issued_last = max(int(time.time() * 1000), _issued_last + 1)
        return _issued_last


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
    are named `domain.Type`.

    Make it with `domain("shop")`. It remembers the aggregates, read models and reactors defined
    on it, so `Kernable.app` runs all of them."""

    def __init__(self, name: str):
        """Use `domain(name)`. Raises `ValueError` unless `name` starts with a letter and holds
        only letters, digits, `-` and `_`."""
        if not _DOMAIN.match(name):
            raise ValueError(f"invalid domain name {name!r}")
        self.name = name
        #: Payload versions and upcasts per event type; types not listed are at version 1.
        self.events: dict[str, EventSchema] = {}
        #: Vault fields per command type; see `command`.
        self.commands: dict[str, CommandSchema] = {}
        #: What query types given by name are about; see `query`.
        self.queries: dict[str, QuerySchema] = {}
        #: The aggregates defined on it, which `Kernable.app` runs.
        self.aggregates: list[AggregateType] = []
        #: The read models and reactors defined on it, which `Kernable.app` runs.
        self.listeners: list[Listener] = []

    def command(self, type: str) -> CommandSchema:
        """Declares which fields of `type` commands the vault keeps (`personal`, `secret`); see
        `CommandSchema`. Clients that submit them need the domain: `kernable.use(domain)`."""
        spec = CommandSchema(type)
        self.commands[type] = spec
        return spec

    def query(self, type: str) -> QuerySchema:
        """Declares what `type` queries (unqualified) are about: returns the `QuerySchema` to chain
        `target` on. Query classes declare it as `target` instead. Clients that ask such queries
        by name need the domain: `kernable.use(domain)`."""
        spec = QuerySchema(type)
        self.queries[type] = spec
        return spec

    def event(self, type, schema: int | None = None, *more) -> EventSchema | None:
        """Declares that this domain writes `type` events at version `schema` (from 1), with an
        `upcast` per older version, and which of their fields the vault keeps; see
        `EventSchema`. The domain's aggregates and listeners use it; listeners of other domains
        get it with `Listener.use(domain)`. Types not declared are at version 1. Example:
        `shop_domain.event("ItemAdded", 2).upcast(1, lambda d: {**d, "qty": 1})`.

        With event classes instead (`shop_domain.event(ItemAdded, CartCleared)`), declares them
        from their class attributes, e.g. in a process that only reads them; an aggregate's
        `apply` declares them too."""
        if _is_message_class(type):
            schemas = self
            for cls in (type, schema, *more):
                if cls is None:
                    continue
                _declare_event(cls, self.events, EventSchema)
                _bind(cls, self.name, schemas)
            return None
        spec = EventSchema(type, schema)
        self.events[type] = spec
        return spec

    def aggregate(self, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1") -> "AggregateType":
        """An aggregate of this domain named `name` (lowercase by convention, like `cart`), whose
        state starts as `initial` (a JSON-like value, usually a dict); see `Aggregate` for
        `snapshot_every` and `schema`. Run it with `kernable.worker(aggregate)` or in an app.
        Example: `cart_aggregate = shop_domain.aggregate("cart", {"items": 0})`. Raises
        `ValueError` if the domain has an aggregate of that name already."""
        if any(a.name == name for a in self.aggregates):
            raise ValueError(f"{self.name} has an aggregate {name} already")
        aggregate = AggregateType(self.name, name, initial, snapshot_every, schema, self.events, self.commands)
        self.aggregates.append(aggregate)
        return aggregate

    def read_model(self, name: str, *, order: int = 0, start: int | None = 0, reconnect_s: float = 1.0,
                   store: str | None = None, schema: str = "1", live: bool = False,
                   park_after: int = 0, instance: str | None = None, single: bool = False) -> "Listener":
        """A read model of this domain named `name`: builds a view from events and answers
        queries from it (`on`, `query`; see `Listener`). Kept in memory and rebuilt from the
        first event on every start (default). Run it with `kernable.listener(read_model)` or in
        an app. Example: `carts_readmodel = shop_domain.read_model("carts")`.

        `order` (default 0): listeners with a lower order process each event first. `start`:
        the position to start after on the first connect (default 0, from the beginning; `None`:
        the kernel's stored position, or the newest event for a new listener). `reconnect_s`:
        seconds to wait before reconnecting after the stream drops (default 1), twice as long
        after each further failed attempt, at most 30.

        With `store` (`sqlite:<path>` or `memory:`), its state lives in the running listener's
        `store`: data and position are written together per batch, and a restart continues after
        the stored position. Change `schema` when the handlers store data differently: the store is
        cleared and the read model rebuilds from the first event. `live` and `park_after` (default
        0: never; a read model that parks misses the event's change): see `reactor`.

        `instance`: runs this process's read model as one instance of a group under its name,
        e.g. one per node of a pair (1 to 128 letters, digits, `-`, `_` and `.`; default
        `KERNABLE_INSTANCE`, which `kernable node` sets; without either, the name has one
        connection). Every instance gets every event, from its own position, and answers
        queries; the group is through an event once one instance has handled it. `single`: see
        `reactor`. Raises `ValueError` if the domain has a listener (read model or reactor) of
        that name already."""
        return self._listener(Listener(self, name, order, start, reconnect_s, store, schema, live=live,
                                       park_after=park_after, instance=instance, single=single))

    def reactor(self, name: str, *, order: int = 10, start: int | None = None, reconnect_s: float = 1.0,
                store: str | None = None, schema: str = "1", live: bool = False,
                park_after: int = _PARK_AFTER, instance: str | None = None, single: bool = True) -> "Listener":
        """A reactor of this domain named `name`: triggers side effects and follow-up commands
        (`on`; see `Listener`). It starts at the newest event when it is new, then resumes after
        the last acknowledged event (`start=None`, default), so it never reacts to what happened
        before it existed. Run it with `kernable.listener(reactor)` or in an app.

        A settling submit from its handler waits for the listeners before it (the read models),
        not for the reactor itself, which handles one event at a time. Submit follow-ups with
        `ctx.submit` and a fixed `id`, so a redelivered event does not run the command twice.
        `order` (default 10), `start`, `reconnect_s`, `store` and `schema`: see `read_model`.

        `park_after` (default 5): how often a handler may fail on one event before the reactor
        parks it and moves on (0: never, it keeps getting the event and everything behind it
        waits). A `Rejected` (also from `ctx.submit`) parks at once; the kernel being unreachable
        or busy (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`), `NotSettled` or the
        store failing never does: the reactor waits and tries again. The kernel keeps parked
        events with the rejection's code and message (for other errors `handler_error` and the
        exception's type; the log has the whole error) until an operator retries or dismisses
        them (`kernable listeners retry`). With a store, a parked event's writes are dropped, the
        rest of its batch kept. Live listeners never park, and no listener parks an event that
        erases a subject: its handler must get past it, so no personal data stays behind.

        `live`: not registered with the kernel: no stored position, always starts at the newest
        event, nobody waits for it, and it is forgotten when it disconnects. For reactions that
        only matter while the process runs, e.g. pushing to connected browsers. Cannot be a
        middleware.

        `instance`: see `read_model`. `single` (default `True` for reactors): the instances take
        turns: one gets events, the others wait and the first of them takes over when it leaves,
        so a side effect happens once. `False`: every instance gets every event; such a listener
        cannot park events. Live listeners are never single. Raises `ValueError` if the domain
        has a listener of that name already."""
        return self._listener(Listener(self, name, order, start, reconnect_s, store, schema, live=live,
                                       park_after=park_after, instance=instance, single=single))

    def _listener(self, listener: "Listener") -> "Listener":
        if any(l.name == listener.name for l in self.listeners):
            raise ValueError(f"{self.name} has a listener {listener.name} already")
        self.listeners.append(listener)
        return listener


def domain(name: str) -> Domain:
    """A domain named `name`, on which you define its aggregates, read models, reactors and
    event and command schemas: `shop_domain = domain("shop")`. Raises `ValueError` unless `name`
    starts with a letter and holds only letters, digits, `-` and `_`."""
    return Domain(name)


class AggregateType:
    """How events change an aggregate's state (`apply`) and which events a command produces
    (`command`). Handlers must be deterministic and free of side effects; they may run again.

    Make it with `Domain.aggregate`. Both work as decorators (`@agg.apply("Type")`) or directly
    (`agg.apply("Type", fn)`). A command handler returns a list of `{"type": ..., "data": ...}`
    and refuses with `raise Rejected(...)`. Handlers take only the parameters they need:
    `def _(state, data)` is enough, `def _(state, data, cmd, ctx)` gets the rest.

    Every `snapshot_every` events (0 turns it off) the worker sends a snapshot of the state, so
    later commands fold only the events after it. The state must survive a JSON round trip.
    Change `schema` when `apply` produces differently shaped state: older snapshots are then
    ignored and the state is rebuilt from the events.

    Uniqueness and conditions across aggregates: `unique` declares keys no other aggregate may
    hold at the same time (an email, an order number), `fact` what other aggregates' commands may
    require of this one (`requires=` of `command`). The kernel checks both atomically with the
    commit.
    """

    def __init__(self, domain: str, name: str, initial: Any, snapshot_every: int = 100, schema: str = "1",
                 events: dict[str, EventSchema] | None = None, commands: dict[str, CommandSchema] | None = None):
        """Use `Domain.aggregate`, which passes the domain's event and command schemas."""
        self.domain = domain
        #: The domain's event schemas.
        self.events = {} if events is None else events
        #: The domain's command schemas: which command fields are personal.
        self.commands = {} if commands is None else commands
        self.name = name
        self.initial = initial
        self.snapshot_every = snapshot_every
        self.schema = schema
        self.appliers: dict[str, Callable] = {}
        self.handlers: dict[str, Callable] = {}
        #: Per command type, the facts of other aggregates it requires; see `command`.
        self.requirements: dict[str, Callable] = {}
        #: The unique keys, by name: `(fn, scope, personal)`; see `unique`.
        self.keys: dict[str, tuple[Callable, str, Callable | None]] = {}
        #: The facts, by name; see `fact`.
        self.facts: dict[str, Callable] = {}
        #: How a state becomes the JSON of a snapshot, and back (an `Aggregate` class: its fields).
        self._to_json: Callable[[Any], Any] = lambda state: state
        self._from_json: Callable[[Any], Any] = lambda data: data

    def apply(self, event_type: str, fn: Callable | None = None):
        """How an event of `event_type` changes the state: `fn(state, data, evt, ctx)` changes
        `state` in place and returns nothing, or returns a new state that replaces it. `state` is a
        copy, so changing it never touches the state it came from; `data` is the payload, upcast
        to the current version; `evt` (an `EventMeta`) and `ctx` (an `ApplyContext`) are
        optional. Works as a decorator too: `@cart_aggregate.apply("ItemAdded")` over
        `def _(state, data): state["items"] += data["qty"]`. Returns the decorated function, or
        the aggregate when `fn` is given.

        With an event class (`@cart_aggregate.apply(ItemAdded)`), `data` is an instance of it."""
        if _is_message_class(event_type, Event):
            cls = event_type
            name = _declare_event(cls, self.events, EventSchema)
            _bind(cls, self.domain, self._schemas())

            def wrap(f: Callable) -> Callable:
                return lambda state, data, evt=None, ctx=None: _call(f, state, cls.from_wire(data), evt, ctx)

            return _register_wrapped(self, self.appliers, name, fn, wrap)
        return _register(self, self.appliers, event_type, fn)

    def _schemas(self):
        """This aggregate's domain's schemas, as a class binding carries them."""
        from types import SimpleNamespace
        return SimpleNamespace(name=self.domain, events=self.events, commands=self.commands)

    def _plain(self, produced: Any) -> Any:
        """Event class instances a handler returned, as `{"type": ..., "data": ...}`."""
        if not isinstance(produced, list):
            return produced
        out = []
        for e in produced:
            if isinstance(e, Event):
                name = _declare_event(type(e), self.events, EventSchema)
                _bind(type(e), self.domain, self._schemas())
                out.append({"type": name, "data": e.to_wire()})
            else:
                out.append(e)
        return out

    def command(self, command_type: str, fn: Callable | None = None, *, requires: Callable | None = None):
        """Which events a `command_type` command produces: `fn(state, data, cmd, ctx)` returns
        them as a list of `{"type": ..., "data": ...}`, or raises
        `Rejected`; `cmd` (a `CommandMeta`) and `ctx` (a `CommandContext`) are optional. `state`
        is the state after every earlier event; do not change it. Works as a decorator too:
        `@cart_aggregate.command("AddItem")` over `def _(state, data): return [{"type":
        "ItemAdded", "data": data}]`. Returns the decorated function, or the aggregate when `fn`
        is given.

        `requires(data, state)` returns the facts other aggregates of the command's tenant must
        hold when it commits, as a list of `Requirement` (`state` is the state before the
        command): `requires=lambda data, state: [Requirement("funds", "vehicle", data["vehicleId"],
        "open")]`. The kernel checks them atomically with the commit and rejects the command with
        `requirement_missing` if one is not held, so no other command can change them in between.
        Raises `ValueError` for a type that starts with `_reindex_`: aggregates with keys or
        facts run `_reindex_<aggregate_type>` themselves (see `unique`), and one of another aggregate
        of the domain could clash."""
        if _is_message_class(command_type, Command):
            cls = command_type
            name = _declare_command(cls, self.commands, CommandSchema)
            _bind(cls, self.domain, self._schemas(), self.name)
            if requires is not None:
                given = requires
                requires = lambda data, state: given(cls.from_wire(data), state)  # noqa: E731

            def wrap(f: Callable) -> Callable:
                return lambda state, data, meta=None, ctx=None: self._plain(_call(f, state, cls.from_wire(data), meta, ctx))

            if requires is not None:
                self.requirements[name] = requires
            return _register_wrapped(self, self.handlers, name, fn, wrap)
        if command_type.startswith("_reindex_"):
            raise ValueError(f"{command_type}: command types starting with _reindex_ are the built-in commands "
                             "of aggregates with keys or facts; name yours differently")
        if requires is not None:
            self.requirements[command_type] = requires
        return _register(self, self.handlers, command_type, fn)

    def unique(self, name: str, fn: Callable | None = None, *, scope: str = "tenant",
               personal: Callable | None = None):
        """A unique key: `fn(state)` returns its value (a `str`), several values (a list of `str`), or
        `None` for none. After every command the aggregate holds exactly the values of its new
        state, and no two aggregates of this type hold the same value of `name` at the same time:
        a command that would take a value another one holds is rejected with `unique_taken`
        (the message names the key, never the value). Normalise the value in `fn` (e.g.
        `.lower()` for an email); the kernel compares its SHA-256 hash.

        `scope`: `"tenant"` (default) for unique within the command's tenant, `"global"` for
        across tenants. `personal(state)`: the person the value belongs to, for personal data
        such as an email; the kernel keeps the key in the vault, and erasing that subject frees
        it. A personal key whose subject is empty or `None` is not held.

        Example: `account_aggregate.unique("email", lambda s: s.get("email"), scope="global")`.
        Works as a decorator too (`@account_aggregate.unique("email")` over `def _(state):`) and
        returns the decorated function, or the aggregate when `fn` is given, so it chains.

        Keys come from the state, so aggregates that existed before a key was declared hold it
        only after their next command: `Kernable.reindex` runs the built-in command
        `_reindex_<aggregate_type>` (`reindex_type`, e.g. `_reindex_account`; no events) on them.
        Workers of an aggregate with keys or facts run it too, so their key needs `run` for
        `<domain>._reindex_<aggregate_type>` (`<domain>.*` covers it). An aggregate whose value another
        one already holds (a duplicate from before) is rejected with `unique_taken` until its
        value changes or becomes `None`. Raises `ValueError` if the name is declared twice, is
        empty, holds a dot or is longer than 128 bytes, or `scope` is neither of the two."""
        _check_index_name("key", name, self.keys, self)
        if scope not in ("tenant", "global"):
            raise ValueError(f'the scope of a key is "tenant" or "global", not {scope!r}')

        def define(f: Callable) -> None:
            self.keys[name] = (f, scope, personal)
            self.handlers.setdefault(reindex_type(self.name), _no_events)

        return _define(self, define, fn)

    def fact(self, name: str, fn: Callable | None = None):
        """A fact: `fn(state)` returns whether the aggregate holds it. After every command the
        aggregate holds the facts that are true for its new state, and commands of other
        aggregates may require them (`requires=` of `command`), checked atomically with their
        commit (`requirement_missing` if not held). Example: `vehicle_aggregate.fact("open",
        lambda s: s["status"] == "open")`. Works as a decorator too and chains like `unique`;
        aggregates that existed before a fact was declared hold it after `Kernable.reindex`.
        Raises `ValueError` if the name is declared twice, is empty, holds a dot or is longer
        than 128 bytes."""
        _check_index_name("fact", name, self.facts, self)

        def define(f: Callable) -> None:
            self.facts[name] = f
            self.handlers.setdefault(reindex_type(self.name), _no_events)

        return _define(self, define, fn)

    def index(self, state: Any) -> Index:
        """The unique keys and facts this aggregate holds for `state`, as the worker computes them
        after a command; for unit tests next to `handle`: `account_aggregate.index(state).keys`.
        Raises the exception of a key's or fact's function, `TypeError` if a key's value is not a
        string."""
        keys: list[HeldKey] = []
        for name, (fn, scope, personal) in self.keys.items():
            values = fn(state)
            if values is None:
                continue
            for value in values if isinstance(values, (list, tuple)) else [values]:
                if not isinstance(value, str):
                    raise TypeError(f"the key {name} of {self.domain}.{self.name} is a string, not {type(value).__name__}")
                subject = ""
                if personal is not None:
                    subject = personal(state) or ""
                    if not isinstance(subject, str):
                        raise TypeError(f"the subject of the key {name} is a string, not {type(subject).__name__}")
                    if not subject:
                        continue
                held = HeldKey(name, value, scope, subject)
                if held not in keys:
                    keys.append(held)
        return Index(keys, [name for name, fn in self.facts.items() if fn(state)])

    def schema_of(self, event_type: str) -> int:
        """The payload version this aggregate writes `event_type` events at."""
        spec = self.events.get(event_type)
        return spec.schema if spec else 1

    def fold(self, history: list[tuple], state: Any = _INITIAL) -> Any:
        """State after `history`, a list of tuples `(type, schema, data)`, optionally followed by
        an `EventMeta` and an `ApplyContext`, starting from `state` (default: `initial`). Events
        are upcast first; events without an `apply` are skipped. For unit tests without a kernel:
        `cart_aggregate.fold([("ItemAdded", 1, {"qty": 2})])`. Raises the handler's exception,
        or an error if an event's schema is newer than this process knows."""
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

    def handle(self, state: Any, command_type: str, data: Any, ctx: CommandContext, *, aggregate_id: str = "", tenant: str = "") -> list[dict]:
        """Runs the `command_type` handler on `state`, e.g. in a unit test, and returns its events.
        `data` reaches it as from a client (after a JSON round trip; `None` as `{}`), `state` as a
        copy. The handler's `CommandMeta` has the command ID of `ctx` (see `CommandContext.new`),
        this aggregate's domain and name, and an empty aggregate ID and tenant: `cart_aggregate.handle(
        state, "AddItem", {"qty": 2}, CommandContext.new("add-1"))`. Raises the handler's `Rejected`
        (or other exception), `ValueError` if there is no handler for `command_type`."""
        fn = self.handlers.get(command_type)
        if fn is None:
            raise ValueError(f"no handler for {command_type} on {self.domain}.{self.name}")
        meta = CommandMeta(ctx._command_id, command_type, self.domain, self.name, aggregate_id, tenant)
        return _call(fn, copy.deepcopy(state), _decode(_encode(data)), meta, ctx)


def _no_events(state, data) -> list[dict]:
    """The built-in `_reindex_<aggregate_type>` command: no events, so the worker sends the keys and facts of the
    current state."""
    return []


def _check_index_name(kind: str, name: str, declared: dict, agg: AggregateType) -> None:
    if not isinstance(name, str) or not name or "." in name or len(name.encode()) > 128:
        raise ValueError(f"a {kind} name is not empty, has no dots and at most 128 bytes, not {name!r}")
    if name in declared:
        raise ValueError(f"{agg.domain}.{agg.name} has a {kind} {name} already")


def _define(owner, define: Callable, fn: Callable | None):
    """Defines `fn` with `define` and returns `owner`, or, without `fn`, a decorator that does."""
    if fn is not None:
        define(fn)
        return owner

    def decorator(f: Callable) -> Callable:
        define(f)
        return f

    return decorator


def _personal(vault: _VaultFields) -> set[str]:
    return {f for fields, _ in vault.personal for f in fields}


def _check_personal(agg: AggregateType, cmd, produced: list[dict]) -> None:
    """Rejects (`personal_in_clear`) an event that holds a personal field of its command, under the
    same name, but does not declare it personal: the value would go into the log in clear, for good."""
    spec = agg.commands.get(cmd.command_type)
    personal = _personal(spec.vault) if spec else set()
    if not personal:
        return
    for e in produced:
        data, event = e.get("data"), agg.events.get(e["type"])
        if not isinstance(data, dict):
            continue
        declared = _personal(event.vault) if event else set()
        for f in sorted(personal - declared):
            if data.get(f) is not None:
                raise Rejected(f"{cmd.command_type} declares {f} personal, but {e['type']} does not: it would be "
                               f"stored in clear (declare it with Domain.event({e['type']!r}, …).personal)",
                               "personal_in_clear")


class Aggregate:
    """An aggregate defined as a class: its fields are the state, and the class is registered as an
    aggregate of `domain` (its name: the class name in lowercase, or `name`). The state is a
    dataclass (no `@dataclass`); mutable defaults (`[]`, `{}`) are copied for every aggregate, and
    snapshots hold its fields as JSON. Handlers get and change an instance: `s.status = "active"`.

        class Cart(Aggregate, domain=shop_domain):
            items: int = 0

        @Cart.apply(ItemAdded)
        def _(s: Cart, d: ItemAdded):
            s.items += d.count

    `Cart.command`, `Cart.unique`, `Cart.fact`, `Cart.fold`, `Cart.handle` and `Cart.index` work
    as on `AggregateType` (what `domain.aggregate` returns, which keeps working with a dict)."""

    __aggregate__: "ClassVar[AggregateType]"

    def __init_subclass__(cls, *, domain: "Domain | None" = None, name: str | None = None, snapshot_every: int = 100,
                          schema: str = "1", **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if domain is None:
            return
        for field_name in list(cls.__dict__.get("__annotations__", {})):
            default = cls.__dict__.get(field_name)
            if isinstance(default, (list, dict, set)):
                setattr(cls, field_name, dataclasses.field(default_factory=lambda v=default: copy.deepcopy(v)))
        dataclasses.dataclass(cls)
        definition = domain.aggregate(name or cls.__name__.lower(), cls(), snapshot_every, schema)
        definition._to_json = lambda state: dataclasses.asdict(state) if isinstance(state, cls) else state
        names = {f.name for f in dataclasses.fields(cls)}
        definition._from_json = lambda data: cls(**{k: v for k, v in data.items() if k in names}) if isinstance(data, dict) else data
        cls.__aggregate__ = definition

    def update(self, **values: Any) -> None:
        """Sets several fields at once: `s.update(status="active", slug=d.slug)`."""
        for key, value in values.items():
            setattr(self, key, value)

    @classmethod
    def apply(cls, event_type, fn: Callable | None = None):
        return cls.__aggregate__.apply(event_type, fn)

    @classmethod
    def command(cls, command_type, fn: Callable | None = None, *, requires: Callable | None = None):
        return cls.__aggregate__.command(command_type, fn, requires=requires)

    @classmethod
    def unique(cls, name: str, fn: Callable | None = None, *, scope: str = "tenant", personal: Callable | None = None):
        return cls.__aggregate__.unique(name, fn, scope=scope, personal=personal)

    @classmethod
    def fact(cls, name: str, fn: Callable | None = None):
        return cls.__aggregate__.fact(name, fn)

    @classmethod
    def fold(cls, history: list[tuple], state: Any = _INITIAL) -> Any:
        return cls.__aggregate__.fold(history, state)

    @classmethod
    def handle(cls, state: Any, command_type: str, data: Any, ctx: "CommandContext", *, aggregate_id: str = "", tenant: str = "") -> list[dict]:
        return cls.__aggregate__.handle(state, command_type, data, ctx, aggregate_id=aggregate_id, tenant=tenant)

    @classmethod
    def index(cls, state: Any) -> "Index":
        return cls.__aggregate__.index(state)


def _register_wrapped(owner, table: dict, name: str, fn: Callable | None, wrap: Callable[[Callable], Callable]):
    """`_register` for a message class: the handler is stored wrapped, the decorator returns it unwrapped."""
    if fn is not None:
        table[name] = wrap(fn)
        return owner

    def decorator(f: Callable) -> Callable:
        table[name] = wrap(f)
        return f

    return decorator


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


class _Open(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
            grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):
    """Refuses every call once the client is closed: grpcio crashes the process on a call over a
    closed channel."""

    def __init__(self):
        self.closed = False

    def _check(self):
        if self.closed:
            raise ClientClosed()

    def intercept_unary_unary(self, continuation, details, request):
        self._check()
        return continuation(details, request)

    def intercept_unary_stream(self, continuation, details, request):
        self._check()
        return continuation(details, request)

    def intercept_stream_unary(self, continuation, details, requests):
        self._check()
        return continuation(details, requests)

    def intercept_stream_stream(self, continuation, details, requests):
        self._check()
        return continuation(details, requests)


class _Deadline(grpc.UnaryUnaryClientInterceptor):
    """Lets every request wait for the connection (e.g. while the kernel restarts) and gives it a
    deadline, unless the call has its own."""

    def __init__(self, timeout_s: float):
        self._timeout_s = timeout_s

    def intercept_unary_unary(self, continuation, details, request):
        timeout = self._timeout_s if details.timeout is None else details.timeout
        return continuation(details._replace(timeout=timeout, wait_for_ready=True), request)


class _Failover(grpc.Channel):
    """The channels to the kernels of a pair (`url` with several addresses): every call goes to
    the current one; on `UNAVAILABLE` (a standby answers `not_primary`, a kernel is gone or
    shutting down) the next one becomes current. A request tries the next address within its
    timeout, waiting at most a second for each to connect; a stream that ends with
    `UNAVAILABLE` reconnects to the next."""

    _PER_ADDRESS_S = 1.0

    def __init__(self, channels: list, raw: list, timeout_s: float):
        self._channels, self._raw, self._timeout_s = channels, raw, timeout_s
        self._current = 0
        self._lock = threading.Lock()

    def current(self) -> int:
        return self._current

    def next(self, from_: int) -> None:
        with self._lock:
            if self._current == from_:
                self._current = (from_ + 1) % len(self._channels)

    def _unary(self, kind: str, method: str, *args, **kwargs):
        calls = [getattr(c, kind)(method, *args, **kwargs) for c in self._channels]
        failover = self

        def attempt(how: str, request, timeout, kw):
            deadline = time.monotonic() + (failover._timeout_s if timeout is None else timeout)
            while True:
                at = failover._current
                left = deadline - time.monotonic()
                try:
                    grpc.channel_ready_future(failover._raw[at]).result(
                        timeout=max(0.0, min(left, failover._PER_ADDRESS_S)))
                except grpc.FutureTimeoutError:
                    if time.monotonic() < deadline:
                        failover.next(at)
                        continue
                try:
                    return getattr(calls[at], how)(request, timeout=max(0.001, deadline - time.monotonic()), **kw)
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.UNAVAILABLE or time.monotonic() >= deadline:
                        raise
                    failover.next(at)

        class _Call:
            def __call__(self, request, timeout=None, **kw):
                return attempt("__call__", request, timeout, kw)

            def with_call(self, request, timeout=None, **kw):
                return attempt("with_call", request, timeout, kw)

            def future(self, request, timeout=None, **kw):
                return calls[failover._current].future(request, timeout=timeout, **kw)

        return _Call()

    def _stream(self, kind: str, method: str, *args, **kwargs):
        calls = [getattr(c, kind)(method, *args, **kwargs) for c in self._channels]
        failover = self

        class _Call:
            def __init__(self, at, inner):
                self._at, self._inner = at, inner

            def __iter__(self):
                return self

            def __next__(self):
                try:
                    return next(self._inner)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        failover.next(self._at)
                    raise

            def __getattr__(self, name):
                return getattr(self._inner, name)

        def call(request, *a, **kw):
            at = failover._current
            return _Call(at, calls[at](request, *a, **kw))

        return call

    def unary_unary(self, method, *args, **kwargs):
        return self._unary("unary_unary", method, *args, **kwargs)

    def stream_unary(self, method, *args, **kwargs):
        return self._unary("stream_unary", method, *args, **kwargs)

    def unary_stream(self, method, *args, **kwargs):
        return self._stream("unary_stream", method, *args, **kwargs)

    def stream_stream(self, method, *args, **kwargs):
        return self._stream("stream_stream", method, *args, **kwargs)

    def subscribe(self, callback, try_to_connect=False):
        self._raw[self._current].subscribe(callback, try_to_connect)

    def unsubscribe(self, callback):
        for c in self._raw:
            c.unsubscribe(callback)

    def close(self):
        for c in self._channels:
            c.close()


def _local(url: str) -> bool:
    """Whether `url` (every address of a comma-separated list) is a Unix socket or a loopback
    address: plain text never leaves the machine."""
    if "," in url:
        return all(_local(u.strip()) for u in url.split(","))
    if url.startswith("unix:"):
        return True
    host = re.sub(r":\d+/?$", "", re.sub(r"^https?://", "", url)).strip("[]")
    return host in ("localhost", "::1") or re.fullmatch(r"127\.\d+\.\d+\.\d+", host) is not None


def _moving(err: grpc.RpcError) -> bool:
    """Whether a kernel refused a request because its tenant is moving to another partition."""
    return err.code() == grpc.StatusCode.UNAVAILABLE and (err.details() or "").startswith("moving")


# How long a request waits for its tenant's move to end.
_MOVE_WAIT_S = 30.0


def _partition_of(m, tenant: str) -> str:
    """The partition map `m` assigns `tenant` to."""
    return next((t.partition for t in m.tenants if t.tenant == tenant), m.default)


def _wrong_partition(err: grpc.RpcError) -> bool:
    """Whether a kernel refused a request as one for a tenant of another partition."""
    return err.code() == grpc.StatusCode.FAILED_PRECONDITION and (err.details() or "").startswith("wrong_partition")


class _Partitioned:
    """The channels of every partition, found by the map the first kernel holds: a request
    goes to its tenant's partition; refused there as `wrong_partition` (the map changed), the
    map is read again and the request sent anew. A kernel without partitions holds no map:
    everything goes to the first kernel, as before."""

    _UNREAD = object()

    def __init__(self, entry: grpc.Channel, channel_for: Callable[[str], grpc.Channel]):
        self.entry = entry
        self._channel_for = channel_for
        self._map: Any = self._UNREAD
        self._parts: dict[str, grpc.Channel] = {}
        self._lock = threading.Lock()

    def current(self, refresh: bool = False, timeout_s: float | None = None):
        """The map (a `PartitionMap`), read once and again after `refresh`; `None` without
        partitions. `timeout_s`: how long to wait for the kernel (default: the client's)."""
        with self._lock:
            if refresh:
                self._map = self._UNREAD
            if self._map is self._UNREAD:
                try:
                    self._map = rpc.PartitionServiceStub(self.entry).Map(pb.MapRequest(protocol=PROTOCOL),
                                                                         timeout=timeout_s)
                except grpc.RpcError as e:
                    if e.code() not in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.UNIMPLEMENTED):
                        raise
                    self._map = None
            return self._map

    def _channel(self, m, name: str) -> grpc.Channel:
        partition = next((p for p in m.partitions if p.name == name), None)
        if partition is None:
            raise ValueError(f"the partition map names no partition {name}")
        addresses = ",".join(partition.addresses)
        with self._lock:
            if addresses not in self._parts:
                self._parts[addresses] = self._channel_for(addresses)
            return self._parts[addresses]

    def for_tenant(self, tenant: str) -> grpc.Channel:
        m = self.current()
        if m is None:
            return self.entry
        return self._channel(m, _partition_of(m, tenant))

    def control(self) -> grpc.Channel:
        """The control partition's channel, which takes memberships."""
        m = self.current()
        return self.entry if m is None else self._channel(m, m.control)

    def all(self, timeout_s: float | None = None) -> "list[tuple[str, grpc.Channel]]":
        """Every partition with its channel; one, named `""`, without partitions."""
        m = self.current(timeout_s=timeout_s)
        if m is None:
            return [("", self.entry)]
        return [(p.name, self._channel(m, p.name)) for p in m.partitions]

    def route(self, tenant: str, call: Callable[[grpc.Channel], Any]) -> Any:
        """`call` on `tenant`'s partition, sent anew after reading the map again on
        `wrong_partition`, and while the tenant moves (`moving`, refused before it took effect)
        for up to 30 s."""
        deadline = time.monotonic() + _MOVE_WAIT_S
        pause = 0.05
        attempt = 0
        while True:
            try:
                return call(self.for_tenant(tenant))
            except grpc.RpcError as e:
                if _moving(e) and time.monotonic() + pause < deadline:
                    time.sleep(pause)
                    pause = min(pause * 2, 1.0)
                    continue
                if attempt == 3 or not _wrong_partition(e):
                    raise
                attempt += 1
                self.current(refresh=True)

    def close(self) -> None:
        self.entry.close()
        for c in self._parts.values():
            c.close()


class Kernable:
    """A connection to the kernel: submits commands (`submit`, `enqueue`), asks queries
    (`query`), runs workers, listeners and apps, and applies memberships. All calls block the
    calling thread; one client may be shared by threads.

    `k.data` (a `DataClient`) holds files of a domain, `k.cache` (a `CacheClient`) its shared
    cache. `Kernable()` without arguments takes everything from the environment or the
    defaults, which fit a kernel started with `kernable start --dev` in the current directory."""

    def __init__(self, url: str | None = None, key: str | None = None, tenant: str = "default",
                 tls: Tls | None = None, context: RequestContext | None = None,
                 grant: str | None = None, service: str | None = None, insecure: bool | None = None,
                 timeout_s: float = DEFAULT_TIMEOUT_S):
        """A client for the kernel at `url`, signing with the key in the file `key`. It
        connects on first use and again after the connection drops.

        `url`: `http://host:port` (`host:port` works too) or, on Unix, `unix:/path/to.sock`;
        for a pair of kernels with a standby both, comma-separated (`http://a:7233,http://b:7233`):
        the client turns to the other one when a kernel answers `UNAVAILABLE` (a standby says
        `not_primary`) or cannot be reached. Default `KERNABLE_URL`, else
        `http://127.0.0.1:7233`, where `kernable start` listens.
        `key`: path to this process's `.key` file; default `KERNABLE_KEY`, else `app.key` in the
        current directory. `tenant`: default tenant of submits, queries, files and the cache
        (default `default`). `tls`: mutual TLS for `host:port`; default `KERNABLE_TLS_CA`,
        `KERNABLE_TLS_CERT` and `KERNABLE_TLS_KEY`, if set (all three or none). `context`:
        default request context for submits, queries, files and the cache, e.g. this service's
        identity, replaced by a request's own. `grant`: path to the key's grant (`kernable grant`), sent with every
        call; default `KERNABLE_GRANT`, else the `.grant` file next to the key file, if there is
        one. `service`: the service this process's workers and listeners belong to. The
        service's membership (see `apply`) decides what the kernel waits for: what it lists
        counts, the service's other workers and listeners are dynamic, and only listeners it lists
        as middlewares may check requests. Default `KERNABLE_SERVICE`, else none (everything
        registered counts). `insecure`: allows plain text to a kernel that is not on this machine;
        without TLS, grants, signed requests and vault values would cross the network readable,
        so such a URL raises `ValueError` unless this is set; default `KERNABLE_INSECURE=1`, else
        False. `timeout_s`: how long a request (submit, query, files, cache, apply) waits in all,
        for the connection (e.g. while the kernel restarts) and its answer; default 60 s. A
        connection the network dropped silently is noticed within 40 s by keepalive pings.

        Workers and listeners print connection lines to stderr up to the level `KERNABLE_LOG`
        names (`off`, `error`, `warn`, `info` (default), `debug`). Raises `OSError` if the key
        or grant file cannot be read, `ValueError` if only some of the TLS variables are set."""
        key = key or os.environ.get("KERNABLE_KEY") or "app.key"
        tls = tls or _tls_from_env()
        self._context = context or RequestContext()
        url = url or os.environ.get("KERNABLE_URL") or "http://127.0.0.1:7233"
        given_url = url
        url = re.sub(r"^https?://", "", url).rstrip("/")
        if insecure is None:
            insecure = os.environ.get("KERNABLE_INSECURE") == "1"
        if not tls and not insecure and not _local(url):
            raise ValueError(f"{given_url} is not on this machine: connect with TLS (tls=...), or pass "
                             "insecure=True to send in plain text")
        def open_channel(url: str):
            if tls:
                def read(path: str) -> bytes:
                    with open(path, "rb") as f:
                        return f.read()

                creds = grpc.ssl_channel_credentials(read(tls.ca), read(tls.key), read(tls.cert))
                return grpc.secure_channel(url, creds, options=_CHANNEL_OPTIONS)
            # grpcio sends the socket path as :authority for unix: URLs, which the kernel's
            # HTTP/2 server rejects; any valid authority works.
            return grpc.insecure_channel(url, options=[("grpc.default_authority", "localhost"),
                                                       *_CHANNEL_OPTIONS])

        grant = grant or os.environ.get("KERNABLE_GRANT")
        if grant is None and os.path.exists(default := key.removesuffix(".key") + ".grant"):
            grant = default
        granted = None
        if grant is not None:
            with open(grant) as f:
                granted = f.read().strip()
        # The outermost interceptor of every channel: checked before anything touches it.
        self._open = _Open()

        def channel_for(addresses: str) -> grpc.Channel:
            """A channel to the kernels at `addresses`, comma-separated (a pair: see `_Failover`)."""
            urls = [re.sub(r"^https?://", "", u.strip()).rstrip("/") for u in addresses.split(",") if u.strip()]
            raw = [open_channel(u) for u in urls]
            channels = []
            for c in raw:
                if granted is not None:
                    c = grpc.intercept_channel(c, _WithGrant(granted))
                channels.append(grpc.intercept_channel(c, _Deadline(timeout_s)))
            channel = channels[0] if len(channels) == 1 else _Failover(channels, raw, timeout_s)
            return grpc.intercept_channel(channel, self._open)

        self._channel = channel_for(url)
        # With partitions, each tenant's requests go to its partition.
        self._parts = _Partitioned(self._channel, channel_for)
        self._key = Key.from_file(key)
        self._log = _Log(given_url, key, self._key.id)
        # Whoever can read the key file can sign as this process.
        mode = os.stat(key).st_mode & 0o777
        if os.name == "posix" and mode & 0o077:
            self._log.print("warn", "key", f"{key} can be read by other users (mode {mode:o}): chmod 600 {key}")
        self._tenant = tenant
        self._service = service or os.environ.get("KERNABLE_SERVICE") or ""
        self._domains: dict[str, Domain] = {}
        #: Files of a domain: `k.data.put(domain, key, data)`, `get`, `delete`.
        self.data = DataClient(self)
        #: A domain's shared cache: `k.cache.set(domain, key, value, ttl_ms)`, `get`, `delete`.
        self.cache = CacheClient(self)

    def use(self, *domains: Domain) -> "Kernable":
        """Seals the vault fields of these domains' commands when submitting them (see
        `Domain.command`), and sends the targets of their queries given by name (see
        `Domain.query`). Returns this client, so it chains: `k = Kernable().use(shop_domain)`."""
        for d in domains:
            self._domains[d.name] = d
        return self

    def worker(self, *aggregates: "AggregateType | type[Aggregate]") -> "Worker":
        """A worker that runs these aggregates' commands, in this client's service; see
        `Worker`. `run()` on it blocks until `stop()`: `k.worker(cart_aggregate).run()`."""
        aggregates = tuple(getattr(a, "__aggregate__", a) for a in aggregates)
        return Worker(self._parts, self._key, aggregates, self._log, self._service)

    def app(self, *, domains: Iterable[Domain], service: str | None = None, version: str | None = None,
            description: str | None = None, metadata: dict[str, str] | None = None) -> "App":
        """An app made of whole domains: every aggregate, read model and reactor defined on them.
        `run()` on it applies the service's membership (the domains' aggregates as workers, their
        read models and reactors as listeners, those with a middleware as middlewares; live ones
        are not listed), then runs all of them. `service` defaults to this client's; `version`
        and `description` go into the membership's metadata, followed by `metadata`.

        Example: `k.app(service="shop", version="1.0.0", domains=[shop_domain]).run()`. Every
        start applies the membership, so the key needs the `manage` grant for the service. One
        service is one app: two processes with the same service would replace each other's
        membership. Raises `ValueError` if neither `service` nor the client names a service."""
        service = service or self._service
        if not service:
            raise ValueError("an app needs a service: pass `service` or set it on the client (KERNABLE_SERVICE)")
        domains = list(domains)
        aggregates = [a for d in domains for a in d.aggregates]
        listeners = [l for d in domains for l in d.listeners]
        durable = [l for l in listeners if not l.live]
        meta = {k: v for k, v in (("version", version), ("description", description)) if v is not None}
        manifest = Manifest(service=service, workers=aggregates,
                            listeners=[l for l in durable if not l._middlewares],
                            middlewares=[l for l in durable if l._middlewares],
                            metadata={**meta, **(metadata or {})})
        worker = Worker(self._parts, self._key, aggregates, self._log, service) if aggregates else None
        handles = [ListenerHandle(self._parts, self._key, l, self._log, service, self) for l in listeners]
        return App(self, manifest, worker, handles, [d.name for d in domains])

    def apply(self, *, service: str | None = None, workers: Iterable[AggregateType] = (),
              listeners: Iterable["Listener"] = (), middlewares: Iterable["Listener"] = (),
              metadata: dict[str, str] | None = None) -> None:
        """Replaces the membership of a service (default: this client's `service`): the workers,
        listeners and middlewares the kernel waits for, plus free metadata such as
        `{"version": "1.0.0"}`. `workers` are the aggregates the service runs (the kernel waits
        for a worker of each), `listeners` the listeners it waits for (the service's others count
        for nobody), `middlewares` the service's listeners that may check requests (they count like
        listeners).

        Apply it once per deploy, e.g. from a deploy script, not from every process: the newest
        apply wins, and an older one that arrives late is refused. The key needs the `manage` grant for the service. A middleware the new
        membership no longer lists stops checking. Example: `k.apply(service="shop",
        workers=[cart_aggregate], listeners=[carts_readmodel], metadata={"version": "1.0.0"})`.
        Raises `ValueError` without a service, `grpc.RpcError` if the kernel refuses it."""
        service = service or self._service
        if not service:
            raise ValueError("apply needs a service: pass `service` or set it on the client (KERNABLE_SERVICE)")
        membership = pb.Membership(service=service, workers=[f"{a.domain}.{a.name}" for a in workers],
                                   listeners=[l.name for l in listeners],
                                   middlewares=[l.name for l in middlewares],
                                   metadata=[pb.Attribute(key=k, value=v) for k, v in (metadata or {}).items()],
                                   issued_at=_issued_now())
        rpc.MembershipServiceStub(self._parts.control()).Apply(
            pb.ApplyRequest(membership=membership, sig=self._key.sign(membership_message(membership)),
                            protocol=PROTOCOL))

    def _command_of(self, command: Command) -> tuple:
        """Domain, aggregate type, type and payload of a command class instance; its domain's vault fields known."""
        b = _binding(type(command), "handled by an aggregate")
        self._domains.setdefault(b.domain, b.schemas)
        return b.domain, b.aggregate_type, type(command).type_name(), command.to_wire()

    def submit(self, domain, aggregate_type: str | None = None, aggregate_id: str | None = None, type: str | None = None, data: Any = None, *,
               tenant: str | None = None, id: str | None = None, wait: str = "settled",
               context: RequestContext | None = None, timeout_s: float | None = None) -> Outcome:
        """Runs a `type` command of `domain` on the aggregate `aggregate_type` with ID
        `aggregate_id` and returns its `Outcome`. `data` is the command's payload (JSON-like; `None` sends `{}`).

        `wait="settled"` (default) responds once every listener that counts has processed the
        events, so read models already show them, and raises `NotSettled` on timeout; from a
        listener's handler, only the listeners with a lower order count. `wait="committed"`
        responds once the events are stored; then query with `min_position`. `id` is an
        idempotency key (default: a new UUID): submitting the same `id` again returns the
        original outcome. `tenant` replaces the client's tenant. `context` replaces the client's
        default context; inside a listener, pass `ctx.follow()`. `timeout_s` replaces the
        client's deadline for this call.

        A rejected command returns `Outcome(committed=False, code=..., message=...)`; it does not
        raise. Raises `grpc.RpcError` if the kernel refuses the call (no grant, unknown aggregate
        type, timeout). Example: `k.submit("shop", "cart", "c1", "AddItem", {"sku": "apple",
        "qty": 2})`.

        With a command class instance: `k.submit(AddItem(sku="apple", qty=2), aggregate_id="c1")`;
        its class says the domain, aggregate type, type and vault fields (no `use` needed).
        """
        if isinstance(domain, Command):
            domain, aggregate_type, type, data = self._command_of(domain)
        mode = pb.WAIT_COMMITTED if wait == "committed" else pb.WAIT_SETTLED
        res = self._send(domain, aggregate_type, aggregate_id, type, data, tenant, id, mode, context, timeout_s)
        if res.WhichOneof("outcome") == "rejected":
            return _rejected_outcome(res.rejected)
        outcome = Outcome(committed=True, events=[_to_event(e) for e in res.committed.events])
        if mode == pb.WAIT_SETTLED and not res.committed.settled:
            raise NotSettled(outcome)
        return outcome

    def enqueue(self, domain, aggregate_type: str | None = None, aggregate_id: str | None = None, type: str | None = None, data: Any = None, *,
                tenant: str | None = None, id: str | None = None, context: RequestContext | None = None,
                timeout_s: float | None = None) -> str:
        """Stores the command in the kernel's queue and returns its ID without waiting for it to
        run (`wait="accepted"`). Submit it again with the same `id` to get the outcome. The
        parameters are those of `submit`."""
        if isinstance(domain, Command):
            domain, aggregate_type, type, data = self._command_of(domain)
        return self._send(domain, aggregate_type, aggregate_id, type, data, tenant, id, pb.WAIT_ACCEPTED,
                          context, timeout_s).command_id

    def _caught_up_everywhere(self, q: "pb.QueryRequest", timeout_s: float | None) -> None:
        """Returns once a read model that answers `q` has processed every partition's head, as
        of the call: a wait-only query to each partition, at the same time."""
        wait = pb.QueryRequest()
        wait.CopyFrom(q)
        wait.payload, wait.min_position, wait.wait_only, wait.listener = b"", 0, True, ""
        wait.client_sig.CopyFrom(self._key.sign(query_message(wait)))
        wait.listener = q.listener
        parts = self._parts.all(timeout_s)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(parts))) as pool:
            waits = [pool.submit(lambda ch=ch: rpc.QueryServiceStub(ch).Query(wait, timeout=timeout_s))
                     for _, ch in parts]
            for w in waits:
                w.result()

    def query(self, domain, type: str | None = None, data: Any = None, *, tenant: str | None = None,
              context: RequestContext | None = None, min_position: int = 0, timeout_s: float | None = None,
              all_partitions: bool = False) -> Any:
        """Asks the read model that answers `domain.type` with `data` and returns its answer,
        decoded from JSON. Queries always go through the kernel, also within one process.
        `min_position`: answer only after the read model has processed this global position
        (e.g. of an event from a `wait="committed"` submit). `tenant` and `context` replace the
        client's; `timeout_s` its deadline for this call. `all_partitions`, for a read model
        across tenants with partitions: answer only once the read model has processed every
        partition's events up to the query, not only those of the tenant's partition (a
        wait-only query to each partition first).

        Raises `Rejected` if the read model or a middleware rejects it, `grpc.RpcError` if the
        kernel refuses the call (e.g. `UNAVAILABLE` when no read model answers it). Example:
        `cart = k.query("shop", "GetCart", {"id": "c1"})`."""
        if isinstance(domain, Query):
            asked = domain
            kind = asked.__class__
            b = _binding(kind, "answered by a read model")
            return _from_wire(self._ask(b.domain, kind.type_name(), asked.to_wire(), _query_target(kind), tenant,
                                        context, min_position, timeout_s, all_partitions))
        spec = self._domains.get(domain)
        declared = spec.queries.get(type) if spec else None
        return self._ask(domain, type, data, declared.target_of if declared else None, tenant, context,
                         min_position, timeout_s, all_partitions)

    def _ask(self, domain: str, type: str, data: Any, declared: tuple[str, str] | None, tenant: str | None,
             context: RequestContext | None, min_position: int, timeout_s: float | None, all_partitions: bool) -> Any:
        """`query`, with the target the query type declares: `(aggregate_type, field)`."""
        query_id = str(uuid.uuid4())
        q = pb.QueryRequest(query_id=query_id, tenant_id=tenant or self._tenant, domain=domain, query_type=type,
                            payload=_encode(data), context=_to_wire_context(context or self._context, query_id),
                            min_position=min_position, issued_at=_issued_now(), protocol=PROTOCOL,
                            listener=_HANDLING.get(), target=_query_target_of(declared, domain, data))
        if all_partitions:
            self._caught_up_everywhere(q, timeout_s)
            q.issued_at = _issued_now()
        q.client_sig.CopyFrom(self._key.sign(query_message(q)))
        res = self._parts.route(q.tenant_id, lambda ch: rpc.QueryServiceStub(ch).Query(q, timeout=timeout_s))
        if res.WhichOneof("outcome") == "rejected":
            raise Rejected(res.rejected.message, res.rejected.code)
        return _decode(res.response)

    def reindex(self, domain: str, aggregate_type: str, aggregate_ids: Iterable[str], *,
                tenant: str | None = None, context: RequestContext | None = None) -> dict[str, Outcome]:
        """Runs the built-in command `_reindex_<aggregate_type>` on these aggregates, one after
        the other, so they hold the unique keys and facts of their current state, e.g. after
        declaring a key (`Aggregate.unique`) for aggregates that existed before. It produces no events. Returns
        the `Outcome` per ID: committed, or rejected, e.g. with `unique_taken` for an aggregate
        whose value another one holds already. An ID without events holds nothing. `tenant` and
        `context` replace the client's. Raises `grpc.RpcError` if the kernel refuses a call
        (e.g. the aggregate's worker declares no keys or facts). Example:
        `k.reindex("identity", "account", ["a1", "a2"])`."""
        command_type = reindex_type(aggregate_type)
        return {id: self.submit(domain, aggregate_type, id, command_type, None, tenant=tenant, wait="committed",
                                context=context) for id in aggregate_ids}

    def listener(self, listener: "Listener") -> "ListenerHandle":
        """A handle that runs a listener defined with `Domain.read_model` or `Domain.reactor`, in
        this client's service; see `ListenerHandle`. `run()` on it blocks until `stop()`:
        `k.listener(carts_readmodel).run()`. Each call gives a new handle."""
        return ListenerHandle(self._parts, self._key, listener, self._log, self._service, self)

    def close(self) -> None:
        """Closes the connection now; only needed in a long-running process that discards the
        client. Requests afterwards raise `ClientClosed`, and its workers and listeners end."""
        self._open.closed = True
        self._parts.close()

    def _send(self, domain, aggregate_type, aggregate_id, command_type, data, tenant, id, wait, context, timeout_s):
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
        return self._parts.route(cmd.tenant_id,
                                 lambda ch: rpc.CommandServiceStub(ch).Submit(request, timeout=timeout_s))


class DataClient:
    """Files of a domain, kept by the kernel under (tenant, domain, key): invoices, uploads,
    exports. They are not part of the signed event log and not erased with the vault; keep
    personal data in vault fields instead. Needs a grant of `data_read` (get) or `data_write`
    (put, delete) for the domain; a refused or invalid request raises `grpc.RpcError`. Each
    request runs the middleware chain first, as `<domain>.PutData`, `.GetData` or `.DeleteData`
    (the middleware sees the key and the request context, never the file); a middleware's
    refusal raises `grpc.RpcError` with code `PERMISSION_DENIED` and details `"<code>:
    <message>"`. Keys are 1 to 512 bytes, files at most 16 MiB (the kernel's `--max-data-bytes`).
    Use it as `k.data`:
    `k.data.put("shop", "invoices/c1.pdf", pdf_bytes, "application/pdf")`. `tenant=` replaces the
    client's tenant, `context=` the client's default request context."""

    def __init__(self, k: Kernable):
        """Use `k.data` of a `Kernable`."""
        self._k = k
        self._stub = rpc.DataServiceStub

    def put(self, domain: str, key: str, data: bytes | str, content_type: str | None = None, *,
            tenant: str | None = None, context: RequestContext | None = None) -> str:
        """Stores `data` under `key`, replacing a file stored there. A `str` is stored as UTF-8;
        `content_type` defaults to `text/plain; charset=utf-8` for a `str` and
        `application/octet-stream` for `bytes`. Returns the SHA-256 of the data (hex) as the
        kernel computed it: for an event to name."""
        if isinstance(data, str):
            data, content_type = data.encode(), content_type or "text/plain; charset=utf-8"
        return self._call("put", "PutData", tenant, context, domain, key, data=data,
                          content_type=content_type or "application/octet-stream").sha256.hex()

    def get(self, domain: str, key: str, *, tenant: str | None = None,
            context: RequestContext | None = None) -> Data | None:
        """The file stored under `key`, or `None` if there is none."""
        res = self._call("get", "GetData", tenant, context, domain, key)
        return Data(res.data, res.content_type, res.sha256.hex()) if res.found else None

    def list(self, domain: str, prefix: str = "", *, after: str = "", tenant: str | None = None,
             context: RequestContext | None = None) -> Listing:
        """The files whose keys start with `prefix` (granted `data_read`), in the order of their
        keys' bytes, up to 1,000 after the key `after`; `next` of the result, if set, is the
        `after` of the next page. To find files no event names, or what a tenant stores."""
        res = self._call("list", "ListData", tenant, context, domain, prefix, after=after)
        return Listing([Listed(f.key, f.content_type, f.size) for f in res.files], res.next or None)

    def delete(self, domain: str, key: str, *, tenant: str | None = None,
               context: RequestContext | None = None) -> bool:
        """Deletes the file stored under `key`; returns whether there was one."""
        return self._call("delete", "DeleteData", tenant, context, domain, key).found

    def _call(self, op, method, tenant, context, domain, key, **fields):
        r = pb.DataRequest(tenant_id=tenant or self._k._tenant, domain=domain, key=key,
                           issued_at=_issued_now(), protocol=PROTOCOL,
                           context=_to_wire_context(context or self._k._context, ""), **fields)
        r.client_sig.CopyFrom(self._k._key.sign(data_message(op, r)))
        return self._k._parts.route(r.tenant_id, lambda ch: getattr(self._stub(ch), method)(r))


class CacheClient:
    """A domain's shared cache, kept in the kernel's memory under (tenant, domain, key): rate
    limits, short-lived tokens, computed values. Not durable: a kernel restart empties it. Values
    are not part of the signed event log and not erased with the vault. Needs a grant of `cache`
    for the domain; a refused or invalid request raises `grpc.RpcError`. Each request runs the
    middleware chain first, as `<domain>.SetCache`, `.GetCache` or `.DeleteCache` (the middleware
    sees the key and the request context, never the value); a middleware's refusal raises
    `grpc.RpcError` with code `PERMISSION_DENIED` and details `"<code>: <message>"`. Keys are 1 to
    512 bytes, values at most 1 MiB. Use it as `k.cache`: `k.cache.set("shop", "rate:c1", "3",
    ttl_ms=60_000)`, then `k.cache.get("shop", "rate:c1")` returns `b"3"`. `tenant=` replaces the
    client's tenant, `context=` the client's default request context."""

    def __init__(self, k: Kernable):
        """Use `k.cache` of a `Kernable`."""
        self._k = k
        self._stub = rpc.CacheServiceStub

    def set(self, domain: str, key: str, value: bytes | str, ttl_ms: int = 0, *,
            tenant: str | None = None, context: RequestContext | None = None) -> None:
        """Stores `value` under `key` for `ttl_ms` milliseconds; `0` keeps it until it is deleted
        or the kernel restarts. A `str` is stored as UTF-8."""
        value = value.encode() if isinstance(value, str) else value
        self._call("set", "SetCache", tenant, context, domain, key, value=value, ttl_ms=ttl_ms)

    def get(self, domain: str, key: str, *, tenant: str | None = None,
            context: RequestContext | None = None) -> bytes | None:
        """The live value under `key` as `bytes` (decode a `str` with `.decode()`), or `None` if
        there is none or it expired."""
        res = self._call("get", "GetCache", tenant, context, domain, key)
        return res.value if res.found else None

    def delete(self, domain: str, key: str, *, tenant: str | None = None,
               context: RequestContext | None = None) -> bool:
        """Deletes the value under `key`; returns whether it had a live one."""
        return self._call("delete", "DeleteCache", tenant, context, domain, key).found

    def _call(self, op, method, tenant, context, domain, key, **fields):
        r = pb.CacheRequest(tenant_id=tenant or self._k._tenant, domain=domain, key=key,
                            issued_at=_issued_now(), protocol=PROTOCOL,
                            context=_to_wire_context(context or self._k._context, ""), **fields)
        r.client_sig.CopyFrom(self._k._key.sign(cache_message(op, r)))
        return self._k._parts.route(r.tenant_id, lambda ch: getattr(self._stub(ch), method)(r))


#: Levels of `KERNABLE_LOG`; a line is printed if its level is at most the configured one.
_LOG_LEVELS = {"off": 0, "error": 1, "warn": 2, "info": 3, "debug": 4}


class _Log:
    """Prints a worker's or listener's connection and activity lines to stderr, up to the level
    of `KERNABLE_LOG` (`off`, `error`, `warn`, `info` (default), `debug`) when the client was
    made."""

    def __init__(self, url: str, key_path: str, key_id: str):
        self.level = _LOG_LEVELS.get(os.environ.get("KERNABLE_LOG", ""), _LOG_LEVELS["info"])
        #: `<url> · key <name> (<key id>…)`: the kernel and the key this process connects with.
        self.peer = f"{url} · key {os.path.basename(key_path).removesuffix('.key')} ({key_id[:12]}…)"

    def enabled(self, level: str) -> bool:
        return _LOG_LEVELS[level] <= self.level

    def style(self, code: int, text: str) -> str:
        """`text` in an ANSI style (1 bold, 2 dim, 31 red, 33 yellow) on a terminal, unless
        `NO_COLOR` is set."""
        # Checked per line: stderr may be redirected after the client was made.
        color = sys.stderr.isatty() and not os.environ.get("NO_COLOR")
        return f"\x1b[{code}m{text}\x1b[0m" if color else text

    def print(self, level: str, what: str, line: str) -> None:
        """`12:04:11  listener carts  <line>`, warnings in yellow and errors in red."""
        if not self.enabled(level):
            return
        text = self.style(31, line) if level == "error" else self.style(33, line) if level == "warn" else line
        print(f"{self.style(2, time.strftime('%H:%M:%S'))}  {self.style(1, what)}  {text}", file=sys.stderr)

    def reconnect(self, what: str, reason: str, seconds: float, attempt: int, refused: bool,
                  stopped: threading.Event) -> None:
        """Logs the end of a session that was not stopped and waits before reconnecting:
        `seconds` after the first failed attempt, twice as long after each further one, at most
        30 s, give or take a fifth, so processes do not reconnect in lockstep after a kernel
        restart. A refusal that will not go away by itself is an error, not a warning. `stopped`
        cuts the wait short."""
        delay = _backoff(seconds, attempt)
        # A kernel of a pair that is not the primary right now may be in a moment.
        if _switching_roles(reason):
            delay = min(delay, _PAIR_RETRY_S)
        self.print("error" if refused else "warn", what,
                   f"{'refused' if refused else 'connection lost'} ({reason}), reconnecting in {delay:.2f} s")
        stopped.wait(delay)

    def store_failed(self, what: str, err: BaseException, seconds: float, attempt: int,
                     stopped: threading.Event) -> None:
        """Logs that a store could not be opened and waits before the next try, backing off
        like `reconnect`."""
        delay = _backoff(seconds, attempt)
        self.print("error", what, f"store: {err}; trying again in {delay:.2f} s")
        stopped.wait(delay)


_PAIR_RETRY_S = 1.0
"""The longest wait before trying a kernel of a pair again that was not the primary."""


def _switching_roles(reason: str) -> bool:
    """Whether `reason` says the kernel is one of a pair that is not the primary right now: a
    standby, one that lost its lease or hands its role over."""
    return (reason.startswith("not_primary") or "lost its lease at the witness" in reason
            or "hands its role over" in reason)


def _backoff(seconds: float, attempt: int) -> float:
    """The pause before try `attempt` (from 1): `seconds`, doubled each time up to 30 s, give or
    take a fifth."""
    return min(seconds * 2 ** (attempt - 1), 30.0) * random.uniform(0.8, 1.2)


class _Watch:
    """Logs `line` as an error if the block still runs after `seconds`: a handler cannot be
    stopped from outside, but a stuck one shows."""

    def __init__(self, log: "_Log", what: str, seconds: float | None, line: str = ""):
        self._timer = None
        if seconds is not None:
            self._timer = threading.Timer(seconds, log.print, ("error", what, line))
            self._timer.daemon = True

    def __enter__(self):
        if self._timer:
            self._timer.start()

    def __exit__(self, *exc):
        if self._timer:
            self._timer.cancel()


def _handling(msg) -> tuple[float | None, str]:
    """After how long a listener's message is too long, and what to log then: a query or check
    after 5 s (the kernel's default `--query-timeout-ms` and `--middleware-timeout-ms`, after
    which the request has failed), a batch after 30 s (everything behind it waits)."""
    kind = msg.WhichOneof("msg")
    if kind == "batch":
        what, seconds = f"the batch up to {msg.batch.up_to}", 30
    elif kind == "query":
        what, seconds = f"the query {msg.query.query.domain}.{msg.query.query.query_type}", 5
    elif kind == "check":
        what, seconds = "a middleware check", 5
    else:
        return None, ""
    return seconds, f"still handling {what} after {seconds} s: everything behind it waits; is a handler stuck?"


def _relative(type: str, domains: set[str]) -> str:
    """`shop.ItemAdded` as `ItemAdded` when `shop` is the only domain in `domains`."""
    domain, _, name = type.partition(".")
    return name if len(domains) == 1 and domain in domains and name else type


def _listener_details(l: "Listener", position: int, name: Callable[[str], str] = lambda t: t) -> list[str]:
    """What a listener does, one part per entry: where it starts, what it reads, answers and
    checks. `name` shortens the types (see `_relative`)."""
    def listed(types: Iterable[str]) -> str:
        return ", ".join(name(t) for t in types)

    details = [f"from {position} · reads {listed(l._handlers)}" if l._handlers else f"from {position}"]
    if l._queries:
        details.append(f"answers {listed(l._queries)}")
    for when, (spec, _) in l._middlewares.items():
        details.append(f"checks {listed(spec.types)} {when}")
    return details


@dataclass
class Manifest:
    """What a service consists of, as `Kernable.apply` takes it: the membership an app applies
    (`app.manifest`, see `Kernable.app`)."""

    #: The service the membership is for.
    service: str
    #: The aggregates the service runs; the kernel waits for a worker of each.
    workers: list[AggregateType] = field(default_factory=list)
    #: The listeners the kernel waits for.
    listeners: list["Listener"] = field(default_factory=list)
    #: The service's listeners that may check requests; they count like listeners.
    middlewares: list["Listener"] = field(default_factory=list)
    #: Free metadata, e.g. `{"version": "1.0.0"}`.
    metadata: dict[str, str] = field(default_factory=dict)


class _AllReady:
    """Set once all of some streams are connected; waits like a `threading.Event`."""

    def __init__(self, events: list[threading.Event]):
        self._events = events

    def is_set(self) -> bool:
        return all(e.is_set() for e in self._events)

    def wait(self, timeout: float | None = None) -> bool:
        """Waits until every stream is connected, at most `timeout` seconds in all; returns whether
        they are."""
        deadline = None if timeout is None else time.monotonic() + timeout
        for e in self._events:
            if not e.wait(None if deadline is None else max(0.0, deadline - time.monotonic())):
                return False
        return True


class App:
    """An app made of whole domains, returned by `kernable.app(...)`: `run()` applies its
    membership (`manifest`), then runs its worker and listeners until `stop()`; `ready` is set
    once all of them are connected.

    `run()` blocks the calling thread. To do something once the app is up (e.g. in a test),
    run it in a thread and wait: `threading.Thread(target=app.run, daemon=True).start()`, then
    `app.ready.wait(5)`, which returns whether everything connected within 5 seconds. Call
    `app.stop()` from another thread or a handler to end `run()`. Attributes: `manifest` (a
    `Manifest`), `worker` (a `Worker`, or `None` without aggregates), `listeners` (a
    `ListenerHandle` per read model and reactor, live ones too) and `ready`.

    One service is one app: two processes with the same service would replace each other's
    membership."""

    def __init__(self, k: Kernable, manifest: Manifest, worker: "Worker | None",
                 listeners: list["ListenerHandle"], domains: Iterable[str] = ()):
        """Use `Kernable.app`."""
        self._kernable = k
        self._log = k._log
        # The names of the app's domains.
        self._domains = set(domains)
        self._stopped = False
        #: The membership `run()` applies.
        self.manifest = manifest
        #: The worker of all the domains' aggregates, if they have any.
        self.worker = worker
        #: A running listener per read model and reactor, live ones too.
        self.listeners = listeners
        streams = [s for s in (worker, *listeners) if s is not None]
        #: Set once every worker and listener of the app is connected.
        self.ready = _AllReady([s.ready for s in streams])
        self._streams = streams

    def run(self) -> None:
        """Applies the membership (see `Kernable.apply`), then runs the worker and the listeners,
        each in a thread of its own, until `stop()`; blocks until then. A failing handler does
        not end it: a command handler's error rejects the command (`handler_error`), a listener
        handler's error makes the listener reconnect and get the event again. If a worker or
        listener itself fails, it stops the others and raises that error (a store that cannot
        be opened is tried again instead); Ctrl-C stops all of them too. Once all are connected, it prints an overview of the app (unless
        `KERNABLE_LOG` is below `info`). Raises `grpc.RpcError` if the kernel refuses the
        membership."""
        started = time.monotonic()
        m = self.manifest
        self._kernable.apply(service=m.service, workers=m.workers, listeners=m.listeners,
                             middlewares=m.middlewares, metadata=m.metadata)
        for s in self._streams:
            s.announce = False
        errors: list[BaseException] = []

        def overview() -> None:
            while not self.ready.wait(0.1):
                if self._stopped:
                    return
            self._overview(round((time.monotonic() - started) * 1000))

        def run(stream: _Stream) -> None:
            try:
                stream.run()
            except BaseException as err:
                errors.append(err)
                self.stop()

        threads = [threading.Thread(target=run, args=(s,), name=f"kernable {s._what}", daemon=True)
                   for s in self._streams]
        threading.Thread(target=overview, name="kernable overview", daemon=True).start()
        for t in threads:
            t.start()
        try:
            for t in threads:
                t.join()
        except BaseException:  # e.g. Ctrl-C: stop everything
            self.stop()
            raise
        if errors:
            raise errors[0]

    def stop(self) -> None:
        """Stops the worker and all listeners; `run()` returns once they are done."""
        self._stopped = True
        for s in self._streams:
            s.stop()

    def _overview(self, ms: int) -> None:
        log = self._log
        if not log.enabled("info"):
            return
        m = self.manifest
        domains = self._domains

        # A single domain's own names go without it; with several, every name is qualified.
        def short(domain: str, name: str) -> str:
            return name if len(domains) == 1 and domain in domains else f"{domain}.{name}"

        def count(n: int, what: str) -> str:
            return f"{n} {what}{'' if n == 1 else 's'}"

        listed = [count(len(m.workers), "worker"), count(len(m.listeners), "listener")]
        if m.middlewares:
            listed.append(count(len(m.middlewares), "middleware"))
        meta = m.metadata
        rows: list[tuple[str, str, list[str]]] = []
        for a in m.workers:
            rows.append(("worker", short(a.domain, a.name), [f"runs {', '.join(sorted(a.handlers))}"]))
        for h in self.listeners:
            l = h.listener
            rows.append(("live" if l.live else "listener", short(l.domain.name, l.name),
                         _listener_details(l, h.started_at, lambda t: _relative(t, domains))))
        width = max((len(name) for _, name, _ in rows), default=0)
        header = " · ".join(p for p in ("kernable", " ".join(p for p in (m.service, meta.get("version")) if p),
                                        meta.get("description")) if p)
        lines = [
            log.style(1, header),
            f"  {'kernel':<10}  {log.peer}",
            f"  {'membership':<10}  applied: {', '.join(listed)}",
            "",
        ]
        for kind, name, (first, *rest) in rows:
            lines.append(f"  {kind:<10}  {name:<{width}}  {first}")
            lines.extend(f"  {'':<10}  {'':<{width}}  {more}" for more in rest)
        lines += ["", f"  {log.style(2, f'ready in {ms} ms')}", ""]
        # One write, so the overview's lines stay together.
        sys.stderr.write("\n".join(lines) + "\n")


class _Stream:
    """A bidirectional stream that reconnects until stopped."""

    def __init__(self, what: str, reconnect_s: float, log: _Log):
        self.ready = threading.Event()
        #: Whether the first connect gets its own line; an `App` prints it in its overview instead.
        self.announce = True
        self._connects = 0
        self._log = log
        self._what = what
        self._reconnect_s = reconnect_s
        self._stopped = False
        #: Set by `stop()`: cuts the wait before a reconnect short.
        self._wake = threading.Event()
        self._call = None
        self._outbox: queue.Queue | None = None

    def run(self) -> None:
        """Processes messages until `stop()`, reconnecting when the stream drops."""
        attempt = 0
        while not self._stopped:
            connects = self._connects
            refused = False
            outbox: queue.Queue = queue.Queue()
            self._outbox = outbox
            try:
                self._call = None
                self._call = self._open(iter(outbox.get, None))
                if self._stopped:  # stopped while it opened: stop() missed this call
                    self._call.cancel()
                outbox.put(self._hello())
                for msg in self._call:
                    reply = self._on(msg)
                    if reply is not None:
                        outbox.put(reply)
                reason = "stream ended"
            except ClientClosed:
                return
            except grpc.RpcError as err:
                reason = err.details()
                refused = err.code() in _REFUSALS
            except Exception as err:  # a failing handler: reconnect and get the event again
                reason = str(err) or type(err).__name__
                if self._call is not None:
                    self._call.cancel()
            finally:
                outbox.put(None)
            attempt = 1 if self._connects > connects else attempt + 1
            if not self._stopped:
                self._log.reconnect(self._what, reason, self._reconnect_s, attempt, refused, self._wake)

    def stop(self) -> None:
        self._stopped = True
        self._wake.set()
        if self._call is not None:
            self._call.cancel()
        for child in getattr(self, "_children", ()):
            child.stop()

    def _partitions(self, source) -> "list[tuple[str, grpc.Channel]] | None":
        """The partitions of `source`, read again, backing off like a reconnect, while no
        kernel answers; `None` once stopped."""
        if not isinstance(source, _Partitioned):
            return [("", source)]
        attempt = 0
        while not self._stopped:
            # Read in a thread of its own: `stop()` ends the wait for it at once.
            box: dict = {}

            def read() -> None:
                try:
                    box["parts"] = source.all()
                except BaseException as err:
                    box["error"] = err

            reader = threading.Thread(target=read, name=f"kernable {self._what} map", daemon=True)
            reader.start()
            while reader.is_alive():
                reader.join(0.05)
                if self._stopped:
                    return None
            try:
                if "error" in box:
                    raise box["error"]
                return box["parts"]
            except ClientClosed:
                return None
            except grpc.RpcError as err:
                attempt += 1
                self._log.reconnect(self._what, err.details(), self._reconnect_s, attempt,
                                    err.code() in _REFUSALS, self._wake)
        return None

    def _run_children(self, children: "list[_Stream]") -> None:
        """Runs a stream per partition, each in a thread, until `stop()`; `ready` once all are."""
        self._children = children
        for child in children:
            child.announce = self.announce
        errors: list[BaseException] = []

        def run(child: "_Stream") -> None:
            try:
                child.run()
            except BaseException as err:
                errors.append(err)
                self.stop()

        def ready() -> None:
            for child in children:
                while not child.ready.wait(0.1):
                    if self._stopped:
                        return
            self.ready.set()

        threads = [threading.Thread(target=run, args=(c,), name=f"kernable {c._what}", daemon=True)
                   for c in children]
        threading.Thread(target=ready, name=f"kernable {self._what} ready", daemon=True).start()
        if self._stopped:
            return
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]

    def _open(self, requests):
        raise NotImplementedError

    def _hello(self):
        raise NotImplementedError

    def _on(self, msg):
        raise NotImplementedError


class Worker(_Stream):
    """Runs the commands of its aggregates, one at a time: the kernel sends each command with
    the aggregate's events (or a snapshot and the events after it), the worker folds the state,
    calls the command handler and sends the new events back, signed. Made with
    `Kernable.worker`.

    `run()` processes commands until `stop()`, reconnecting when the session drops (after 1 s);
    it blocks the calling thread. `ready` (a `threading.Event`) is set once the kernel
    registered the worker: run it in a thread and wait with `worker.ready.wait(5)`, e.g.
    `threading.Thread(target=worker.run, daemon=True).start()`. `stop()` ends `run()`, also
    from another thread."""

    def __init__(self, channel: "grpc.Channel | _Partitioned", key: Key, aggregates, log: _Log,
                 service: str = ""):
        """Use `Kernable.worker`."""
        super().__init__("worker", 1.0, log)
        # A channel, or the partitions: then a session per partition.
        self._source = channel
        self._stub = None
        self._key = key
        self._aggregates = list(aggregates)
        self._by_command = {f"{a.domain}.{t}": a for a in aggregates for t in a.handlers}
        # By stream: the kernel names the aggregate type of every command.
        self._by_stream = {(a.domain, a.name): a for a in aggregates}
        # The service this worker belongs to; its membership decides whether the kernel waits for it.
        self._service = service
        # Executes whose history is still coming in parts, by lease; per session.
        self._partial = {}

    def run(self) -> None:
        """Processes commands until `stop()`, reconnecting when the session drops; with
        partitions, a session per partition."""
        parts = self._partitions(self._source)
        if parts is None:
            return
        if len(parts) > 1 or parts[0][0] != "":
            self._run_children([Worker(channel, self._key, self._aggregates, self._log, self._service)
                                for _, channel in parts])
            return
        self._stub = rpc.WorkerGatewayStub(parts[0][1])
        super().run()

    def _open(self, requests):
        return self._stub.Session(requests)

    def _hello(self):
        self._partial = {}
        # Every event type the aggregates know: the kernel refuses an outdated worker.
        schemas = {f"{a.domain}.{t}": a.schema_of(t) for a in self._by_command.values()
                   for t in [*a.appliers, *a.events]}
        register = pb.Register(worker_id=f"py-{uuid.uuid4().hex[:8]}", key_id=self._key.id, protocol=PROTOCOL,
                               max_concurrency=1,
                               aggregates=[pb.WorkerAggregate(aggregate_type=f"{a.domain}.{a.name}",
                                                              command_types=[f"{a.domain}.{t}" for t in a.handlers])
                                           for a in self._aggregates],
                               event_schemas=[pb.EventSchema(event_type=t, schema=s) for t, s in schemas.items()],
                               service=self._service)
        return pb.WorkerMessage(register=register)

    def _on(self, msg):
        kind = msg.WhichOneof("msg")
        if kind == "challenge":
            sig = self._key.sign(challenge_message(msg.challenge.nonce)).sig
            return pb.WorkerMessage(challenge_response=pb.ChallengeResponse(sig=sig))
        if kind == "registered":
            if self._connects > 0:
                self._log.print("info", self._what, "connected again")
            elif self.announce:
                self._log.print("info", self._what, f"connected to {self._log.peer} · runs {', '.join(sorted(self._by_command))}")
            self._connects += 1
            self.ready.set()
        elif kind in ("execute", "execute_more"):
            x = self._assembled(msg)
            if x is None:
                return None
            cmd = x.command
            stream = f"{cmd.domain}.{cmd.command_type} {cmd.aggregate_type}/{cmd.aggregate_id}"
            # A handler cannot be stopped from outside; one that runs past its lease at least shows.
            with _Watch(self._log, self._what, max(0.0, x.deadline / 1000 - time.time()),
                        f"{stream} still running after its lease ended: the kernel gives it to another worker; "
                        "is the handler stuck?"):
                reply = self._execute(x)
            if self._log.enabled("debug"):
                done = (f"rejected {reply.rejection.reason.code}" if reply.HasField("rejection")
                        else f"committed {len(reply.result.events)} events")
                self._log.print("debug", self._what,
                                f"{cmd.domain}.{cmd.command_type} {cmd.aggregate_type}/{cmd.aggregate_id} {done}")
            return reply
        return None

    def _assembled(self, msg):
        """The Execute to run once its last part is here (a long history comes in parts), else None."""
        if msg.WhichOneof("msg") == "execute":
            x = msg.execute
        else:
            rest = msg.execute_more
            x = self._partial.pop(rest.lease_id, None)
            if x is None:
                return None
            x.history.extend(rest.history)
            x.vault.extend(rest.vault)
            x.more = rest.more
        if x.more:
            self._partial[x.lease_id] = x
            return None
        return x

    def _execute(self, x):
        cmd = x.command
        try:
            agg = self._by_stream.get((cmd.domain, cmd.aggregate_type))
            if agg is None or cmd.command_type not in agg.handlers:
                raise Rejected(f"no handler for {cmd.command_type} on {cmd.domain}.{cmd.aggregate_type}",
                               "unknown_command")
            if x.HasField("snapshot") and x.snapshot.schema != agg.schema:
                raise Rejected(f"snapshot schema {x.snapshot.schema} is not {agg.schema}", "snapshot_stale")
            history = [(e.event_type, e.schema, _unseal(_decode(e.payload), x.vault)[0],
                        _meta(e.event_type, e.version, cmd, e.command_id), ApplyContext(_from_wire_context(e.context)))
                       for e in x.history]
            state = agg.fold(history, agg._from_json(_decode(x.snapshot.state)) if x.HasField("snapshot") else _INITIAL)
            meta = CommandMeta(cmd.command_id, cmd.command_type, cmd.domain, cmd.aggregate_type, cmd.aggregate_id,
                               cmd.tenant_id)
            ctx = CommandContext(_from_wire_context(cmd.context), _time(x.deadline), _time(x.received_at), cmd.command_id)
            data = _unseal(_decode(cmd.payload), x.vault)[0]
            # A copy: a handler that changes it must not change the snapshot.
            produced = _call(agg.handlers[cmd.command_type], copy.deepcopy(state), data, meta, ctx)
            _check_personal(agg, cmd, produced)
            requires = self._requires(agg, cmd, data, state)
            events, vault = [], []
            for e in produced:
                spec = agg.events.get(e["type"])
                payload, refs, values = _seal(e.get("data"), spec.vault if spec else None, cmd.aggregate_id)
                vault.extend(values)
                erases = str(_call(spec.eraser, e.get("data"), cmd.aggregate_id) or "") if spec and spec.eraser else ""
                events.append(pb.NewEvent(event_type=e["type"], schema=agg.schema_of(e["type"]), payload=payload,
                                          vault_refs=refs, erases=erases))
            indexed = bool(agg.keys or agg.facts)
            after = (self._after(agg, cmd, x.expected_version, state, produced)
                     if indexed or _snapshot_due(agg, x.expected_version, len(produced)) else None)
            snapshot = self._snapshot(agg, cmd, x.expected_version, after, produced)
            # Every command, also one without events: the set the aggregate holds now.
            index = agg.index(after) if indexed else Index()
        except SchemaError:
            raise  # not the command's fault: this worker cannot read the aggregate; stop until updated
        except Exception as err:
            # An unexpected error's text may hold personal data or internals, and a rejection is
            # kept for good: it names only the error's type; the log has the rest.
            if not isinstance(err, Rejected):
                self._log.print("error", "worker", f"{cmd.domain}.{cmd.command_type} "
                                f"{cmd.aggregate_type}/{cmd.aggregate_id} failed:\n{traceback.format_exc()}")
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="handler_error", message=type(err).__name__))
            sig = self._key.sign(rejection_message(cmd.command_id, reason))
            return pb.WorkerMessage(rejection=pb.Rejection(lease_id=x.lease_id, reason=reason, worker_sig=sig))
        sig = self._key.sign(result_message(cmd, x.expected_version, events))
        result = pb.Result(lease_id=x.lease_id, events=events, worker_sig=sig, snapshot=snapshot, vault=vault)
        # The full set of keys and facts after the events (none: holds nothing), and what the
        # command requires; signed apart from the events, only when there is any.
        result.keys.extend(pb.UniqueKey(name=k.name, value_hash=hashlib.sha256(k.value.encode()).digest(),
                                        subject=k.subject, **{"global": k.scope == "global"}) for k in index.keys)
        result.facts.extend(index.facts)
        result.requires.extend(requires)
        if result.keys or result.facts or result.requires:
            result.index_sig.CopyFrom(self._key.sign(index_message(cmd, x.expected_version, result.keys, result.facts,
                                                                   result.requires)))
        return pb.WorkerMessage(result=result)

    @staticmethod
    def _requires(agg: AggregateType, cmd, data, state) -> list:
        """The facts of other aggregates the command requires, from its data and the state before it."""
        fn = agg.requirements.get(cmd.command_type)
        if fn is None:
            return []
        wire = []
        for r in _call(fn, data, copy.deepcopy(state)) or []:
            if not isinstance(r, Requirement):
                raise TypeError(f"requires of {cmd.domain}.{cmd.command_type} returns Requirement, not {type(r).__name__}")
            wire.append(pb.Requirement(domain=r.domain or cmd.domain, aggregate_type=r.aggregate_type,
                                       aggregate_id=r.aggregate_id, fact=r.fact, key=r.key,
                                       value_hash=hashlib.sha256(r.value.encode()).digest() if r.key else b"",
                                       absent=r.absent))
        return wire

    @staticmethod
    def _after(agg: AggregateType, cmd, before: int, state, events):
        """The state after the produced `events`, applied as they will be read back (a JSON round
        trip), so it equals a replay."""
        if not events:
            return state
        apply_ctx = ApplyContext(_from_wire_context(cmd.context))
        read = [(e["type"], agg.schema_of(e["type"]), _decode(_encode(e.get("data"))),
                 _meta(e["type"], before + i + 1, cmd, cmd.command_id), apply_ctx) for i, e in enumerate(events)]
        return agg.fold(read, state)

    def _snapshot(self, agg: AggregateType, cmd, before: int, after, events):
        """A snapshot of the state `after` the produced `events`, if they cross a multiple of
        `snapshot_every`."""
        if not _snapshot_due(agg, before, len(events)):
            return None
        version = before + len(events)
        state_bytes = _encode(agg._to_json(after))
        sig = self._key.sign(snapshot_message(cmd, version, agg.schema, state_bytes))
        return pb.Snapshot(version=version, schema=agg.schema, state=state_bytes, worker_sig=sig)


def _snapshot_due(agg: AggregateType, before: int, produced: int) -> bool:
    """Whether `produced` events after version `before` cross a multiple of `snapshot_every`."""
    every = agg.snapshot_every
    return every > 0 and produced > 0 and (before + produced) // every != before // every


class Listener:
    """A listener definition: a durable, named event listener of a domain, made with
    `Domain.read_model` or `Domain.reactor` and run with `kernable.listener(listener)`, which
    returns a `ListenerHandle`. Events arrive in order, one at a time, after every listener with a
    lower order has processed them. Delivery is at least once: if a handler raises, the listener
    reconnects and the event is delivered again, until a reactor gives up on it and parks it
    (`park_after` of `Domain.reactor`).

    Type names without a domain (`ItemAdded`, `GetCart`) are the listener's own domain's; types of
    other domains are qualified (`identity.AccountRegistered`, `identity.*`).

    Handlers get event payloads upcast to the versions of the listener's domain and of the domains
    it `use`s. An event with a newer schema than known stops the listener (it retries and logs)
    until it is updated, so it never misreads data.

    `on`, `query` and `middleware` work as decorators (`@carts_readmodel.on("ItemAdded")`) or
    directly (`carts_readmodel.on("ItemAdded", fn)`); handlers take only the parameters they
    need. They run in the thread of the listener's `run()`, one at a time, so plain dicts are
    safe to use as state there; guard state that other threads also touch."""

    def __init__(self, domain: Domain, name: str, order: int, start: int | None, reconnect_s: float,
                 store: str | None = None, schema: str = "1", *, live: bool = False, park_after: int = 0,
                 instance: str | None = None, single: bool = False):
        """Use `Domain.read_model` or `Domain.reactor`."""
        #: The instance ID it connects with (`None`: `KERNABLE_INSTANCE`, else none); see `Domain.read_model`.
        self.instance = instance
        #: Whether the instances take turns; see `Domain.reactor`.
        self.single = single
        #: Attempts before an event its handler keeps failing on is parked; 0: never.
        self.park_after = park_after
        #: The domain the listener belongs to: unqualified type names are its types.
        self.domain = domain
        self.name = name
        self.order = order
        self.start = start
        self.reconnect_s = reconnect_s
        #: Where the running listener keeps its state (`sqlite:<path>` or `memory:`), if anywhere.
        self.store_url = store
        self.schema = schema
        #: Not registered with the kernel: see `Domain.reactor`.
        self.live = live
        self._handlers: dict[str, Callable] = {}
        self._raw: set[str] = set()
        self._domains: dict[str, dict[str, EventSchema]] = {domain.name: domain.events}
        self._queries: dict[str, Callable] = {}
        #: The middleware handlers and their specs, by when they run: "before" and "after".
        self._middlewares: dict[str, tuple[pb.Middleware, Callable]] = {}

    def _qualify(self, type: str) -> str:
        return type if "." in type else f"{self.domain.name}.{type}"

    def use(self, *domains: Domain) -> "Listener":
        """Upcasts the events of these other domains with their event schemas (see
        `Domain.event`); the listener's own domain's apply already. Returns the listener, so it
        chains: `shop_domain.read_model("orders").use(billing_domain)`."""
        for d in domains:
            self._domains[d.name] = d.events
        return self

    def on(self, event_type: str, fn: Callable | None = None, *, raw: bool = False):
        """Registers a handler for an event type (`ItemAdded`, or `orders.OrderPlaced` of another
        domain) or a whole domain (`orders.*`); works as a decorator too: `fn(data, evt, ctx)` gets
        the event's payload, a `StoredEventMeta` and a `ListenerContext` (both optional). A `raw`
        handler gets payloads as stored, without upcasting, whatever their schema (for archives
        and exports). Example: `@carts_readmodel.on("ItemAdded")` over `def _(data, evt):
        rows[evt.aggregate_id] = data["qty"]`. If the handler raises, the listener reconnects and
        the event is delivered again, so handlers must be idempotent. Raises `ValueError` if
        `event_type` is none of these."""
        if _is_message_class(event_type, Event):
            cls = event_type
            b = _binding(cls, "declared (an aggregate's apply, or Domain.event)")
            self._domains.setdefault(b.domain, b.schemas.events)
            qualified = f"{b.domain}.{cls.type_name()}"
            self._raw.discard(qualified)

            def wrap(f: Callable) -> Callable:
                return lambda data, evt=None, ctx=None: _call(f, cls.from_wire(data), evt, ctx)

            return _register_wrapped(self, self._handlers, qualified, fn, wrap)
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
        the events, so an answer reflects every event handled before it. Return the answer
        (JSON-like; `None` reaches the caller as `None`); raise `Rejected` to refuse (any other exception refuses with
        `handler_error`). A handler cannot query its own listener (the answer would wait behind
        itself): the kernel refuses it. Example: `@carts_readmodel.query("GetCart")` over
        `def _(data): return rows[data["id"]]`. Raises `ValueError` if `query_type` is invalid."""
        if _is_message_class(query_type, Query):
            cls = query_type
            _bind(cls, self.domain.name, self.domain)
            qualified = f"{self.domain.name}.{cls.type_name()}"
            # Its target counts also when the query is asked by name, by clients that `use` the domain.
            target = _query_target(cls)
            if target and cls.type_name() not in self.domain.queries:
                self.domain.query(cls.type_name()).target(*target)

            def wrap(f: Callable) -> Callable:
                return lambda data, qry=None, ctx=None: _to_wire(_call(f, cls.from_wire(data), qry, ctx))

            return _register_wrapped(self, self._queries, qualified, fn, wrap)
        qualified = self._qualify(query_type)
        if not _QUERY.match(qualified):
            raise ValueError(f"answer a query type (Query or domain.Query), not {query_type!r}")
        return _register(self, self._queries, qualified, fn)

    def middleware(self, types: list[str], order: int = 0, fn: Callable | None = None, *, when: str = "before"):
        """Makes this listener a middleware; works as a decorator too. The kernel asks `fn` about
        every command and query of `types` (`*` for all, `Type` of the own domain, `domain.*` or
        `domain.Type`) before accepting or routing it (or after, see `when`), in chain order (lower
        `order` first, then by listener name), and only once this listener has caught up with the
        events. `fn(check, outcome, ctx)` gets a `Check`, after the request what came of it, and a
`MiddlewareContext` with the listener's store (both optional): an
        `Outcome` for a command, a `QueryOutcome` for a query (`check.kind` tells which); before,
        None. `types` cover file and cache requests too, before they run: `domain.PutData`,
        `.GetData`, `.DeleteData`, `.SetCache`, `.GetCache` and `.DeleteCache` (and `domain.*` or
        `*` include them); `check.kind` is then `"data"` or `"cache"`, `check.key` the key and
        `check.data` None, and a refusal reaches the client as `PERMISSION_DENIED`. Return False or
        raise `Rejected` to refuse; any other error refuses too, and so does a return value other
        than None, True or False. The registration is stored:
        while this listener is away, the kernel refuses those requests (`kernable listeners
        remove` unregisters it). `fn` cannot submit or query what it checks itself (it would wait
        behind itself): the kernel refuses it. A listener has at most one middleware before and
        one after.
        `when`: "before" (default) checks a request before the kernel accepts it; raising refuses
        it. "after" gets the request's outcome too: raising refuses a query's response; a
        command's outcome, and a rejected query, it only observes, best effort (it misses what
        happens while it is away, and nobody waits for it).
        Negative orders are reserved for kernable's kits (a grant of `reserved_order`). A live listener cannot be a middleware.
        Example: `@guard_readmodel.middleware(["shop.*"])` over a `def _(check):` that raises
        `Rejected("not allowed", "forbidden")` unless `check.request.sender_identity` may send
        `check.type`.
        Raises `ValueError` for an invalid type or `when`, a second middleware at the same
        `when`, or a live listener."""
        if self.live:
            raise ValueError(f"{self.name} is live: a live listener cannot be a middleware")
        qualified = [t if t == "*" else self._qualify(t) for t in types]
        for t, q in zip(types, qualified):
            if not _CHECKED.match(q):
                raise ValueError(f"check `*`, a type (Type or domain.Type) or a domain (domain.*), not {t!r}")
        if when not in ("before", "after"):
            raise ValueError(f"run a middleware \"before\" or \"after\", not {when!r}")
        if when in self._middlewares:
            raise ValueError(f"{self.name} has a middleware {when} already")
        spec = pb.Middleware(order=order, types=qualified, when=pb.WHEN_AFTER if when == "after" else pb.WHEN_BEFORE)
        if fn is not None:
            self._middlewares[when] = (spec, fn)
            return self

        def decorator(f: Callable) -> Callable:
            self._middlewares[when] = (spec, f)
            return f

        return decorator


class ListenerHandle(_Stream):
    """A running `Listener`, returned by `kernable.listener(listener)`: `run()` processes events,
    queries and checks until `stop()`; `ready` is set once the kernel accepted it.

    `run()` blocks the calling thread. To run several listeners in one process, run all but
    one in threads (`threading.Thread(target=handle.run, daemon=True).start()`), or use
    `Kernable.app`. `ready` is a `threading.Event`: `handle.ready.wait(5)` returns whether the
    kernel accepted the listener within 5 seconds. `stop()` ends `run()`, also from another
    thread. Attributes: `listener` (the definition), `position` (global position of the last
    handled event, or skipped, for other event types) and `store` (the listener's `Store`, while
    `run()` has it open)."""

    def __init__(self, channel: "grpc.Channel | _Partitioned", key: Key, listener: Listener, log: _Log,
                 service: str = "", kernable: "Kernable | None" = None, part: str = "",
                 store: "Store | None" = None, parent: "ListenerHandle | None" = None):
        """Use `Kernable.listener`. `part` and `store`: a partition's stream, sharing the
        store of the handle that runs it."""
        # For handlers' follow-up commands and queries.
        self._kernable = kernable
        # How often the handler failed on an event, by position, until it gets past it.
        self._attempts: dict[int, int] = {}
        super().__init__(f"listener {listener.name}", listener.reconnect_s, log)
        # The service this listener belongs to; its membership decides whether it counts.
        self._service = service
        #: The listener definition this runs.
        self.listener = listener
        #: The listener's store, while `run()` has it open (with `store=`).
        self.store: Store | None = store
        # A channel, or the partitions: then a stream per partition, sharing the store.
        self._source = channel
        self._part = part
        self._shared = store is not None
        # As a partition's stream: the handle that runs it. As that handle: how far each
        # partition's stream has handled, for the order across moves (S4).
        self._parent = parent
        self._progress: dict[str, int] = {}
        self._moves = threading.Condition()
        # Tenants this stream read the map again for.
        self._checked: set[str] = set()
        self._stub = None
        self._key = key
        self._start = listener.start
        #: Global position of the last handled event (or skipped, for other event types).
        self.position = 0
        # Newest event when the kernel accepted the listener; events up to it are replayed.
        self._replay_up_to = 0
        # The position sent in the last hello, if any.
        self._sent_start: int | None = None
        #: Position after which the kernel delivered on the first connect.
        self.started_at = 0

    def run(self) -> None:
        """Processes events, queries and checks until `stop()`, reconnecting when the stream
        drops; with a store, opens it first and continues after its position, and closes it when
        `run()` ends. A store that cannot be opened (a full disk) is tried again, backing off like
        a reconnect. With partitions, a stream per partition, each from its own position in the
        shared store."""
        parts = self._partitions(self._source)
        if parts is None:
            return
        partitioned = len(parts) > 1 or parts[0][0] != ""
        if self.listener.store_url and self.store is None:
            attempt = 0
            while self.store is None:
                try:
                    self.store = Store(self.listener.store_url, self.listener.schema)
                except Exception as err:  # sqlite3 and OS errors alike
                    attempt += 1
                    self._log.store_failed(self._what, err, self._reconnect_s, attempt, self._wake)
                    if self._stopped:
                        return
            if not partitioned:
                self.position = self.store.committed
                # A new (or cleared) store says nothing: the listener's own start applies (a
                # reactor starts at the newest event).
                if self.store.committed > 0:
                    self._start = self.store.committed
        if self._shared:
            self.position = self.store.committed_for(self._part)
            if self.position > 0:
                self._start = self.position
        try:
            if partitioned:
                self._run_children([ListenerHandle(channel, self._key, self.listener, self._log, self._service,
                                                   self._kernable, part=name, store=self.store,
                                                   parent=self)
                                    for name, channel in parts])
                return
            self._stub = rpc.ListenerServiceStub(parts[0][1])
            super().run()
        finally:
            # Stopping releases the store; a later run opens it again. A partition's stream
            # leaves it to the handle that owns it.
            if self.store is not None and not self._shared:
                self.store.close()
                self.store = None

    def _open(self, requests):
        return self._stub.Listen(requests)

    def _hello(self):
        d = self.listener
        instance = d.instance if d.instance is not None else os.environ.get("KERNABLE_INSTANCE", "")
        hello = pb.Hello(name=d.name, key_id=self._key.id, protocol=PROTOCOL, order=d.order,
                         event_types=list(d._handlers),
                         query_types=list(d._queries),
                         middlewares=[spec for spec, _ in d._middlewares.values()], live=d.live,
                         service=self._service, instance=instance,
                         single=bool(instance) and d.single and not d.live)
        # Reconnects resume after the last handled event, once the kernel sent one (an instance
        # that only waited has none).
        start = self.position if self._connects > 0 else self._start
        self._sent_start = start
        if start is not None:
            hello.from_position = start
        return pb.ListenerMessage(hello=hello)

    def _on(self, msg):
        # Requests sent from the handlers name this listener, so the kernel never makes them
        # wait for it (it handles one message at a time).
        token = _HANDLING.set(self.listener.name)
        try:
            with _Watch(self._log, self._what, *_handling(msg)):
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
            if self._parent is not None:
                self._parent._advanced(self._part, msg.ready.after_position)
            self._replay_up_to = msg.ready.head_position
            self._log_connected(msg.ready.after_position)
            self.ready.set()
        elif kind == "waiting":
            # Another instance runs the group: this one is ready to take over.
            self._log.print("info", self._what, f"waiting: instance {msg.waiting.active} is active")
            self.ready.set()
        elif kind == "batch":
            # A kernel that goes back (a bug, or not the kernel) must not make a store apply
            # events twice: the session ends, and the next resumes after what was handled.
            if msg.batch.up_to < self.position:
                raise RuntimeError(f"the kernel sent a batch up to {msg.batch.up_to}, behind position {self.position}")
            self._in_order(msg.batch.events)
            # With a store, the batch's data and position are committed together, before the ack.
            if self.store:
                self.store._begin()
            try:
                parked = self._handle_batch(msg.batch)
                if self.store:
                    self.store._commit(msg.batch.up_to, self._part)
            except BaseException:
                if self.store:
                    self.store._rollback()
                    self.position = self.store.committed_for(self._part)
                raise
            self.position = msg.batch.up_to
            if self._parent is not None:
                self._parent._advanced(self._part, msg.batch.up_to)
            # Values removed after an erasure must not stay behind in the WAL, outside the batch.
            if self.store and any(e.erases for e in [*msg.batch.events, *msg.batch.retries]):
                self.store._checkpoint()
            return pb.ListenerMessage(ack=pb.Ack(position=msg.batch.up_to, parked=parked))
        elif kind == "query":
            answer = self._answer(msg.query.call_id, msg.query.query)
            if self._log.enabled("debug"):
                q = msg.query.query
                done = (f"rejected {q.domain}.{q.query_type} {answer.rejected.code}" if answer.HasField("rejected")
                        else f"answered {q.domain}.{q.query_type}")
                self._log.print("debug", self._what, done)
            return pb.ListenerMessage(answer=answer)
        elif kind == "check":
            return pb.ListenerMessage(verdict=self._verdict(msg.check))
        return None

    def _advanced(self, part: str, position: int) -> None:
        """On the parent: partition `part`'s stream has handled everything up to `position`."""
        with self._moves:
            if self._progress.get(part, -1) < position:
                self._progress[part] = position
                self._moves.notify_all()

    def _in_order(self, events) -> None:
        """Before a batch of a partition's stream: the events of a tenant that moved here
        (S4) after its move wait until the stream of the partition it came from has passed the
        tenant's last event there, so a tenant's events keep their order across the move. Waits
        at most 30 s (a partition that is down), then goes on with a warning."""
        parent = self._parent
        if parent is None or not isinstance(parent._source, _Partitioned) or not events:
            return
        source = parent._source
        m = source.current()
        if m is None:
            return

        def known(m, tenant: str) -> bool:
            return _partition_of(m, tenant) == self._part or any(
                x.tenant == tenant and self._part in (x.to, getattr(x, "from")) for x in m.moved)

        for e in events:
            if not known(m, e.tenant_id) and e.tenant_id not in self._checked:
                # The tenant came here after the map was read; another kernel may take a moment
                # to hold the new one.
                self._checked.add(e.tenant_id)
                for i in range(3):
                    if known(m, e.tenant_id):
                        break
                    if i > 0:
                        time.sleep(0.5)
                    m = source.current(refresh=True) or m
            for x in m.moved:
                origin = getattr(x, "from")
                if x.tenant != e.tenant_id or x.to != self._part or e.global_position <= x.target_after:
                    continue
                if not any(p.name == origin for p in m.partitions):
                    continue
                parent._reached(self, origin, x.source_head, e.tenant_id)

    def _reached(self, child: "ListenerHandle", part: str, position: int, tenant: str) -> None:
        """On the parent: returns once partition `part`'s stream has handled up to `position`, or
        after 30 s."""
        deadline = time.monotonic() + 30
        with self._moves:
            while self._progress.get(part, -1) < position:
                if child._stopped:
                    raise RuntimeError("stopped")
                left = deadline - time.monotonic()
                if left <= 0:
                    self._log.print("warn", child._what,
                                    f"tenant {tenant} moved here from partition {part}, whose stream has not "
                                    f"passed position {position} in 30 s; going on without its order")
                    return
                self._moves.wait(min(left, 0.1))

    def _log_connected(self, after_position: int) -> None:
        """Logs that the kernel accepted the listener; the position is the start sent (the
        kernel's, if none was)."""
        position = after_position if self._sent_start is None else self._sent_start
        if self._connects == 0:
            self.started_at = position
            if self.announce:
                self._log.print("info", self._what, " · ".join(
                    [f"connected to {self._log.peer}", *_listener_details(self.listener, position)]))
        else:
            self._log.print("info", self._what, f"connected again at position {position}")
        self._connects += 1

    def _handle_batch(self, batch) -> "list[pb.Parked]":
        """Handles a batch: first the retries, then the events after the position. Returns the
        events it parked; raises if a handler failed on one it does not park."""
        d = self.listener
        park_after = 0 if d.live else d.park_after
        parked = []
        for e, retry in [*((e, True) for e in batch.retries), *((e, False) for e in batch.events)]:
            if not retry and e.global_position <= self.position:
                continue  # handled already
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
                                      e.command_id, e.global_position, schema, redacted, _time(e.created_at))
                ctx = ListenerContext(e.global_position <= self._replay_up_to, _from_wire_context(e.context),
                                      self.store, e.command_id, e.tenant_id, d.domain.name, self._kernable)
                saved = self.store is not None and park_after > 0
                if saved:
                    self.store._savepoint()
                try:
                    _call(handler, data, evt, ctx)
                except Exception as err:
                    # An erasing event is never parked: its handler must get past it, or what it
                    # removes would stay behind.
                    parked.append(self._give_up(f"{e.domain}.{e.event_type}", e.global_position, err,
                                                0 if e.erases else park_after))
                    if saved:
                        self.store._rollback_event()
                else:
                    if saved:
                        self.store._release()
                    self._attempts.pop(e.global_position, None)
                    self._log.print("debug", self._what, f"{e.domain}.{e.event_type} at {e.global_position}")
            if not retry:
                self.position = e.global_position
        return parked

    def _give_up(self, type: str, position: int, err: Exception, park_after: int) -> "pb.Parked":
        """What to do after the handler failed on the event at `position`: park it (a rejection
        at once, another error after `park_after` attempts), or raise `err`, so the listener gets
        the event again (the kernel or the store away, or attempts left)."""
        if park_after <= 0 or _transient(err):
            raise err
        attempts = self._attempts.get(position, 0) + 1
        self._attempts[position] = attempts
        if not isinstance(err, Rejected) and attempts < park_after:
            raise err
        del self._attempts[position]
        if isinstance(err, Rejected):
            self._log.print("warn", self._what, f"{type} at {position} rejected ({err.code}), parked: {err}")
            return pb.Parked(position=position, code=err.code, message=str(err), attempts=attempts)
        self._log.print("error", self._what, f"{type} at {position} failed {attempts} times, parked: {err!r}")
        return pb.Parked(position=position, code="handler_error", message=_type_name(err), attempts=attempts)

    def _verdict(self, call) -> pb.Verdict:
        try:
            when = "after" if call.when == pb.WHEN_AFTER else "before"
            middleware = self.listener._middlewares.get(when)
            if middleware is None:
                raise Rejected(f"no middleware {when}", "not_a_middleware")
            verdict = _call(middleware[1], _to_check(call), _to_outcome(call) if when == "after" else None, MiddlewareContext(self.store))
            if verdict is False:
                raise Rejected(f"refused by {self.listener.name}", "refused")
            if verdict is not None and verdict is not True:
                # Fails closed: a middleware that returns something else is a mistake.
                raise Rejected(f"{self.listener.name} returned {type(verdict).__name__}, not True, False or None",
                               "middleware_error")
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
            # An answer of None is JSON `null`, not `{}`: "nothing" must not look like an empty object.
            return pb.QueryAnswer(call_id=call_id, response=b"null" if result is None else _encode(result))
        except Exception as err:
            reason = (pb.Rejected(code=err.code, message=str(err)) if isinstance(err, Rejected)
                      else pb.Rejected(code="handler_error", message=repr(err)))
            return pb.QueryAnswer(call_id=call_id, rejected=reason)


def _encode(data: Any) -> bytes:
    # NaN and Infinity are not JSON: other SDKs could not read them back.
    return json.dumps({} if data is None else data, allow_nan=False).encode()


def _decode(payload: bytes) -> Any:
    return json.loads(payload) if payload else {}


def _time(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _meta(event_type: str, version: int, cmd, command_id: str) -> EventMeta:
    """What `apply` gets about an event of `cmd`'s aggregate, caused by command `command_id`."""
    return EventMeta(event_type, version, cmd.tenant_id, cmd.domain, cmd.aggregate_type, cmd.aggregate_id,
                     command_id)


def _transient(err: Exception) -> bool:
    """Whether a failed handler should simply try again: the kernel was unreachable or busy, a
    submit did not settle in time, or the store failed. Never parks."""
    if isinstance(err, (NotSettled, sqlite3.Error)):
        return True
    return isinstance(err, grpc.RpcError) and callable(getattr(err, "code", None)) and err.code() in _BUSY


def _type_name(err: BaseException) -> str:
    """The error's type, all a parked event keeps of a handler error."""
    return type(err).__name__


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
        sender_session=c.sender_session, on_behalf_of=c.on_behalf_of,
        correlation_id=c.correlation_id or command_id, causation_id=c.causation_id, trace_id=c.trace_id,
        bypass_authorization=c.bypass_authorization,
        attributes=[pb.Attribute(key=k, value=v) for k, v in c.attributes.items()])
    if c.expected_version is not None:
        wire.expected_version = c.expected_version
    return wire


def _from_wire_context(c) -> RequestContext:
    return RequestContext(
        c.sender_tenant, c.sender_identity, c.sender_account, c.sender_session, c.on_behalf_of,
        c.expected_version if c.HasField("expected_version") else None, c.correlation_id, c.causation_id,
        c.trace_id, c.bypass_authorization, {a.key: a.value for a in c.attributes})


def _to_check(call) -> Check:
    if call.WhichOneof("request") == "command":
        c = call.command
        return Check("command", c.command_id, c.tenant_id, c.domain, c.command_type,
                     _unseal(_decode(c.payload), call.vault)[0],
                     _from_wire_context(c.context), c.aggregate_type, c.aggregate_id,
                     target=Target(f"{c.domain}.{c.aggregate_type}", c.aggregate_id))
    kind = call.WhichOneof("request")
    if kind in ("data", "cache"):
        r = getattr(call, kind)
        type = call.type.rsplit(".", 1)[-1]  # qualified, e.g. `shop.PutData`
        return Check(kind, "", r.tenant_id, r.domain, type, None, _from_wire_context(r.context), key=r.key)
    q = call.query
    target = Target(q.target.aggregate_type, q.target.aggregate_id) if q.target.aggregate_id else None
    return Check("query", q.query_id, q.tenant_id, q.domain, q.query_type, _decode(q.payload),
                 _from_wire_context(q.context), target=target)


def _rejected_outcome(r) -> Outcome:
    """The outcome of a rejected command, with the key or requirement the kernel named."""
    q = r.requirement if r.HasField("requirement") else None
    return Outcome(committed=False, code=r.code, message=r.message, key=r.key,
                   requirement=Requirement(q.domain, q.aggregate_type, q.aggregate_id, q.fact, key=q.key,
                                           absent=q.absent) if q else None)


def _to_outcome(call) -> Outcome | QueryOutcome:
    """What came of the request a middleware after it checks."""
    if call.WhichOneof("outcome") == "command_result":
        r = call.command_result
        if r.WhichOneof("outcome") == "committed":
            return Outcome(committed=True, events=[_to_event(e) for e in r.committed.events])
        return _rejected_outcome(r.rejected)
    r = call.query_result
    if r.WhichOneof("outcome") == "response":
        return QueryOutcome(answered=True, data=_decode(r.response))
    return QueryOutcome(answered=False, code=r.rejected.code, message=r.rejected.message)


def _to_event(e, vault=None) -> StoredEvent:
    data, redacted = _unseal(_decode(e.payload), vault)
    return StoredEvent(position=e.global_position, tenant=e.tenant_id, domain=e.domain, aggregate_type=e.aggregate_type,
                 aggregate_id=e.aggregate_id, version=e.version, type=e.event_type,
                 data=data, command_id=e.command_id, request=_from_wire_context(e.context),
                 schema=e.schema, redacted=redacted, created_at=_time(e.created_at))
