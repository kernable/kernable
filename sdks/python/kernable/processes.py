"""Business processes (design/processes.md): a flow across commands, events and queries, built in
code next to the domain, run by the kernel. `Domain.process(name)` returns the builder; its
`model()` is what `Kernable.register_process` registers, and `Kernable.app` registers the
processes of its domains.

    fulfilment = (
        shop.process("fulfilment")
        .start(OrderSubmitted, key="aggregate_id", data={"order_id": "aggregate_id", "total": "data.total"})
        .command("confirm", Confirm, aggregate="case.order_id")
        .decide("large", when="case.total > 1000", then="approval", otherwise="paid")
        .query("approval", NeedsApproval, data={"order_id": "case.order_id"}, paths={True: "approved", False: "paid"})
        .wait("approved", OrderApproved, key="aggregate_id")
        .wait("paid", PaymentReceived, key="data.order_id", timeout="3d", on_timeout="cancel")
        .command("ship", Ship, aggregate="case.order_id", rejected={"out_of_stock": "notify"}, next="done")
        .command("notify", NotifyDelay, aggregate="case.order_id", next="done")
        .command("cancel", Cancel, aggregate="case.order_id", data={"reason": value("unpaid")})
        .end("done")
    )

Steps run in the order written unless a step names `next`; `decide` and `query` name their
targets. References are strings in Python style, converted to the wire names (camelCase) in the
model: `aggregate_id`, `data.<field>` (the event's, in `start` and `wait`), `case.<field>` (the
case's data) and, in a query step, `answer` or `answer.<field>`. A literal is `value(...)`.
Conditions are small expressions over `case.` fields: `== != < <= > >=`, `and`, `or`, `not`.
Durations are `30s`, `15m`, `12h`, `3d` (or ms). Messages are classes or names: unqualified of
the process's domain (`"OrderSubmitted"`, commands `"order.Confirm"`), or qualified
(`"payments.PaymentReceived"`, commands `"shop.order.Confirm"`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .messages import Command, Event, Query, binding, camel, is_message_class

_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_DURATION = re.compile(r"^(\d+)(s|m|h|d)$")
_UNIT_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
# A quoted string (left as it is) or a reference whose field names are converted.
_CONDITION_TOKEN = re.compile(r"""('[^']*'|"[^"]*")|\b((?:case|data|answer)(?:\.[A-Za-z_][A-Za-z0-9_]*)+)""")


@dataclass(frozen=True)
class Value:
    """A literal in a mapping, made with `value(...)`: `{"value": ...}` in the model."""

    value: Any


def value(x: Any) -> Value:
    """A literal (JSON-like) where a mapping takes a reference: `data={"reason": value("unpaid")}`."""
    return Value(x)


def duration_ms(d: str | int) -> int:
    """`30s`, `15m`, `12h`, `3d` (or ms) as ms. Raises `ValueError` unless positive and well-formed."""
    if isinstance(d, int) and not isinstance(d, bool):
        ms = d
    elif isinstance(d, str) and (m := _DURATION.match(d)):
        ms = int(m.group(1)) * _UNIT_MS[m.group(2)]
    else:
        raise ValueError(f"{d!r} is no duration: 30s, 15m, 12h or 3d")
    if ms <= 0:
        raise ValueError(f"{d!r} is no positive duration")
    return ms


def _path(p: str) -> str:
    """A dotted path with each field name as on the wire."""
    return ".".join(camel(part) for part in p.split("."))


def _reference(ref: Any, where: str, allowed: tuple[str, ...]) -> str:
    """A reference in Python style as on the wire; `allowed` are the roots it may start with."""
    if not isinstance(ref, str) or not ref:
        raise ValueError(f"{where}: a reference is a string such as 'case.order_id', not {ref!r}")
    root, _, rest = ref.partition(".")
    if root in ("aggregate_id", "aggregateId") and not rest and "aggregate_id" in allowed:
        return "aggregateId"
    if root == "answer" and "answer" in allowed:
        return "answer" if not rest else f"answer.{_path(rest)}"
    if root in ("case", "data") and root in allowed and rest:
        return f"{root}.{_path(rest)}"
    names = ", ".join("aggregate_id" if a == "aggregate_id" else f"{a}.<field>" for a in allowed)
    raise ValueError(f"{where}: {ref!r} is no reference here; use {names}")


def _mapping(data: Any, where: str, allowed: tuple[str, ...]) -> dict:
    """A mapping field → reference or `value(...)`, with wire names."""
    if not isinstance(data, dict):
        raise ValueError(f"{where}: data is a dict of field → reference or value(...)")
    out = {}
    for k, v in data.items():
        if not isinstance(k, str) or not k:
            raise ValueError(f"{where}: {k!r} is no field name")
        out[camel(k)] = {"value": v.value} if isinstance(v, Value) else _reference(v, f"{where}, {k}", allowed)
    return out


def condition(text: Any, where: str) -> str:
    """A condition with the field names of its references as on the wire; literals stay as they are.
    The kernel parses it when the process is registered."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError(f"{where}: a condition is a string such as 'case.total > 1000'")
    return _CONDITION_TOKEN.sub(lambda m: m.group(1) or m.group(2).split(".", 1)[0] + "." + _path(m.group(2).split(".", 1)[1]), text)


def _type(message: Any, base: type, what: str, where: str, domain: str) -> str:
    """A message class's qualified type, or a name: qualified as given, unqualified of `domain`
    (`OrderApproved` for events and queries, `order.Confirm` for commands)."""
    parts = 3 if base is Command else 2
    if is_message_class(message, base):
        b = binding(message, "registered")
        if base is Command:
            if not b.aggregate_type:
                raise ValueError(f"{where}: {message.__name__} is handled by no aggregate yet")
            return f"{b.domain}.{b.aggregate_type}.{message.type_name()}"
        return f"{b.domain}.{message.type_name()}"
    if isinstance(message, str) and all(message.split(".")):
        n = len(message.split("."))
        if n == parts:
            return message
        if n == parts - 1:
            return f"{domain}.{message}"
    shape = "aggregate.Type or domain.aggregate.Type" if base is Command else "Type or domain.Type"
    raise ValueError(f"{where}: {message!r} is no {what}: give its class or its name ({shape})")


class Process:
    """A business process of a domain, built step by step: made with `Domain.process(name)`;
    each method adds a step and returns the process, so they chain. See the module's docstring
    for an example. `model()` builds the model the kernel registers, checking what it can."""

    def __init__(self, domain: str, name: str):
        """Use `Domain.process`. Raises `ValueError` if `name` is invalid."""
        if not _NAME.match(name or ""):
            raise ValueError(f"invalid process name {name!r}: letters, digits, '_' and '-'")
        #: The domain it is defined on.
        self.domain = domain
        #: The process's name within its domain.
        self.name = name
        self._start: tuple | None = None
        self._steps: list[tuple[str, str, dict]] = []
        self._cancels: list[tuple[Any, str, str]] = []

    @property
    def qualified(self) -> str:
        """`<domain>.<name>`: `shop.fulfilment`."""
        return f"{self.domain}.{self.name}"

    def _add(self, name: str, kind: str, **options: Any) -> "Process":
        self._steps.append((name, kind, options))
        return self

    def start(self, event: type[Event] | str, *, key: str, data: dict | None = None) -> "Process":
        """Starts a case when `event` commits. `key` is where the case's key is in the event
        (`aggregate_id`, or `data.<field>`): one case per key. `data` maps case fields to
        references into the event (`{"order_id": "aggregate_id", "total": "data.total"}`) or
        `value(...)`."""
        if self._start is not None:
            raise ValueError(f"process {self.qualified}: one start only")
        self._start = (event, key, data)
        return self

    def command(self, name: str, command: type[Command] | str, *, aggregate: str, data: dict | None = None,
                rejected: dict[str, str] | None = None, next: str | None = None) -> "Process":
        """Sends `command` as the process to the aggregate whose ID `aggregate` names
        (`case.order_id`), with `data` from the case (field → reference or `value(...)`).
        `rejected` maps rejection codes to the steps that handle them; any other rejection
        raises an incident. Then `next`, or the step written after it."""
        return self._add(name, "command", command=command, aggregate=aggregate, data=data, rejected=rejected, next=next)

    def wait(self, name: str, event: type[Event] | str, *, key: str, data: dict | None = None,
             timeout: str | int | None = None, on_timeout: str | None = None, next: str | None = None) -> "Process":
        """Waits for an `event` of this case: the one whose `key` (`aggregate_id` or
        `data.<field>`) is the case's key. `data` merges fields of the event into the case's data.
        With `timeout` (`3d`), the case moves to `on_timeout` if the event does not come in time.
        Then `next`, or the step written after it."""
        if (timeout is None) != (on_timeout is None):
            raise ValueError(f"process {self.qualified}, step {name}: timeout and on_timeout go together")
        return self._add(name, "wait", event=event, key=key, data=data, timeout=timeout, on_timeout=on_timeout, next=next)

    def timer(self, name: str, *, after: str | int | None = None, at: str | None = None, next: str | None = None) -> "Process":
        """Waits `after` a duration (`12h`), or until the time (ms) a reference names (`at`:
        `case.due_at`). Then `next`, or the step written after it."""
        if (after is None) == (at is None):
            raise ValueError(f"process {self.qualified}, step {name}: a timer takes after or at")
        return self._add(name, "timer", after=after, at=at, next=next)

    def decide(self, name: str, *, when: str | None = None, then: str | None = None,
               cases: list | None = None, otherwise: str | None = None) -> "Process":
        """Picks a path by the case's data: `when` (`"case.total > 1000"`) goes to `then`, or
        `cases=[("case.total > 1000", "approval"), ...]` (or dicts with `when` and `then`), the
        first that holds; else `otherwise`, or the step written after it."""
        if cases is None:
            if when is None or then is None:
                raise ValueError(f"process {self.qualified}, step {name}: decide takes when and then, or cases")
            cases = [(when, then)]
        elif when is not None or then is not None:
            raise ValueError(f"process {self.qualified}, step {name}: decide takes when and then, or cases, not both")
        pairs = []
        for c in cases:
            if isinstance(c, dict):
                c = (c.get("when"), c.get("then"))
            if not isinstance(c, (tuple, list)) or len(c) != 2:
                raise ValueError(f"process {self.qualified}, step {name}: each case is (when, then)")
            pairs.append(tuple(c))
        if not pairs:
            raise ValueError(f"process {self.qualified}, step {name}: decide needs a case")
        return self._add(name, "decide", cases=pairs, otherwise=otherwise)

    def query(self, name: str, query: type[Query] | str, *, data: dict | None = None, answer: str | None = None,
              paths: dict, into: str | None = None, otherwise: str | None = None) -> "Process":
        """Asks a read model `query` with `data` from the case; its answer picks the path:
        `paths` maps answers (`{True: "approved", False: "paid"}`, or strings and numbers) to
        steps, matched against the answer itself or, for an object, its field `answer`. `into`
        keeps the answer in the case's data under that field. An answer no path names goes to
        `otherwise` (without it: an incident)."""
        if not isinstance(paths, dict) or not paths:
            raise ValueError(f"process {self.qualified}, step {name}: a query needs paths, e.g. {{True: 'yes', False: 'no'}}")
        return self._add(name, "query", query=query, data=data, answer=answer, paths=paths, into=into, otherwise=otherwise)

    def end(self, name: str) -> "Process":
        """Ends the case."""
        return self._add(name, "end")

    def cancel_on(self, event: type[Event] | str, *, key: str, end: str) -> "Process":
        """Ends a running case at once when an `event` of its key commits, whatever step it is
        at: `.cancel_on(AccountClosed, key="aggregate_id", end="closed")`. Its timer is withdrawn,
        an outcome it still awaits is ignored, an incident ends with it. Events the case's own
        commands cause do not count. `end` must be an `end` step."""
        self._cancels.append((event, key, end))
        return self

    def model(self) -> dict:
        """The model the kernel registers (design/processes.md "The model format"): types
        qualified, references with wire names, durations in ms, and an explicit `next` on every
        step but `end`. Raises `ValueError` naming the process and step for what can be checked
        here: no start, duplicate or invalid step names, a step without successor, a target that
        names no step, malformed references and durations. The kernel checks the rest."""
        p = self.qualified
        if self._start is None:
            raise ValueError(f"process {p}: no start")
        if not self._steps:
            raise ValueError(f"process {p}: no steps")
        names = [s[0] for s in self._steps]
        seen: set[str] = set()
        for n in names:
            if not isinstance(n, str) or not _NAME.match(n):
                raise ValueError(f"process {p}: invalid step name {n!r}: letters, digits, '_' and '-'")
            if n in seen:
                raise ValueError(f"process {p}: two steps named {n}")
            seen.add(n)

        event, key, data = self._start
        where = f"process {p}, start"
        start = {"on": _type(event, Event, "event", where, self.domain), "key": _reference(key, where, ("aggregate_id", "data"))}
        if data is not None:
            start["data"] = _mapping(data, where, ("aggregate_id", "data"))

        def target(t: Any, where: str) -> str:
            if t not in seen:
                raise ValueError(f"{where}: {t!r} names no step")
            return t

        steps = []
        for i, (name, kind, o) in enumerate(self._steps):
            where = f"process {p}, step {name}"
            following = names[i + 1] if i + 1 < len(names) else None

            def successor() -> str:
                n = o.get("next") if o.get("next") is not None else following
                if n is None:
                    raise ValueError(f"{where}: no next step (end the process with end(...))")
                return target(n, where)

            step: dict[str, Any] = {"name": name, "kind": kind}
            if kind == "command":
                step["command"] = _type(o["command"], Command, "command", where, self.domain)
                step["aggregate"] = _reference(o["aggregate"], where, ("case",))
                if o["data"] is not None:
                    step["data"] = _mapping(o["data"], where, ("case",))
                if o["rejected"]:
                    step["rejected"] = {code: target(t, where) for code, t in o["rejected"].items()}
                step["next"] = successor()
            elif kind == "wait":
                step["event"] = _type(o["event"], Event, "event", where, self.domain)
                step["key"] = _reference(o["key"], where, ("aggregate_id", "data"))
                if o["data"] is not None:
                    step["data"] = _mapping(o["data"], where, ("aggregate_id", "data", "case"))
                if o["timeout"] is not None:
                    step["timeoutMs"] = _duration(o["timeout"], where)
                    step["onTimeout"] = target(o["on_timeout"], where)
                step["next"] = successor()
            elif kind == "timer":
                if o["after"] is not None:
                    step["afterMs"] = _duration(o["after"], where)
                else:
                    step["at"] = _reference(o["at"], where, ("case",))
                step["next"] = successor()
            elif kind == "decide":
                step["when"] = [{"if": condition(c, where), "next": target(t, where)} for c, t in o["cases"]]
                otherwise = o["otherwise"] if o["otherwise"] is not None else following
                if otherwise is None:
                    raise ValueError(f"{where}: no otherwise step")
                step["otherwise"] = target(otherwise, where)
            elif kind == "query":
                step["query"] = _type(o["query"], Query, "query", where, self.domain)
                if o["data"] is not None:
                    step["data"] = _mapping(o["data"], where, ("case",))
                if o["answer"] is not None:
                    a = o["answer"]
                    if not isinstance(a, str) or not a:
                        raise ValueError(f"{where}: answer is a field of the answer, such as 'needs_approval'")
                    step["answer"] = _path(a)
                step["paths"] = []
                for v, t in o["paths"].items():
                    if not (v is None or isinstance(v, (bool, int, float, str))):
                        raise ValueError(f"{where}: a path's answer is a boolean, number or string, not {v!r}")
                    step["paths"].append({"value": v, "next": target(t, where)})
                if o["into"] is not None:
                    if not isinstance(o["into"], str) or not o["into"]:
                        raise ValueError(f"{where}: into is a case field, such as 'approval'")
                    step["into"] = _path(o["into"])
                if o["otherwise"] is not None:
                    step["otherwise"] = target(o["otherwise"], where)
            steps.append(step)
        model = {"name": p, "start": start, "steps": steps}
        if self._cancels:
            kinds = {name: kind for name, kind, _ in self._steps}
            model["cancelOn"] = []
            for event, key, end in self._cancels:
                where = f"process {p}, cancel_on"
                if end not in kinds:
                    raise ValueError(f"{where}: {end!r} names no step")
                if kinds[end] != "end":
                    raise ValueError(f"{where}: {end!r} is no end step")
                model["cancelOn"].append({"event": _type(event, Event, "event", where, self.domain),
                                          "key": _reference(key, where, ("aggregate_id", "data")), "end": end})
        return model


def _duration(d: Any, where: str) -> int:
    try:
        return duration_ms(d)
    except ValueError as err:
        raise ValueError(f"{where}: {err}") from None
