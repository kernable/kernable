"""Events, commands and queries as classes: defined once, typed, used by reference.

Every subclass of `Event`, `Command` and `Query` is a keyword-only dataclass, without
`@dataclass`. The class name is its type; class attributes (`ClassVar`, or plain assignments
without an annotation) declare what `Domain.event` and `Domain.command` declare: `version`,
`upcasts`, `vault` (a dict with `personal`, `secret` and, for an event, `erases`: kept apart from
the fields, so a field may be called `secret` too), `raw` (fields passed through as they are,
e.g. a browser's WebAuthn answer), and `type` to override the name.

Fields are snake_case in Python and camelCase in the log (`account_id` is `accountId`), so a
Python and a TypeScript process write and read the same events. Nested dicts are converted too,
for keys that look like names; other keys (IDs, paths) stay as they are.

    class TenantCreated(Event):
        slug: str
        status: Literal["active", "pending"]

    @tenant_aggregate.apply(TenantCreated)
    def _(state, d: TenantCreated):
        state["slug"] = d.slug
"""

from __future__ import annotations

import dataclasses
import re
from types import SimpleNamespace
from typing import Any, Callable, ClassVar, Generic, TypeVar

_CAMEL_KEY = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+$")
_SNAKE_KEY = re.compile(r"^[a-z][a-z0-9]*([A-Z][a-z0-9]*)+$")
_TYPE = re.compile(r"^[A-Z][A-Za-z0-9_]+$")


def camel(name: str) -> str:
    """`account_id` → `accountId`; anything that is no snake_case name stays as it is."""
    return re.sub(r"_([a-z0-9])", lambda m: m.group(1).upper(), name) if _CAMEL_KEY.match(name) else name


def snake(name: str) -> str:
    """`accountId` → `account_id`; anything that is no camelCase name stays as it is."""
    return re.sub(r"([A-Z])", lambda m: "_" + m.group(1).lower(), name) if _SNAKE_KEY.match(name) else name


def to_wire(value: Any) -> Any:
    """A Python value as the log holds it: messages and dicts with camelCase keys."""
    if isinstance(value, Message):
        return value.to_wire()
    if isinstance(value, dict):
        return {camel(k) if isinstance(k, str) else k: to_wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_wire(v) for v in value]
    return value


def from_wire(value: Any) -> Any:
    """A value of the log as Python holds it: dicts with snake_case keys."""
    if isinstance(value, dict):
        return {snake(k) if isinstance(k, str) else k: from_wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [from_wire(v) for v in value]
    return value


class Message:
    """The base of `Event`, `Command` and `Query`: every subclass becomes a keyword-only dataclass."""

    type: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        dataclasses.dataclass(kw_only=True)(cls)

    @classmethod
    def type_name(cls) -> str:
        """The type: `type` set on the class itself, else the class name."""
        name = cls.__dict__.get("type", cls.__name__)
        if not isinstance(name, str) or not _TYPE.match(name):
            raise ValueError(f"{name!r} is no message type: name the class like one, or set type = '...'")
        return name

    def to_wire(self) -> dict:
        """The fields with camelCase names, the way the log holds them; an unset optional field is left out."""
        out = {}
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value is None and f.default is None:
                continue
            out[camel(f.name)] = value if f.name in (getattr(self, "raw", None) or ()) else to_wire(value)
        return out

    @classmethod
    def from_wire(cls, data: Any) -> "Message":
        """An instance from a payload of the log; a field it lacks (an older event, an erased value) is None."""
        if not isinstance(data, dict):
            data = {}
        raw = set(getattr(cls, "raw", None) or ())
        values = {snake(k): (v if snake(k) in raw else from_wire(v)) for k, v in data.items()}
        return cls(**{f.name: values.get(f.name) for f in dataclasses.fields(cls) if f.init})


class Event(Message):
    """What happened, in the past tense; see the module's docstring."""


class Command(Message):
    """What should happen, imperative; see the module's docstring."""


R = TypeVar("R")


class Query(Message, Generic[R]):
    """What is asked, with `R` its answer: `class GetTenant(Query[TenantRow])`."""


# --- Where a class is used: its domain, and for a command its aggregate type --------------------

_BINDING = "__kernable_binding__"


def bind(cls: type, domain: str, schemas: Any, aggregate_type: str | None = None) -> None:
    """Records where `cls` is used; a class belongs to one domain (and one aggregate type)."""
    own = cls.__dict__.get(_BINDING)
    if own and (own.domain != domain or (aggregate_type and own.aggregate_type and own.aggregate_type != aggregate_type)):
        raise ValueError(f"{cls.__name__} is used by {own.domain} already")
    setattr(cls, _BINDING, SimpleNamespace(domain=domain, schemas=schemas, aggregate_type=aggregate_type or (own.aggregate_type if own else None)))


def binding(cls: type, what: str) -> SimpleNamespace:
    """Where `cls` is used, or an error naming what to do."""
    b = cls.__dict__.get(_BINDING)
    if b is None:
        raise ValueError(f"{cls.__name__} is not {what} yet: import the module that registers it first")
    return b


def is_message_class(value: Any, base: type = Message) -> bool:
    return isinstance(value, type) and issubclass(value, base)


def _snake_subject(subject: Callable) -> Callable:
    """A subject function written for snake_case data, given the log's camelCase payload."""
    return lambda data, aggregate_id: subject(from_wire(data), aggregate_id)


def declare_event(cls: type, events: dict, make_schema: Callable[[str, int], Any]) -> str:
    """Declares an event class's schema (version, upcasts, vault fields) unless declared by name."""
    name = cls.type_name()
    if name not in events:
        spec = make_schema(name, int(getattr(cls, "version", 1)))
        for start, fn in sorted((getattr(cls, "upcasts", None) or {}).items()):
            spec.upcast(int(start), (lambda f: lambda d: to_wire(f(from_wire(d))))(fn))
        vault = getattr(cls, "vault", None) or {}
        for p in vault.get("personal", []):
            spec.personal([camel(f) for f in p["fields"]], _snake_subject(p["subject"]))
        erases = vault.get("erases")
        if erases:
            spec.erases(_snake_subject(erases))
        events[name] = spec
    return name


def declare_command(cls: type, commands: dict, make_schema: Callable[[str], Any]) -> str:
    """Declares a command class's vault fields unless declared by name."""
    name = cls.type_name()
    if name not in commands:
        spec = make_schema(name)
        vault = getattr(cls, "vault", None) or {}
        for p in vault.get("personal", []):
            spec.personal([camel(f) for f in p["fields"]], _snake_subject(p["subject"]))
        secret = vault.get("secret")
        if secret:
            spec.secret([camel(f) for f in secret])
        commands[name] = spec
    return name
