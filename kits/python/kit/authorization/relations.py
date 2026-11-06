"""Relations derived from events: who stands in which relation to an aggregate. Declare
them next to your aggregates, before the kit's authorization listener starts:

    relation("shop.order", "owner").set("shop.OrderPlaced", lambda e: e.data["customer"])
    parent("funds.vehicle").from_("funds.VehicleCreated", lambda e: {"type": "funds.fund", "id": e.data["fund"]})

`e.data` is the event's payload with snake_case keys; `e.aggregate_id`, `e.tenant` name the aggregate."""

import re
from dataclasses import dataclass, field
from typing import Any, Callable

_QUALIFIED = re.compile(r"^[a-z][\w-]*\.[\w-]+$")


@dataclass
class RelationEvent:
    data: dict
    aggregate_id: str
    tenant: str


@dataclass
class RelationRule:
    type: str
    name: str
    event: str
    op: str
    ids: Callable[[RelationEvent], Any]


@dataclass
class ParentRule:
    type: str
    event: str
    parent: Callable[[RelationEvent], Any]


relation_rules: list[RelationRule] = []
parent_rules: list[ParentRule] = []


def _check(type_: str, event: str) -> None:
    if not _QUALIFIED.match(type_):
        raise ValueError(f"a relation's aggregate type is qualified (shop.order): {type_}")
    if not _QUALIFIED.match(event):
        raise ValueError(f"a relation's event type is qualified (shop.OrderPlaced): {event}")


@dataclass
class _Relation:
    type: str
    name: str

    def set(self, event: str, ids: Callable[[RelationEvent], Any]) -> "_Relation":
        """`ids(event)` gains the relation: one identity ID, a list, or none."""
        _check(self.type, event)
        relation_rules.append(RelationRule(self.type, self.name, event, "set", ids))
        return self

    def unset(self, event: str, ids: Callable[[RelationEvent], Any]) -> "_Relation":
        """`ids(event)` loses it."""
        _check(self.type, event)
        relation_rules.append(RelationRule(self.type, self.name, event, "unset", ids))
        return self


def relation(type_: str, name: str) -> _Relation:
    """Declares relation `name` of aggregates of `type_`, set and unset by events of that aggregate."""
    if not re.fullmatch(r"[a-z]\w*", name):
        raise ValueError(f"a relation's name is lowercase: {name}")
    if name in ("self", "owners"):
        raise ValueError(f"{name} is built in")
    return _Relation(type_, name)


@dataclass
class _Parent:
    type: str

    def from_(self, event: str, parent: Callable[[RelationEvent], Any]) -> None:
        _check(self.type, event)
        parent_rules.append(ParentRule(self.type, event, parent))


def parent(type_: str) -> _Parent:
    """Declares the parent of aggregates of `type_`, for permissions `via_parent`."""
    return _Parent(type_)


def relation_events() -> list[str]:
    """Every event type a relation or parent is derived from."""
    return sorted({r.event for r in relation_rules} | {p.event for p in parent_rules})
