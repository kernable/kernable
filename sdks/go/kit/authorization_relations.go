package kit

// Relations derived from events: who stands in which relation to an aggregate. Declare them next
// to your aggregates, before the kit's domains are built:
//
//	kit.Relation("shop.order", "owner").Set("shop.OrderPlaced", func(e kit.RelationEvent) []string { return kit.IDs(e.Data["customer"]) })
//	kit.Parent("funds.vehicle").From("funds.VehicleCreated", func(e kit.RelationEvent) *kit.Target { … })
//
// e.Data is the event's payload as the log holds it (camelCase); e.AggregateID and e.Tenant
// name the aggregate.

import (
	"fmt"
	"slices"
	"strings"
	"sync"
)

// Target is an aggregate a relation or a scope points at: {type: "shop.order", id}.
type Target struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// RelationEvent is what a relation's or parent's function gets of an event.
type RelationEvent struct {
	Data        map[string]any
	AggregateID string
	Tenant      string
}

// IDs is the identity IDs in a payload field: one ID, a list of them, or none.
func IDs(value any) []string {
	switch v := value.(type) {
	case string:
		if v != "" {
			return []string{v}
		}
	case []any:
		var out []string
		for _, x := range v {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

type relationRule struct {
	typ, name, event string
	set              bool
	ids              func(RelationEvent) []string
}

type parentRule struct {
	typ, event string
	parent     func(RelationEvent) *Target
}

var (
	rulesMu   sync.RWMutex
	relations []relationRule
	parents   []parentRule
)

func qualifiedName(t string) bool {
	d, n, ok := strings.Cut(t, ".")
	return ok && d != "" && d[0] >= 'a' && d[0] <= 'z' && namePart(d) && namePart(n)
}

func checkRule(typ, event string) {
	if !qualifiedName(typ) {
		panic(fmt.Sprintf("a relation's aggregate type is qualified (shop.order): %s", typ))
	}
	if !qualifiedName(event) {
		panic(fmt.Sprintf("a relation's event type is qualified (shop.OrderPlaced): %s", event))
	}
}

// RelationOf is a relation of an aggregate type, set and unset by events: see Relation.
type RelationOf struct{ typ, name string }

func (r RelationOf) rule(event string, set bool, ids func(RelationEvent) []string) RelationOf {
	checkRule(r.typ, event)
	rulesMu.Lock()
	defer rulesMu.Unlock()
	relations = append(relations, relationRule{typ: r.typ, name: r.name, event: event, set: set, ids: ids})
	return r
}

// Set: ids(event) gain the relation.
func (r RelationOf) Set(event string, ids func(RelationEvent) []string) RelationOf {
	return r.rule(event, true, ids)
}

// Unset: ids(event) lose it.
func (r RelationOf) Unset(event string, ids func(RelationEvent) []string) RelationOf {
	return r.rule(event, false, ids)
}

// Relation declares relation name of aggregates of typ (qualified: shop.order), set and unset by
// events of that aggregate. Panics if name is not lowercase, or is built in (self, owners).
func Relation(typ, name string) RelationOf {
	lower := name != "" && name[0] >= 'a' && name[0] <= 'z'
	for _, c := range name {
		lower = lower && (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_')
	}
	if !lower {
		panic("a relation's name is lowercase: " + name)
	}
	if name == "self" || name == "owners" {
		panic(name + " is built in")
	}
	return RelationOf{typ: typ, name: name}
}

// ParentOf is the parent of an aggregate type, for permissions ViaParent: see Parent.
type ParentOf struct{ typ string }

// From derives the parent from event.
func (p ParentOf) From(event string, parent func(RelationEvent) *Target) {
	checkRule(p.typ, event)
	rulesMu.Lock()
	defer rulesMu.Unlock()
	parents = append(parents, parentRule{typ: p.typ, event: event, parent: parent})
}

// Parent declares the parent of aggregates of typ.
func Parent(typ string) ParentOf { return ParentOf{typ: typ} }

func relationRules() []relationRule {
	rulesMu.RLock()
	defer rulesMu.RUnlock()
	return slices.Clone(relations)
}

func parentRules() []parentRule {
	rulesMu.RLock()
	defer rulesMu.RUnlock()
	return slices.Clone(parents)
}

// relationEvents is every event type a relation or parent is derived from.
func relationEvents() []string {
	var all []string
	for _, r := range relationRules() {
		all = append(all, r.event)
	}
	for _, p := range parentRules() {
		all = append(all, p.event)
	}
	slices.Sort(all)
	return slices.Compact(all)
}
