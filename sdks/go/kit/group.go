package kit

// group: a named set of permissions per tenant (design/kits.md "group"). The owners group is
// virtual: owners live on the Tenant. The authorization middleware is a listener of this domain
// too.

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	kernable "github.com/kernable/kernable/sdks/go"
)

// Permission is a type pattern in the grant syntax (shop.PlaceOrder, shop.*, *), optionally tied
// to a relation of the target aggregate (When), which may hold on its parent (ViaParent).
type Permission struct {
	Pattern   string  `json:"pattern"`
	When      *string `json:"when,omitempty"`
	ViaParent *bool   `json:"viaParent,omitempty"`
}

// GroupDomain is the group domain with its parts.
type GroupDomain struct {
	Domain *kernable.Domain
	Group  *kernable.Aggregate[Group]
	Groups *kernable.Listener
	// The authorization middleware.
	Authorization *kernable.Listener
	rows          *groupRows
}

// NewGroupDomain is the group domain, every part registered.
func NewGroupDomain() *GroupDomain {
	d := kernable.NewDomain("group")
	g := &GroupDomain{Domain: d, Group: groupAggregate(d)}
	handleGroup(g.Group)
	g.Groups, g.rows = groupsReadModel(d)
	answerGroup(g.Groups, g.rows)
	g.Authorization = authorizationMiddleware(d)
	return g
}

// --- Events -------------------------------------------------------------------------------------

type GroupCreated struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Permissions []Permission `json:"permissions"`
}

// GroupUpdated holds only what changed.
type GroupUpdated struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

// GroupPermissionsSet holds the whole list in one event: simpler to read in the audit.
type GroupPermissionsSet struct {
	Permissions []Permission `json:"permissions"`
}

type GroupDeleted struct{}

// --- Aggregate ------------------------------------------------------------------------------------

type Group struct {
	Active      bool         `json:"active"`
	Deleted     bool         `json:"deleted"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Permissions []Permission `json:"permissions"`
}

func groupAggregate(d *kernable.Domain) *kernable.Aggregate[Group] {
	a := kernable.NewAggregate(d, "group", Group{}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "GroupCreated", func(_ *ac, s *Group, e GroupCreated) {
		*s = Group{Active: true, Name: e.Name, Description: e.Description, Permissions: e.Permissions}
	})
	kernable.Apply(a, "GroupUpdated", func(_ *ac, s *Group, e GroupUpdated) {
		if e.Name != nil {
			s.Name = *e.Name
		}
		if e.Description != nil {
			s.Description = *e.Description
		}
	})
	kernable.Apply(a, "GroupPermissionsSet", func(_ *ac, s *Group, e GroupPermissionsSet) { s.Permissions = e.Permissions })
	kernable.Apply(a, "GroupDeleted", func(_ *ac, s *Group, _ GroupDeleted) { s.Active, s.Deleted = false, true })
	a.Unique("name", func(s Group) []string {
		if !s.Active {
			return nil
		}
		return []string{strings.ToLower(TrimSpace(s.Name))}
	})
	a.Fact("active", func(s Group) bool { return s.Active })
	return a
}

// --- Commands --------------------------------------------------------------------------------------

func word(value string, first func(rune) bool) bool {
	if value == "" {
		return false
	}
	for i, c := range value {
		if i == 0 && !first(c) {
			return false
		}
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func lower(c rune) bool { return c >= 'a' && c <= 'z' }

func alpha(c rune) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// relationName is a relation's name: lowercase first, then letters, digits and _.
func relationName(value string) bool { return word(value, lower) && !strings.Contains(value, "-") }

// ValidPattern is *, domain.* or domain.Type.
func ValidPattern(p string) bool {
	if p == "*" {
		return true
	}
	domain, rest, ok := strings.Cut(p, ".")
	if !ok {
		return false
	}
	nameOK := rest == "*" || (word(rest, alpha) && !strings.Contains(rest, "-"))
	return word(domain, lower) && nameOK
}

// checkPermissions is the permissions, checked and normalised.
func checkPermissions(value []Permission) ([]Permission, error) {
	out := []Permission{}
	for _, p := range value {
		if !ValidPattern(p.Pattern) {
			return nil, rejected("Not a type pattern: "+p.Pattern, "invalid_permissions")
		}
		if p.When != nil && !relationName(*p.When) {
			return nil, rejected("Not a relation: "+*p.When, "invalid_permissions")
		}
		var when *string
		if p.When != nil && *p.When != "" {
			when = p.When
		}
		var via *bool
		if when != nil && p.ViaParent != nil && *p.ViaParent {
			via = ptr(true)
		}
		out = append(out, Permission{Pattern: p.Pattern, When: when, ViaParent: via})
	}
	return out, nil
}

func groupName(value string) (string, error) {
	trimmed := TrimSpace(value)
	if trimmed == "" || utf8.RuneCountInString(value) > 100 || strings.EqualFold(trimmed, "owners") && isASCII(trimmed) {
		return "", rejected("A group needs a name other than owners", "invalid_name")
	}
	return trimmed, nil
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// CreateGroup: the middleware lets the sender hand out only permissions it holds.
type CreateGroup struct {
	Name        string       `json:"name"`
	Description *string      `json:"description,omitempty"`
	Permissions []Permission `json:"permissions"`
}

type UpdateGroup struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type SetPermissions struct {
	Permissions []Permission `json:"permissions"`
}

// DeleteGroup: its memberships stop counting at once (the middleware); a reactor removes them later.
type DeleteGroup struct{}

func activeGroup(s Group) error {
	if s.Active {
		return nil
	}
	return rejected("No such group", "not_found")
}

func handleGroup(group *kernable.Aggregate[Group]) {
	kernable.Handle(group, "CreateGroup", func(_ *cc, s Group, d CreateGroup) (events, error) {
		if s.Active || s.Deleted {
			return nil, rejected("The group exists", "exists")
		}
		name, err := groupName(d.Name)
		if err != nil {
			return nil, err
		}
		permissions, err := checkPermissions(d.Permissions)
		if err != nil {
			return nil, err
		}
		return ev("GroupCreated", GroupCreated{Name: name, Description: deref(d.Description), Permissions: permissions}), nil
	})
	kernable.Handle(group, "UpdateGroup", func(_ *cc, s Group, d UpdateGroup) (events, error) {
		if err := activeGroup(s); err != nil {
			return nil, err
		}
		var name *string
		if d.Name != nil {
			n, err := groupName(*d.Name)
			if err != nil {
				return nil, err
			}
			name = &n
		}
		return ev("GroupUpdated", GroupUpdated{Name: name, Description: d.Description}), nil
	})
	kernable.Handle(group, "SetPermissions", func(_ *cc, s Group, d SetPermissions) (events, error) {
		if err := activeGroup(s); err != nil {
			return nil, err
		}
		permissions, err := checkPermissions(d.Permissions)
		if err != nil {
			return nil, err
		}
		return ev("GroupPermissionsSet", GroupPermissionsSet{Permissions: permissions}), nil
	})
	kernable.Handle(group, "DeleteGroup", func(_ *cc, s Group, _ DeleteGroup) (events, error) {
		if err := activeGroup(s); err != nil {
			return nil, err
		}
		return ev("GroupDeleted", GroupDeleted{}), nil
	})
}

// --- Read model and queries ----------------------------------------------------------------------

type GroupRow struct {
	ID          string       `json:"id"`
	Tenant      string       `json:"tenant"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Permissions []Permission `json:"permissions"`
}

// groupRows is tenant/groupId → the group.
type groupRows struct {
	sync.RWMutex
	rows *ordered[*GroupRow]
}

func groupsReadModel(d *kernable.Domain) (*kernable.Listener, *groupRows) {
	t := &groupRows{rows: newOrdered[*GroupRow]()}
	l := d.ReadModel("group.groups", kernable.Order(OrderReadModels))
	on(l, t, "GroupCreated", func(c *lc, e GroupCreated) {
		t.rows.set(rowKey(c.Event.Tenant, c.Event.AggregateID), &GroupRow{ID: c.Event.AggregateID, Tenant: c.Event.Tenant,
			Name: e.Name, Description: e.Description, Permissions: e.Permissions})
	})
	on(l, t, "GroupUpdated", func(c *lc, e GroupUpdated) {
		if g, ok := t.rows.get(rowKey(c.Event.Tenant, c.Event.AggregateID)); ok {
			if e.Name != nil {
				g.Name = *e.Name
			}
			if e.Description != nil {
				g.Description = *e.Description
			}
		}
	})
	on(l, t, "GroupPermissionsSet", func(c *lc, e GroupPermissionsSet) {
		if g, ok := t.rows.get(rowKey(c.Event.Tenant, c.Event.AggregateID)); ok {
			g.Permissions = e.Permissions
		}
	})
	on(l, t, "GroupDeleted", func(c *lc, _ GroupDeleted) { t.rows.remove(rowKey(c.Event.Tenant, c.Event.AggregateID)) })
	on(l, t, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) {
		t.rows.retain(func(_ string, g *GroupRow) bool { return g.Tenant != c.Event.AggregateID })
	})
	return l, t
}

// ListGroups is the tenant's groups, the virtual owners group first.
type ListGroups struct{}

type GetGroup struct {
	GroupID string `json:"groupId"`
}

func answerGroup(l *kernable.Listener, t *groupRows) {
	kernable.Answer(l, "ListGroups", func(c *qc, _ ListGroups) ([]json.RawMessage, error) {
		owners := json.RawMessage(`{"id":"owners","name":"owners","description":"The tenant's owners: every permission","permissions":[{"pattern":"*"}]}`)
		all := []json.RawMessage{owners}
		t.RLock()
		defer t.RUnlock()
		for _, g := range t.rows.values() {
			if g.Tenant == c.Tenant {
				b, err := json.Marshal(g)
				if err != nil {
					return nil, err
				}
				all = append(all, b)
			}
		}
		return all, nil
	})
	kernable.Answer(l, "GetGroup", func(c *qc, q GetGroup) (*GroupRow, error) {
		t.RLock()
		defer t.RUnlock()
		g, ok := t.rows.get(rowKey(c.Tenant, q.GroupID))
		if !ok {
			return nil, notFound("No such group")
		}
		row := *g
		row.Permissions = slices.Clone(g.Permissions)
		return &row, nil
	})
}

var _ = fmt.Sprint
