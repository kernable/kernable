package kit

// The tenancy queries: each type with its handler.

import (
	"slices"
	"strings"

	kernable "github.com/kernable/kernable/sdks/go"
)

type qc = kernable.QueryContext

type GetTenant struct {
	TenantID string `json:"tenantId"`
}

// GetTenantBySlug is for subdomains and login pages, through the API only: nothing open lists
// tenants.
type GetTenantBySlug struct {
	Slug string `json:"slug"`
}

type TenantRef struct {
	ID     string       `json:"id"`
	Name   string       `json:"name"`
	Status TenantStatus `json:"status"`
}

type ListTenants struct {
	Status *TenantStatus `json:"status,omitempty"`
	// The last slug of the previous page.
	After *string `json:"after,omitempty"`
	Limit *uint   `json:"limit,omitempty"`
}

type TenantPage struct {
	Tenants []TenantRow `json:"tenants"`
	More    bool        `json:"more"`
}

type GetSignUpPolicy struct{}

func tenantNotFound() error { return rejected("No such tenant", "not_found") }

func answerTenancy(d *kernable.Domain, l *kernable.Listener, t *tenantRows) {
	d.Query("GetTenant").Target("tenancy.tenant", "tenantId")
	kernable.Answer(l, "GetTenant", func(_ *qc, q GetTenant) (*TenantRow, error) {
		t.RLock()
		defer t.RUnlock()
		r, ok := t.rows[q.TenantID]
		if !ok {
			return nil, tenantNotFound()
		}
		c := *r
		return &c, nil
	})
	kernable.Answer(l, "GetTenantBySlug", func(_ *qc, q GetTenantBySlug) (*TenantRef, error) {
		t.RLock()
		defer t.RUnlock()
		for _, id := range sortedRowKeys(t.rows) {
			if r := t.rows[id]; r.Slug == q.Slug {
				return &TenantRef{ID: r.ID, Name: r.Name, Status: r.Status}, nil
			}
		}
		return nil, tenantNotFound()
	})
	kernable.Answer(l, "ListTenants", func(_ *qc, q ListTenants) (TenantPage, error) {
		limit := uint(50)
		if q.Limit != nil {
			limit = min(max(*q.Limit, 1), 500)
		}
		t.RLock()
		defer t.RUnlock()
		var rows []TenantRow
		for _, r := range t.rows {
			if (q.Status == nil || r.Status == *q.Status) && (q.After == nil || r.Slug > *q.After) {
				rows = append(rows, *r)
			}
		}
		slices.SortFunc(rows, func(a, b TenantRow) int { return strings.Compare(a.Slug, b.Slug) })
		page := TenantPage{More: uint(len(rows)) > limit, Tenants: rows[:min(uint(len(rows)), limit)]}
		if page.Tenants == nil {
			page.Tenants = []TenantRow{}
		}
		return page, nil
	})
	kernable.Answer(l, "GetSignUpPolicy", func(_ *qc, _ GetSignUpPolicy) (SignUpPolicy, error) {
		t.RLock()
		defer t.RUnlock()
		return t.signUp, nil
	})
}

// sortedRowKeys is the keys of a map in order (as a BTreeMap iterates).
func sortedRowKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
