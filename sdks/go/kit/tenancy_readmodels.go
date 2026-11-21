package kit

// The tenancy read models: one row per tenant; a deleted tenant leaves the read side.

import (
	"slices"
	"sync"

	kernable "github.com/kernable/kernable/sdks/go"
)

type TenantRow struct {
	ID          string       `json:"id"`
	Slug        string       `json:"slug"`
	Name        string       `json:"name"`
	Status      TenantStatus `json:"status"`
	Suspension  *Suspension  `json:"suspension"`
	MfaRequired bool         `json:"mfaRequired"`
	Owners      []string     `json:"owners"`
}

// tenantRows is what the read model holds: the rows by tenant ID, and the platform's sign-up
// policy.
type tenantRows struct {
	sync.RWMutex
	rows   map[string]*TenantRow
	signUp SignUpPolicy
}

type lc = kernable.ListenerContext

// on registers a read model's handler that changes its state under the write lock.
func on[D any](l *kernable.Listener, mu sync.Locker, eventType string, fn func(c *lc, d D)) {
	kernable.On(l, eventType, func(c *lc, d D) error {
		mu.Lock()
		defer mu.Unlock()
		fn(c, d)
		return nil
	})
}

func tenantsReadModel(d *kernable.Domain) (*kernable.Listener, *tenantRows) {
	t := &tenantRows{rows: map[string]*TenantRow{}, signUp: defaultSignUp()}
	l := d.ReadModel("tenancy.tenants", kernable.Order(OrderReadModels))
	row := func(id string, f func(*TenantRow)) {
		if r, ok := t.rows[id]; ok {
			f(r)
		}
	}
	on(l, t, "TenantCreated", func(c *lc, e TenantCreated) {
		id := c.Event.AggregateID
		t.rows[id] = &TenantRow{ID: id, Slug: e.Slug, Name: e.Name, Status: e.Status, Owners: []string{}}
	})
	on(l, t, "TenantActivated", func(c *lc, _ TenantActivated) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.Status = TenantStatusActive })
	})
	on(l, t, "TenantRenamed", func(c *lc, e TenantRenamed) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.Name = e.Name })
	})
	on(l, t, "TenantSlugChanged", func(c *lc, e TenantSlugChanged) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.Slug = e.Slug })
	})
	on(l, t, "TenantSuspended", func(c *lc, e TenantSuspended) {
		row(c.Event.AggregateID, func(r *TenantRow) {
			r.Status, r.Suspension = TenantStatusSuspended, &Suspension{Reason: e.Reason, At: e.At}
		})
	})
	on(l, t, "TenantReactivated", func(c *lc, _ TenantReactivated) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.Status, r.Suspension = TenantStatusActive, nil })
	})
	on(l, t, "TenantDeleted", func(c *lc, _ TenantDeleted) { delete(t.rows, c.Event.AggregateID) })
	on(l, t, "OwnerAdded", func(c *lc, e OwnerAdded) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.Owners = append(r.Owners, e.IdentityID) })
	})
	on(l, t, "OwnerRemoved", func(c *lc, e OwnerRemoved) {
		row(c.Event.AggregateID, func(r *TenantRow) {
			r.Owners = slices.DeleteFunc(r.Owners, func(o string) bool { return o == e.IdentityID })
		})
	})
	on(l, t, "MfaRequirementChanged", func(c *lc, e MfaRequirementChanged) {
		row(c.Event.AggregateID, func(r *TenantRow) { r.MfaRequired = e.Required })
	})
	on(l, t, "SignUpPolicyChanged", func(_ *lc, e SignUpPolicyChanged) {
		t.signUp = SignUpPolicy{Tenants: e.Tenants, Accounts: e.Accounts}
	})
	return l, t
}
