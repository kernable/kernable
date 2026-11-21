package kit

// The account read models: accounts with their links and sessions, and the tenants' SSOs.

import (
	"fmt"
	"slices"
	"sync"

	kernable "github.com/kernable/kernable/sdks/go"
)

type AccountRow struct {
	ID     string        `json:"id"`
	Email  *string       `json:"email"`
	Name   *string       `json:"name"`
	Status AccountStatus `json:"status"`
}

type SessionRow struct {
	ID        string  `json:"id"`
	AccountID string  `json:"accountId"`
	Method    string  `json:"method"`
	IP        *string `json:"ip"`
	UserAgent *string `json:"userAgent"`
	OpenedAt  int64   `json:"openedAt"`
	LastAt    int64   `json:"lastAt"`
	Active    bool    `json:"active"`
}

type accountRows struct {
	sync.RWMutex
	rows    map[string]*AccountRow
	byEmail map[string]string
	// provider:subject → account ID
	byLink map[string]string
	// account ID → session ID → session
	sessions map[string]map[string]*SessionRow
}

func accountsReadModel(d *kernable.Domain) (*kernable.Listener, *accountRows) {
	t := &accountRows{rows: map[string]*AccountRow{}, byEmail: map[string]string{}, byLink: map[string]string{}, sessions: map[string]map[string]*SessionRow{}}
	l := d.ReadModel("account.accounts", kernable.Order(OrderReadModels))
	setStatus := func(id string, status AccountStatus) {
		if a, ok := t.rows[id]; ok {
			a.Status = status
		}
	}
	on(l, t, "AccountRegistered", func(c *lc, e AccountRegistered) {
		id := c.Event.AggregateID
		status := AccountStatusPending
		if e.Verified {
			status = AccountStatusActive
		}
		if e.Email != nil {
			t.byEmail[*e.Email] = id
		}
		t.rows[id] = &AccountRow{ID: id, Email: e.Email, Name: e.Name, Status: status}
	})
	on(l, t, "EmailVerified", func(c *lc, _ EmailVerified) {
		if a, ok := t.rows[c.Event.AggregateID]; ok && a.Status == AccountStatusPending {
			a.Status = AccountStatusActive
		}
	})
	// The new address finds the account, the old one no more.
	on(l, t, "EmailChanged", func(c *lc, e EmailChanged) {
		if a, ok := t.rows[c.Event.AggregateID]; ok {
			if a.Email != nil {
				delete(t.byEmail, *a.Email)
			}
			a.Email = e.Email
			if e.Email != nil {
				t.byEmail[*e.Email] = c.Event.AggregateID
			}
		}
	})
	on(l, t, "ProviderLinked", func(c *lc, e ProviderLinked) {
		if e.Subject != nil {
			t.byLink[fmt.Sprintf("%s:%s", e.Provider, *e.Subject)] = c.Event.AggregateID
		}
	})
	on(l, t, "ProviderUnlinked", func(_ *lc, e ProviderUnlinked) {
		if e.Subject != nil {
			delete(t.byLink, fmt.Sprintf("%s:%s", e.Provider, *e.Subject))
		}
	})
	on(l, t, "AccountSuspended", func(c *lc, _ AccountSuspended) { setStatus(c.Event.AggregateID, AccountStatusSuspended) })
	on(l, t, "AccountReactivated", func(c *lc, _ AccountReactivated) { setStatus(c.Event.AggregateID, AccountStatusActive) })
	// A deleted account leaves the read side; its email is free.
	on(l, t, "AccountDeleted", func(c *lc, _ AccountDeleted) {
		if a, ok := t.rows[c.Event.AggregateID]; ok {
			delete(t.rows, c.Event.AggregateID)
			if a.Email != nil {
				delete(t.byEmail, *a.Email)
			}
		}
		delete(t.sessions, c.Event.AggregateID)
	})
	on(l, t, "SessionOpened", func(c *lc, e SessionOpened) {
		m, ok := t.sessions[e.AccountID]
		if !ok {
			m = map[string]*SessionRow{}
			t.sessions[e.AccountID] = m
		}
		m[c.Event.AggregateID] = &SessionRow{ID: c.Event.AggregateID, AccountID: e.AccountID, Method: e.Method, IP: e.IP,
			UserAgent: e.UserAgent, OpenedAt: e.At, LastAt: e.At, Active: true}
	})
	on(l, t, "SessionRefreshed", func(c *lc, e SessionRefreshed) {
		if s, ok := t.sessions[e.AccountID][c.Event.AggregateID]; ok {
			s.LastAt, s.IP = e.At, e.IP
		}
	})
	on(l, t, "SessionEnded", func(c *lc, _ SessionEnded) {
		for _, m := range t.sessions {
			if s, ok := m[c.Event.AggregateID]; ok {
				s.Active = false
			}
		}
	})
	return l, t
}

// --- A tenant's single sign-on ----------------------------------------------------------------

type SsoDomainRow struct {
	Domain   string `json:"domain"`
	Txt      string `json:"txt"`
	Verified bool   `json:"verified"`
}

type SsoRow struct {
	ID       string `json:"id"`
	Tenant   string `json:"tenant"`
	Status   string `json:"status"`
	Issuer   string `json:"issuer"`
	ClientID string `json:"clientId"`
	// Read with vault_read; only the API gets it (GetSsoSecret).
	ClientSecret *string        `json:"clientSecret"`
	Domains      []SsoDomainRow `json:"domains"`
	Enforced     bool           `json:"enforced"`
}

// ssoRows is tenant/ssoId → the SSO.
type ssoRows struct {
	sync.RWMutex
	rows map[string]*SsoRow
}

func ssoKey(tenant, ssoID string) string { return tenant + "/" + ssoID }

func ssosReadModel(d *kernable.Domain) (*kernable.Listener, *ssoRows) {
	t := &ssoRows{rows: map[string]*SsoRow{}}
	l := d.ReadModel("account.ssos", kernable.Order(OrderReadModels))
	row := func(c *lc, f func(*SsoRow)) {
		if r, ok := t.rows[ssoKey(c.Event.Tenant, c.Event.AggregateID)]; ok {
			f(r)
		}
	}
	on(l, t, "SsoCreated", func(c *lc, e SsoCreated) {
		t.rows[ssoKey(c.Event.Tenant, c.Event.AggregateID)] = &SsoRow{ID: c.Event.AggregateID, Tenant: c.Event.Tenant, Status: "active",
			Issuer: e.Issuer, ClientID: e.ClientID, ClientSecret: e.ClientSecret, Domains: []SsoDomainRow{}}
	})
	on(l, t, "SsoUpdated", func(c *lc, e SsoUpdated) {
		row(c, func(r *SsoRow) { r.Issuer, r.ClientID, r.ClientSecret = e.Issuer, e.ClientID, e.ClientSecret })
	})
	on(l, t, "SsoDomainAdded", func(c *lc, e SsoDomainAdded) {
		row(c, func(r *SsoRow) { r.Domains = append(r.Domains, SsoDomainRow{Domain: e.Domain, Txt: e.Txt}) })
	})
	on(l, t, "SsoDomainVerified", func(c *lc, e SsoDomainVerified) {
		row(c, func(r *SsoRow) {
			for i := range r.Domains {
				if r.Domains[i].Domain == e.Domain {
					r.Domains[i].Verified = true
				}
			}
		})
	})
	on(l, t, "SsoDomainRemoved", func(c *lc, e SsoDomainRemoved) {
		row(c, func(r *SsoRow) {
			r.Domains = slices.DeleteFunc(r.Domains, func(x SsoDomainRow) bool { return x.Domain == e.Domain })
		})
	})
	on(l, t, "SsoEnforcementChanged", func(c *lc, e SsoEnforcementChanged) { row(c, func(r *SsoRow) { r.Enforced = e.Enforced }) })
	on(l, t, "SsoDisabled", func(c *lc, _ SsoDisabled) { row(c, func(r *SsoRow) { r.Status = "disabled" }) })
	on(l, t, "SsoEnabled", func(c *lc, _ SsoEnabled) { row(c, func(r *SsoRow) { r.Status = "active" }) })
	on(l, t, "SsoDeleted", func(c *lc, _ SsoDeleted) { delete(t.rows, ssoKey(c.Event.Tenant, c.Event.AggregateID)) })
	on(l, t, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) {
		for k, r := range t.rows {
			if r.Tenant == c.Event.AggregateID {
				delete(t.rows, k)
			}
		}
	})
	return l, t
}
