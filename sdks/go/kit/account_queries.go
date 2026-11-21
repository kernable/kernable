package kit

// The account queries: each type with its handler.

import (
	"fmt"
	"slices"
	"strings"

	kernable "github.com/kernable/kernable/sdks/go"
)

func notFound(message string) error { return rejected(message, "not_found") }

type Found struct {
	ID     string        `json:"id"`
	Status AccountStatus `json:"status"`
}

// FindAccountByEmail is the API's only: an open lookup would reveal who has an account.
type FindAccountByEmail struct {
	Email string `json:"email"`
}

type GetAccount struct {
	AccountID string `json:"accountId"`
}

// ListSessions is the account's sessions, the open ones first: "my devices".
type ListSessions struct {
	AccountID string `json:"accountId"`
}

// FindAccountByLink is the API's only: the account an external login is linked to.
type FindAccountByLink struct {
	Provider string `json:"provider"`
	Subject  string `json:"subject"`
}

type SsoFound struct {
	Tenant   string `json:"tenant"`
	SsoID    string `json:"ssoId"`
	Issuer   string `json:"issuer"`
	ClientID string `json:"clientId"`
	Enforced bool   `json:"enforced"`
}

// FindSsoByDomain is the API's only: the active SSO that verified the domain (any tenant).
type FindSsoByDomain struct {
	Domain string `json:"domain"`
}

type SsoSecret struct {
	Issuer       string  `json:"issuer"`
	ClientID     string  `json:"clientId"`
	ClientSecret *string `json:"clientSecret"`
	// The verified domains whose people it may sign in.
	Domains []string `json:"domains"`
}

// GetSsoSecret is the API's only: what it signs in with, the client secret included.
type GetSsoSecret struct {
	SsoID string `json:"ssoId"`
}

// GetSso is the tenant's owners': the configuration without the secret, with the TXT values to
// publish.
type GetSso struct {
	SsoID string `json:"ssoId"`
}

// SsoView is an SSO without its secret.
type SsoView struct {
	ID       string         `json:"id"`
	Tenant   string         `json:"tenant"`
	Status   string         `json:"status"`
	Issuer   string         `json:"issuer"`
	ClientID string         `json:"clientId"`
	Domains  []SsoDomainRow `json:"domains"`
	Enforced bool           `json:"enforced"`
}

func answerAccount(d *kernable.Domain, accounts *kernable.Listener, t *accountRows, ssos *kernable.Listener, s *ssoRows) {
	d.Query("GetAccount").Target("account.account", "accountId")
	d.Query("ListSessions").Target("account.account", "accountId")
	found := func(id string, ok bool, message string) (*Found, error) {
		if !ok {
			return nil, notFound(message)
		}
		a, ok := t.rows[id]
		if !ok {
			return nil, notFound(message)
		}
		return &Found{ID: a.ID, Status: a.Status}, nil
	}
	kernable.Answer(accounts, "FindAccountByEmail", func(_ *qc, q FindAccountByEmail) (*Found, error) {
		t.RLock()
		defer t.RUnlock()
		id, ok := t.byEmail[NormalEmail(q.Email)]
		return found(id, ok, "No such account")
	})
	kernable.Answer(accounts, "GetAccount", func(_ *qc, q GetAccount) (*AccountRow, error) {
		t.RLock()
		defer t.RUnlock()
		a, ok := t.rows[q.AccountID]
		if !ok {
			return nil, notFound("No such account")
		}
		c := *a
		return &c, nil
	})
	kernable.Answer(accounts, "ListSessions", func(_ *qc, q ListSessions) ([]SessionRow, error) {
		t.RLock()
		defer t.RUnlock()
		m := t.sessions[q.AccountID]
		rows := []SessionRow{}
		for _, id := range sortedRowKeys(m) {
			rows = append(rows, *m[id])
		}
		slices.SortStableFunc(rows, func(a, b SessionRow) int {
			if a.Active != b.Active {
				if a.Active {
					return -1
				}
				return 1
			}
			switch {
			case a.LastAt > b.LastAt:
				return -1
			case a.LastAt < b.LastAt:
				return 1
			}
			return 0
		})
		return rows, nil
	})
	kernable.Answer(accounts, "FindAccountByLink", func(_ *qc, q FindAccountByLink) (*Found, error) {
		t.RLock()
		defer t.RUnlock()
		id, ok := t.byLink[fmt.Sprintf("%s:%s", q.Provider, q.Subject)]
		return found(id, ok, "Not linked")
	})
	kernable.Answer(ssos, "FindSsoByDomain", func(_ *qc, q FindSsoByDomain) (*SsoFound, error) {
		domain := strings.ToLower(q.Domain)
		s.RLock()
		defer s.RUnlock()
		for _, k := range sortedRowKeys(s.rows) {
			r := s.rows[k]
			if r.Status == "active" && slices.ContainsFunc(r.Domains, func(x SsoDomainRow) bool { return x.Domain == domain && x.Verified }) {
				return &SsoFound{Tenant: r.Tenant, SsoID: r.ID, Issuer: r.Issuer, ClientID: r.ClientID, Enforced: r.Enforced}, nil
			}
		}
		return nil, notFound("No SSO for this domain")
	})
	kernable.Answer(ssos, "GetSsoSecret", func(c *qc, q GetSsoSecret) (*SsoSecret, error) {
		s.RLock()
		defer s.RUnlock()
		r, ok := s.rows[ssoKey(c.Tenant, q.SsoID)]
		if !ok {
			return nil, notFound("No such SSO")
		}
		out := &SsoSecret{Issuer: r.Issuer, ClientID: r.ClientID, ClientSecret: r.ClientSecret, Domains: []string{}}
		for _, x := range r.Domains {
			if x.Verified {
				out.Domains = append(out.Domains, x.Domain)
			}
		}
		return out, nil
	})
	kernable.Answer(ssos, "GetSso", func(c *qc, q GetSso) (*SsoView, error) {
		s.RLock()
		defer s.RUnlock()
		r, ok := s.rows[ssoKey(c.Tenant, q.SsoID)]
		if !ok {
			return nil, notFound("No such SSO")
		}
		return &SsoView{ID: r.ID, Tenant: r.Tenant, Status: r.Status, Issuer: r.Issuer, ClientID: r.ClientID,
			Domains: slices.Clone(r.Domains), Enforced: r.Enforced}, nil
	})
}
