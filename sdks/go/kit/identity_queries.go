package kit

// The identity queries: each type with its handler.

import kernable "github.com/kernable/kernable/sdks/go"

type GetIdentity struct {
	IdentityID string `json:"identityId"`
}

type ListIdentities struct{}

type MyIdentity struct {
	Tenant     string         `json:"tenant"`
	IdentityID string         `json:"identityId"`
	Status     IdentityStatus `json:"status"`
}

// ListMyIdentities is every tenant the account has an identity in.
type ListMyIdentities struct {
	AccountID string `json:"accountId"`
}

type FoundIdentity struct {
	IdentityID string         `json:"identityId"`
	Status     IdentityStatus `json:"status"`
}

// FindIdentityByAccount is the API's only: it sets SenderIdentity from it.
type FindIdentityByAccount struct {
	AccountID string `json:"accountId"`
}

// FindApiKey is the API's only: what it checks an API key against.
type FindApiKey struct {
	KeyID string `json:"keyId"`
}

func answerIdentity(d *kernable.Domain, l *kernable.Listener, t *identityRows) {
	d.Query("GetIdentity").Target("identity.identity", "identityId")
	d.Query("ListMyIdentities").Target("account.account", "accountId")
	kernable.Answer(l, "GetIdentity", func(c *qc, q GetIdentity) (*IdentityRow, error) {
		t.RLock()
		defer t.RUnlock()
		r, ok := t.rows.get(rowKey(c.Tenant, q.IdentityID))
		if !ok {
			return nil, notFound("No such identity")
		}
		row := copyIdentity(r)
		return &row, nil
	})
	kernable.Answer(l, "ListIdentities", func(c *qc, _ ListIdentities) ([]IdentityRow, error) {
		t.RLock()
		defer t.RUnlock()
		rows := []IdentityRow{}
		for _, r := range t.rows.values() {
			if r.Tenant == c.Tenant {
				rows = append(rows, copyIdentity(r))
			}
		}
		return rows, nil
	})
	kernable.Answer(l, "ListMyIdentities", func(_ *qc, q ListMyIdentities) ([]MyIdentity, error) {
		t.RLock()
		defer t.RUnlock()
		rows := []MyIdentity{}
		for _, r := range t.rows.values() {
			if r.AccountID != nil && *r.AccountID == q.AccountID {
				rows = append(rows, MyIdentity{Tenant: r.Tenant, IdentityID: r.ID, Status: r.Status})
			}
		}
		return rows, nil
	})
	kernable.Answer(l, "FindIdentityByAccount", func(c *qc, q FindIdentityByAccount) (*FoundIdentity, error) {
		t.RLock()
		defer t.RUnlock()
		for _, r := range t.rows.values() {
			if r.Tenant == c.Tenant && r.AccountID != nil && *r.AccountID == q.AccountID {
				return &FoundIdentity{IdentityID: r.ID, Status: r.Status}, nil
			}
		}
		return nil, notFound("No identity in this tenant")
	})
	kernable.Answer(l, "FindApiKey", func(_ *qc, q FindApiKey) (*ApiKeyRow, error) {
		t.RLock()
		defer t.RUnlock()
		k, ok := t.keys[q.KeyID]
		if !ok {
			return nil, notFound("No such key")
		}
		c := *k
		return &c, nil
	})
}
