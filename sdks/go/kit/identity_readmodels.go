package kit

// The identity read models: identities with their memberships and API keys.

import (
	"slices"
	"sync"

	kernable "github.com/kernable/kernable/sdks/go"
)

type MembershipRow struct {
	MembershipID string           `json:"membershipId"`
	Group        string           `json:"group"`
	Scope        *Scope           `json:"scope"`
	ExpiresAt    *int64           `json:"expiresAt"`
	Status       MembershipStatus `json:"status"`
	GrantedBy    *string          `json:"grantedBy"`
	GrantedAt    int64            `json:"grantedAt"`
}

type KeyRow struct {
	KeyID     string `json:"keyId"`
	Label     string `json:"label"`
	ExpiresAt *int64 `json:"expiresAt"`
	Revoked   bool   `json:"revoked"`
}

type IdentityRow struct {
	ID          string          `json:"id"`
	Tenant      string          `json:"tenant"`
	Kind        IdentityKind    `json:"kind"`
	AccountID   *string         `json:"accountId"`
	DisplayName *string         `json:"displayName"`
	Status      IdentityStatus  `json:"status"`
	Memberships []MembershipRow `json:"memberships"`
	APIKeys     []KeyRow        `json:"apiKeys"`
}

// ApiKeyRow is what the API checks a key against (FindApiKey).
type ApiKeyRow struct {
	Tenant     string `json:"tenant"`
	IdentityID string `json:"identityId"`
	Hash       string `json:"hash"`
	ExpiresAt  *int64 `json:"expiresAt"`
	Revoked    bool   `json:"revoked"`
}

type identityRows struct {
	sync.RWMutex
	// tenant/identityId
	rows *ordered[*IdentityRow]
	// keyId → the key
	keys map[string]*ApiKeyRow
}

func rowKey(tenant, identityID string) string { return tenant + "/" + identityID }

func (t *identityRows) row(c *lc) *IdentityRow {
	r, _ := t.rows.get(rowKey(c.Event.Tenant, c.Event.AggregateID))
	return r
}

func identitiesReadModel(d *kernable.Domain) (*kernable.Listener, *identityRows) {
	t := &identityRows{rows: newOrdered[*IdentityRow](), keys: map[string]*ApiKeyRow{}}
	l := d.ReadModel("identity.identities", kernable.Order(OrderReadModels))
	setMembership := func(c *lc, id string, status MembershipStatus) {
		if r := t.row(c); r != nil {
			for i := range r.Memberships {
				if r.Memberships[i].MembershipID == id {
					r.Memberships[i].Status = status
				}
			}
		}
	}
	setStatus := func(c *lc, status IdentityStatus) {
		if r := t.row(c); r != nil {
			r.Status = status
		}
	}
	on(l, t, "IdentityCreated", func(c *lc, e IdentityCreated) {
		t.rows.set(rowKey(c.Event.Tenant, c.Event.AggregateID), &IdentityRow{ID: c.Event.AggregateID, Tenant: c.Event.Tenant, Kind: e.Kind,
			AccountID: e.AccountID, DisplayName: e.DisplayName, Status: IdentityStatusActive, Memberships: []MembershipRow{}, APIKeys: []KeyRow{}})
	})
	on(l, t, "ProfileUpdated", func(c *lc, e ProfileUpdated) {
		if r := t.row(c); r != nil && e.DisplayName != nil {
			r.DisplayName = e.DisplayName
		}
	})
	// Who granted it and when come from the event's request context and time, not its payload.
	on(l, t, "MembershipGranted", func(c *lc, e MembershipGranted) {
		if r := t.row(c); r != nil {
			var by *string
			if s := c.Event.Request.SenderIdentity; s != "" {
				by = &s
			}
			r.Memberships = append(r.Memberships, MembershipRow{MembershipID: e.MembershipID, Group: e.Group, Scope: e.Scope,
				ExpiresAt: e.ExpiresAt, Status: MembershipStatusActive, GrantedBy: by, GrantedAt: c.Event.CreatedAt.UnixMilli()})
		}
	})
	on(l, t, "MembershipSuspended", func(c *lc, e MembershipSuspended) { setMembership(c, e.MembershipID, MembershipStatusSuspended) })
	on(l, t, "MembershipResumed", func(c *lc, e MembershipResumed) { setMembership(c, e.MembershipID, MembershipStatusActive) })
	on(l, t, "MembershipRevoked", func(c *lc, e MembershipRevoked) {
		if r := t.row(c); r != nil {
			r.Memberships = slices.DeleteFunc(r.Memberships, func(m MembershipRow) bool { return m.MembershipID == e.MembershipID })
		}
	})
	on(l, t, "ApiKeyCreated", func(c *lc, e ApiKeyCreated) {
		t.keys[e.KeyID] = &ApiKeyRow{Tenant: c.Event.Tenant, IdentityID: c.Event.AggregateID, Hash: e.Hash, ExpiresAt: e.ExpiresAt}
		if r := t.row(c); r != nil {
			r.APIKeys = append(r.APIKeys, KeyRow{KeyID: e.KeyID, Label: e.Label, ExpiresAt: e.ExpiresAt})
		}
	})
	on(l, t, "ApiKeyRevoked", func(c *lc, e ApiKeyRevoked) {
		if k, ok := t.keys[e.KeyID]; ok {
			k.Revoked = true
		}
		if r := t.row(c); r != nil {
			for i := range r.APIKeys {
				if r.APIKeys[i].KeyID == e.KeyID {
					r.APIKeys[i].Revoked = true
				}
			}
		}
	})
	on(l, t, "IdentitySuspended", func(c *lc, _ IdentitySuspended) { setStatus(c, IdentityStatusSuspended) })
	on(l, t, "IdentityReactivated", func(c *lc, _ IdentityReactivated) { setStatus(c, IdentityStatusActive) })
	on(l, t, "IdentityDeleted", func(c *lc, _ IdentityDeleted) { t.rows.remove(rowKey(c.Event.Tenant, c.Event.AggregateID)) })
	on(l, t, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) {
		t.rows.retain(func(_ string, r *IdentityRow) bool { return r.Tenant != c.Event.AggregateID })
	})
	return l, t
}

func copyIdentity(r *IdentityRow) IdentityRow {
	c := *r
	c.Memberships = slices.Clone(r.Memberships)
	c.APIKeys = slices.Clone(r.APIKeys)
	return c
}
