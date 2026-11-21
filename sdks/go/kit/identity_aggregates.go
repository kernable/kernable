package kit

// The identity aggregates: state, how each event changes it, unique keys and facts.

import (
	"slices"

	kernable "github.com/kernable/kernable/sdks/go"
)

type Membership struct {
	MembershipID string           `json:"membershipId"`
	Group        string           `json:"group"`
	Scope        *Scope           `json:"scope"`
	ExpiresAt    *int64           `json:"expiresAt"`
	Status       MembershipStatus `json:"status"`
}

type Key struct {
	KeyID     string `json:"keyId"`
	Hash      string `json:"hash"`
	Label     string `json:"label"`
	ExpiresAt *int64 `json:"expiresAt"`
	Revoked   bool   `json:"revoked"`
}

// Identity is a person or service in one tenant.
type Identity struct {
	ID        string         `json:"id"`
	Status    IdentityStatus `json:"status"`
	Kind      IdentityKind   `json:"kind"`
	AccountID *string        `json:"accountId"`
	// A process identity's process: shop.fulfilment.
	Process     *string      `json:"process"`
	Memberships []Membership `json:"memberships"`
	APIKeys     []Key        `json:"apiKeys"`
}

func (s *Identity) membership(id string) *Membership {
	for i := range s.Memberships {
		if s.Memberships[i].MembershipID == id {
			return &s.Memberships[i]
		}
	}
	return nil
}

func (s Identity) live() bool {
	return s.Status != IdentityStatusNone && s.Status != IdentityStatusDeleted
}

func identityAggregate(d *kernable.Domain) *kernable.Aggregate[Identity] {
	a := kernable.NewAggregate(d, "identity", Identity{Status: IdentityStatusNone, Kind: KindPerson}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "IdentityCreated", func(c *ac, s *Identity, e IdentityCreated) {
		s.ID, s.Status, s.Kind, s.AccountID = c.AggregateID, IdentityStatusActive, e.Kind, e.AccountID
		if e.Kind == KindProcess {
			s.Process = e.DisplayName
		}
	})
	kernable.Apply(a, "ProfileUpdated", func(_ *ac, _ *Identity, _ ProfileUpdated) {})
	kernable.Apply(a, "MembershipGranted", func(_ *ac, s *Identity, e MembershipGranted) {
		s.Memberships = append(s.Memberships, Membership{MembershipID: e.MembershipID, Group: e.Group, Scope: e.Scope,
			ExpiresAt: e.ExpiresAt, Status: MembershipStatusActive})
	})
	kernable.Apply(a, "MembershipSuspended", func(_ *ac, s *Identity, e MembershipSuspended) {
		if m := s.membership(e.MembershipID); m != nil {
			m.Status = MembershipStatusSuspended
		}
	})
	kernable.Apply(a, "MembershipResumed", func(_ *ac, s *Identity, e MembershipResumed) {
		if m := s.membership(e.MembershipID); m != nil {
			m.Status = MembershipStatusActive
		}
	})
	kernable.Apply(a, "MembershipRevoked", func(_ *ac, s *Identity, e MembershipRevoked) {
		s.Memberships = slices.DeleteFunc(s.Memberships, func(m Membership) bool { return m.MembershipID == e.MembershipID })
	})
	kernable.Apply(a, "ApiKeyCreated", func(_ *ac, s *Identity, e ApiKeyCreated) {
		s.APIKeys = append(s.APIKeys, Key{KeyID: e.KeyID, Hash: e.Hash, Label: e.Label, ExpiresAt: e.ExpiresAt})
	})
	kernable.Apply(a, "ApiKeyRevoked", func(_ *ac, s *Identity, e ApiKeyRevoked) {
		for i := range s.APIKeys {
			if s.APIKeys[i].KeyID == e.KeyID {
				s.APIKeys[i].Revoked = true
			}
		}
	})
	kernable.Apply(a, "IdentitySuspended", func(_ *ac, s *Identity, _ IdentitySuspended) { s.Status = IdentityStatusSuspended })
	kernable.Apply(a, "IdentityReactivated", func(_ *ac, s *Identity, _ IdentityReactivated) { s.Status = IdentityStatusActive })
	kernable.Apply(a, "IdentityDeleted", func(_ *ac, s *Identity, _ IdentityDeleted) {
		s.Status, s.Memberships, s.APIKeys = IdentityStatusDeleted, nil, nil
	})
	// One identity per account and tenant.
	a.Unique("account", func(s Identity) []string {
		if s.AccountID == nil || !s.live() {
			return nil
		}
		return []string{*s.AccountID}
	})
	// One identity per process and tenant.
	a.Unique("process", func(s Identity) []string {
		if s.Process == nil || !s.live() {
			return nil
		}
		return []string{*s.Process}
	})
	a.Fact("active", func(s Identity) bool { return s.Status == IdentityStatusActive })
	return a
}
