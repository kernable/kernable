package kit

// The identity commands: each type with its handler.

import (
	"strings"
	"unicode/utf8"

	kernable "github.com/kernable/kernable/sdks/go"
)

func activeIdentity(s Identity) error {
	if s.Status == IdentityStatusActive {
		return nil
	}
	return rejected("The identity is not active", "invalid_state")
}

func namePart(p string) bool {
	if p == "" {
		return false
	}
	for _, c := range p {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// validScope is {type: "domain.aggregate", id}.
func validScope(v *Scope) (*Scope, error) {
	if v == nil {
		return nil, nil
	}
	d, a, ok := strings.Cut(v.Type, ".")
	typed := ok && d != "" && d[0] >= 'a' && d[0] <= 'z' && namePart(d) && namePart(a)
	if typed && v.ID != "" {
		return v, nil
	}
	return nil, rejected("A scope is {type: 'domain.aggregate', id}", "invalid_scope")
}

// CreateIdentity is internal: from an accepted invitation or a self-service tenant.
type CreateIdentity struct {
	AccountID   string  `json:"accountId"`
	DisplayName *string `json:"displayName,omitempty"`
	Title       *string `json:"title,omitempty"`
	Email       *string `json:"email,omitempty"`
}

type CreateServiceIdentity struct {
	DisplayName string `json:"displayName"`
}

// CreateProcessIdentity is internal: the identity of a business process (shop.fulfilment), made
// by the kit's identity.processes reactor when an app registers the process; one per process.
type CreateProcessIdentity struct {
	Process string `json:"process"`
}

type UpdateProfile struct {
	DisplayName *string `json:"displayName,omitempty"`
	Title       *string `json:"title,omitempty"`
	Email       *string `json:"email,omitempty"`
}

// GrantMembership: the middleware lets the sender grant only groups whose permissions it holds.
type GrantMembership struct {
	MembershipID string `json:"membershipId"`
	Group        string `json:"group"`
	Scope        *Scope `json:"scope,omitempty"`
	ExpiresAt    *int64 `json:"expiresAt,omitempty"`
}

type SuspendMembership struct {
	MembershipID string `json:"membershipId"`
}

type ResumeMembership struct {
	MembershipID string `json:"membershipId"`
}

type RevokeMembership struct {
	MembershipID string `json:"membershipId"`
}

// CreateApiKey: the API generates the key and shows it once; only its hash stays.
type CreateApiKey struct {
	KeyID     string  `json:"keyId"`
	Secret    string  `json:"secret"`
	Label     *string `json:"label,omitempty"`
	ExpiresAt *int64  `json:"expiresAt,omitempty"`
}

type RevokeApiKey struct {
	KeyID string `json:"keyId"`
}

type SuspendIdentity struct {
	Reason *string `json:"reason,omitempty"`
}

type ReactivateIdentity struct{}

// DeleteIdentity erases the identity's personal data. Never while a tenant holds it as owner:
// atomic, through the tenant's owner key (an absent requirement).
type DeleteIdentity struct{}

func identityCommandVault(d *kernable.Domain) {
	d.Command("CreateIdentity").Personal([]string{"displayName", "title", "email"}, ownIdentity)
	d.Command("CreateServiceIdentity").Personal([]string{"displayName"}, ownIdentity)
	d.Command("UpdateProfile").Personal([]string{"displayName", "title", "email"}, ownIdentity)
	d.Command("CreateApiKey").Secret("secret")
}

func hasMembership(s Identity, id string, status *MembershipStatus, message string) error {
	for _, m := range s.Memberships {
		if m.MembershipID == id && (status == nil || m.Status == *status) {
			return nil
		}
	}
	return rejected(message, "not_found")
}

func eqScope(a, b *Scope) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func handleIdentity(identity *kernable.Aggregate[Identity]) {
	kernable.Handle(identity, "CreateIdentity", func(_ *cc, s Identity, d CreateIdentity) (events, error) {
		if s.Status != IdentityStatusNone {
			return nil, rejected("The identity exists", "exists")
		}
		if d.AccountID == "" {
			return nil, rejected("Which account?", "invalid_account")
		}
		return ev("IdentityCreated", IdentityCreated{Kind: KindPerson, AccountID: ptr(d.AccountID), DisplayName: ptr(deref(d.DisplayName)),
			Title: d.Title, Email: d.Email}), nil
	})
	kernable.Handle(identity, "CreateServiceIdentity", func(_ *cc, s Identity, d CreateServiceIdentity) (events, error) {
		if s.Status != IdentityStatusNone {
			return nil, rejected("The identity exists", "exists")
		}
		if TrimSpace(d.DisplayName) == "" {
			return nil, rejected("A service needs a name", "invalid_name")
		}
		return ev("IdentityCreated", IdentityCreated{Kind: KindService, DisplayName: ptr(TrimSpace(d.DisplayName))}), nil
	})
	kernable.Handle(identity, "CreateProcessIdentity", func(_ *cc, s Identity, d CreateProcessIdentity) (events, error) {
		if s.Status != IdentityStatusNone {
			return nil, rejected("The identity exists", "exists")
		}
		domain, name, ok := strings.Cut(d.Process, ".")
		if !ok || !namePart(domain) || !namePart(name) {
			return nil, rejected("A qualified process name: domain.process", "invalid_process")
		}
		return ev("IdentityCreated", IdentityCreated{Kind: KindProcess, DisplayName: ptr(d.Process)}), nil
	})
	kernable.Handle(identity, "UpdateProfile", func(_ *cc, s Identity, d UpdateProfile) (events, error) {
		if err := activeIdentity(s); err != nil {
			return nil, err
		}
		return ev("ProfileUpdated", ProfileUpdated{DisplayName: d.DisplayName, Title: d.Title, Email: d.Email}), nil
	})
	kernable.Handle(identity, "GrantMembership", func(_ *cc, s Identity, d GrantMembership) (events, error) {
		if err := activeIdentity(s); err != nil {
			return nil, err
		}
		if d.MembershipID == "" || d.Group == "" {
			return nil, rejected("A membership needs an ID and a group", "invalid_membership")
		}
		scope, err := validScope(d.Scope)
		if err != nil {
			return nil, err
		}
		for _, m := range s.Memberships {
			if m.MembershipID == d.MembershipID || (m.Group == d.Group && eqScope(m.Scope, scope)) {
				return nil, rejected("Already a member", "exists")
			}
		}
		return ev("MembershipGranted", MembershipGranted{MembershipID: d.MembershipID, Group: d.Group, Scope: scope, ExpiresAt: d.ExpiresAt}), nil
	}, kernable.Requires(func(d GrantMembership, _ Identity) []kernable.Requirement {
		return []kernable.Requirement{{Domain: "group", AggregateType: "group", AggregateID: d.Group, Fact: "active"}}
	}))
	active, suspended := MembershipStatusActive, MembershipStatusSuspended
	kernable.Handle(identity, "SuspendMembership", func(_ *cc, s Identity, d SuspendMembership) (events, error) {
		if err := hasMembership(s, d.MembershipID, &active, "No such active membership"); err != nil {
			return nil, err
		}
		return ev("MembershipSuspended", MembershipSuspended{MembershipID: d.MembershipID}), nil
	})
	kernable.Handle(identity, "ResumeMembership", func(_ *cc, s Identity, d ResumeMembership) (events, error) {
		if err := hasMembership(s, d.MembershipID, &suspended, "No such suspended membership"); err != nil {
			return nil, err
		}
		return ev("MembershipResumed", MembershipResumed{MembershipID: d.MembershipID}), nil
	})
	kernable.Handle(identity, "RevokeMembership", func(_ *cc, s Identity, d RevokeMembership) (events, error) {
		if err := hasMembership(s, d.MembershipID, nil, "No such membership"); err != nil {
			return nil, err
		}
		return ev("MembershipRevoked", MembershipRevoked{MembershipID: d.MembershipID}), nil
	})
	kernable.Handle(identity, "CreateApiKey", func(_ *cc, s Identity, d CreateApiKey) (events, error) {
		if err := activeIdentity(s); err != nil {
			return nil, err
		}
		if s.Kind != KindService && s.Kind != KindProcess {
			return nil, rejected("Only a service or a process has API keys", "not_a_service")
		}
		idOK := len(d.KeyID) >= 8 && len(d.KeyID) <= 32
		for i := 0; idOK && i < len(d.KeyID); i++ {
			c := d.KeyID[i]
			idOK = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		}
		if !idOK || utf8.RuneCountInString(d.Secret) < 32 {
			return nil, rejected("A key ID of 8 to 32 letters and digits, and a secret of at least 32 characters", "invalid_key")
		}
		for _, k := range s.APIKeys {
			if k.KeyID == d.KeyID {
				return nil, rejected("The key exists", "exists")
			}
		}
		return ev("ApiKeyCreated", ApiKeyCreated{KeyID: d.KeyID, Hash: SHA256(APIKey(d.KeyID, d.Secret)), Label: deref(d.Label), ExpiresAt: d.ExpiresAt}), nil
	})
	kernable.Handle(identity, "RevokeApiKey", func(_ *cc, s Identity, d RevokeApiKey) (events, error) {
		for _, k := range s.APIKeys {
			if k.KeyID == d.KeyID && !k.Revoked {
				return ev("ApiKeyRevoked", ApiKeyRevoked{KeyID: d.KeyID}), nil
			}
		}
		return nil, rejected("No such key", "not_found")
	})
	kernable.Handle(identity, "SuspendIdentity", func(_ *cc, s Identity, d SuspendIdentity) (events, error) {
		if err := activeIdentity(s); err != nil {
			return nil, err
		}
		reason := "by_group"
		if d.Reason != nil {
			reason = *d.Reason
		}
		return ev("IdentitySuspended", IdentitySuspended{Reason: reason}), nil
	})
	kernable.Handle(identity, "ReactivateIdentity", func(_ *cc, s Identity, _ ReactivateIdentity) (events, error) {
		if s.Status != IdentityStatusSuspended {
			return nil, rejected("Only a suspended identity", "invalid_state")
		}
		return ev("IdentityReactivated", IdentityReactivated{}), nil
	})
	kernable.Handle(identity, "DeleteIdentity", func(_ *cc, s Identity, _ DeleteIdentity) (events, error) {
		if !s.live() {
			return nil, rejected("No such identity", "not_found")
		}
		return ev("IdentityDeleted", IdentityDeleted{}), nil
	}, kernable.Requires(func(_ DeleteIdentity, s Identity) []kernable.Requirement {
		if s.ID == "" {
			return nil
		}
		return []kernable.Requirement{{Domain: "tenancy", AggregateType: "tenant", Key: "owner", Value: s.ID, Fact: "exists", Absent: true}}
	}))
}
