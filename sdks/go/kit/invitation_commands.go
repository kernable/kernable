package kit

// The invitation commands: each type with its handler.

import (
	"slices"

	kernable "github.com/kernable/kernable/sdks/go"
)

func invitedGroups(value []InvitedGroup) ([]InvitedGroup, error) {
	if slices.ContainsFunc(value, func(g InvitedGroup) bool { return g.Group == "" }) {
		return nil, rejected("Each group needs its ID", "invalid_groups")
	}
	if value == nil {
		value = []InvitedGroup{}
	}
	return value, nil
}

// activeTenant: the tenant (named by the token; the handler checks it is the command's) must be
// active when one joins it.
func activeTenant(tenant string, ok bool) []kernable.Requirement {
	if !ok {
		return nil
	}
	return []kernable.Requirement{{Domain: "tenancy", AggregateType: "tenant", AggregateID: tenant, Fact: "active"}}
}

func groupsActive(groups []InvitedGroup) []kernable.Requirement {
	var out []kernable.Requirement
	for _, g := range groups {
		out = append(out, kernable.Requirement{Domain: "group", AggregateType: "group", AggregateID: g.Group, Fact: "active"})
	}
	return out
}

// provenInvitation is the token, checked against the invitation: open, this tenant's, not
// expired, the secret right.
func provenInvitation(s Invitation, value, tenant, aggregateID string, now int64) (string, error) {
	t, ok := ParseInvitationToken(value)
	if !ok || s.Status != InvitationStatusOpen || t.Tenant != tenant || t.InvitationID != aggregateID || t.TokenID != s.TokenID ||
		s.Token == nil || !EqualSecrets(*s.Token, t.Secret) {
		return "", rejected("The invitation is not valid", "invalid_token")
	}
	if now > s.ExpiresAt {
		return "", rejected("The invitation has expired", "expired_token")
	}
	return t.TokenID, nil
}

// CreateInvitation invites Email into Groups. The middleware lets the sender attach only groups
// it could grant, and owner invitations only to owners and the platform.
type CreateInvitation struct {
	Email  string         `json:"email"`
	Groups []InvitedGroup `json:"groups,omitempty"`
	Owner  *bool          `json:"owner,omitempty"`
}

type ResendInvitation struct{}
type RevokeInvitation struct{}

// DeclineInvitation is public: the token is the proof.
type DeclineInvitation struct {
	Token string `json:"token"`
}

// AcceptInvitation is the API's: it says which account accepts, and its email, which must be the
// invited one. The tenant must be active when it commits.
type AcceptInvitation struct {
	Token      string `json:"token"`
	AccountID  string `json:"accountId"`
	Email      string `json:"email"`
	NewAccount bool   `json:"newAccount"`
}

// RecordInvitationMailSent is internal: the mail reactor, once the mail is out.
type RecordInvitationMailSent struct {
	TokenID string `json:"tokenId"`
}

// EraseInvitation erases the invited email; an open invitation is revoked first.
type EraseInvitation struct{}

// CreateInvitationLink is a reusable invitation link into Groups (never as owner). The API
// generates the secret and shows the token once; the event keeps its hash.
type CreateInvitationLink struct {
	Secret string         `json:"secret"`
	Groups []InvitedGroup `json:"groups,omitempty"`
	Label  *string        `json:"label,omitempty"`
	// Until when it works (ms since epoch); nil: until revoked.
	ExpiresAt *int64 `json:"expiresAt,omitempty"`
	// How many people may join through it; nil: no limit.
	MaxUses *int64 `json:"maxUses,omitempty"`
}

type RevokeInvitationLink struct{}

// JoinWithInvitationLink is the API's: the signed-in account joins the tenant through the link.
type JoinWithInvitationLink struct {
	Token     string `json:"token"`
	AccountID string `json:"accountId"`
}

func invitationCommandVault(d *kernable.Domain) {
	d.Command("DeclineInvitation").Secret("token")
	d.Command("AcceptInvitation").Secret("token")
	d.Command("CreateInvitationLink").Secret("secret")
	d.Command("JoinWithInvitationLink").Secret("token")
}

// validLinkSecret: at least 32 URL-safe characters.
func validLinkSecret(secret string) bool {
	if len(secret) < 32 {
		return false
	}
	for i := 0; i < len(secret); i++ {
		c := secret[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func handleInvitation(invitation *kernable.Aggregate[Invitation], link *kernable.Aggregate[InvitationLink]) {
	kernable.Handle(invitation, "CreateInvitation", func(c *cc, s Invitation, d CreateInvitation) (events, error) {
		if s.Status != InvitationStatusNone {
			return nil, rejected("The invitation exists", "exists")
		}
		email := NormalEmail(d.Email)
		if !ValidEmail(email) {
			return nil, rejected("Not an email address", "invalid_email")
		}
		groups, err := invitedGroups(d.Groups)
		if err != nil {
			return nil, err
		}
		return ev("InvitationCreated", InvitationCreated{Email: ptr(email), Groups: groups, Owner: d.Owner != nil && *d.Owner,
			TokenID: RandomSecret(9), Token: ptr(RandomSecret(32)), ExpiresAt: c.Now().UnixMilli() + InvitationDays*Day}), nil
	}, kernable.Requires(func(d CreateInvitation, _ Invitation) []kernable.Requirement { return groupsActive(d.Groups) }))
	kernable.Handle(invitation, "ResendInvitation", func(c *cc, s Invitation, _ ResendInvitation) (events, error) {
		if s.Status != InvitationStatusOpen {
			return nil, rejected("Only an open invitation is resent", "invalid_state")
		}
		return ev("InvitationResent", InvitationResent{PreviousTokenID: s.TokenID, TokenID: RandomSecret(9), Token: ptr(RandomSecret(32)),
			ExpiresAt: c.Now().UnixMilli() + InvitationDays*Day}), nil
	})
	kernable.Handle(invitation, "RevokeInvitation", func(_ *cc, s Invitation, _ RevokeInvitation) (events, error) {
		if s.Status != InvitationStatusOpen {
			return nil, rejected("Only an open invitation is revoked", "invalid_state")
		}
		return ev("InvitationRevoked", InvitationRevoked{TokenID: s.TokenID}), nil
	})
	kernable.Handle(invitation, "DeclineInvitation", func(c *cc, s Invitation, d DeclineInvitation) (events, error) {
		id, err := provenInvitation(s, d.Token, c.Tenant, c.AggregateID, c.Now().UnixMilli())
		if err != nil {
			return nil, err
		}
		return ev("InvitationDeclined", InvitationDeclined{TokenID: id}), nil
	})
	kernable.Handle(invitation, "AcceptInvitation", func(c *cc, s Invitation, d AcceptInvitation) (events, error) {
		id, err := provenInvitation(s, d.Token, c.Tenant, c.AggregateID, c.Now().UnixMilli())
		if err != nil {
			return nil, err
		}
		if s.Email == nil || NormalEmail(d.Email) != *s.Email {
			return nil, rejected("The invitation is for another email address", "wrong_email")
		}
		if d.AccountID == "" {
			return nil, rejected("Which account?", "invalid_account")
		}
		return ev("InvitationAccepted", InvitationAccepted{TokenID: id, AccountID: d.AccountID, NewAccount: d.NewAccount,
			Groups: s.Groups, Owner: s.Owner}), nil
	}, kernable.Requires(func(d AcceptInvitation, _ Invitation) []kernable.Requirement {
		t, ok := ParseInvitationToken(d.Token)
		return activeTenant(t.Tenant, ok)
	}))
	kernable.Handle(invitation, "RecordInvitationMailSent", func(_ *cc, _ Invitation, d RecordInvitationMailSent) (events, error) {
		return ev("InvitationMailSent", InvitationMailSent{TokenID: d.TokenID}), nil
	})
	kernable.Handle(invitation, "EraseInvitation", func(_ *cc, s Invitation, _ EraseInvitation) (events, error) {
		if s.Status == InvitationStatusNone || s.Erased {
			return nil, rejected("Nothing to erase", "not_found")
		}
		if s.Status == InvitationStatusOpen {
			return events{kernable.Event("InvitationRevoked", InvitationRevoked{TokenID: s.TokenID}), kernable.Event("InvitationErased", InvitationErased{})}, nil
		}
		return ev("InvitationErased", InvitationErased{}), nil
	})

	kernable.Handle(link, "CreateInvitationLink", func(c *cc, s InvitationLink, d CreateInvitationLink) (events, error) {
		if s.Status != LinkStatusNone {
			return nil, rejected("The link exists", "exists")
		}
		if !validLinkSecret(d.Secret) {
			return nil, rejected("A secret of at least 32 URL-safe characters", "invalid_secret")
		}
		if d.MaxUses != nil && *d.MaxUses <= 0 {
			return nil, rejected("maxUses is a positive whole number", "invalid_max_uses")
		}
		groups, err := invitedGroups(d.Groups)
		if err != nil {
			return nil, err
		}
		return ev("InvitationLinkCreated", InvitationLinkCreated{Groups: groups, Hash: SHA256(LinkTokenOf(c.Tenant, c.AggregateID, d.Secret)),
			Label: deref(d.Label), ExpiresAt: d.ExpiresAt, MaxUses: d.MaxUses}), nil
	}, kernable.Requires(func(d CreateInvitationLink, _ InvitationLink) []kernable.Requirement { return groupsActive(d.Groups) }))
	kernable.Handle(link, "RevokeInvitationLink", func(_ *cc, s InvitationLink, _ RevokeInvitationLink) (events, error) {
		if s.Status != LinkStatusOpen {
			return nil, rejected("Only an open link is revoked", "invalid_state")
		}
		return ev("InvitationLinkRevoked", InvitationLinkRevoked{}), nil
	})
	kernable.Handle(link, "JoinWithInvitationLink", func(c *cc, s InvitationLink, d JoinWithInvitationLink) (events, error) {
		t, ok := ParseLinkToken(d.Token)
		if !ok || s.Status != LinkStatusOpen || t.Tenant != c.Tenant || t.LinkID != c.AggregateID || s.Hash == nil || !EqualSecrets(*s.Hash, t.Hash) {
			return nil, rejected("The link is not valid", "invalid_token")
		}
		if s.ExpiresAt != nil && c.Now().UnixMilli() > *s.ExpiresAt {
			return nil, rejected("The link has expired", "expired_token")
		}
		if s.MaxUses != nil && int64(len(s.Accounts)) >= *s.MaxUses {
			return nil, rejected("The link is used up", "used_up")
		}
		if d.AccountID == "" {
			return nil, rejected("Which account?", "invalid_account")
		}
		if slices.Contains(s.Accounts, d.AccountID) {
			return nil, rejected("Joined through this link already", "exists")
		}
		return ev("InvitationLinkUsed", InvitationLinkUsed{AccountID: d.AccountID, Groups: s.Groups}), nil
	}, kernable.Requires(func(d JoinWithInvitationLink, _ InvitationLink) []kernable.Requirement {
		t, ok := ParseLinkToken(d.Token)
		return activeTenant(t.Tenant, ok)
	}))
}
