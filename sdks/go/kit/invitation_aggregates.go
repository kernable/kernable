package kit

// The invitation aggregates: state, how each event changes it, unique keys.

import kernable "github.com/kernable/kernable/sdks/go"

type Invitation struct {
	Status  InvitationStatus `json:"status"`
	Erased  bool             `json:"erased"`
	Email   *string          `json:"email"`
	Groups  []InvitedGroup   `json:"groups"`
	Owner   bool             `json:"owner"`
	TokenID string           `json:"tokenId"`
	// The token while open; in the vault, so nil once used or erased.
	Token     *string `json:"token"`
	ExpiresAt int64   `json:"expiresAt"`
	AccountID *string `json:"accountId"`
}

func invitationAggregate(d *kernable.Domain) *kernable.Aggregate[Invitation] {
	a := kernable.NewAggregate(d, "invitation", Invitation{Status: InvitationStatusNone}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "InvitationCreated", func(_ *ac, s *Invitation, e InvitationCreated) {
		s.Status, s.Email, s.Groups, s.Owner = InvitationStatusOpen, e.Email, e.Groups, e.Owner
		s.TokenID, s.Token, s.ExpiresAt = e.TokenID, e.Token, e.ExpiresAt
	})
	kernable.Apply(a, "InvitationResent", func(_ *ac, s *Invitation, e InvitationResent) {
		s.TokenID, s.Token, s.ExpiresAt = e.TokenID, e.Token, e.ExpiresAt
	})
	kernable.Apply(a, "InvitationRevoked", func(_ *ac, s *Invitation, _ InvitationRevoked) { s.Status, s.Token = InvitationStatusRevoked, nil })
	kernable.Apply(a, "InvitationDeclined", func(_ *ac, s *Invitation, _ InvitationDeclined) { s.Status, s.Token = InvitationStatusDeclined, nil })
	kernable.Apply(a, "InvitationAccepted", func(_ *ac, s *Invitation, e InvitationAccepted) {
		s.Status, s.Token, s.AccountID = InvitationStatusAccepted, nil, ptr(e.AccountID)
	})
	kernable.Apply(a, "InvitationMailSent", func(_ *ac, _ *Invitation, _ InvitationMailSent) {})
	kernable.Apply(a, "InvitationErased", func(_ *ac, s *Invitation, _ InvitationErased) { s.Erased, s.Email = true, nil })
	// One open invitation per email and tenant.
	a.Unique("email", func(s Invitation) []string {
		if s.Email == nil || *s.Email == "" || s.Status != InvitationStatusOpen {
			return nil
		}
		return []string{*s.Email}
	})
	return a
}

type InvitationLink struct {
	Status LinkStatus     `json:"status"`
	Groups []InvitedGroup `json:"groups"`
	// The token's hash: the token itself is shown once, by the API.
	Hash      *string `json:"hash"`
	Label     string  `json:"label"`
	ExpiresAt *int64  `json:"expiresAt"`
	MaxUses   *int64  `json:"maxUses"`
	// The accounts that joined through it: each once.
	Accounts []string `json:"accounts"`
}

func linkAggregate(d *kernable.Domain) *kernable.Aggregate[InvitationLink] {
	a := kernable.NewAggregate(d, "link", InvitationLink{Status: LinkStatusNone}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "InvitationLinkCreated", func(_ *ac, s *InvitationLink, e InvitationLinkCreated) {
		s.Status, s.Groups, s.Hash, s.Label, s.ExpiresAt, s.MaxUses = LinkStatusOpen, e.Groups, ptr(e.Hash), e.Label, e.ExpiresAt, e.MaxUses
	})
	kernable.Apply(a, "InvitationLinkRevoked", func(_ *ac, s *InvitationLink, _ InvitationLinkRevoked) { s.Status = LinkStatusRevoked })
	kernable.Apply(a, "InvitationLinkUsed", func(_ *ac, s *InvitationLink, e InvitationLinkUsed) { s.Accounts = append(s.Accounts, e.AccountID) })
	return a
}
