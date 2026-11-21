package kit

// The invitation read models and queries.

import (
	"slices"
	"sync"
	"time"

	kernable "github.com/kernable/kernable/sdks/go"
)

type InvitationRow struct {
	ID        string           `json:"id"`
	Tenant    string           `json:"tenant"`
	Email     *string          `json:"email"`
	Groups    []InvitedGroup   `json:"groups"`
	Owner     bool             `json:"owner"`
	Status    InvitationStatus `json:"status"`
	ExpiresAt int64            `json:"expiresAt"`
	InvitedBy *string          `json:"invitedBy"`
	InvitedAt int64            `json:"invitedAt"`
	// The open token's ID and the SHA-256 of its secret: PeekInvitation answers only for the right
	// token. Never shown in lists.
	tokenID, tokenHash string
}

type LinkRow struct {
	ID        string         `json:"id"`
	Tenant    string         `json:"tenant"`
	Groups    []InvitedGroup `json:"groups"`
	Label     string         `json:"label"`
	Status    LinkStatus     `json:"status"`
	ExpiresAt *int64         `json:"expiresAt"`
	MaxUses   *int64         `json:"maxUses"`
	// How many joined through it.
	Uses      int64   `json:"uses"`
	CreatedBy *string `json:"createdBy"`
	CreatedAt int64   `json:"createdAt"`
	// The link token's hash: PeekInvitationLink answers only for the right token. Never shown.
	hash string
}

type invitationRows struct {
	sync.RWMutex
	// tenant/invitationId → the invitation.
	rows *ordered[*InvitationRow]
	// tenant/linkId → the invitation link.
	links *ordered[*LinkRow]
}

func senderOf(c *lc) *string {
	if s := c.Event.Request.SenderIdentity; s != "" {
		return &s
	}
	return nil
}

func invitationsReadModel(d *kernable.Domain) (*kernable.Listener, *invitationRows) {
	t := &invitationRows{rows: newOrdered[*InvitationRow](), links: newOrdered[*LinkRow]()}
	l := d.ReadModel("invitation.invitations", kernable.Order(OrderReadModels))
	update := func(c *lc, f func(*InvitationRow)) {
		if r, ok := t.rows.get(rowKey(c.Event.Tenant, c.Event.AggregateID)); ok {
			f(r)
		}
	}
	updateLink := func(c *lc, f func(*LinkRow)) {
		if r, ok := t.links.get(rowKey(c.Event.Tenant, c.Event.AggregateID)); ok {
			f(r)
		}
	}
	on(l, t, "InvitationCreated", func(c *lc, e InvitationCreated) {
		r := &InvitationRow{ID: c.Event.AggregateID, Tenant: c.Event.Tenant, Email: e.Email, Groups: e.Groups, Owner: e.Owner,
			Status: InvitationStatusOpen, ExpiresAt: e.ExpiresAt, InvitedBy: senderOf(c), InvitedAt: c.Event.CreatedAt.UnixMilli()}
		if e.Token != nil {
			r.tokenID, r.tokenHash = e.TokenID, SHA256(*e.Token)
		}
		t.rows.set(rowKey(c.Event.Tenant, c.Event.AggregateID), r)
	})
	on(l, t, "InvitationResent", func(c *lc, e InvitationResent) {
		update(c, func(r *InvitationRow) {
			r.ExpiresAt = e.ExpiresAt
			r.tokenID, r.tokenHash = "", ""
			if e.Token != nil {
				r.tokenID, r.tokenHash = e.TokenID, SHA256(*e.Token)
			}
		})
	})
	closed := func(status InvitationStatus) func(*InvitationRow) {
		return func(r *InvitationRow) { r.Status, r.tokenID, r.tokenHash = status, "", "" }
	}
	on(l, t, "InvitationRevoked", func(c *lc, _ InvitationRevoked) { update(c, closed(InvitationStatusRevoked)) })
	on(l, t, "InvitationDeclined", func(c *lc, _ InvitationDeclined) { update(c, closed(InvitationStatusDeclined)) })
	on(l, t, "InvitationAccepted", func(c *lc, _ InvitationAccepted) { update(c, closed(InvitationStatusAccepted)) })
	on(l, t, "InvitationErased", func(c *lc, _ InvitationErased) { update(c, func(r *InvitationRow) { r.Email = nil }) })
	on(l, t, "InvitationLinkCreated", func(c *lc, e InvitationLinkCreated) {
		t.links.set(rowKey(c.Event.Tenant, c.Event.AggregateID), &LinkRow{ID: c.Event.AggregateID, Tenant: c.Event.Tenant, Groups: e.Groups,
			Label: e.Label, Status: LinkStatusOpen, ExpiresAt: e.ExpiresAt, MaxUses: e.MaxUses, CreatedBy: senderOf(c),
			CreatedAt: c.Event.CreatedAt.UnixMilli(), hash: e.Hash})
	})
	on(l, t, "InvitationLinkRevoked", func(c *lc, _ InvitationLinkRevoked) { updateLink(c, func(r *LinkRow) { r.Status = LinkStatusRevoked }) })
	on(l, t, "InvitationLinkUsed", func(c *lc, _ InvitationLinkUsed) { updateLink(c, func(r *LinkRow) { r.Uses++ }) })
	on(l, t, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) {
		tenant := c.Event.AggregateID
		t.rows.retain(func(_ string, r *InvitationRow) bool { return r.Tenant != tenant })
		t.links.retain(func(_ string, r *LinkRow) bool { return r.Tenant != tenant })
	})
	return l, t
}

// ListInvitations is the tenant's invitations; the open ones appear in user lists.
type ListInvitations struct{}

type Peeked struct {
	Tenant       string  `json:"tenant"`
	InvitationID string  `json:"invitationId"`
	Email        *string `json:"email"`
	Owner        bool    `json:"owner"`
	Expired      bool    `json:"expired"`
}

// PeekInvitation is the API's only: what the acceptance page shows for a token, only for the
// right token (its secret checked).
type PeekInvitation struct {
	Token string `json:"token"`
}

// ListInvitationLinks is the tenant's invitation links, with how often each was used.
type ListInvitationLinks struct{}

type PeekedLink struct {
	Tenant string `json:"tenant"`
	LinkID string `json:"linkId"`
	Label  string `json:"label"`
	Usable bool   `json:"usable"`
}

// PeekInvitationLink is the API's only: whether a link token can be used, for the join page,
// only for the right token (its hash checked).
type PeekInvitationLink struct {
	Token string `json:"token"`
}

func answerInvitation(l *kernable.Listener, t *invitationRows) {
	kernable.Answer(l, "ListInvitations", func(c *qc, _ ListInvitations) ([]InvitationRow, error) {
		t.RLock()
		defer t.RUnlock()
		rows := []InvitationRow{}
		for _, r := range t.rows.values() {
			if r.Tenant == c.Tenant {
				row := *r
				row.Groups = slices.Clone(r.Groups)
				rows = append(rows, row)
			}
		}
		return rows, nil
	})
	kernable.Answer(l, "PeekInvitation", func(_ *qc, q PeekInvitation) (*Peeked, error) {
		invalid := rejected("The invitation is not valid", "invalid_token")
		tok, ok := ParseInvitationToken(q.Token)
		if !ok {
			return nil, invalid
		}
		t.RLock()
		defer t.RUnlock()
		r, ok := t.rows.get(rowKey(tok.Tenant, tok.InvitationID))
		if !ok || r.Status != InvitationStatusOpen || r.tokenID == "" || r.tokenID != tok.TokenID || !EqualSecrets(r.tokenHash, SHA256(tok.Secret)) {
			return nil, invalid
		}
		return &Peeked{Tenant: r.Tenant, InvitationID: r.ID, Email: r.Email, Owner: r.Owner, Expired: time.Now().UnixMilli() > r.ExpiresAt}, nil
	})
	kernable.Answer(l, "ListInvitationLinks", func(c *qc, _ ListInvitationLinks) ([]LinkRow, error) {
		t.RLock()
		defer t.RUnlock()
		rows := []LinkRow{}
		for _, r := range t.links.values() {
			if r.Tenant == c.Tenant {
				row := *r
				row.Groups = slices.Clone(r.Groups)
				rows = append(rows, row)
			}
		}
		return rows, nil
	})
	kernable.Answer(l, "PeekInvitationLink", func(_ *qc, q PeekInvitationLink) (*PeekedLink, error) {
		invalid := rejected("The link is not valid", "invalid_token")
		tok, ok := ParseLinkToken(q.Token)
		if !ok {
			return nil, invalid
		}
		t.RLock()
		defer t.RUnlock()
		r, ok := t.links.get(rowKey(tok.Tenant, tok.LinkID))
		if !ok || r.Status != LinkStatusOpen || !EqualSecrets(r.hash, tok.Hash) {
			return nil, invalid
		}
		now := time.Now().UnixMilli()
		return &PeekedLink{Tenant: r.Tenant, LinkID: r.ID, Label: r.Label,
			Usable: (r.ExpiresAt == nil || now <= *r.ExpiresAt) && (r.MaxUses == nil || r.Uses < *r.MaxUses)}, nil
	})
}
