package kit

// invitation: one Invitation per invitation, under its tenant's ID (design/kits.md
// "invitation"). NewInvitationDomain registers the parts.

import kernable "github.com/kernable/kernable/sdks/go"

// InvitationDomain is the invitation domain with its parts.
type InvitationDomain struct {
	Domain      *kernable.Domain
	Invitation  *kernable.Aggregate[Invitation]
	Link        *kernable.Aggregate[InvitationLink]
	Invitations *kernable.Listener
	rows        *invitationRows
}

// NewInvitationDomain is the invitation domain, every part registered.
func NewInvitationDomain() *InvitationDomain {
	d := kernable.NewDomain("invitation")
	invitationVault(d)
	invitationCommandVault(d)
	i := &InvitationDomain{Domain: d, Invitation: invitationAggregate(d), Link: linkAggregate(d)}
	handleInvitation(i.Invitation, i.Link)
	i.Invitations, i.rows = invitationsReadModel(d)
	answerInvitation(i.Invitations, i.rows)
	invitationReactors(d)
	return i
}
