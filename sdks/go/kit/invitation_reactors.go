package kit

// The invitation reactors: side effects and follow-up commands.

import kernable "github.com/kernable/kernable/sdks/go"

type invited struct {
	tenant, invitationID, tokenID string
	secret, email                 *string
	owner                         bool
}

func sendInvitation(c *lc, i invited) error {
	// Used or erased meanwhile: nothing to send.
	if i.secret == nil || i.email == nil || *i.email == "" {
		return nil
	}
	link := InvitationTokenOf(i.tenant, i.invitationID, i.tokenID, *i.secret)
	subject := "You are invited"
	if i.owner {
		subject = "You are invited to own a tenant"
	}
	CurrentMailer().Send(Mail{To: *i.email, Subject: subject, Text: "Accept the invitation with this code: " + link, Purpose: PurposeInvitation, Token: link})
	_, err := c.Submit(kernable.Command{Domain: "invitation", AggregateType: "invitation", AggregateID: i.invitationID, Type: "RecordInvitationMailSent",
		Data: RecordInvitationMailSent{TokenID: i.tokenID}, ID: "mail-" + i.tokenID, Tenant: i.tenant, Context: internal(c)})
	return err
}

func invitationReactors(d *kernable.Domain) {
	mails := d.Reactor("invitation.mail", kernable.Order(OrderReactors))
	kernable.On(mails, "InvitationCreated", func(c *lc, e InvitationCreated) error {
		return sendInvitation(c, invited{tenant: c.Event.Tenant, invitationID: c.Event.AggregateID, tokenID: e.TokenID, secret: e.Token, email: e.Email, owner: e.Owner})
	})
	kernable.On(mails, "InvitationResent", func(c *lc, e InvitationResent) error {
		var rows []InvitationRow
		if err := queryInto(c, kernable.Query{Domain: "invitation", Type: "ListInvitations", Data: ListInvitations{}, Tenant: c.Event.Tenant, Context: internal(nil)}, &rows); err != nil {
			return err
		}
		i := invited{tenant: c.Event.Tenant, invitationID: c.Event.AggregateID, tokenID: e.TokenID, secret: e.Token}
		for _, r := range rows {
			if r.ID == c.Event.AggregateID {
				i.email, i.owner = r.Email, r.Owner
			}
		}
		return sendInvitation(c, i)
	})

	owners := d.Reactor("invitation.owners", kernable.Order(OrderReactors))
	// A tenant the platform creates gets its first owner by an owner invitation.
	kernable.On(owners, "tenancy.TenantCreated", func(c *lc, e TenantCreated) error {
		if e.Owner.Email == nil || *e.Owner.Email == "" {
			return nil
		}
		_, err := c.Submit(kernable.Command{Domain: "invitation", AggregateType: "invitation", AggregateID: NewID(), Type: "CreateInvitation",
			Data: CreateInvitation{Email: *e.Owner.Email, Owner: ptr(true)}, ID: "owner-invitation-" + c.Event.AggregateID,
			Tenant: c.Event.AggregateID, Context: internal(c)})
		return err
	})

	cleanup := d.Reactor("invitation.cleanup", kernable.Order(OrderCleanup))
	// A deleted tenant's invitations are revoked and their emails erased, before the read models drop them.
	kernable.On(cleanup, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) error {
		tenant := c.Event.AggregateID
		var rows []InvitationRow
		if err := queryInto(c, kernable.Query{Domain: "invitation", Type: "ListInvitations", Data: ListInvitations{}, Tenant: tenant, Context: internal(nil)}, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			if r.Email == nil {
				continue
			}
			if _, err := c.Submit(kernable.Command{Domain: "invitation", AggregateType: "invitation", AggregateID: r.ID, Type: "EraseInvitation",
				Data: EraseInvitation{}, ID: "erase-" + r.ID, Tenant: tenant, Context: internal(c)}); err != nil {
				return err
			}
		}
		return nil
	})
}
