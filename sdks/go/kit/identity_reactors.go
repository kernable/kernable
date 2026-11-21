package kit

// The identity reactors: side effects and follow-up commands.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	kernable "github.com/kernable/kernable/sdks/go"
)

// processRegistered is the kernel's announcement of a process version (domain process,
// aggregate registry).
type processRegistered struct {
	Name string `json:"name"`
}

// followUp is a follow-up command of this event, in tenant, with a fixed ID.
func followUp(c *lc, cmd kernable.Command, id, tenant string) kernable.Command {
	cmd.ID, cmd.Tenant, cmd.Context = id, tenant, internal(c)
	return cmd
}

// tolerating submits, accepting a rejection with code (done before).
func tolerating(c *lc, code string, cmd kernable.Command) error {
	if _, err := c.Submit(cmd); err != nil && !isRejected(err, code) {
		return err
	}
	return nil
}

func queryInto(c *lc, q kernable.Query, into any) error {
	answer, err := c.Client().Query(c, q)
	if err != nil {
		return err
	}
	return json.Unmarshal(answer, into)
}

func identityCommand(id, commandType string, data any) kernable.Command {
	return kernable.Command{Domain: "identity", AggregateType: "identity", AggregateID: id, Type: commandType, Data: data}
}

func firstAggregate(events []kernable.StoredEvent) (string, error) {
	if len(events) == 0 {
		return "", errors.New("no identity created")
	}
	return events[0].AggregateID, nil
}

// join: an account joins a tenant with groups (and as owner): its identity there is created, or
// kept if it has one, and gains the groups. source makes the follow-up command IDs unique per join.
func join(c *lc, tenant, source, accountID string, groups []InvitedGroup, owner bool) error {
	var found FoundIdentity
	err := queryInto(c, kernable.Query{Domain: "identity", Type: "FindIdentityByAccount", Data: FindIdentityByAccount{AccountID: accountID},
		Tenant: tenant, Context: internal(nil)}, &found)
	identityID := found.IdentityID
	var r *kernable.Rejected
	switch {
	case errors.As(err, &r):
		created, err := c.Submit(followUp(c, identityCommand(NewID(), "CreateIdentity", CreateIdentity{AccountID: accountID}),
			"invitation-identity-"+source, tenant))
		if err != nil {
			return err
		}
		if identityID, err = firstAggregate(created); err != nil {
			return err
		}
	case err != nil:
		return err
	}
	for i, g := range groups {
		grant := GrantMembership{MembershipID: fmt.Sprintf("%s-%d", source, i), Group: g.Group, Scope: g.Scope}
		if err := tolerating(c, "exists", followUp(c, identityCommand(identityID, "GrantMembership", grant),
			fmt.Sprintf("invitation-membership-%s-%d", source, i), tenant)); err != nil {
			return err
		}
	}
	if owner {
		add := kernable.Command{Domain: "tenancy", AggregateType: "tenant", AggregateID: tenant, Type: "AddOwner", Data: AddOwner{IdentityID: identityID}}
		if err := tolerating(c, "exists", followUp(c, add, "invitation-owner-"+source, tenant)); err != nil {
			return err
		}
	}
	return nil
}

func identityReactors(d *kernable.Domain) {
	owners := d.Reactor("identity.owners", kernable.Order(OrderReactors))
	// Creates the first owner of a self-service tenant, then makes it the owner.
	kernable.On(owners, "tenancy.TenantCreated", func(c *lc, e TenantCreated) error {
		if e.Owner.AccountID == nil {
			return nil
		}
		tenant := c.Event.AggregateID
		// A fixed command ID: a redelivered event returns the first outcome, with the first identity's ID.
		created, err := c.Submit(followUp(c, identityCommand(NewID(), "CreateIdentity", CreateIdentity{AccountID: *e.Owner.AccountID}),
			"owner-identity-"+tenant, tenant))
		if err != nil {
			return err
		}
		identityID, err := firstAggregate(created)
		if err != nil {
			return err
		}
		add := kernable.Command{Domain: "tenancy", AggregateType: "tenant", AggregateID: tenant, Type: "AddOwner", Data: AddOwner{IdentityID: identityID}}
		_, err = c.Submit(followUp(c, add, "owner-"+tenant, tenant))
		return err
	})

	invitations := d.Reactor("identity.invitations", kernable.Order(OrderReactors))
	// An accepted invitation becomes an identity with its groups, and an owner for an owner
	// invitation; so does a join through an invitation link, never as owner.
	kernable.On(invitations, "invitation.InvitationAccepted", func(c *lc, e InvitationAccepted) error {
		return join(c, c.Event.Tenant, c.Event.AggregateID, e.AccountID, e.Groups, e.Owner)
	})
	kernable.On(invitations, "invitation.InvitationLinkUsed", func(c *lc, e InvitationLinkUsed) error {
		return join(c, c.Event.Tenant, c.Event.AggregateID+"-"+e.AccountID, e.AccountID, e.Groups, false)
	})

	// From the first event when new: an app registers its processes as it starts, maybe before
	// this reactor's first connect; starting at the newest event would miss them (each step is
	// safe to repeat).
	processes := d.Reactor("identity.processes", kernable.Order(OrderReactors), kernable.StartAfter(0))
	// A registered business process gets an identity in the platform tenant (no permissions: a
	// platform admin grants it groups) and an API key, and acts as them. Fixed IDs from the
	// process's name: a new version or a redelivery changes nothing.
	kernable.On(processes, "process.ProcessRegistered", func(c *lc, e processRegistered) error {
		name := e.Name
		identityID := "process-" + name
		if err := tolerating(c, "exists", followUp(c, identityCommand(identityID, "CreateProcessIdentity", CreateProcessIdentity{Process: name}),
			"process-identity-"+name, PlatformTenant)); err != nil {
			return err
		}
		// The key's secret is discarded: only its ID travels, as the commands' session. A
		// redelivery gets the first outcome, and with it the first key's ID.
		keyID := strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
				return r
			}
			return 'x'
		}, RandomSecret(9))
		label := "process " + name
		created, err := c.Submit(followUp(c, identityCommand(identityID, "CreateApiKey", CreateApiKey{KeyID: keyID, Secret: RandomSecret(32), Label: &label}),
			"process-key-"+name, PlatformTenant))
		if err != nil {
			return err
		}
		keyID = ""
		for _, ev := range created {
			if ev.Type == "ApiKeyCreated" {
				var k ApiKeyCreated
				if err := json.Unmarshal(ev.Data, &k); err == nil {
					keyID = k.KeyID
				}
			}
		}
		if keyID == "" {
			return errors.New("no API key created")
		}
		_, err = c.ProcessAct(name, kernable.RequestContext{SenderTenant: PlatformTenant, SenderIdentity: identityID, SenderSession: keyID})
		return err
	})

	cleanup := d.Reactor("identity.cleanup", kernable.Order(OrderCleanup))
	// A deleted tenant's identities are erased, before the read models drop the tenant.
	kernable.On(cleanup, "tenancy.TenantDeleted", func(c *lc, _ TenantDeleted) error {
		tenant := c.Event.AggregateID
		var rows []IdentityRow
		if err := queryInto(c, kernable.Query{Domain: "identity", Type: "ListIdentities", Data: ListIdentities{}, Tenant: tenant, Context: internal(nil)}, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := c.Submit(followUp(c, identityCommand(r.ID, "DeleteIdentity", DeleteIdentity{}), "delete-"+r.ID, tenant)); err != nil {
				return err
			}
		}
		return nil
	})

	follow := d.Reactor("identity.follow", kernable.Order(OrderReactors))
	// A deleted account's identities go in every tenant. One that still owns a tenant is refused
	// (an owner must be replaced first) and parks here for an operator.
	kernable.On(follow, "account.AccountDeleted", func(c *lc, _ AccountDeleted) error {
		var rows []MyIdentity
		if err := queryInto(c, kernable.Query{Domain: "identity", Type: "ListMyIdentities", Data: ListMyIdentities{AccountID: c.Event.AggregateID},
			Tenant: c.Event.Tenant, Context: internal(nil), AllPartitions: true}, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			if _, err := c.Submit(followUp(c, identityCommand(r.IdentityID, "DeleteIdentity", DeleteIdentity{}), "delete-"+r.IdentityID, r.Tenant)); err != nil {
				return err
			}
		}
		return nil
	})
	// A deleted group's memberships stopped counting at once; here they leave the identities.
	kernable.On(follow, "group.GroupDeleted", func(c *lc, _ GroupDeleted) error {
		var rows []IdentityRow
		if err := queryInto(c, kernable.Query{Domain: "identity", Type: "ListIdentities", Data: ListIdentities{}, Tenant: c.Event.Tenant, Context: internal(nil)}, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			for _, m := range r.Memberships {
				if m.Group != c.Event.AggregateID {
					continue
				}
				if _, err := c.Submit(followUp(c, identityCommand(r.ID, "RevokeMembership", RevokeMembership{MembershipID: m.MembershipID}),
					fmt.Sprintf("revoke-%s-%s", r.ID, m.MembershipID), c.Event.Tenant)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
