package kit

// identity: a person or service in one tenant, holding its group memberships (design/kits.md
// "identity"). NewIdentityDomain registers the parts.

import kernable "github.com/kernable/kernable/sdks/go"

// IdentityDomain is the identity domain with its parts.
type IdentityDomain struct {
	Domain     *kernable.Domain
	Identity   *kernable.Aggregate[Identity]
	Identities *kernable.Listener
	rows       *identityRows
}

// NewIdentityDomain is the identity domain, every part registered.
func NewIdentityDomain() *IdentityDomain {
	d := kernable.NewDomain("identity")
	identityVault(d)
	identityCommandVault(d)
	i := &IdentityDomain{Domain: d, Identity: identityAggregate(d)}
	handleIdentity(i.Identity)
	i.Identities, i.rows = identitiesReadModel(d)
	answerIdentity(d, i.Identities, i.rows)
	identityReactors(d)
	return i
}
