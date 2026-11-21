package kit

// tenancy: one Tenant per tenant, living under its own tenant ID (design/kits.md "tenancy"). The
// domain, the types its parts share, and NewTenancy, which registers the parts.

import kernable "github.com/kernable/kernable/sdks/go"

// SignUpPolicy says who may open tenants and register accounts themselves.
type SignUpPolicy struct {
	Tenants  TenantSignUp  `json:"tenants"`
	Accounts AccountSignUp `json:"accounts"`
}

func defaultSignUp() SignUpPolicy {
	return SignUpPolicy{Tenants: TenantSignUpOff, Accounts: AccountSignUpOff}
}

// Owner is email when the platform created the tenant (an owner invitation follows), AccountID
// by self-service.
type Owner struct {
	Email     *string `json:"email,omitempty"`
	AccountID *string `json:"accountId,omitempty"`
}

// Suspension is why and since when a tenant is suspended.
type Suspension struct {
	Reason SuspensionReason `json:"reason"`
	At     int64            `json:"at"`
}

// Tenancy is the tenancy domain with its parts.
type Tenancy struct {
	Domain  *kernable.Domain
	Tenant  *kernable.Aggregate[Tenant]
	Tenants *kernable.Listener
	rows    *tenantRows
}

// NewTenancy is the tenancy domain, every part registered (a part not registered here does not
// exist).
func NewTenancy() *Tenancy {
	d := kernable.NewDomain("tenancy")
	t := &Tenancy{Domain: d, Tenant: tenantAggregate(d)}
	handleTenancy(t.Tenant)
	t.Tenants, t.rows = tenantsReadModel(d)
	answerTenancy(d, t.Tenants, t.rows)
	return t
}
