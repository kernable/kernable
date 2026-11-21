package kit

// The tenancy aggregates: state, how each event changes it, unique keys and facts.

import (
	"slices"

	kernable "github.com/kernable/kernable/sdks/go"
)

// Tenant is the tenant's state, and the aggregate tenancy.tenant; its ID is the tenant's.
type Tenant struct {
	ID     string       `json:"id"`
	Status TenantStatus `json:"status"`
	Slug   *string      `json:"slug"`
	Name   *string      `json:"name"`
	// Identity IDs: the only place owners are kept; owners is a virtual group.
	Owners      []string    `json:"owners"`
	Suspension  *Suspension `json:"suspension"`
	MfaRequired bool        `json:"mfaRequired"`
	// Only the platform tenant's matters.
	SignUp SignUpPolicy `json:"signUp"`
}

// Exists is created and not deleted.
func (t Tenant) Exists() bool { return t.Status != TenantStatusNone && t.Status != TenantStatusDeleted }

func ptr[T any](v T) *T { return &v }

type ac = kernable.ApplyContext

func tenantAggregate(d *kernable.Domain) *kernable.Aggregate[Tenant] {
	tenant := kernable.NewAggregate(d, "tenant", Tenant{Status: TenantStatusNone, SignUp: defaultSignUp()}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(tenant, "TenantCreated", func(c *ac, s *Tenant, e TenantCreated) {
		s.ID, s.Status, s.Slug, s.Name = c.AggregateID, e.Status, ptr(e.Slug), ptr(e.Name)
	})
	kernable.Apply(tenant, "TenantActivated", func(_ *ac, s *Tenant, _ TenantActivated) { s.Status = TenantStatusActive })
	kernable.Apply(tenant, "TenantRenamed", func(_ *ac, s *Tenant, e TenantRenamed) { s.Name = ptr(e.Name) })
	kernable.Apply(tenant, "TenantSlugChanged", func(_ *ac, s *Tenant, e TenantSlugChanged) { s.Slug = ptr(e.Slug) })
	kernable.Apply(tenant, "TenantSuspended", func(_ *ac, s *Tenant, e TenantSuspended) {
		s.Status, s.Suspension = TenantStatusSuspended, &Suspension{Reason: e.Reason, At: e.At}
	})
	kernable.Apply(tenant, "TenantReactivated", func(_ *ac, s *Tenant, _ TenantReactivated) {
		s.Status, s.Suspension = TenantStatusActive, nil
	})
	kernable.Apply(tenant, "TenantDeleted", func(_ *ac, s *Tenant, _ TenantDeleted) {
		s.Status, s.Owners = TenantStatusDeleted, nil
	})
	kernable.Apply(tenant, "OwnerAdded", func(_ *ac, s *Tenant, e OwnerAdded) { s.Owners = append(s.Owners, e.IdentityID) })
	kernable.Apply(tenant, "OwnerRemoved", func(_ *ac, s *Tenant, e OwnerRemoved) {
		s.Owners = slices.DeleteFunc(s.Owners, func(o string) bool { return o == e.IdentityID })
	})
	kernable.Apply(tenant, "MfaRequirementChanged", func(_ *ac, s *Tenant, e MfaRequirementChanged) { s.MfaRequired = e.Required })
	kernable.Apply(tenant, "SignUpPolicyChanged", func(_ *ac, s *Tenant, e SignUpPolicyChanged) {
		s.SignUp = SignUpPolicy{Tenants: e.Tenants, Accounts: e.Accounts}
	})

	// A slug is unique across tenants while the tenant is not deleted: deleting frees it.
	tenant.Unique("slug", func(s Tenant) []string {
		if s.Slug == nil || !s.Exists() {
			return nil
		}
		return []string{*s.Slug}
	}, kernable.Global[Tenant]())
	// Lets identity.DeleteIdentity require that no tenant holds the identity as owner.
	tenant.Unique("owner", func(s Tenant) []string { return s.Owners })
	tenant.Fact("active", func(s Tenant) bool { return s.Status == TenantStatusActive })
	tenant.Fact("exists", func(s Tenant) bool { return s.Exists() })
	return tenant
}
