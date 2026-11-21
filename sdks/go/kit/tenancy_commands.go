package kit

// The tenancy commands: each type with its handler.

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	kernable "github.com/kernable/kernable/sdks/go"
)

type cc = kernable.CommandContext
type events = []kernable.NewEvent

func ev(eventType string, data any) events { return kernable.Events(kernable.Event(eventType, data)) }

func rejected(message, code string) error { return kernable.Reject(code, message) }

func validSlug(value, tenantID string) (string, error) {
	shaped := len(value) >= 3 && len(value) <= 63 && value[0] != '-' && value[len(value)-1] != '-' && !strings.Contains(value, "--")
	for i := 0; shaped && i < len(value); i++ {
		c := value[i]
		shaped = c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-'
	}
	if !shaped || (tenantID != PlatformTenant && slices.Contains(ReservedSlugs, value)) {
		return "", rejected("A slug is 3 to 63 lowercase letters, digits and single dashes, and not reserved", "invalid_slug")
	}
	return value, nil
}

func tenantName(value string) (string, error) {
	trimmed := TrimSpace(value)
	if trimmed == "" || utf8.RuneCountInString(value) > 200 {
		return "", rejected("A tenant needs a name", "invalid_name")
	}
	return trimmed, nil
}

func tenantExists(s Tenant) error {
	if s.Exists() {
		return nil
	}
	return rejected("No such tenant", "not_found")
}

func isPlatform(c *cc) bool { return c.AggregateID == PlatformTenant }

type CreateTenant struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
	// Email when the platform creates it (an owner invitation follows), AccountID by self-service.
	Owner Owner `json:"owner"`
	// active, or pending for a self-service tenant waiting for approval.
	Status *TenantStatus `json:"status,omitempty"`
}

type ActivateTenant struct{}
type RenameTenant struct {
	Name string `json:"name"`
}
type ChangeTenantSlug struct {
	Slug string `json:"slug"`
}
type SuspendTenant struct {
	Reason SuspensionReason `json:"reason"`
}
type ReactivateTenant struct{}
type DeleteTenant struct{}
type AddOwner struct {
	IdentityID string `json:"identityId"`
}
type RemoveOwner struct {
	IdentityID string `json:"identityId"`
}
type RequireMfa struct {
	Required bool `json:"required"`
}
type ChangeSignUpPolicy struct {
	Tenants  TenantSignUp  `json:"tenants"`
	Accounts AccountSignUp `json:"accounts"`
}

func handleTenancy(tenant *kernable.Aggregate[Tenant]) {
	kernable.Handle(tenant, "CreateTenant", func(c *cc, s Tenant, d CreateTenant) (events, error) {
		// A tenant's aggregate ID is its tenant ID.
		if c.AggregateID != c.Tenant {
			return nil, rejected("A tenant lives under its own tenant ID", "invalid_tenant")
		}
		if s.Status != TenantStatusNone {
			return nil, rejected("The tenant exists", "exists")
		}
		email := ""
		if d.Owner.Email != nil {
			email = NormalEmail(*d.Owner.Email)
		}
		var first Owner
		switch {
		case d.Owner.AccountID != nil && *d.Owner.AccountID != "":
			first = Owner{AccountID: d.Owner.AccountID}
		case ValidEmail(email):
			first = Owner{Email: &email}
		case isPlatform(c):
			// The platform's owners come from the bootstrap.
			first = Owner{Email: ptr("")}
		default:
			return nil, rejected("A tenant needs an owner: an email or an account", "invalid_owner")
		}
		status := TenantStatusActive
		if d.Status != nil && *d.Status == TenantStatusPending {
			status = TenantStatusPending
		}
		slug, err := validSlug(d.Slug, c.AggregateID)
		if err != nil {
			return nil, err
		}
		name, err := tenantName(d.Name)
		if err != nil {
			return nil, err
		}
		return ev("TenantCreated", TenantCreated{Slug: slug, Name: name, Owner: first, Status: status}), nil
	})

	kernable.Handle(tenant, "ActivateTenant", func(_ *cc, s Tenant, _ ActivateTenant) (events, error) {
		if s.Status != TenantStatusPending {
			return nil, rejected("Only a pending tenant can be activated", "invalid_state")
		}
		return ev("TenantActivated", TenantActivated{}), nil
	})

	kernable.Handle(tenant, "RenameTenant", func(_ *cc, s Tenant, d RenameTenant) (events, error) {
		if s.Status != TenantStatusPending && s.Status != TenantStatusActive {
			return nil, rejected("Cannot rename this tenant", "invalid_state")
		}
		name, err := tenantName(d.Name)
		if err != nil {
			return nil, err
		}
		return ev("TenantRenamed", TenantRenamed{Name: name}), nil
	})

	kernable.Handle(tenant, "ChangeTenantSlug", func(c *cc, s Tenant, d ChangeTenantSlug) (events, error) {
		if s.Status != TenantStatusActive {
			return nil, rejected("Only an active tenant changes its slug", "invalid_state")
		}
		slug, err := validSlug(d.Slug, c.AggregateID)
		if err != nil {
			return nil, err
		}
		return ev("TenantSlugChanged", TenantSlugChanged{Slug: slug}), nil
	})

	kernable.Handle(tenant, "SuspendTenant", func(c *cc, s Tenant, d SuspendTenant) (events, error) {
		if isPlatform(c) {
			return nil, rejected("The platform tenant cannot be suspended", "platform")
		}
		if s.Status != TenantStatusPending && s.Status != TenantStatusActive {
			return nil, rejected("Cannot suspend this tenant", "invalid_state")
		}
		if (d.Reason == RejectedTenant) != (s.Status == TenantStatusPending) {
			return nil, rejected("A pending tenant is rejected; an active one suspended for another reason", "invalid_reason")
		}
		return ev("TenantSuspended", TenantSuspended{Reason: d.Reason, At: c.Now().UnixMilli()}), nil
	})

	kernable.Handle(tenant, "ReactivateTenant", func(_ *cc, s Tenant, _ ReactivateTenant) (events, error) {
		rejectedTenant := s.Suspension != nil && s.Suspension.Reason == RejectedTenant
		if s.Status != TenantStatusSuspended || rejectedTenant {
			return nil, rejected("Only a suspended tenant is reactivated", "invalid_state")
		}
		return ev("TenantReactivated", TenantReactivated{}), nil
	})

	// Final: the read models drop the tenant, reactors erase its identities, the slug becomes
	// free; the events stay.
	kernable.Handle(tenant, "DeleteTenant", func(c *cc, s Tenant, _ DeleteTenant) (events, error) {
		if isPlatform(c) {
			return nil, rejected("The platform tenant cannot be deleted", "platform")
		}
		if s.Status != TenantStatusSuspended || s.Suspension == nil {
			return nil, rejected("Only a suspended tenant is deleted", "invalid_state")
		}
		if c.Now().UnixMilli() < s.Suspension.At+DeleteAfterDays*Day {
			return nil, rejected(fmt.Sprintf("A tenant is deleted %d days after its suspension", DeleteAfterDays), "too_early")
		}
		return ev("TenantDeleted", TenantDeleted{}), nil
	})

	kernable.Handle(tenant, "AddOwner", func(_ *cc, s Tenant, d AddOwner) (events, error) {
		if err := tenantExists(s); err != nil {
			return nil, err
		}
		if d.IdentityID == "" {
			return nil, rejected("Which identity?", "invalid_identity")
		}
		if slices.Contains(s.Owners, d.IdentityID) {
			return nil, rejected("Already an owner", "exists")
		}
		return ev("OwnerAdded", OwnerAdded{IdentityID: d.IdentityID}), nil
	}, kernable.Requires(func(d AddOwner, _ Tenant) []kernable.Requirement {
		return []kernable.Requirement{{Domain: "identity", AggregateType: "identity", AggregateID: d.IdentityID, Fact: "active"}}
	}))

	kernable.Handle(tenant, "RemoveOwner", func(_ *cc, s Tenant, d RemoveOwner) (events, error) {
		if err := tenantExists(s); err != nil {
			return nil, err
		}
		if !slices.Contains(s.Owners, d.IdentityID) {
			return nil, rejected("Not an owner", "not_found")
		}
		if len(s.Owners) == 1 {
			return nil, rejected("The last owner cannot be removed", "last_owner")
		}
		return ev("OwnerRemoved", OwnerRemoved{IdentityID: d.IdentityID}), nil
	})

	kernable.Handle(tenant, "RequireMfa", func(_ *cc, s Tenant, d RequireMfa) (events, error) {
		if s.Status != TenantStatusActive {
			return nil, rejected("Only an active tenant", "invalid_state")
		}
		return ev("MfaRequirementChanged", MfaRequirementChanged{Required: d.Required}), nil
	})

	kernable.Handle(tenant, "ChangeSignUpPolicy", func(c *cc, s Tenant, d ChangeSignUpPolicy) (events, error) {
		if !isPlatform(c) {
			return nil, rejected("Only the platform tenant has a sign-up policy", "platform")
		}
		if err := tenantExists(s); err != nil {
			return nil, err
		}
		return ev("SignUpPolicyChanged", SignUpPolicyChanged{Tenants: d.Tenants, Accounts: d.Accounts}), nil
	})
}
