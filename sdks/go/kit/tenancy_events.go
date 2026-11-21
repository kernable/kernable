package kit

// The tenancy events: what happened to tenants, the vocabulary other domains read.

type TenantCreated struct {
	Slug  string `json:"slug"`
	Name  string `json:"name"`
	Owner Owner  `json:"owner"`
	// active or pending.
	Status TenantStatus `json:"status"`
}

type TenantActivated struct{}

type TenantRenamed struct {
	Name string `json:"name"`
}

type TenantSlugChanged struct {
	Slug string `json:"slug"`
}

type TenantSuspended struct {
	Reason SuspensionReason `json:"reason"`
	// In the payload: the retention before deletion is computed from it.
	At int64 `json:"at"`
}

type TenantReactivated struct{}

type TenantDeleted struct{}

type OwnerAdded struct {
	IdentityID string `json:"identityId"`
}

type OwnerRemoved struct {
	IdentityID string `json:"identityId"`
}

type MfaRequirementChanged struct {
	Required bool `json:"required"`
}

type SignUpPolicyChanged struct {
	Tenants  TenantSignUp  `json:"tenants"`
	Accounts AccountSignUp `json:"accounts"`
}
