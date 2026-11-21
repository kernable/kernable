package kit

import (
	"encoding/json"
	"fmt"
	"slices"
)

// enum decodes a string that must be one of values: as in the other kits, any other value fails
// the command (handler_error).
func enum(data []byte, values []string, into *string) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if !slices.Contains(values, s) {
		return fmt.Errorf("unknown variant %q, expected one of %v", s, values)
	}
	*into = s
	return nil
}

// TenantStatus is where a tenant is in its life.
type TenantStatus string

const (
	TenantStatusNone      TenantStatus = "none"
	TenantStatusPending   TenantStatus = "pending"
	TenantStatusActive    TenantStatus = "active"
	TenantStatusSuspended TenantStatus = "suspended"
	TenantStatusDeleted   TenantStatus = "deleted"
)

func (s *TenantStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"none", "pending", "active", "suspended", "deleted"}, (*string)(s))
}

// SuspensionReason is why a tenant was suspended.
type SuspensionReason string

const (
	OwnerRequested SuspensionReason = "owner_requested"
	Unpaid         SuspensionReason = "unpaid"
	Abuse          SuspensionReason = "abuse"
	ByPlatform     SuspensionReason = "by_platform"
	// RejectedTenant: a pending tenant the platform turned down.
	RejectedTenant SuspensionReason = "rejected"
)

func (s *SuspensionReason) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"owner_requested", "unpaid", "abuse", "by_platform", "rejected"}, (*string)(s))
}

// TenantSignUp is whether people may open tenants themselves, and in which status they start.
type TenantSignUp string

const (
	TenantSignUpOff     TenantSignUp = "off"
	TenantSignUpPending TenantSignUp = "pending"
	TenantSignUpActive  TenantSignUp = "active"
)

func (s *TenantSignUp) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"off", "pending", "active"}, (*string)(s))
}

// AccountSignUp is whether people may register accounts themselves.
type AccountSignUp string

const (
	AccountSignUpOff  AccountSignUp = "off"
	AccountSignUpOpen AccountSignUp = "open"
)

func (s *AccountSignUp) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"off", "open"}, (*string)(s))
}

// AccountStatus is where an account is in its life.
type AccountStatus string

const (
	AccountStatusNone      AccountStatus = "none"
	AccountStatusPending   AccountStatus = "pending"
	AccountStatusActive    AccountStatus = "active"
	AccountStatusSuspended AccountStatus = "suspended"
	AccountStatusDeleted   AccountStatus = "deleted"
)

func (s *AccountStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"none", "pending", "active", "suspended", "deleted"}, (*string)(s))
}

type AccountSuspensionReason string

const (
	AccountOwnerRequested AccountSuspensionReason = "owner_requested"
	AccountAbuse          AccountSuspensionReason = "abuse"
	AccountByPlatform     AccountSuspensionReason = "by_platform"
)

func (s *AccountSuspensionReason) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"owner_requested", "abuse", "by_platform"}, (*string)(s))
}

type TokenPurpose string

const (
	TokenVerify      TokenPurpose = "verify"
	TokenReset       TokenPurpose = "reset"
	TokenEmailChange TokenPurpose = "email_change"
)

func (s *TokenPurpose) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"verify", "reset", "email_change"}, (*string)(s))
}

// SessionEnd is why a session ended.
type SessionEnd string

const (
	EndSignedOut        SessionEnd = "signed_out"
	EndRevoked          SessionEnd = "revoked"
	EndAccountSuspended SessionEnd = "account_suspended"
	EndPasswordChanged  SessionEnd = "password_changed"
	EndAccountDeleted   SessionEnd = "account_deleted"
	EndReuse            SessionEnd = "reuse"
	EndIdle             SessionEnd = "idle"
	EndExpired          SessionEnd = "expired"
)

func (s *SessionEnd) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"signed_out", "revoked", "account_suspended", "password_changed", "account_deleted", "reuse", "idle", "expired"}, (*string)(s))
}

// FailureReason is why a sign-in failed (an event, not a rejection).
type FailureReason string

const (
	FailWrongPassword FailureReason = "wrong_password"
	FailLocked        FailureReason = "locked"
	FailNotVerified   FailureReason = "not_verified"
	FailNotActive     FailureReason = "not_active"
	FailNotLinked     FailureReason = "not_linked"
)

func (s *FailureReason) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"wrong_password", "locked", "not_verified", "not_active", "not_linked"}, (*string)(s))
}

type SecondFactor string

const (
	FactorTotp     SecondFactor = "totp"
	FactorRecovery SecondFactor = "recovery"
	FactorPasskey  SecondFactor = "passkey"
)

func (s *SecondFactor) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"totp", "recovery", "passkey"}, (*string)(s))
}

type IdentityStatus string

const (
	IdentityStatusNone      IdentityStatus = "none"
	IdentityStatusActive    IdentityStatus = "active"
	IdentityStatusSuspended IdentityStatus = "suspended"
	IdentityStatusDeleted   IdentityStatus = "deleted"
)

func (s *IdentityStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"none", "active", "suspended", "deleted"}, (*string)(s))
}

type IdentityKind string

const (
	KindPerson  IdentityKind = "person"
	KindService IdentityKind = "service"
	// KindProcess is a business process's: created by the kit when an app registers it.
	KindProcess IdentityKind = "process"
)

func (s *IdentityKind) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"person", "service", "process"}, (*string)(s))
}

type MembershipStatus string

const (
	MembershipStatusActive    MembershipStatus = "active"
	MembershipStatusSuspended MembershipStatus = "suspended"
)

func (s *MembershipStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"active", "suspended"}, (*string)(s))
}

type InvitationStatus string

const (
	InvitationStatusNone     InvitationStatus = "none"
	InvitationStatusOpen     InvitationStatus = "open"
	InvitationStatusAccepted InvitationStatus = "accepted"
	InvitationStatusDeclined InvitationStatus = "declined"
	InvitationStatusRevoked  InvitationStatus = "revoked"
)

func (s *InvitationStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"none", "open", "accepted", "declined", "revoked"}, (*string)(s))
}

type LinkStatus string

const (
	LinkStatusNone    LinkStatus = "none"
	LinkStatusOpen    LinkStatus = "open"
	LinkStatusRevoked LinkStatus = "revoked"
)

func (s *LinkStatus) UnmarshalJSON(b []byte) error {
	return enum(b, []string{"none", "open", "revoked"}, (*string)(s))
}
