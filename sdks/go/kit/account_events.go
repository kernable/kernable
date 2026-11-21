package kit

// The account events: what happened to accounts, sessions and SSOs, the vocabulary other domains
// read.

import (
	"fmt"

	kernable "github.com/kernable/kernable/sdks/go"
)

func str(d map[string]any, field string) string { s, _ := d[field].(string); return s }

// own is the account itself as the subject of its personal data.
func own(_ map[string]any, aggregateID string) string { return aggregateID }

func tokenSubject(d map[string]any, _ string) string { return "token:" + str(d, "tokenId") }

func ofAccount(d map[string]any, _ string) string { return str(d, "accountId") }

func ofSso(_ map[string]any, ssoID string) string { return fmt.Sprintf("sso:%s", ssoID) }

// accountVault declares the vault fields of the account events.
func accountVault(d *kernable.Domain) {
	d.Event("AccountRegistered", 1).Personal([]string{"email", "name", "passwordHash"}, own)
	d.Event("VerificationRequested", 1).Personal([]string{"email"}, own).Personal([]string{"token"}, tokenSubject)
	d.Event("EmailVerified", 1).Erases(tokenSubject)
	d.Event("SignedIn", 1).Personal([]string{"ip", "userAgent"}, own)
	d.Event("SignInFailed", 1).Personal([]string{"ip"}, own)
	d.Event("SessionOpened", 1).Personal([]string{"ip", "userAgent"}, ofAccount)
	d.Event("SessionRefreshed", 1).Personal([]string{"ip"}, ofAccount)
	d.Event("AccountDeleted", 1).Erases(own)
	d.Event("PasswordChanged", 1).Personal([]string{"hash"}, own)
	d.Event("PasswordResetRequested", 1).Personal([]string{"email"}, own).Personal([]string{"token"}, tokenSubject)
	d.Event("PasswordReset", 1).Personal([]string{"hash"}, own).Erases(tokenSubject)
	d.Event("EmailChangeRequested", 1).Personal([]string{"email"}, own).Personal([]string{"token"}, tokenSubject)
	d.Event("EmailChanged", 1).Personal([]string{"email"}, own).Erases(tokenSubject)
	d.Event("RegistrationAttempted", 1).Personal([]string{"email"}, own)
	d.Event("TotpStarted", 1).Personal([]string{"secret"}, own)
	d.Event("PasskeyAdded", 1).Personal([]string{"credentialId", "label"}, own)
	d.Event("PasskeyRemoved", 1).Personal([]string{"credentialId"}, own)
	d.Event("PasskeyUsed", 1).Personal([]string{"credentialId"}, own)
	d.Event("ProviderLinked", 1).Personal([]string{"subject", "email"}, own)
	d.Event("ProviderUnlinked", 1).Personal([]string{"subject"}, own)
	d.Event("SsoCreated", 1).Personal([]string{"clientSecret"}, ofSso)
	d.Event("SsoUpdated", 1).Personal([]string{"clientSecret"}, ofSso)
	d.Event("SsoDeleted", 1).Erases(ofSso)
}

type AccountRegistered struct {
	Email *string `json:"email"`
	Name  *string `json:"name"`
	// argon2id; nil for an account without a password (passkeys, single sign-on).
	PasswordHash *string `json:"passwordHash"`
	// True when an accepted invitation proved the email: active at once.
	Verified bool  `json:"verified"`
	At       int64 `json:"at"`
}

// VerificationRequested: the token stays in the vault under its own subject until it is used.
type VerificationRequested struct {
	TokenID   string  `json:"tokenId"`
	Token     *string `json:"token"`
	Email     *string `json:"email"`
	ExpiresAt int64   `json:"expiresAt"`
}

// EmailVerified erases the used token.
type EmailVerified struct {
	TokenID string `json:"tokenId"`
}

type SignedIn struct {
	SessionID        string   `json:"sessionId"`
	Method           string   `json:"method"`
	Factors          []string `json:"factors"`
	RefreshTokenHash string   `json:"refreshTokenHash"`
	IP               *string  `json:"ip"`
	UserAgent        *string  `json:"userAgent"`
	// The account has a second factor on: the session must prove it before anything else.
	Mfa bool  `json:"mfa"`
	At  int64 `json:"at"`
}

// SignInFailed is an event, not a rejection: audited, and it counts toward the lockout.
type SignInFailed struct {
	Method string        `json:"method"`
	Reason FailureReason `json:"reason"`
	IP     *string       `json:"ip"`
	At     int64         `json:"at"`
}

type SessionOpened struct {
	AccountID        string   `json:"accountId"`
	Method           string   `json:"method"`
	Factors          []string `json:"factors"`
	RefreshTokenHash string   `json:"refreshTokenHash"`
	IP               *string  `json:"ip"`
	UserAgent        *string  `json:"userAgent"`
	At               int64    `json:"at"`
}

type SessionRefreshed struct {
	Generation       uint64  `json:"generation"`
	RefreshTokenHash string  `json:"refreshTokenHash"`
	IP               *string `json:"ip"`
	AccountID        string  `json:"accountId"`
	// The session's factors, so the API issues the new access token without asking.
	Factors []string `json:"factors"`
	At      int64    `json:"at"`
}

type SessionFactorAdded struct {
	Factor string `json:"factor"`
}

type SessionEnded struct {
	Reason SessionEnd `json:"reason"`
}

// AccountSuspended ends every session (a reactor); sign-in is refused.
type AccountSuspended struct {
	Reason AccountSuspensionReason `json:"reason"`
	At     int64                   `json:"at"`
}

type AccountReactivated struct{}

// AccountDeleted erases the account's personal data, in its events and its sessions'.
type AccountDeleted struct{}

// PasswordChanged ends the account's other sessions (a reactor); KeepSessionID is the one it was
// changed from.
type PasswordChanged struct {
	Hash          *string `json:"hash"`
	KeepSessionID *string `json:"keepSessionId"`
	At            int64   `json:"at"`
}

type PasswordResetRequested struct {
	TokenID   string  `json:"tokenId"`
	Token     *string `json:"token"`
	Email     *string `json:"email"`
	ExpiresAt int64   `json:"expiresAt"`
}

// PasswordReset erases the used token; ends every session (a reactor).
type PasswordReset struct {
	TokenID string  `json:"tokenId"`
	Hash    *string `json:"hash"`
	At      int64   `json:"at"`
}

// EmailChangeRequested: the new address gets a token; it holds nothing until confirmed.
type EmailChangeRequested struct {
	TokenID   string  `json:"tokenId"`
	Token     *string `json:"token"`
	Email     *string `json:"email"`
	ExpiresAt int64   `json:"expiresAt"`
}

type EmailChanged struct {
	TokenID string  `json:"tokenId"`
	Email   *string `json:"email"`
}

// RegistrationAttempted: someone registered with this account's address; it gets a mail saying
// it exists.
type RegistrationAttempted struct {
	Email *string `json:"email"`
	At    int64   `json:"at"`
}

type TotpStarted struct {
	Secret *string `json:"secret"`
}

type TotpConfirmed struct {
	Step int64 `json:"step"`
	At   int64 `json:"at"`
}

type TotpDisabled struct{}

type RecoveryCodesGenerated struct {
	Hashes []string `json:"hashes"`
}

type SecondFactorProved struct {
	SessionID        string       `json:"sessionId"`
	Method           SecondFactor `json:"method"`
	Step             *int64       `json:"step"`
	RecoveryCodeHash *string      `json:"recoveryCodeHash"`
}

type SecondFactorFailed struct {
	SessionID string `json:"sessionId"`
	At        int64  `json:"at"`
}

type PasskeyAdded struct {
	CredentialID *string `json:"credentialId"`
	PublicKey    string  `json:"publicKey"`
	Counter      uint64  `json:"counter"`
	Label        *string `json:"label"`
	At           int64   `json:"at"`
}

type PasskeyRemoved struct {
	CredentialID *string `json:"credentialId"`
}

type PasskeyUsed struct {
	CredentialID *string `json:"credentialId"`
	Counter      uint64  `json:"counter"`
}

type ProviderLinked struct {
	Provider string  `json:"provider"`
	Subject  *string `json:"subject"`
	Email    *string `json:"email"`
	At       int64   `json:"at"`
}

type ProviderUnlinked struct {
	Provider string  `json:"provider"`
	Subject  *string `json:"subject"`
}

type SsoCreated struct {
	Issuer       string  `json:"issuer"`
	ClientID     string  `json:"clientId"`
	ClientSecret *string `json:"clientSecret"`
}

type SsoUpdated struct {
	Issuer       string  `json:"issuer"`
	ClientID     string  `json:"clientId"`
	ClientSecret *string `json:"clientSecret"`
}

type SsoDomainAdded struct {
	Domain string `json:"domain"`
	Txt    string `json:"txt"`
}

type SsoDomainVerified struct {
	Domain string `json:"domain"`
}

type SsoDomainRemoved struct {
	Domain string `json:"domain"`
}

type SsoEnforcementChanged struct {
	Enforced bool `json:"enforced"`
}

type SsoDisabled struct{}
type SsoEnabled struct{}

// SsoDeleted erases the SSO's secret.
type SsoDeleted struct{}
