package kit

// The account aggregates: state, how each event changes it, unique keys and facts.

import (
	"fmt"
	"slices"

	kernable "github.com/kernable/kernable/sdks/go"
)

type Password struct {
	Hash  *string `json:"hash"`
	SetAt int64   `json:"setAt"`
}

type Failures struct {
	Count  uint32 `json:"count"`
	LastAt int64  `json:"lastAt"`
}

// OpenToken is an emailed token waiting to be used; the token itself is in the vault until then.
type OpenToken struct {
	TokenID   string       `json:"tokenId"`
	Purpose   TokenPurpose `json:"purpose"`
	Token     *string      `json:"token"`
	ExpiresAt int64        `json:"expiresAt"`
}

type Totp struct {
	Secret    *string `json:"secret"`
	Confirmed bool    `json:"confirmed"`
	LastStep  int64   `json:"lastStep"`
}

type Passkey struct {
	CredentialID *string `json:"credentialId"`
	PublicKey    string  `json:"publicKey"`
	Counter      uint64  `json:"counter"`
	Label        *string `json:"label"`
}

type Link struct {
	Provider string  `json:"provider"`
	Subject  *string `json:"subject"`
}

type AccountSuspension struct {
	Reason AccountSuspensionReason `json:"reason"`
	At     int64                   `json:"at"`
}

// Account is a person's account, in the platform tenant.
type Account struct {
	ID            string             `json:"id"`
	Status        AccountStatus      `json:"status"`
	Email         *string            `json:"email"`
	EmailVerified bool               `json:"emailVerified"`
	Name          *string            `json:"name"`
	Password      *Password          `json:"password"`
	Failures      Failures           `json:"failures"`
	OpenTokens    []OpenToken        `json:"openTokens"`
	Suspension    *AccountSuspension `json:"suspension"`
	// A new address waiting for its confirmation: it holds no key yet.
	PendingEmail *string `json:"pendingEmail"`
	Totp         *Totp   `json:"totp"`
	// SHA-256 of the unused recovery codes.
	RecoveryCodes []string  `json:"recoveryCodes"`
	Passkeys      []Passkey `json:"passkeys"`
	Links         []Link    `json:"links"`
	// When the last "you already have an account" mail went out (RegistrationAttempted); 0: never.
	AttemptNotedAt int64 `json:"attemptNotedAt"`
}

// LockedUntil is until when the account is locked after failed sign-ins: computed, never stored.
func (a Account) LockedUntil() int64 {
	count := a.Failures.Count
	if count < LockoutAfter {
		return 0
	}
	minutes := int64(LockoutMaxMinutes)
	if e := count - LockoutAfter; e < 62 {
		minutes = min(int64(1)<<e, LockoutMaxMinutes)
	}
	return a.Failures.LastAt + minutes*Minute
}

// Mfa: TOTP is on (confirmed), so every session must prove a second factor.
func (a Account) Mfa() bool { return a.Totp != nil && a.Totp.Confirmed }

func (a *Account) open(purpose TokenPurpose, tokenID string, token *string, expiresAt int64) {
	a.OpenTokens = slices.DeleteFunc(a.OpenTokens, func(t OpenToken) bool { return t.Purpose == purpose })
	a.OpenTokens = append(a.OpenTokens, OpenToken{TokenID: tokenID, Purpose: purpose, Token: token, ExpiresAt: expiresAt})
}

func (a *Account) close(tokenID string) {
	a.OpenTokens = slices.DeleteFunc(a.OpenTokens, func(t OpenToken) bool { return t.TokenID == tokenID })
}

func eqPtr(a, b *string) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func accountAggregate(d *kernable.Domain) *kernable.Aggregate[Account] {
	a := kernable.NewAggregate(d, "account", Account{Status: AccountStatusNone}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "AccountRegistered", func(c *ac, s *Account, e AccountRegistered) {
		s.ID = c.AggregateID
		s.Status = AccountStatusPending
		if e.Verified {
			s.Status = AccountStatusActive
		}
		s.Email, s.EmailVerified, s.Name = e.Email, e.Verified, e.Name
		s.Password = nil
		if e.PasswordHash != nil {
			s.Password = &Password{Hash: e.PasswordHash, SetAt: e.At}
		}
	})
	kernable.Apply(a, "VerificationRequested", func(_ *ac, s *Account, e VerificationRequested) {
		s.open(TokenVerify, e.TokenID, e.Token, e.ExpiresAt)
	})
	kernable.Apply(a, "EmailVerified", func(_ *ac, s *Account, e EmailVerified) {
		s.EmailVerified = true
		if s.Status == AccountStatusPending {
			s.Status = AccountStatusActive
		}
		s.close(e.TokenID)
	})
	kernable.Apply(a, "AccountSuspended", func(_ *ac, s *Account, e AccountSuspended) {
		s.Status, s.Suspension = AccountStatusSuspended, &AccountSuspension{Reason: e.Reason, At: e.At}
	})
	kernable.Apply(a, "AccountReactivated", func(_ *ac, s *Account, _ AccountReactivated) {
		s.Status, s.Suspension = AccountStatusActive, nil
	})
	kernable.Apply(a, "AccountDeleted", func(_ *ac, s *Account, _ AccountDeleted) {
		s.Status, s.Password, s.OpenTokens = AccountStatusDeleted, nil, nil
	})
	kernable.Apply(a, "PasswordChanged", func(_ *ac, s *Account, e PasswordChanged) {
		s.Password = &Password{Hash: e.Hash, SetAt: e.At}
	})
	kernable.Apply(a, "PasswordResetRequested", func(_ *ac, s *Account, e PasswordResetRequested) {
		s.open(TokenReset, e.TokenID, e.Token, e.ExpiresAt)
	})
	kernable.Apply(a, "PasswordReset", func(_ *ac, s *Account, e PasswordReset) {
		s.Password = &Password{Hash: e.Hash, SetAt: e.At}
		s.Failures = Failures{}
		s.close(e.TokenID)
	})
	kernable.Apply(a, "EmailChangeRequested", func(_ *ac, s *Account, e EmailChangeRequested) {
		s.PendingEmail = e.Email
		s.open(TokenEmailChange, e.TokenID, e.Token, e.ExpiresAt)
	})
	kernable.Apply(a, "EmailChanged", func(_ *ac, s *Account, e EmailChanged) {
		s.Email, s.EmailVerified, s.PendingEmail = e.Email, true, nil
		s.close(e.TokenID)
	})
	kernable.Apply(a, "TotpStarted", func(_ *ac, s *Account, e TotpStarted) { s.Totp = &Totp{Secret: e.Secret} })
	kernable.Apply(a, "TotpConfirmed", func(_ *ac, s *Account, e TotpConfirmed) {
		if s.Totp != nil {
			s.Totp.Confirmed, s.Totp.LastStep = true, e.Step
		}
	})
	kernable.Apply(a, "TotpDisabled", func(_ *ac, s *Account, _ TotpDisabled) { s.Totp, s.RecoveryCodes = nil, nil })
	kernable.Apply(a, "RecoveryCodesGenerated", func(_ *ac, s *Account, e RecoveryCodesGenerated) { s.RecoveryCodes = e.Hashes })
	kernable.Apply(a, "SecondFactorProved", func(_ *ac, s *Account, e SecondFactorProved) {
		if e.Step != nil && s.Totp != nil {
			s.Totp.LastStep = *e.Step
		}
		if e.RecoveryCodeHash != nil {
			s.RecoveryCodes = slices.DeleteFunc(s.RecoveryCodes, func(h string) bool { return h == *e.RecoveryCodeHash })
		}
		s.Failures = Failures{}
	})
	kernable.Apply(a, "SecondFactorFailed", func(_ *ac, s *Account, e SecondFactorFailed) {
		s.Failures = Failures{Count: s.Failures.Count + 1, LastAt: e.At}
	})
	kernable.Apply(a, "PasskeyAdded", func(_ *ac, s *Account, e PasskeyAdded) {
		s.Passkeys = append(s.Passkeys, Passkey{CredentialID: e.CredentialID, PublicKey: e.PublicKey, Counter: e.Counter, Label: e.Label})
	})
	kernable.Apply(a, "PasskeyRemoved", func(_ *ac, s *Account, e PasskeyRemoved) {
		s.Passkeys = slices.DeleteFunc(s.Passkeys, func(p Passkey) bool { return eqPtr(p.CredentialID, e.CredentialID) })
	})
	kernable.Apply(a, "PasskeyUsed", func(_ *ac, s *Account, e PasskeyUsed) {
		for i := range s.Passkeys {
			if eqPtr(s.Passkeys[i].CredentialID, e.CredentialID) {
				s.Passkeys[i].Counter = e.Counter
			}
		}
	})
	kernable.Apply(a, "ProviderLinked", func(_ *ac, s *Account, e ProviderLinked) {
		s.Links = append(s.Links, Link{Provider: e.Provider, Subject: e.Subject})
	})
	kernable.Apply(a, "ProviderUnlinked", func(_ *ac, s *Account, e ProviderUnlinked) {
		s.Links = slices.DeleteFunc(s.Links, func(x Link) bool { return x.Provider == e.Provider && eqPtr(x.Subject, e.Subject) })
	})
	kernable.Apply(a, "RegistrationAttempted", func(_ *ac, s *Account, e RegistrationAttempted) { s.AttemptNotedAt = e.At })
	// A sign-in that still owes its second factor is not complete: its failures stay, so wrong
	// codes after the right password add up to the lockout (SecondFactorProved clears them).
	kernable.Apply(a, "SignedIn", func(_ *ac, s *Account, e SignedIn) {
		if !e.Mfa {
			s.Failures = Failures{}
		}
	})
	kernable.Apply(a, "SignInFailed", func(_ *ac, s *Account, e SignInFailed) {
		if e.Reason == FailWrongPassword {
			s.Failures = Failures{Count: s.Failures.Count + 1, LastAt: e.At}
		}
	})

	self := kernable.PersonalKey(func(s Account) string { return s.ID })
	// Each external login belongs to one account.
	a.Unique("link", func(s Account) []string {
		if s.Status == AccountStatusDeleted {
			return nil
		}
		var out []string
		for _, x := range s.Links {
			if x.Subject != nil {
				out = append(out, fmt.Sprintf("%s:%s", x.Provider, *x.Subject))
			}
		}
		return out
	}, self)
	// Held from registration on, while not deleted: nobody registers an email twice.
	a.Unique("email", func(s Account) []string {
		if s.Email == nil || s.Status == AccountStatusNone || s.Status == AccountStatusDeleted {
			return nil
		}
		return []string{*s.Email}
	}, self)
	a.Fact("active", func(s Account) bool { return s.Status == AccountStatusActive })
	a.Fact("mfa", func(s Account) bool { return s.Mfa() })
	return a
}

// --- Sessions ------------------------------------------------------------------------------

type SessionStatus string

const (
	SessionNone   SessionStatus = "none"
	SessionActive SessionStatus = "active"
	SessionEndedS SessionStatus = "ended"
)

// Session is one sign-in.
type Session struct {
	Status           SessionStatus `json:"status"`
	AccountID        string        `json:"accountId"`
	Method           string        `json:"method"`
	Factors          []string      `json:"factors"`
	Generation       uint64        `json:"generation"`
	RefreshTokenHash string        `json:"refreshTokenHash"`
	// The hashes of the last refresh tokens before the current one: a reuse is one of them.
	PreviousTokenHashes []string    `json:"previousTokenHashes"`
	OpenedAt            int64       `json:"openedAt"`
	LastRefreshAt       int64       `json:"lastRefreshAt"`
	EndReason           *SessionEnd `json:"endReason"`
}

func sessionAggregate(d *kernable.Domain) *kernable.Aggregate[Session] {
	a := kernable.NewAggregate(d, "session", Session{Status: SessionNone}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "SessionOpened", func(_ *ac, s *Session, e SessionOpened) {
		*s = Session{Status: SessionActive, AccountID: e.AccountID, Method: e.Method, Factors: e.Factors, Generation: 1,
			RefreshTokenHash: e.RefreshTokenHash, OpenedAt: e.At, LastRefreshAt: e.At}
	})
	kernable.Apply(a, "SessionRefreshed", func(_ *ac, s *Session, e SessionRefreshed) {
		if s.RefreshTokenHash != "" {
			s.PreviousTokenHashes = append(s.PreviousTokenHashes, s.RefreshTokenHash)
			if extra := len(s.PreviousTokenHashes) - ReuseWindow; extra > 0 {
				s.PreviousTokenHashes = s.PreviousTokenHashes[extra:]
			}
		}
		s.Generation, s.RefreshTokenHash, s.LastRefreshAt = e.Generation, e.RefreshTokenHash, e.At
	})
	kernable.Apply(a, "SessionFactorAdded", func(_ *ac, s *Session, e SessionFactorAdded) { s.Factors = append(s.Factors, e.Factor) })
	kernable.Apply(a, "SessionEnded", func(_ *ac, s *Session, e SessionEnded) {
		reason := e.Reason
		s.Status, s.EndReason = SessionEndedS, &reason
	})
	return a
}

// --- account.sso: a tenant's single sign-on, under the tenant's ID ------------------------

type SsoStatus string

const (
	SsoNone      SsoStatus = "none"
	SsoActive    SsoStatus = "active"
	SsoDisabledS SsoStatus = "disabled"
	SsoDeletedS  SsoStatus = "deleted"
)

type SsoDomain struct {
	Domain   string `json:"domain"`
	Txt      string `json:"txt"`
	Verified bool   `json:"verified"`
}

// Sso is a tenant's single sign-on.
type Sso struct {
	Status       SsoStatus   `json:"status"`
	Issuer       string      `json:"issuer"`
	ClientID     string      `json:"clientId"`
	ClientSecret *string     `json:"clientSecret"`
	Domains      []SsoDomain `json:"domains"`
	Enforced     bool        `json:"enforced"`
}

func ssoAggregate(d *kernable.Domain) *kernable.Aggregate[Sso] {
	a := kernable.NewAggregate(d, "sso", Sso{Status: SsoNone}, kernable.StateSchema(SnapshotSchema))
	kernable.Apply(a, "SsoCreated", func(_ *ac, s *Sso, e SsoCreated) {
		s.Status, s.Issuer, s.ClientID, s.ClientSecret = SsoActive, e.Issuer, e.ClientID, e.ClientSecret
	})
	kernable.Apply(a, "SsoUpdated", func(_ *ac, s *Sso, e SsoUpdated) {
		s.Issuer, s.ClientID, s.ClientSecret = e.Issuer, e.ClientID, e.ClientSecret
	})
	kernable.Apply(a, "SsoDomainAdded", func(_ *ac, s *Sso, e SsoDomainAdded) {
		s.Domains = append(s.Domains, SsoDomain{Domain: e.Domain, Txt: e.Txt})
	})
	kernable.Apply(a, "SsoDomainVerified", func(_ *ac, s *Sso, e SsoDomainVerified) {
		for i := range s.Domains {
			if s.Domains[i].Domain == e.Domain {
				s.Domains[i].Verified = true
			}
		}
	})
	kernable.Apply(a, "SsoDomainRemoved", func(_ *ac, s *Sso, e SsoDomainRemoved) {
		s.Domains = slices.DeleteFunc(s.Domains, func(x SsoDomain) bool { return x.Domain == e.Domain })
	})
	kernable.Apply(a, "SsoEnforcementChanged", func(_ *ac, s *Sso, e SsoEnforcementChanged) { s.Enforced = e.Enforced })
	kernable.Apply(a, "SsoDisabled", func(_ *ac, s *Sso, _ SsoDisabled) { s.Status = SsoDisabledS })
	kernable.Apply(a, "SsoEnabled", func(_ *ac, s *Sso, _ SsoEnabled) { s.Status = SsoActive })
	kernable.Apply(a, "SsoDeleted", func(_ *ac, s *Sso, _ SsoDeleted) {
		s.Status, s.ClientSecret, s.Domains = SsoDeletedS, nil, nil
	})
	// A verified domain belongs to one tenant's SSO.
	a.Unique("domain", func(s Sso) []string {
		if s.Status == SsoDeletedS {
			return nil
		}
		var out []string
		for _, x := range s.Domains {
			if x.Verified {
				out = append(out, x.Domain)
			}
		}
		return out
	}, kernable.Global[Sso]())
	return a
}
