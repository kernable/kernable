package kit

import "sync"

// Settings of the kit. The kit is yours: change them here (design/kits.md, "Constants").

// PlatformTenant is the tenant the platform's own accounts, sessions, groups and administrators
// live in.
const PlatformTenant = "platform"

// ReservedSlugs are slugs nobody may take for a tenant.
var ReservedSlugs = []string{"platform", "www", "api", "admin", "app", "mail", "auth", "login", "static"}

const (
	// DeleteAfterDays is how long a suspended tenant waits before it may be deleted.
	DeleteAfterDays = 0
	// LockoutAfter failed sign-ins lock the account for a while: 1, 2, 4 … minutes, at most
	// LockoutMaxMinutes.
	LockoutAfter      = 5
	LockoutMaxMinutes = 15
	VerificationHours = 48
	ResetMinutes      = 60
	// MailCooldownMinutes: at most one mail per account in this many minutes for what anyone can
	// ask for by email (a password reset, the "you already have an account" mail).
	MailCooldownMinutes = 5
	AccessTokenMinutes  = 15
	SessionIdleDays     = 14
	SessionMaxDays      = 90
	InvitationDays      = 7
	// ReuseWindow is how many of a session's older refresh tokens are recognised as a reuse
	// (which ends the session).
	ReuseWindow = 32
	// ChallengeSeconds is how long a passkey challenge holds.
	ChallengeSeconds = 300
	// AuthorizationStore is the authorization middleware's store; UseAuthorizationStore changes it.
	AuthorizationStore = "sqlite:authorization.db"
	// SnapshotSchema is the snapshot format of the kit's aggregates in Go: another language's kit
	// keeps its own.
	SnapshotSchema = "go-1"
)

// Listener orders the kit takes: its keys need a grant of reserved_order. Cleanup: reactors that
// must see a deleted tenant's rows before the read models drop them.
const (
	OrderAuthorization = -30
	OrderCleanup       = -25
	OrderReadModels    = -20
	OrderReactors      = -10
)

const (
	Minute int64 = 60_000
	Hour         = 60 * Minute
	Day          = 24 * Hour
)

// RelyingParty is the relying party passkeys are bound to (WebAuthn): your site's domain, name
// and origins. UsePasskeys sets it at runtime; the defaults suit local development.
type RelyingParty struct {
	// Your site's domain: example.com.
	RPID string
	// The name authenticators show.
	RPName string
	// The origins your pages are served from: https://example.com.
	Origins []string
}

var passkeysMu sync.RWMutex
var relyingParty *RelyingParty

// UsePasskeys binds passkeys to your site: call it before the app runs and the API serves.
func UsePasskeys(rp RelyingParty) {
	passkeysMu.Lock()
	defer passkeysMu.Unlock()
	relyingParty = &rp
}

// Passkeys is the relying party passkeys are bound to now.
func Passkeys() RelyingParty {
	passkeysMu.RLock()
	defer passkeysMu.RUnlock()
	if relyingParty == nil {
		return RelyingParty{RPID: "localhost", RPName: "kernable", Origins: []string{"http://localhost:3000"}}
	}
	return *relyingParty
}
