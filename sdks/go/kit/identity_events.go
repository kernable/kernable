package kit

// The identity events: what happened to identities, the vocabulary other domains read.

import (
	"strings"
	"unicode/utf8"

	kernable "github.com/kernable/kernable/sdks/go"
)

// Scope is one aggregate a membership is limited to (or what has it as parent):
// {type: "shop.order", id}.
type Scope struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// ownIdentity is the identity as the subject of its personal data.
func ownIdentity(_ map[string]any, identityID string) string { return "identity:" + identityID }

// APIKey is an API key as its holder sends it: key_<keyId>_<secret>.
func APIKey(keyID, secret string) string { return "key_" + keyID + "_" + secret }

// ParseAPIKey is the key ID and the whole key's hash, or false if it has another shape.
func ParseAPIKey(value string) (keyID, hash string, ok bool) {
	rest, found := strings.CutPrefix(value, "key_")
	if !found {
		return "", "", false
	}
	keyID, secret, found := strings.Cut(rest, "_")
	if !found || len(keyID) < 8 || len(keyID) > 32 || utf8.RuneCountInString(secret) < 32 {
		return "", "", false
	}
	for i := 0; i < len(keyID); i++ {
		c := keyID[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return "", "", false
		}
	}
	return keyID, SHA256(value), true
}

func identityVault(d *kernable.Domain) {
	d.Event("IdentityCreated", 1).Personal([]string{"displayName", "title", "email"}, ownIdentity)
	d.Event("ProfileUpdated", 1).Personal([]string{"displayName", "title", "email"}, ownIdentity)
	d.Event("IdentityDeleted", 1).Erases(ownIdentity)
}

type IdentityCreated struct {
	Kind        IdentityKind `json:"kind"`
	AccountID   *string      `json:"accountId"`
	DisplayName *string      `json:"displayName"`
	Title       *string      `json:"title"`
	Email       *string      `json:"email"`
}

type ProfileUpdated struct {
	DisplayName *string `json:"displayName"`
	Title       *string `json:"title"`
	Email       *string `json:"email"`
}

type MembershipGranted struct {
	MembershipID string `json:"membershipId"`
	Group        string `json:"group"`
	// nil: the whole tenant; else one aggregate (or what has it as parent).
	Scope     *Scope `json:"scope"`
	ExpiresAt *int64 `json:"expiresAt"`
}

type MembershipSuspended struct {
	MembershipID string `json:"membershipId"`
}

type MembershipResumed struct {
	MembershipID string `json:"membershipId"`
}

type MembershipRevoked struct {
	MembershipID string `json:"membershipId"`
}

type ApiKeyCreated struct {
	KeyID string `json:"keyId"`
	// SHA-256 of the whole key: it has high entropy, so the hash needs no vault.
	Hash      string `json:"hash"`
	Label     string `json:"label"`
	ExpiresAt *int64 `json:"expiresAt"`
}

type ApiKeyRevoked struct {
	KeyID string `json:"keyId"`
}

type IdentitySuspended struct {
	Reason string `json:"reason"`
}

type IdentityReactivated struct{}

// IdentityDeleted erases the identity's personal data.
type IdentityDeleted struct{}
