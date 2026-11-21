package kit

// The invitation events: what happened to invitations, the vocabulary other domains read.

import (
	"strings"
	"unicode"

	kernable "github.com/kernable/kernable/sdks/go"
)

// InvitedGroup is a group the invited person joins, for the whole tenant or one aggregate.
type InvitedGroup struct {
	Group string `json:"group"`
	Scope *Scope `json:"scope"`
}

// LinkTokenOf is an invitation link as people get it: link_<tenantId>.<linkId>.<secret>.
func LinkTokenOf(tenant, linkID, secret string) string {
	return "link_" + tenant + "." + linkID + "." + secret
}

// LinkToken is the parts of an invitation link token: the tenant, the link and the whole token's hash.
type LinkToken struct{ Tenant, LinkID, Hash string }

// ParseLinkToken is the tenant, the link and the whole token's hash, or false.
func ParseLinkToken(value string) (LinkToken, bool) {
	rest, ok := strings.CutPrefix(value, "link_")
	if !ok {
		return LinkToken{}, false
	}
	p := strings.Split(rest, ".")
	part := func(s string) bool { return s != "" && strings.IndexFunc(s, unicode.IsSpace) < 0 }
	secret := func(s string) bool {
		if len(s) < 32 {
			return false
		}
		for i := 0; i < len(s); i++ {
			c := s[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return false
			}
		}
		return true
	}
	if len(p) != 3 || !part(p[0]) || !part(p[1]) || !secret(p[2]) {
		return LinkToken{}, false
	}
	return LinkToken{Tenant: p[0], LinkID: p[1], Hash: SHA256(value)}, true
}

func ownInvitation(_ map[string]any, invitationID string) string { return "invitation:" + invitationID }

func previousToken(d map[string]any, _ string) string { return "token:" + str(d, "previousTokenId") }

func invitationVault(d *kernable.Domain) {
	d.Event("InvitationCreated", 1).Personal([]string{"email"}, ownInvitation).Personal([]string{"token"}, tokenSubject)
	d.Event("InvitationResent", 1).Personal([]string{"token"}, tokenSubject).Erases(previousToken)
	d.Event("InvitationRevoked", 1).Erases(tokenSubject)
	d.Event("InvitationDeclined", 1).Erases(tokenSubject)
	d.Event("InvitationAccepted", 1).Erases(tokenSubject)
	d.Event("InvitationErased", 1).Erases(ownInvitation)
}

// InvitationCreated: the email belongs to the invitation (its own subject); the token sits in the
// vault until used.
type InvitationCreated struct {
	Email  *string        `json:"email"`
	Groups []InvitedGroup `json:"groups"`
	// An owner invitation: accepting makes the person an owner.
	Owner     bool    `json:"owner"`
	TokenID   string  `json:"tokenId"`
	Token     *string `json:"token"`
	ExpiresAt int64   `json:"expiresAt"`
}

// InvitationResent is a new token; it erases the old one.
type InvitationResent struct {
	PreviousTokenID string  `json:"previousTokenId"`
	TokenID         string  `json:"tokenId"`
	Token           *string `json:"token"`
	ExpiresAt       int64   `json:"expiresAt"`
}

type InvitationRevoked struct {
	TokenID string `json:"tokenId"`
}

type InvitationDeclined struct {
	TokenID string `json:"tokenId"`
}

// InvitationAccepted: the identity reactor creates the identity with these groups (and the owner).
type InvitationAccepted struct {
	TokenID    string         `json:"tokenId"`
	AccountID  string         `json:"accountId"`
	NewAccount bool           `json:"newAccount"`
	Groups     []InvitedGroup `json:"groups"`
	Owner      bool           `json:"owner"`
}

type InvitationMailSent struct {
	TokenID string `json:"tokenId"`
}

// InvitationErased erases the invited email: when the tenant is deleted, or the person asks.
type InvitationErased struct{}

// InvitationLinkCreated is a reusable invitation: anyone signed in who has its token may join the
// tenant with these groups, until it is revoked, expires or is used up. Only the token's hash is
// kept: the API shows the token once.
type InvitationLinkCreated struct {
	Groups    []InvitedGroup `json:"groups"`
	Hash      string         `json:"hash"`
	Label     string         `json:"label"`
	ExpiresAt *int64         `json:"expiresAt"`
	MaxUses   *int64         `json:"maxUses"`
}

type InvitationLinkRevoked struct{}

// InvitationLinkUsed: someone joined through the link; the identity reactor creates their
// identity with these groups.
type InvitationLinkUsed struct {
	AccountID string         `json:"accountId"`
	Groups    []InvitedGroup `json:"groups"`
}
