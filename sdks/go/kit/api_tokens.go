package kit

// Access tokens: short-lived, signed with Ed25519 (JWT, EdDSA), checked by the API itself without
// asking the kernel. The middleware still refuses an ended session at once. The same bytes as the
// other kits' tokens, so either kit checks the other's.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"
)

type Claims struct {
	// The account.
	Sub string `json:"sub"`
	// The session.
	Sid string `json:"sid"`
	// The factors the session had when the token was issued.
	Fac []string `json:"fac"`
	Iat int64    `json:"iat"`
	Exp int64    `json:"exp"`
}

// SigningKeys sign and check access tokens.
type SigningKeys struct{ Signing ed25519.PrivateKey }

func (k SigningKeys) verifying() ed25519.PublicKey { return k.Signing.Public().(ed25519.PublicKey) }

// NewSigningKeys are new keys, e.g. for development and tests.
func NewSigningKeys() SigningKeys {
	return SigningKeys{Signing: ed25519.NewKeyFromSeed(RandomBytes(32))}
}

// SigningKeysFrom reads keys from a PEM file holding the private key (PKCS #8).
func SigningKeysFrom(path string) (SigningKeys, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return SigningKeys{}, fmt.Errorf("%s: %w", path, err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return SigningKeys{}, fmt.Errorf("%s: no PEM block", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return SigningKeys{}, fmt.Errorf("%s: %w", path, err)
	}
	ed, ok := key.(ed25519.PrivateKey)
	if !ok {
		return SigningKeys{}, fmt.Errorf("%s: not an Ed25519 key", path)
	}
	return SigningKeys{Signing: ed}, nil
}

// jwtHeader is {"alg":"EdDSA","typ":"JWT"}, as the other kits write it.
const jwtHeader = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9"

func compactJSON(v any) []byte {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	return bytes.TrimRight(b.Bytes(), "\n")
}

// IssueAccessToken issues a token for account sub's session sid with its factors; nowMs 0 is now.
func IssueAccessToken(keys SigningKeys, sub, sid string, fac []string, nowMs int64) string {
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}
	iat := nowMs / 1000
	if nowMs < 0 && nowMs%1000 != 0 {
		iat--
	}
	if fac == nil {
		fac = []string{}
	}
	body := base64.RawURLEncoding.EncodeToString(compactJSON(Claims{Sub: sub, Sid: sid, Fac: fac, Iat: iat, Exp: iat + AccessTokenMinutes*60}))
	signature := ed25519.Sign(keys.Signing, []byte(jwtHeader+"."+body))
	return jwtHeader + "." + body + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// CheckAccessToken is the claims of a valid, unexpired token, else nil.
func CheckAccessToken(keys SigningKeys, token string, nowMs int64) *Claims {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != jwtHeader {
		return nil
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(keys.verifying(), []byte(parts[0]+"."+parts[1]), signature) {
		return nil
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var c Claims
	if json.Unmarshal(body, &c) != nil {
		return nil
	}
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}
	if c.Exp*1000 <= nowMs {
		return nil
	}
	return &c
}
