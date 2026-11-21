package kit

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/url"
	"testing"
	"time"
)

func TestThePrimaryVerifiedAddressIsThePersons(t *testing.T) {
	list := func(s string) any {
		var v any
		_ = json.Unmarshal([]byte(s), &v)
		return v
	}
	if e, ok := OAuth2Email(list(`[{"email":"old@example.com","primary":false,"verified":true},{"email":"ada@example.com","primary":true,"verified":true}]`)); !ok || e != "ada@example.com" {
		t.Fatal(e)
	}
	// A primary address that is not verified does not count; another verified one does.
	if e, ok := OAuth2Email(list(`[{"email":"ada@example.com","primary":true,"verified":false},{"email":"work@example.com","primary":false,"verified":true}]`)); !ok || e != "work@example.com" {
		t.Fatal(e)
	}
	if _, ok := OAuth2Email(list(`[{"email":"ada@example.com","primary":true,"verified":false}]`)); ok {
		t.Fatal("an unverified address counts")
	}
	if _, ok := OAuth2Email(list(`{"message":"Bad credentials"}`)); ok {
		t.Fatal("an error counts")
	}
}

func TestGitHubNamesItsEndpointsOnItsWebAndAPIHosts(t *testing.T) {
	o := GitHubProvider("id", "secret", "https://github.com/", "https://api.github.com").OAuth2
	if o.Authorize != "https://github.com/login/oauth/authorize" || o.Token != "https://github.com/login/oauth/access_token" ||
		o.User != "https://api.github.com/user" || o.Emails != "https://api.github.com/user/emails" {
		t.Fatalf("%+v", o)
	}
	if OIDCProvider("https://accounts.example.com", "id", "s").OAuth2 != nil {
		t.Fatal("an OIDC provider with OAuth2 endpoints")
	}
}

func TestEncodings(t *testing.T) {
	if got := percent("Acme Inc:ada@example.com/ü~"); got != "Acme%20Inc%3Aada%40example.com%2F%C3%BC~" {
		t.Fatal(got)
	}
	if got := formEncode("openid email*-._~/"); got != "openid+email*-._%7E%2F" {
		t.Fatal(got)
	}
	u, _ := url.Parse("http://localhost/cb?state=a%2Bb+c&code=x%ZZ")
	if s := queryValue(u, "state"); s == nil || *s != "a+b c" {
		t.Fatal(s)
	}
	if s := queryValue(u, "code"); s == nil || *s != "x%ZZ" {
		t.Fatal(s)
	}
	if queryValue(u, "nonce") != nil {
		t.Fatal("a missing parameter")
	}
}

func TestIDTokensOfEveryAlgorithm(t *testing.T) {
	enc := base64.RawURLEncoding.EncodeToString
	now := time.Now().Unix()
	token := func(alg, kid string, claims map[string]any, sign func([]byte) []byte) string {
		h, _ := json.Marshal(map[string]any{"alg": alg, "kid": kid})
		c, _ := json.Marshal(claims)
		signed := enc(h) + "." + enc(c)
		return signed + "." + enc(sign([]byte(signed)))
	}
	claims := map[string]any{"iss": "https://issuer", "aud": "client", "exp": now + 60, "sub": "u1"}
	digest := func(m []byte) []byte { d := sha256.Sum256(m); return d[:] }

	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	point, _ := ec.PublicKey.Bytes()
	edPub, edPriv, _ := ed25519.GenerateKey(rand.Reader)
	r, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks, _ := json.Marshal(map[string]any{"keys": []any{
		map[string]any{"kty": "EC", "crv": "P-256", "kid": "ec", "x": enc(point[1:33]), "y": enc(point[33:])},
		map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": "ed", "x": enc(edPub)},
		map[string]any{"kty": "RSA", "kid": "rsa", "n": enc(r.N.Bytes()), "e": enc(big.NewInt(int64(r.E)).Bytes())},
	}})
	es256 := func(m []byte) []byte {
		x, y, _ := ecdsa.Sign(rand.Reader, ec, digest(m))
		out := make([]byte, 64)
		x.FillBytes(out[:32])
		y.FillBytes(out[32:])
		return out
	}
	rs256 := func(m []byte) []byte { s, _ := rsa.SignPKCS1v15(rand.Reader, r, crypto.SHA256, digest(m)); return s }
	ps256 := func(m []byte) []byte {
		s, _ := rsa.SignPSS(rand.Reader, r, crypto.SHA256, digest(m), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
		return s
	}
	for _, c := range []struct {
		alg, kid string
		sign     func([]byte) []byte
	}{{"ES256", "ec", es256}, {"EdDSA", "ed", func(m []byte) []byte { return ed25519.Sign(edPriv, m) }}, {"RS256", "rsa", rs256}, {"PS256", "rsa", ps256}} {
		got, err := verifyIDToken(token(c.alg, c.kid, claims, c.sign), jwks, "client", "https://issuer")
		if err != nil || got["sub"] != "u1" {
			t.Fatalf("%s: %v %v", c.alg, got, err)
		}
	}
	refused := map[string]string{
		"another audience": token("ES256", "ec", map[string]any{"iss": "https://issuer", "aud": "other", "exp": now + 60}, es256),
		"another issuer":   token("ES256", "ec", map[string]any{"iss": "https://other", "aud": "client", "exp": now + 60}, es256),
		"expired":          token("ES256", "ec", map[string]any{"iss": "https://issuer", "aud": "client", "exp": now - 120}, es256),
		"no expiry":        token("ES256", "ec", map[string]any{"iss": "https://issuer", "aud": "client"}, es256),
		"another key":      token("ES256", "rsa", claims, es256),
		"unknown key":      token("ES256", "nope", claims, es256),
		"none":             token("none", "ec", claims, func([]byte) []byte { return nil }),
		"wrong signature":  token("ES256", "ec", claims, func(m []byte) []byte { s := es256(m); s[3] ^= 1; return s }),
	}
	for name, tok := range refused {
		if _, err := verifyIDToken(tok, jwks, "client", "https://issuer"); err == nil {
			t.Fatalf("%s passes", name)
		}
	}
	// An audience list holds the client.
	list := token("ES256", "ec", map[string]any{"iss": "https://issuer", "aud": []string{"x", "client"}, "exp": now + 60}, es256)
	if _, err := verifyIDToken(list, jwks, "client", "https://issuer"); err != nil {
		t.Fatal(err)
	}
}
