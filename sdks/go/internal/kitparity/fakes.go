package main

// A software authenticator and a minimal OpenID Connect provider, as the other kits' parity
// runners use them.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/kernable/kernable/sdks/go/kit"
)

var b64 = base64.RawURLEncoding.EncodeToString

// authenticator answers WebAuthn the way a browser with a platform authenticator does (P-256,
// attestation none, user verified), without a browser.
type authenticator struct {
	rpID, origin, userID string
	credentialID         []byte
	key                  *ecdsa.PrivateKey
	counter              uint32
}

func newAuthenticator(rpID, origin, userID string) *authenticator {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return &authenticator{rpID: rpID, origin: origin, userID: userID, credentialID: kit.RandomBytes(16), key: key}
}

func (a *authenticator) authData(flags byte, attested []byte) []byte {
	h := sha256.Sum256([]byte(a.rpID))
	data := append(h[:], flags)
	data = binary.BigEndian.AppendUint32(data, a.counter)
	return append(data, attested...)
}

func (a *authenticator) clientData(kind, challenge, origin string) []byte {
	if origin == "" {
		origin = a.origin
	}
	b, _ := json.Marshal(map[string]any{"type": kind, "challenge": challenge, "origin": origin, "crossOrigin": false})
	return b
}

var det, _ = cbor.CoreDetEncOptions().EncMode()

// register is the browser's answer to registration options with this challenge.
func (a *authenticator) register(challenge string) any {
	point, err := a.key.PublicKey.Bytes()
	if err != nil {
		panic(err)
	}
	// COSE_Key: kty EC2 (2), alg ES256 (-7), crv P-256 (1), x, y.
	cose, _ := det.Marshal(map[int64]any{1: 2, 3: -7, -1: 1, -2: point[1:33], -3: point[33:]})
	attested := make([]byte, 16)
	attested = binary.BigEndian.AppendUint16(attested, uint16(len(a.credentialID)))
	attested = append(attested, a.credentialID...)
	attested = append(attested, cose...)
	// Flags: user present (0x01), user verified (0x04), attested credential data (0x40).
	attestation, _ := det.Marshal(map[string]any{"fmt": "none", "attStmt": map[string]any{}, "authData": a.authData(0x45, attested)})
	return map[string]any{
		"id": b64(a.credentialID), "rawId": b64(a.credentialID), "type": "public-key", "clientExtensionResults": map[string]any{},
		"response": map[string]any{
			"clientDataJSON": b64(a.clientData("webauthn.create", challenge, "")), "attestationObject": b64(attestation),
			"transports": []any{"internal"},
		},
	}
}

// assert is the browser's answer to sign-in options with this challenge, from origin (default
// the right one).
func (a *authenticator) assert(challenge, origin string) any {
	a.counter++
	authData := a.authData(0x05, nil)
	clientData := a.clientData("webauthn.get", challenge, origin)
	h := sha256.Sum256(clientData)
	signed := sha256.Sum256(append(append([]byte{}, authData...), h[:]...))
	signature, err := ecdsa.SignASN1(rand.Reader, a.key, signed[:])
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"id": b64(a.credentialID), "rawId": b64(a.credentialID), "type": "public-key", "clientExtensionResults": map[string]any{},
		"response": map[string]any{
			"clientDataJSON": b64(clientData), "authenticatorData": b64(authData), "signature": b64(signature),
			"userHandle": b64([]byte(a.userID)),
		},
	}
}

type grant struct {
	user                          map[string]any
	challenge, nonce, redirectURI string
}

// oidcProvider is a minimal OpenID Connect provider: discovery, JWKS, a token endpoint with PKCE
// and an ES256 ID token. login plays the user who signs in at the provider.
type oidcProvider struct {
	issuer, clientID, clientSecret string
	key                            *ecdsa.PrivateKey
	mu                             sync.Mutex
	codes                          map[string]grant
}

func startOIDCProvider() *oidcProvider {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	p := &oidcProvider{issuer: "http://" + listener.Addr().String(), clientID: "kit-client", clientSecret: kit.RandomSecret(16),
		key: key, codes: map[string]grant{}}
	reply := func(w http.ResponseWriter, status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		i := p.issuer
		reply(w, 200, map[string]any{
			"issuer": i, "authorization_endpoint": i + "/authorize", "token_endpoint": i + "/token", "jwks_uri": i + "/jwks",
			"response_types_supported": []string{"code"}, "id_token_signing_alg_values_supported": []string{"ES256"},
			"code_challenge_methods_supported": []string{"S256"},
		})
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, _ *http.Request) {
		point, _ := p.key.PublicKey.Bytes()
		reply(w, 200, map[string]any{"keys": []any{map[string]any{"kty": "EC", "crv": "P-256", "x": b64(point[1:33]),
			"y": b64(point[33:]), "kid": "k1", "alg": "ES256", "use": "sig"}}})
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, secret, _ := r.BasicAuth()
		p.mu.Lock()
		g, found := p.codes[r.PostForm.Get("code")]
		delete(p.codes, r.PostForm.Get("code"))
		p.mu.Unlock()
		// The client sends its ID and secret percent-encoded (RFC 6749 2.3.1).
		id, _ = url.QueryUnescape(id)
		secret, _ = url.QueryUnescape(secret)
		if id != p.clientID || secret != p.clientSecret {
			reply(w, 401, map[string]any{"error": "invalid_client"})
			return
		}
		h := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
		if !found || g.redirectURI != r.PostForm.Get("redirect_uri") || b64(h[:]) != g.challenge {
			reply(w, 400, map[string]any{"error": "invalid_grant"})
			return
		}
		now := time.Now().Unix()
		header := b64([]byte(`{"alg":"ES256","kid":"k1","typ":"JWT"}`))
		claims := map[string]any{"iss": p.issuer, "aud": p.clientID, "iat": now, "exp": now + 300, "nonce": g.nonce}
		for k, v := range g.user {
			claims[k] = v
		}
		body, _ := json.Marshal(claims)
		payload := b64(body)
		digest := sha256.Sum256([]byte(header + "." + payload))
		rr, ss, err := ecdsa.Sign(rand.Reader, p.key, digest[:])
		if err != nil {
			panic(err)
		}
		signature := make([]byte, 64)
		rr.FillBytes(signature[:32])
		ss.FillBytes(signature[32:])
		reply(w, 200, map[string]any{"access_token": "at", "token_type": "Bearer", "expires_in": 300,
			"id_token": header + "." + payload + "." + b64(signature)})
	})
	go func() { _ = http.Serve(listener, mux) }()
	return p
}

// login is the URL the browser comes back to after the user signed in at the provider for this
// authorization URL.
func (p *oidcProvider) login(authorizationURL string, user map[string]any) string {
	u, err := url.Parse(authorizationURL)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	code := kit.RandomSecret(16)
	p.mu.Lock()
	p.codes[code] = grant{user: user, challenge: q.Get("code_challenge"), nonce: q.Get("nonce"), redirectURI: q.Get("redirect_uri")}
	p.mu.Unlock()
	back := q.Get("redirect_uri")
	sep := "?"
	if strings.Contains(back, "?") {
		sep = "&"
	}
	return back + sep + "code=" + url.QueryEscape(code) + "&state=" + url.QueryEscape(q.Get("state"))
}
