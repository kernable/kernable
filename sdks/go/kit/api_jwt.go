package kit

// ID tokens of OpenID Connect providers: the signature checked against the provider's JWKS, then
// the audience, issuer and expiry, as the other kits' JWT libraries check them.

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	_ "crypto/sha256" // registers SHA-256 for crypto.Hash.New
	_ "crypto/sha512" // and SHA-384, SHA-512
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// jwk is one key of a JWKS, the fields the supported algorithms need.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// idTokenLeeway is how far an expiry may lie in the past: clocks differ.
const idTokenLeeway = 60

func b64url(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// verifyIDToken is the claims of token if a key of jwks signed it, its audience holds clientID,
// its issuer is issuer and it has not expired.
func verifyIDToken(token string, jwks json.RawMessage, clientID, issuer string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("not a JWT")
	}
	rawHeader, err := b64url(parts[0])
	if err != nil {
		return nil, err
	}
	var header struct {
		Alg string  `json:"alg"`
		Kid *string `json:"kid"`
	}
	if err := json.Unmarshal(rawHeader, &header); err != nil {
		return nil, err
	}
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(jwks, &set); err != nil {
		return nil, err
	}
	var key *jwk
	for i := range set.Keys {
		if header.Kid == nil || set.Keys[i].Kid == *header.Kid {
			key = &set.Keys[i]
			break
		}
	}
	if key == nil {
		return nil, errors.New("no key for the token")
	}
	signature, err := b64url(parts[2])
	if err != nil {
		return nil, err
	}
	if err := verifyJWS(header.Alg, key, []byte(parts[0]+"."+parts[1]), signature); err != nil {
		return nil, err
	}
	body, err := b64url(parts[1])
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(strings.NewReader(string(body)))
	d.UseNumber()
	var claims map[string]any
	if err := d.Decode(&claims); err != nil {
		return nil, err
	}
	exp, ok := claims["exp"].(json.Number)
	if !ok {
		return nil, errors.New("no exp")
	}
	e, err := exp.Float64()
	if err != nil || int64(e) < time.Now().Unix()-idTokenLeeway {
		return nil, errors.New("expired")
	}
	if !audienceHolds(claims["aud"], clientID) {
		return nil, errors.New("another audience")
	}
	if iss, _ := claims["iss"].(string); iss != issuer {
		return nil, errors.New("another issuer")
	}
	return claims, nil
}

func audienceHolds(aud any, clientID string) bool {
	switch a := aud.(type) {
	case string:
		return a == clientID
	case []any:
		for _, v := range a {
			if s, _ := v.(string); s == clientID {
				return true
			}
		}
	}
	return false
}

func verifyJWS(alg string, k *jwk, signed, signature []byte) error {
	hashOf := func(h crypto.Hash) []byte {
		w := h.New()
		w.Write(signed)
		return w.Sum(nil)
	}
	hashes := map[string]crypto.Hash{"256": crypto.SHA256, "384": crypto.SHA384, "512": crypto.SHA512}
	switch {
	case alg == "EdDSA":
		x, err := b64url(k.X)
		if err != nil || k.Kty != "OKP" || k.Crv != "Ed25519" || len(x) != ed25519.PublicKeySize {
			return errors.New("not an Ed25519 key")
		}
		if !ed25519.Verify(ed25519.PublicKey(x), signed, signature) {
			return errors.New("wrong signature")
		}
		return nil
	case strings.HasPrefix(alg, "RS") || strings.HasPrefix(alg, "PS"):
		h, ok := hashes[alg[2:]]
		if !ok || k.Kty != "RSA" {
			return fmt.Errorf("unsupported %s", alg)
		}
		n, err1 := b64url(k.N)
		e, err2 := b64url(k.E)
		if err1 != nil || err2 != nil || len(e) == 0 || len(e) > 8 {
			return errors.New("not an RSA key")
		}
		pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
		if alg[0] == 'R' {
			return rsa.VerifyPKCS1v15(pub, h, hashOf(h), signature)
		}
		return rsa.VerifyPSS(pub, h, hashOf(h), signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case alg == "ES256" || alg == "ES384":
		curve, h, size, crv := elliptic.P256(), crypto.SHA256, 32, "P-256"
		if alg == "ES384" {
			curve, h, size, crv = elliptic.P384(), crypto.SHA384, 48, "P-384"
		}
		x, err1 := b64url(k.X)
		y, err2 := b64url(k.Y)
		if err1 != nil || err2 != nil || k.Kty != "EC" || k.Crv != crv || len(x) != size || len(y) != size {
			return errors.New("not an EC key")
		}
		pub, err := ecdsa.ParseUncompressedPublicKey(curve, append(append([]byte{4}, x...), y...))
		if err != nil {
			return err
		}
		if len(signature) != 2*size {
			return errors.New("wrong signature")
		}
		r, s := new(big.Int).SetBytes(signature[:size]), new(big.Int).SetBytes(signature[size:])
		if !ecdsa.Verify(pub, hashOf(h), r, s) {
			return errors.New("wrong signature")
		}
		return nil
	}
	return fmt.Errorf("unsupported %s", alg)
}
