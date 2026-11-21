package kit

// Passkeys (WebAuthn), verified here: a browser's answer to registration options with
// attestation none, and to sign-in options, with the algorithms the options offer, as the other
// kits: EdDSA (Ed25519), ES256 (P-256) and RS256 (RSA, which Windows Hello may use). The account
// keeps the COSE key as base64url, the same as the other kits, so every kit verifies every kit's
// passkeys.

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

// PasskeyAlgorithms are the COSE algorithms passkeys may use, in the order the registration
// options offer them.
var PasskeyAlgorithms = []int64{-8, -7, -257}

// RegisteredPasskey is a registered passkey: its ID, its COSE public key (both base64url) and its
// counter.
type RegisteredPasskey struct {
	CredentialID string
	PublicKey    string
	Counter      uint64
}

func b64field(response map[string]any, field string) ([]byte, error) {
	r, _ := response["response"].(map[string]any)
	text, ok := r[field].(string)
	if !ok {
		return nil, errors.New("a base64url field is missing")
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(text, "="))
	if err != nil {
		return nil, fmt.Errorf("base64url: %v", err)
	}
	return b, nil
}

// clientData checks clientDataJSON: its type, the challenge the API issued, and one of our origins.
func clientData(response map[string]any, kind, challenge string) ([]byte, error) {
	raw, err := b64field(response, "clientDataJSON")
	if err != nil {
		return nil, err
	}
	var data struct {
		Type      any `json:"type"`
		Challenge any `json:"challenge"`
		Origin    any `json:"origin"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("clientDataJSON: %v", err)
	}
	if data.Type != kind {
		return nil, fmt.Errorf("not a %s answer", kind)
	}
	if c, ok := data.Challenge.(string); !ok || c != strings.TrimRight(challenge, "=") {
		return nil, errors.New("another challenge")
	}
	if o, ok := data.Origin.(string); !ok || !slices.Contains(Passkeys().Origins, o) {
		return nil, errors.New("another origin")
	}
	return raw, nil
}

// authenticatorData is the flags and counter, after checking the relying party and that the user
// was present and verified.
func authenticatorData(data []byte) (byte, uint64, error) {
	if len(data) < 37 {
		return 0, 0, errors.New("short authenticator data")
	}
	rp := sha256.Sum256([]byte(Passkeys().RPID))
	if !bytes.Equal(data[:32], rp[:]) {
		return 0, 0, errors.New("another relying party")
	}
	flags := data[32]
	if flags&0x01 == 0 || flags&0x04 == 0 {
		return 0, 0, errors.New("the user was not present and verified")
	}
	return flags, uint64(binary.BigEndian.Uint32(data[33:37])), nil
}

type publicKey struct {
	ed  ed25519.PublicKey
	ec  *ecdsa.PublicKey
	rsa *rsa.PublicKey
}

// cosePublicKey is the public key of a COSE key: OKP Ed25519 (EdDSA), EC2 P-256 (ES256) or RSA
// (RS256).
func cosePublicKey(cose []byte) (publicKey, error) {
	var m map[int64]any
	if err := cbor.Unmarshal(cose, &m); err != nil {
		return publicKey{}, fmt.Errorf("COSE key: %v", err)
	}
	is := func(k, v int64) bool {
		switch x := m[k].(type) {
		case int64:
			return x == v
		case uint64:
			return v >= 0 && x == uint64(v)
		}
		return false
	}
	b := func(k int64) []byte { x, _ := m[k].([]byte); return x }
	switch {
	case is(1, 1) && is(3, -8) && is(-1, 6):
		x := b(-2)
		if len(x) != 32 {
			return publicKey{}, errors.New("an Ed25519 key")
		}
		return publicKey{ed: ed25519.PublicKey(x)}, nil
	case is(1, 2) && is(3, -7) && is(-1, 1):
		x, y := b(-2), b(-3)
		if len(x) != 32 || len(y) != 32 {
			return publicKey{}, errors.New("a P-256 coordinate")
		}
		k, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{0x04}, x...), y...))
		if err != nil {
			return publicKey{}, fmt.Errorf("P-256 key: %v", err)
		}
		return publicKey{ec: k}, nil
	case is(1, 3) && is(3, -257):
		n, e := b(-1), b(-2)
		if len(n) == 0 || len(e) == 0 || len(e) > 8 {
			return publicKey{}, errors.New("an RSA key")
		}
		var exp uint64
		for _, c := range e {
			exp = exp<<8 | uint64(c)
		}
		return publicKey{rsa: &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(exp)}}, nil
	}
	return publicKey{}, errors.New("only EdDSA (Ed25519), ES256 (P-256) and RS256 passkeys")
}

func (k publicKey) verify(signed, signature []byte) error {
	ok := false
	switch {
	case k.ed != nil:
		ok = len(signature) == ed25519.SignatureSize && ed25519.Verify(k.ed, signed, signature)
	case k.ec != nil:
		h := sha256.Sum256(signed)
		ok = ecdsa.VerifyASN1(k.ec, h[:], signature)
	case k.rsa != nil:
		h := sha256.Sum256(signed)
		ok = rsa.VerifyPKCS1v15(k.rsa, crypto.SHA256, h[:], signature) == nil
	}
	if !ok {
		return errors.New("the signature does not match")
	}
	return nil
}

// VerifyRegistration verifies a registration answer (navigator.credentials.create) for challenge.
func VerifyRegistration(response map[string]any, challenge string) (RegisteredPasskey, error) {
	if _, err := clientData(response, "webauthn.create", challenge); err != nil {
		return RegisteredPasskey{}, err
	}
	raw, err := b64field(response, "attestationObject")
	if err != nil {
		return RegisteredPasskey{}, err
	}
	var attestation map[string]cbor.RawMessage
	if err := cbor.Unmarshal(raw, &attestation); err != nil {
		return RegisteredPasskey{}, fmt.Errorf("attestation: %v", err)
	}
	var format string
	var statement map[any]any
	if cbor.Unmarshal(attestation["fmt"], &format) != nil || format != "none" ||
		cbor.Unmarshal(attestation["attStmt"], &statement) != nil || len(statement) != 0 {
		return RegisteredPasskey{}, errors.New("only attestation none")
	}
	var data []byte
	if cbor.Unmarshal(attestation["authData"], &data) != nil || data == nil {
		return RegisteredPasskey{}, errors.New("no authenticator data")
	}
	flags, counter, err := authenticatorData(data)
	if err != nil {
		return RegisteredPasskey{}, err
	}
	if flags&0x40 == 0 || len(data) < 55 {
		return RegisteredPasskey{}, errors.New("no credential in the answer")
	}
	idLength := int(binary.BigEndian.Uint16(data[53:55]))
	if len(data) < 55+idLength {
		return RegisteredPasskey{}, errors.New("short credential ID")
	}
	id := data[55 : 55+idLength]
	rest := data[55+idLength:]
	if len(rest) == 0 {
		return RegisteredPasskey{}, errors.New("no public key")
	}
	// The key ends the data unless extensions follow: take exactly one CBOR item.
	var item cbor.RawMessage
	after, err := cbor.UnmarshalFirst(rest, &item)
	if err != nil {
		return RegisteredPasskey{}, fmt.Errorf("COSE key: %v", err)
	}
	cose := rest[:len(rest)-len(after)]
	if _, err := cosePublicKey(cose); err != nil {
		return RegisteredPasskey{}, err
	}
	return RegisteredPasskey{
		CredentialID: base64.RawURLEncoding.EncodeToString(id),
		PublicKey:    base64.RawURLEncoding.EncodeToString(cose),
		Counter:      counter,
	}, nil
}

// VerifyAuthentication verifies a sign-in answer (navigator.credentials.get) for challenge with a
// stored passkey; the new counter, which must grow unless the authenticator keeps none (0).
func VerifyAuthentication(response map[string]any, challenge, storedKey string, counter uint64) (uint64, error) {
	client, err := clientData(response, "webauthn.get", challenge)
	if err != nil {
		return 0, err
	}
	data, err := b64field(response, "authenticatorData")
	if err != nil {
		return 0, err
	}
	_, newCounter, err := authenticatorData(data)
	if err != nil {
		return 0, err
	}
	cose, err := base64.RawURLEncoding.DecodeString(storedKey)
	if err != nil {
		return 0, fmt.Errorf("stored key: %v", err)
	}
	key, err := cosePublicKey(cose)
	if err != nil {
		return 0, err
	}
	signature, err := b64field(response, "signature")
	if err != nil {
		return 0, err
	}
	h := sha256.Sum256(client)
	if err := key.verify(append(slices.Clone(data), h[:]...), signature); err != nil {
		return 0, err
	}
	if (newCounter > 0 || counter > 0) && newCounter <= counter {
		return 0, errors.New("the counter did not grow: a cloned passkey?")
	}
	return newCounter, nil
}
