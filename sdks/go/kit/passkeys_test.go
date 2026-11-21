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
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

const origin = "http://localhost:3000"

func clientJSON(kind, challenge string) []byte {
	b, _ := json.Marshal(map[string]string{"type": kind, "challenge": challenge, "origin": origin})
	return b
}

// roundTrip registers cose and signs in with sign, as a browser and an authenticator would.
func roundTrip(t *testing.T, cose map[int64]any, sign func([]byte) []byte) {
	t.Helper()
	enc := func(v any) []byte {
		b, err := cbor.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	b64 := base64.RawURLEncoding.EncodeToString
	rp := sha256.Sum256([]byte(Passkeys().RPID))
	id := make([]byte, 16)
	for i := range id {
		id[i] = 9
	}
	auth := append([]byte{}, rp[:]...)
	auth = append(auth, 0x45) // user present, verified, credential data
	auth = binary.BigEndian.AppendUint32(auth, 1)
	auth = append(auth, make([]byte, 16)...)
	auth = binary.BigEndian.AppendUint16(auth, uint16(len(id)))
	auth = append(auth, id...)
	auth = append(auth, enc(cose)...)
	attestation := enc(map[string]any{"fmt": "none", "attStmt": map[string]any{}, "authData": auth})
	registration := map[string]any{"response": map[string]any{
		"clientDataJSON":    b64(clientJSON("webauthn.create", "c1")),
		"attestationObject": b64(attestation),
	}}
	registered, err := VerifyRegistration(registration, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if registered.CredentialID != b64(id) || registered.Counter != 1 {
		t.Fatalf("registered %+v", registered)
	}
	data := append([]byte{}, rp[:]...)
	data = append(data, 0x05)
	data = binary.BigEndian.AppendUint32(data, 2)
	client := clientJSON("webauthn.get", "c2")
	h := sha256.Sum256(client)
	signature := sign(append(append([]byte{}, data...), h[:]...))
	signIn := map[string]any{"response": map[string]any{
		"clientDataJSON": b64(client), "authenticatorData": b64(data), "signature": b64(signature),
	}}
	counter, err := VerifyAuthentication(signIn, "c2", registered.PublicKey, registered.Counter)
	if err != nil || counter != 2 {
		t.Fatalf("sign in: %d %v", counter, err)
	}
	signature[len(signature)-1] ^= 1
	signIn["response"].(map[string]any)["signature"] = b64(signature)
	if _, err := VerifyAuthentication(signIn, "c2", registered.PublicKey, registered.Counter); err == nil {
		t.Fatal("a wrong signature passes")
	}
	if _, err := VerifyAuthentication(signIn, "other", registered.PublicKey, registered.Counter); err == nil {
		t.Fatal("another challenge passes")
	}
}

func TestPasskeysOfEveryAlgorithm(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	roundTrip(t, map[int64]any{1: 1, 3: -8, -1: 6, -2: []byte(pub)}, func(m []byte) []byte { return ed25519.Sign(priv, m) })

	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	point, _ := ec.PublicKey.Bytes() // 0x04 || x || y
	roundTrip(t, map[int64]any{1: 2, 3: -7, -1: 1, -2: point[1:33], -3: point[33:]}, func(m []byte) []byte {
		h := sha256.Sum256(m)
		s, _ := ecdsa.SignASN1(rand.Reader, ec, h[:])
		return s
	})

	r, _ := rsa.GenerateKey(rand.Reader, 2048)
	roundTrip(t, map[int64]any{1: 3, 3: -257, -1: r.N.Bytes(), -2: []byte{1, 0, 1}}, func(m []byte) []byte {
		h := sha256.Sum256(m)
		s, _ := rsa.SignPKCS1v15(rand.Reader, r, crypto.SHA256, h[:])
		return s
	})
}
