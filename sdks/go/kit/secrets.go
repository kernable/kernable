package kit

// Tokens, hashes, passwords and TOTP: the same as the other languages' kits.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// RandomBytes is size random bytes.
func RandomBytes(size int) []byte {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// RandomSecret is a new random secret: size bytes, base64url without padding.
func RandomSecret(size int) string { return base64.RawURLEncoding.EncodeToString(RandomBytes(size)) }

// NewID is a new random ID in the UUID v4 form, e.g. for an aggregate a reactor creates.
func NewID() string {
	b := RandomBytes(16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// SHA256 in hex: how high-entropy tokens are stored (refresh tokens, API keys, recovery codes).
func SHA256(value string) string {
	s := sha256.Sum256([]byte(value))
	return hex.EncodeToString(s[:])
}

// EqualSecrets compares two secrets in constant time (for equal lengths).
func EqualSecrets(a, b string) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Token is <aggregateId>.<tokenId>.<secret> (emailed tokens), <sessionId>.<generation>.<secret>
// (refresh tokens).
func Token(a string, b any, c string) string { return fmt.Sprintf("%s.%v.%s", a, b, c) }

// ParseToken is the three parts of a token, or false if it has another shape.
func ParseToken(value string) (string, string, string, bool) {
	p := strings.Split(value, ".")
	if len(p) != 3 || p[0] == "" || p[1] == "" || p[2] == "" {
		return "", "", "", false
	}
	return p[0], p[1], p[2], true
}

// InvitationTokenOf is an invitation's token: it names its tenant too, four parts.
func InvitationTokenOf(tenant, invitationID, tokenID, secret string) string {
	return strings.Join([]string{tenant, invitationID, tokenID, secret}, ".")
}

// InvitationToken is the parts of an invitation token.
type InvitationToken struct{ Tenant, InvitationID, TokenID, Secret string }

// ParseInvitationToken is the parts of an invitation token, or false.
func ParseInvitationToken(value string) (InvitationToken, bool) {
	p := strings.Split(value, ".")
	if len(p) != 4 {
		return InvitationToken{}, false
	}
	for _, x := range p {
		if x == "" {
			return InvitationToken{}, false
		}
	}
	return InvitationToken{p[0], p[1], p[2], p[3]}, true
}

// argon2id with OWASP's parameters (19 MiB, 2 iterations, 1 lane): hashes the other kits check,
// and back.
const (
	argonMemory  = 19 * 1024
	argonTime    = 2
	argonThreads = 1
	argonLength  = 32
)

// HashPassword is an argon2id hash in the PHC string format.
func HashPassword(password string) string {
	salt := RandomBytes(16)
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonLength)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key))
}

// DummyPasswordHash is an argon2id hash of a password nobody uses, with the same parameters:
// checked instead when there is no hash to check, so an unknown address or an account without a
// password takes as long as a wrong password. The same in every language's kit.
const DummyPasswordHash = "$argon2id$v=19$m=19456,t=2,p=1$xZHjBkZVyzUUEIsFk5QnZg$ATIGagX8bkGblnc8NAAmXvUMTDX3izgmV74SUrCIjZg"

// CheckPassword is whether password is the one stored hashes. Without a hash, a check against
// DummyPasswordHash that fails, taking as long.
func CheckPassword(stored, password string) bool {
	if stored == "" {
		verifyArgon(DummyPasswordHash, password)
		return false
	}
	return verifyArgon(stored, password)
}

func verifyArgon(phc, password string) bool {
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	var m, t uint32
	var p uint8
	for _, kv := range strings.Split(parts[3], ",") {
		k, v, _ := strings.Cut(kv, "=")
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return false
		}
		switch k {
		case "m":
			m = uint32(n)
		case "t":
			t = uint32(n)
		case "p":
			p = uint8(n)
		}
	}
	salt, err1 := base64.RawStdEncoding.DecodeString(parts[4])
	want, err2 := base64.RawStdEncoding.DecodeString(parts[5])
	if err1 != nil || err2 != nil || m == 0 || t == 0 || p == 0 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

// IsSpace is one of the characters the kits take for space: controls, spaces, line and paragraph
// separators and the byte order mark. One explicit set, the same in every language's kit.
func IsSpace(c rune) bool {
	switch {
	case c <= 0x20, c == 0x7f, c == 0x85, c == 0xa0, c == 0x1680, c >= 0x2000 && c <= 0x200a,
		c == 0x2028, c == 0x2029, c == 0x202f, c == 0x205f, c == 0x3000, c == 0xfeff:
		return true
	}
	return false
}

// HasSpace is whether value holds one of the kits' space characters.
func HasSpace(value string) bool { return strings.IndexFunc(value, IsSpace) >= 0 }

// TrimSpace is value without the kits' space characters at either end.
func TrimSpace(value string) string { return strings.TrimFunc(value, IsSpace) }

// NormalEmail is lowercased and trimmed: how emails are compared and held as unique keys.
func NormalEmail(email string) string { return strings.ToLower(TrimSpace(email)) }

// EmailDomain is the domain of an address, lowercased: what follows its @.
func EmailDomain(email string) string {
	e := NormalEmail(email)
	if at := strings.LastIndexByte(e, '@'); at >= 0 {
		return e[at+1:]
	}
	return e
}

// ValidEmail: one @, a local part, a domain of at least two labels, none of them empty, none of
// the kits' space characters, at most 254 bytes in UTF-8. The same rule in every language's kit.
func ValidEmail(email string) bool {
	user, host, ok := strings.Cut(email, "@")
	if !ok || len(email) > 254 || user == "" || strings.Contains(host, "@") || HasSpace(email) {
		return false
	}
	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return false
	}
	for _, l := range labels {
		if l == "" {
			return false
		}
	}
	return true
}

// ValidPassword: 10 to 1024 characters.
func ValidPassword(password string) bool {
	n := utf8.RuneCountInString(password)
	return n >= 10 && n <= 1024
}

// --- TOTP (RFC 6238): HMAC-SHA1, 30 s steps, 6 digits, base32 secrets -------------------------

// TOTPSecret is a new TOTP secret: 20 random bytes in base32, as authenticator apps take it.
func TOTPSecret() string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(RandomBytes(20))
}

// TOTPCode is the 6-digit code of secret for the 30-second step.
func TOTPCode(secret string, step int64) string {
	key, _ := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.TrimRight(strings.ToUpper(secret), "="))
	mac := hmac.New(sha1.New, key)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(step))
	mac.Write(b[:])
	d := mac.Sum(nil)
	offset := d[19] & 0x0f
	n := binary.BigEndian.Uint32(d[offset:offset+4]) & 0x7fffffff
	return fmt.Sprintf("%06d", n%1_000_000)
}

// TOTPStep is the step code belongs to, at now (ms) give or take one step (clock drift).
func TOTPStep(secret, code string, now int64) (int64, bool) {
	if len(code) != 6 || strings.IndexFunc(code, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, false
	}
	step := now / 30_000
	if now < 0 && now%30_000 != 0 {
		step--
	}
	for _, s := range []int64{step, step - 1, step + 1} {
		if EqualSecrets(TOTPCode(secret, s), code) {
			return s, true
		}
	}
	return 0, false
}
