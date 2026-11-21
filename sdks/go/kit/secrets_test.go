package kit

import (
	"encoding/base32"
	"os"
	"os/exec"
	"testing"
)

func TestTOTPMatchesRFC6238(t *testing.T) {
	// RFC 6238, appendix B: the SHA-1 secret "12345678901234567890" at 59 s is 94287082 (8 digits).
	secret := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString([]byte("12345678901234567890"))
	if c := TOTPCode(secret, 59/30); c != "287082" {
		t.Fatalf("code %s", c)
	}
	if s, ok := TOTPStep(secret, "287082", 59_000); !ok || s != 1 {
		t.Fatalf("step %d %v", s, ok)
	}
}

func TestPasswordsRoundTripAndCheckAnotherKitsHash(t *testing.T) {
	h := HashPassword("correct horse battery")
	if len(h) < 40 || h[:31] != "$argon2id$v=19$m=19456,t=2,p=1$" {
		t.Fatalf("hash %s", h)
	}
	if !CheckPassword(h, "correct horse battery") || CheckPassword(h, "wrong") || CheckPassword("", "x") {
		t.Fatal("check")
	}
	// A hash of the Python kit's argon2-cffi, with the same parameters.
	if !CheckPassword("$argon2id$v=19$m=19456,t=2,p=1$998/Vyn+f15pIPrn0OpN2w$MeOrcRmqCCP42CnLuKzkmNUd3Z4Teq+R2tKtOWamJ7E", "correct horse battery") {
		t.Fatal("the Python kit's hash")
	}
	// And Go's hash as Python's kit checks it.
	py := "../../python/.venv/bin/python"
	if _, err := os.Stat(py); err == nil {
		out, err := exec.Command(py, "-c", "import sys; from argon2 import PasswordHasher; PasswordHasher().verify(sys.argv[1], 'correct horse battery'); print('ok')", h).CombinedOutput()
		if err != nil || string(out) != "ok\n" {
			t.Fatalf("Python checks Go's hash: %s %v", out, err)
		}
	}
}
