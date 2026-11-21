package kernable

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// The generated code (internal/pb) is committed; it must come from the current proto.
func TestGeneratedFromCurrentProto(t *testing.T) {
	proto, err := os.ReadFile("../../proto/kernable/v1/kernable.proto")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("internal/pb/kernable.proto.sha256")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(proto)
	if hex.EncodeToString(sum[:]) != strings.TrimSpace(string(want)) {
		t.Fatal("proto/kernable/v1/kernable.proto changed: run sdks/go/gen.sh")
	}
}
