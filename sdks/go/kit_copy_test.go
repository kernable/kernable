package kernable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The kit carries copies (a Go module holds no links): the shared fixtures every language's kit
// passes, and the license. They must be the repository's.
func TestTheKitCarriesTheSharedFixtures(t *testing.T) {
	shared, _ := filepath.Glob("../../kits/fixtures/*")
	if len(shared) == 0 {
		t.Skip("not in the kernable repository")
	}
	copied, _ := filepath.Glob("kit/fixtures/*")
	if len(copied) != len(shared) {
		t.Fatalf("kit/fixtures has %d files, kits/fixtures %d: cp ../../kits/fixtures/* kit/fixtures/", len(copied), len(shared))
	}
	pairs := [][2]string{{"../../LICENSE", "kit/LICENSE"}, {"../../LICENSE", "LICENSE"}}
	for _, f := range shared {
		pairs = append(pairs, [2]string{f, filepath.Join("kit/fixtures", filepath.Base(f))})
	}
	for _, p := range pairs {
		a, err := os.ReadFile(p[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(p[1])
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s differs from %s: copy it over", p[1], p[0])
		}
	}
}
