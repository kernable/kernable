package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTheCopyIsAPackageOfTheModule(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/app\n\ngo 1.26.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := modulePath(filepath.Join(root, "internal", "authkit"))
	if err != nil || got != "example.com/app/internal/authkit" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := modulePath(t.TempDir()); err == nil {
		t.Fatal("a directory outside a module")
	}
	src := "// Package example …\npackage example\n\nimport (\n\t\"fmt\"\n\n\t\"github.com/kernable/kernable/sdks/go/kit\"\n\t\"github.com/kernable/kernable/sdks/go/kit/example\"\n\tkernable \"github.com/kernable/kernable/sdks/go\"\n)\n"
	out := rewrite(src, "authkit", "example.com/app/internal/authkit", "// header\n\n")
	for _, want := range []string{"// header\n\n// Package example", "\tkit \"example.com/app/internal/authkit\"\n", "\t\"example.com/app/internal/authkit/example\"\n",
		"kernable \"github.com/kernable/kernable/sdks/go\""} {
		if !strings.Contains(out, want) {
			t.Fatalf("no %q in\n%s", want, out)
		}
	}
	if got := rewrite("package kit\n", "authkit", "x", ""); got != "package authkit\n" {
		t.Fatal(got)
	}
	if got := rewrite("package kit\n", "kit", "x", ""); got != "package kit\n" {
		t.Fatal(got)
	}
}
