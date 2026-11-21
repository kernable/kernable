// Command kernable-sdk is the SDK's own tool. `kernable-sdk add kit [dir]` copies the kit of this
// SDK's version into your module: tenancy, accounts, identities, groups, invitations and
// authorization as source that is yours to change.
//
//	go run github.com/kernable/kernable/sdks/go/cmd/kernable-sdk@v0.3.0 add kit [dir] [--force]
//
// The copy is a package of your module named after dir (default kit); its example imports it by
// your module's path.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"

	"github.com/kernable/kernable/sdks/go/kit"
)

const kitPath = "github.com/kernable/kernable/sdks/go/kit"

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kernable-sdk add kit [dir] [--force]   (dir default: kit)")
	os.Exit(2)
}

// version is this SDK's module version, as `go run …@version` built it.
func version() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "(devel)"
}

// modulePath is the import path of dir: the module whose go.mod is in dir or above it, and the
// path below it.
func modulePath(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for root := abs; ; root = filepath.Dir(root) {
		raw, err := os.ReadFile(filepath.Join(root, "go.mod"))
		if err == nil {
			module := ""
			sc := bufio.NewScanner(bytes.NewReader(raw))
			for sc.Scan() {
				if f := strings.Fields(sc.Text()); len(f) >= 2 && f[0] == "module" {
					module = strings.Trim(f[1], `"`)
					break
				}
			}
			if module == "" {
				return "", fmt.Errorf("%s names no module", filepath.Join(root, "go.mod"))
			}
			rel, err := filepath.Rel(root, abs)
			if err != nil {
				return "", err
			}
			return path.Join(module, filepath.ToSlash(rel)), nil
		}
		if filepath.Dir(root) == root {
			return "", errors.New("no go.mod here or above: run it inside your module (go mod init)")
		}
	}
}

var (
	packageClause = regexp.MustCompile(`(?m)^package kit$`)
	kitImport     = regexp.MustCompile(`(?m)^(\s*)"` + regexp.QuoteMeta(kitPath) + `(/[^"]*)?"`)
)

// rewrite is a Go file of the kit for the copy: the package named name, imports of the kit by
// importPath, and a header.
func rewrite(text, name, importPath, header string) string {
	text = packageClause.ReplaceAllString(text, "package "+name)
	text = kitImport.ReplaceAllStringFunc(text, func(m string) string {
		sub := kitImport.FindStringSubmatch(m)
		alias := ""
		if sub[2] == "" && name != "kit" {
			alias = "kit " // the code says kit.X
		}
		return sub[1] + alias + `"` + importPath + sub[2] + `"`
	})
	return header + text
}

func addKit(dir string, force bool) error {
	name := filepath.Base(dir)
	if !regexp.MustCompile(`^[a-z][a-z0-9]*$`).MatchString(name) {
		return fmt.Errorf("%s names the copied package: lower-case letters and digits (kit, authkit)", dir)
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 && !force {
		return fmt.Errorf("%s is not empty (the kit is copied, not merged); choose another directory, or --force", dir)
	}
	importPath, err := modulePath(dir)
	if err != nil {
		return err
	}
	v := version()
	header := fmt.Sprintf("// From the kernable kit %s (MIT, see LICENSE): yours to change.\n\n", v)
	files := 0
	err = fs.WalkDir(kit.Source, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || p == "source.go" {
			return err
		}
		raw, err := kit.Source.ReadFile(p)
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".go") {
			// Formatted as gofmt would: the new import path may sort elsewhere.
			if raw, err = format.Source([]byte(rewrite(string(raw), name, importPath, header))); err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
		}
		target := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		files++
		return os.WriteFile(target, raw, 0o644)
	})
	if err != nil {
		return err
	}
	pin := "go get github.com/kernable/kernable/sdks/go@" + v
	if v == "(devel)" {
		pin = "(a development build: use the SDK version it was built from)"
	}
	fmt.Printf("The kernable kit %s is in %s (%d files, package %s), yours to change.\n"+
		"Pin the SDK to the same version:  %s\n"+
		"Then:  go mod tidy && go test ./%s/...\n"+
		"Import it as %q; see %s.\n",
		v, dir, files, name, pin, filepath.ToSlash(filepath.Clean(dir)), importPath, filepath.Join(dir, "README.md"))
	return nil
}

func main() {
	args := os.Args[1:]
	force := false
	var rest []string
	for _, a := range args {
		if a == "--force" {
			force = true
		} else {
			rest = append(rest, a)
		}
	}
	if len(rest) < 2 || rest[0] != "add" || rest[1] != "kit" || len(rest) > 3 {
		usage()
	}
	dir := "kit"
	if len(rest) == 3 {
		dir = rest[2]
	}
	if err := addKit(dir, force); err != nil {
		fmt.Fprintln(os.Stderr, "kernable-sdk:", err)
		os.Exit(1)
	}
}
