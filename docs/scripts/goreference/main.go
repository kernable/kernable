// The public API of the Go SDK with its doc comments, as Markdown: types with their exported
// fields, constructors and methods, then the other functions, in the order of the sources.
//
//	go run . <sdk dir>
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/doc"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

func main() {
	dir := os.Args[1]
	fset := token.NewFileSet()
	var files []*ast.File
	names, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	slices.Sort(names)
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, parser.ParseComments)
		if err != nil {
			fail(err)
		}
		files = append(files, f)
	}
	pkg, err := doc.NewFromFiles(fset, files, "github.com/kernable/kernable/sdks/go")
	if err != nil {
		fail(err)
	}
	var b strings.Builder
	sig := func(decl *ast.FuncDecl) string {
		d := *decl
		d.Body, d.Doc = nil, nil
		return render(fset, &d)
	}
	for _, t := range pkg.Types {
		fmt.Fprintf(&b, "### %s\n\n", t.Name)
		fmt.Fprintf(&b, "%s\n\n", text(t.Doc))
		if fields := fieldsOf(t.Decl); fields != "" {
			b.WriteString(fields + "\n")
		}
		for _, c := range t.Consts {
			fmt.Fprintf(&b, "```go\n%s\n```\n\n", render(fset, c.Decl))
		}
		for _, f := range t.Funcs {
			fmt.Fprintf(&b, "- `%s`: %s\n", sig(f.Decl), inline(f.Doc))
		}
		for _, m := range t.Methods {
			fmt.Fprintf(&b, "- `%s`: %s\n", sig(m.Decl), inline(m.Doc))
		}
		if len(t.Funcs)+len(t.Methods) > 0 {
			b.WriteString("\n")
		}
	}
	if len(pkg.Funcs) > 0 {
		b.WriteString("### Functions\n\n")
		for _, f := range pkg.Funcs {
			fmt.Fprintf(&b, "- `%s`: %s\n", sig(f.Decl), inline(f.Doc))
		}
		b.WriteString("\n")
	}
	for _, c := range pkg.Consts {
		fmt.Fprintf(&b, "### Constants\n\n```go\n%s\n```\n\n%s\n\n", render(fset, c.Decl), text(c.Doc))
	}
	fmt.Print(strings.TrimRight(b.String(), "\n") + "\n")
}

// fieldsOf lists a struct's exported fields with their comments.
func fieldsOf(decl *ast.GenDecl) string {
	var out []string
	for _, spec := range decl.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			continue
		}
		for _, f := range st.Fields.List {
			var names []string
			for _, n := range f.Names {
				if n.IsExported() {
					names = append(names, n.Name)
				}
			}
			if len(f.Names) == 0 {
				// An embedded field.
				names = append(names, exprString(f.Type))
			}
			if len(names) == 0 {
				continue
			}
			line := fmt.Sprintf("- `%s %s`", strings.Join(names, ", "), exprString(f.Type))
			if len(f.Names) == 0 {
				line = fmt.Sprintf("- `%s` (embedded)", names[0])
			}
			if f.Doc != nil {
				line += ": " + inline(f.Doc.Text())
			}
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, "\n") + "\n"
}

func exprString(e ast.Expr) string {
	var b bytes.Buffer
	_ = format.Node(&b, token.NewFileSet(), e)
	return b.String()
}

func render(fset *token.FileSet, node any) string {
	var b bytes.Buffer
	if err := format.Node(&b, fset, node); err != nil {
		fail(err)
	}
	return strings.TrimSpace(b.String())
}

var docLink = regexp.MustCompile(`(^|[\s(])\[([A-Z][\w.]+)\]`)

// text is a doc comment as Markdown: code blocks (indented lines) fenced.
func text(s string) string {
	var out []string
	inCode := false
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		code := strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "    ")
		if code && !inCode {
			out = append(out, "```go")
			inCode = true
		} else if !code && inCode && line != "" {
			out = append(out, "```")
			inCode = false
		}
		if code {
			line = strings.TrimPrefix(strings.TrimPrefix(line, "\t"), "    ")
		} else {
			line = escape(line)
		}
		out = append(out, line)
	}
	if inCode {
		out = append(out, "```")
	}
	return docLink.ReplaceAllString(strings.Join(out, "\n"), "$1`$2`")
}

// inline is a doc comment on one line.
func inline(s string) string {
	return docLink.ReplaceAllString(escape(strings.Join(strings.Fields(s), " ")), "$1`$2`")
}

// escape keeps < and > outside of code spans from reading as HTML in the page.
func escape(line string) string {
	parts := strings.Split(line, "`")
	for i := 0; i < len(parts); i += 2 {
		parts[i] = strings.NewReplacer("<", "&lt;", ">", "&gt;").Replace(parts[i])
	}
	return strings.Join(parts, "`")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
