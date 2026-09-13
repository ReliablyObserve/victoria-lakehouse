package config

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

const fieldDocsGolden = "testdata/field-docs.json"

// fieldDoc is one documented Go identifier: a struct field (for a key) or
// a struct type (for a section).
type fieldDoc struct {
	Name string `json:"name"`
	Doc  string `json:"doc"`
}

// fieldDocSet is what testdata/field-docs.json holds: the doc comment of
// every leaf config key and of every section, as written in the Go sources
// of this package.
type fieldDocSet struct {
	Keys     map[string]fieldDoc `json:"keys"`
	Sections map[string]fieldDoc `json:"sections"`
}

// readFieldDocs parses the non-test Go files in dir and walks the Config
// struct tree the way configFields does, collecting doc comments.
func readFieldDocs(dir string) (*fieldDocSet, error) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type structDoc struct {
		st   *ast.StructType
		name string
		doc  string
	}
	structs := map[string]structDoc{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts := spec.(*ast.TypeSpec)
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				doc := ts.Doc.Text()
				if doc == "" {
					doc = gen.Doc.Text()
				}
				structs[ts.Name.Name] = structDoc{st: st, name: ts.Name.Name, doc: normalizeDoc(doc)}
			}
		}
	}

	out := &fieldDocSet{Keys: map[string]fieldDoc{}, Sections: map[string]fieldDoc{}}
	var walk func(st *ast.StructType, prefix string)
	walk = func(st *ast.StructType, prefix string) {
		for _, f := range st.Fields.List {
			if f.Tag == nil || len(f.Names) == 0 {
				continue
			}
			tag, ok := reflect.StructTag(strings.Trim(f.Tag.Value, "`")).Lookup("yaml")
			key := strings.Split(tag, ",")[0]
			if !ok || key == "" || key == "-" {
				continue
			}
			if prefix != "" {
				key = prefix + "." + key
			}
			doc := normalizeDoc(f.Doc.Text() + f.Comment.Text())
			if ident, ok := f.Type.(*ast.Ident); ok {
				if sub, ok := structs[ident.Name]; ok {
					section := fieldDoc{Name: f.Names[0].Name, Doc: doc}
					if doc == "" {
						section = fieldDoc{Name: sub.name, Doc: sub.doc}
					}
					out.Sections[key] = section
					walk(sub.st, key)
					continue
				}
			}
			out.Keys[key] = fieldDoc{Name: f.Names[0].Name, Doc: doc}
		}
	}
	walk(structs["Config"].st, "")
	return out, nil
}

// normalizeDoc joins wrapped comment lines into paragraphs separated by a
// blank line.
func normalizeDoc(text string) string {
	var paras []string
	for _, p := range strings.Split(strings.TrimSpace(text), "\n\n") {
		if p = strings.Join(strings.Fields(p), " "); p != "" {
			paras = append(paras, p)
		}
	}
	return strings.Join(paras, "\n\n")
}

func (d *fieldDocSet) json() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(d); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// TestFieldDocsGolden pins the doc comments of every config key in
// testdata/field-docs.json. The generated configuration reference in
// docs/configuration.md takes its descriptions from that file, so editing a
// field comment means regenerating it (`make config-surface`).
func TestFieldDocsGolden(t *testing.T) {
	docs, err := readFieldDocs(".")
	if err != nil {
		t.Fatal(err)
	}

	var astKeys, reflectKeys []string
	for k := range docs.Keys {
		astKeys = append(astKeys, k)
	}
	sort.Strings(astKeys)
	for _, f := range configFields() {
		reflectKeys = append(reflectKeys, f.key)
	}
	if !reflect.DeepEqual(astKeys, reflectKeys) {
		t.Fatalf("doc comments cover %d keys, the config has %d; the source walk and configFields disagree", len(astKeys), len(reflectKeys))
	}

	got, err := docs.json()
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CONFIG_SURFACE_UPDATE") == "1" {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fieldDocsGolden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := testutil.DiffGolden(fieldDocsGolden, got); err != nil {
		t.Fatalf("%v\nregenerate with `make config-surface` and commit the result", err)
	}
}

func TestReadFieldDocs_Errors(t *testing.T) {
	if _, err := readFieldDocs(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing directory must fail")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte("package x\nfunc {"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readFieldDocs(dir); err == nil {
		t.Error("unparsable Go must fail")
	}
}

func TestNormalizeDoc(t *testing.T) {
	in := "Line one\n  continues here.\n\nSecond   paragraph.\n\n\n"
	if got := normalizeDoc(in); got != "Line one continues here.\n\nSecond paragraph." {
		t.Errorf("normalizeDoc = %q", got)
	}
}
