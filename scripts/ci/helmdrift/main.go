// helmdrift is the CI gate that keeps the Helm chart's config surface in sync
// with the binaries' config surface. Both lakehouse binaries (logs + traces)
// parse the SAME internal/config structs from the YAML file the chart renders
// out of .Values.lakehouseConfig — so every yaml-tagged key in internal/config
// must be covered by the chart (documented in values.yaml AND validated by
// values.schema.json) or consciously grandfathered in the allowlist.
//
// A new config key that is not covered FAILS CI with instructions. Burn the
// allowlist down over time; never add to it without a reason.
//
// Usage: go run ./scripts/ci/helmdrift (from the repo root)
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	configDir     = "internal/config"
	valuesPath    = "charts/victoria-lakehouse/values.yaml"
	schemaPath    = "charts/victoria-lakehouse/values.schema.json"
	allowlistPath = "scripts/ci/helm-drift-allowlist.txt"
)

func main() {
	binaryPaths, err := configPaths()
	if err != nil {
		fatal("parse %s: %v", configDir, err)
	}
	valuesPaths, err := chartValuesPaths()
	if err != nil {
		fatal("parse %s: %v", valuesPath, err)
	}
	schemaPaths, err := chartSchemaPaths()
	if err != nil {
		fatal("parse %s: %v", schemaPath, err)
	}
	allow, err := allowlist()
	if err != nil {
		fatal("parse %s: %v", allowlistPath, err)
	}

	var missing []string
	for _, p := range binaryPaths {
		if covered(p, valuesPaths) && covered(p, schemaPaths) {
			continue
		}
		if allowed(p, allow) {
			continue
		}
		where := ""
		if !covered(p, valuesPaths) {
			where = valuesPath
		}
		if !covered(p, schemaPaths) {
			if where != "" {
				where += " AND "
			}
			where += schemaPath
		}
		missing = append(missing, fmt.Sprintf("%-55s missing in %s", p, where))
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "::error::Helm config drift: %d binary config key(s) not covered by the chart:\n", len(missing))
		for _, m := range missing {
			fmt.Fprintln(os.Stderr, "  "+m)
		}
		fmt.Fprintln(os.Stderr, "\nFix: document the key under lakehouseConfig in "+valuesPath+
			" AND add it to "+schemaPath+" — or, with a written reason, add the path to "+allowlistPath+".")
		os.Exit(1)
	}
	fmt.Printf("helmdrift: %d binary config keys, all covered (values.yaml + schema) or allowlisted\n", len(binaryPaths))
}

// configPaths extracts dot-separated yaml key paths (section.key…) from the
// Config struct tree in internal/config via the AST — no reflection on a built
// binary needed, and it works for both modules (they share this package).
func configPaths() ([]string, error) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, configDir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		return nil, err
	}
	structs := map[string]*ast.StructType{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				if st, ok := ts.Type.(*ast.StructType); ok {
					structs[ts.Name.Name] = st
				}
				return true
			})
		}
	}
	root, ok := structs["Config"]
	if !ok {
		return nil, fmt.Errorf("type Config not found")
	}
	var out []string
	var walk func(st *ast.StructType, prefix string)
	walk = func(st *ast.StructType, prefix string) {
		for _, f := range st.Fields.List {
			key := yamlKey(f)
			if key == "" || key == "-" {
				continue
			}
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			// Recurse into same-package named struct types; everything else
			// (scalars, durations, slices, maps, externals) is a leaf.
			if ident, ok := f.Type.(*ast.Ident); ok {
				if sub, ok := structs[ident.Name]; ok {
					walk(sub, path)
					continue
				}
			}
			out = append(out, path)
		}
	}
	walk(root, "")
	sort.Strings(out)
	return out, nil
}

func yamlKey(f *ast.Field) string {
	if f.Tag == nil {
		return ""
	}
	tag := strings.Trim(f.Tag.Value, "`")
	y, ok := reflect.StructTag(tag).Lookup("yaml")
	if !ok {
		return ""
	}
	return strings.Split(y, ",")[0]
}

// chartValuesPaths collects key paths under lakehouseConfig in values.yaml.
func chartValuesPaths() (map[string]bool, error) {
	raw, err := os.ReadFile(valuesPath)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	lc, _ := doc["lakehouseConfig"].(map[string]any)
	paths := map[string]bool{}
	collect(lc, "", paths)
	return paths, nil
}

// chartSchemaPaths collects property paths under lakehouseConfig in the schema.
func chartSchemaPaths() (map[string]bool, error) {
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	props, _ := dig(doc, "properties", "lakehouseConfig", "properties").(map[string]any)
	paths := map[string]bool{}
	var walk func(props map[string]any, prefix string)
	walk = func(props map[string]any, prefix string) {
		for k, v := range props {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			paths[path] = true
			if sub, ok := dig(v, "properties").(map[string]any); ok {
				walk(sub, path)
			}
		}
	}
	walk(props, "")
	return paths, nil
}

func collect(m map[string]any, prefix string, out map[string]bool) {
	for k, v := range m {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		out[path] = true
		if sub, ok := v.(map[string]any); ok {
			collect(sub, path, out)
		}
	}
}

func dig(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}

// covered requires the EXACT path in the surface. No parent-section fallback:
// it would let every new key in an already-documented section slip through —
// the exact case this gate exists to catch.
func covered(path string, surface map[string]bool) bool {
	return surface[path]
}

func allowed(path string, allow map[string]bool) bool {
	if allow[path] {
		return true
	}
	for p := range allow {
		if strings.HasSuffix(p, ".*") && strings.HasPrefix(path, strings.TrimSuffix(p, "*")) {
			return true
		}
	}
	return false
}

func allowlist() (map[string]bool, error) {
	raw, err := os.ReadFile(allowlistPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		return nil, err
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[line] = true
	}
	return out, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "helmdrift: "+format+"\n", args...)
	os.Exit(1)
}
