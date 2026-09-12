package inventory

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	caseRe    = regexp.MustCompile(`case\s+("[^"]+"(?:\s*,\s*"[^"]+")*)\s*:`)
	caseValRe = regexp.MustCompile(`"/[^"]+"`)
	mapKeyRe  = regexp.MustCompile(`(?m)^\s*"(/[^"]+)"\s*:\s*[A-Za-z_]`)
	prefixRe  = regexp.MustCompile(`strings\.HasPrefix\(\s*path\s*,\s*"(/[^"]+)"`)
	eqRe      = regexp.MustCompile(`path\s*==\s*"(/[^"]+)"`)
)

// gatePrefixes are outer dispatch gates, not endpoints; they are filtered from routes.
var gatePrefixes = map[string]bool{
	"/insert/":             true,
	"/select/":             true,
	"/delete/":             true,
	"/internal/select/":    true,
	"/internal/delete/":    true,
	"/select/vmui/static/": true,
}

// vlRouteFiles lists where VictoriaLogs registers HTTP paths (relative to the VL dir).
var vlRouteFiles = []string{
	"app/vlselect/main.go",
	"app/vlselect/internalselect/internalselect.go",
	"app/vlinsert", // per-format sub-packages (loki/loki.go, elasticsearch/elasticsearch.go, ...)
	"app/vlstorage/main.go",
}

// vtRouteFiles lists where VictoriaTraces registers HTTP paths (relative to the VT dir).
var vtRouteFiles = []string{
	"app/vtselect/main.go",
	"app/vtselect/logsql.go",
	"app/vtselect/internalselect/internalselect.go",
	"app/vtselect/traces/jaeger/jaeger.go",
	"app/vtselect/traces/tempo/tempo.go",
	"app/vtinsert", // opentelemetry/otlphttp.go
	"app/vtstorage/main.go",
}

func ExtractVLRoutes(vlDir string) ([]Item, error) { return extractRoutes(vlDir, vlRouteFiles, "vl") }
func ExtractVTRoutes(vtDir string) ([]Item, error) { return extractRoutes(vtDir, vtRouteFiles, "vt") }

func extractRoutes(root string, entries []string, surface string) ([]Item, error) {
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("upstream dir %q: not a directory", root)
	}
	seen := map[string]Item{}
	for _, e := range entries {
		p := filepath.Join(root, e)
		st, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("upstream file %s: %w", e, err)
		}
		var files []string
		if st.IsDir() {
			if err := filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
					files = append(files, path)
				}
				return nil
			}); err != nil {
				return nil, fmt.Errorf("walk upstream dir %s: %w", e, err)
			}
		} else {
			files = []string{p}
		}
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				return nil, err
			}
			rel, _ := filepath.Rel(root, f)
			rel = filepath.ToSlash(rel)
			dataStr := string(data)

			// Handle multi-value case statements: case "/a", "/b":
			for _, m := range caseRe.FindAllStringSubmatch(dataStr, -1) {
				caseBlock := m[1] // the full ("...", "...") block
				for _, valMatch := range caseValRe.FindAllString(caseBlock, -1) {
					// Extract route from quoted string
					name := valMatch[1 : len(valMatch)-1]
					// gatePrefixes alone is enough here: /select/vmui/static/
					// is only ever matched via strings.HasPrefix (below), never
					// as a literal case value, and gatePrefixes already
					// excludes that exact literal too.
					if !gatePrefixes[name] {
						if _, ok := seen[name]; !ok {
							seen[name] = Item{Kind: "route", Surface: surface, Name: name, Source: rel}
						}
					}
				}
			}

			// Handle map-literal keys: "/path": handler,
			for _, m := range mapKeyRe.FindAllStringSubmatch(dataStr, -1) {
				name := m[1]
				if !gatePrefixes[name] {
					if _, ok := seen[name]; !ok {
						seen[name] = Item{Kind: "route", Surface: surface, Name: name, Source: rel}
					}
				}
			}

			// Handle strings.HasPrefix calls
			for _, m := range prefixRe.FindAllStringSubmatch(dataStr, -1) {
				name := m[1]
				if !gatePrefixes[name] {
					if _, ok := seen[name]; !ok {
						seen[name] = Item{Kind: "route", Surface: surface, Name: name, Source: rel}
					}
				}
			}

			// Handle path == "..." comparisons
			for _, m := range eqRe.FindAllStringSubmatch(dataStr, -1) {
				name := m[1]
				if !gatePrefixes[name] {
					if _, ok := seen[name]; !ok {
						seen[name] = Item{Kind: "route", Surface: surface, Name: name, Source: rel}
					}
				}
			}
		}
	}
	out := make([]Item, 0, len(seen))
	for _, it := range seen {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
