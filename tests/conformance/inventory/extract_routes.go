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
	caseRe   = regexp.MustCompile(`case\s+"(/[^"]+)"`)
	prefixRe = regexp.MustCompile(`strings\.HasPrefix\(\s*path\s*,\s*"(/[^"]+)"`)
	eqRe     = regexp.MustCompile(`path\s*==\s*"(/[^"]+)"`)
)

// vlRouteFiles lists where VictoriaLogs registers HTTP paths (relative to the VL dir).
var vlRouteFiles = []string{
	"app/vlselect/main.go",
	"app/vlinsert/main.go",
	"app/vlinsert", // per-format sub-packages (loki/loki.go, elasticsearch/elasticsearch.go, ...)
}

// vtRouteFiles lists where VictoriaTraces registers HTTP paths (relative to the VT dir).
var vtRouteFiles = []string{
	"app/vtselect/main.go",
	"app/vtselect/traces/jaeger/jaeger.go",
	"app/vtselect/traces/tempo/tempo.go",
	"app/vtinsert/main.go",
	"app/vtinsert", // opentelemetry/otlphttp.go
}

func ExtractVLRoutes(vlDir string) ([]Item, error) { return extractRoutes(vlDir, vlRouteFiles) }
func ExtractVTRoutes(vtDir string) ([]Item, error) { return extractRoutes(vtDir, vtRouteFiles) }

func extractRoutes(root string, entries []string) ([]Item, error) {
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
			for _, re := range []*regexp.Regexp{caseRe, prefixRe, eqRe} {
				for _, m := range re.FindAllStringSubmatch(string(data), -1) {
					name := m[1]
					if _, ok := seen[name]; !ok {
						seen[name] = Item{Kind: "route", Name: name, Source: rel}
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
