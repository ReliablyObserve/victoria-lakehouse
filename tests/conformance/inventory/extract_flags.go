package inventory

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var flagRe = regexp.MustCompile(`(?:flag\.(?:Int|Int64|Uint|Uint64|String|Bool|Float64|Duration|Var)|flagutil\.New[A-Za-z]+|safe[A-Z][A-Za-z]*)\(\s*"([a-zA-Z0-9_.\-]+)"`)

// Packages whose flags reach a user of the binaries (relative to the VL / VT dir).
var VLFlagPackages = []string{"app/vlselect", "app/vlselect/logsql", "app/vlselect/internalselect", "app/vlinsert", "app/vlstorage"}
var VTFlagPackages = []string{"app/vtselect", "app/vtselect/logsql", "app/vtselect/internalselect", "app/vtselect/traces/tracecommon", "app/vtinsert", "app/vtstorage", "app/victoria-traces/servicegraph"}

// LinkedIntoLH records which upstream packages the Lakehouse binaries import.
// A flag defined in a package that is not linked (for example app/vlselect, whose
// request dispatcher Lakehouse does not mount) is not honored by Lakehouse;
// the coverage report marks such flags accordingly.
var LinkedIntoLH = map[string]bool{
	"app/vlselect": false, "app/vlselect/logsql": true, "app/vlselect/internalselect": true, "app/vlinsert": true, "app/vlstorage": true,
	"app/vtselect": false, "app/vtselect/logsql": false, "app/vtselect/internalselect": false, "app/vtselect/traces/tracecommon": true, "app/vtinsert": true, "app/vtstorage": true, "app/victoria-traces/servicegraph": true,
}

// ExtractFlags scans the non-test .go files directly inside each pkgDir (and, for
// packages ending in "insert", its per-format subdirectories) for flag definitions.
func ExtractFlags(root string, pkgDirs []string, linked map[string]bool) ([]Item, error) {
	// keyed by flag name: a Go binary cannot register the same flag name twice,
	// so within one upstream root a name is unique; first occurrence wins.
	seen := map[string]Item{}
	for _, pkg := range pkgDirs {
		dirs := []string{filepath.Join(root, pkg)}
		if strings.HasSuffix(pkg, "insert") {
			ents, err := os.ReadDir(filepath.Join(root, pkg))
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", pkg, err)
			}
			for _, e := range ents {
				if e.IsDir() {
					dirs = append(dirs, filepath.Join(root, pkg, e.Name()))
				}
			}
		}
		for _, d := range dirs {
			ents, err := os.ReadDir(d)
			if err != nil {
				return nil, err
			}
			for _, e := range ents {
				if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
					continue
				}
				data, err := os.ReadFile(filepath.Join(d, e.Name()))
				if err != nil {
					return nil, err
				}
				rel, _ := filepath.Rel(root, filepath.Join(d, e.Name()))
				for _, m := range flagRe.FindAllStringSubmatch(string(data), -1) {
					if _, ok := seen[m[1]]; !ok {
						seen[m[1]] = Item{Kind: "flag", Name: m[1], Source: filepath.ToSlash(rel), Linked: linked[pkg]}
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
