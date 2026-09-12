package registry

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadDir reads every *.yaml file under dir (recursively), validates each row,
// and rejects duplicate ids. Rows are returned sorted: native first, then
// lh-shim, then lh-addition, then by id — the report order.
func LoadDir(dir string) (*Registry, error) {
	st, err := os.Stat(dir)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("registry dir %q: %w", dir, err)
	}
	reg := &Registry{ByID: map[string]*Row{}}
	var errs []string
	seenIDs := make(map[string]bool) // Track duplicates during loading
	walk := func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var rows []Row
		if err := yaml.Unmarshal(data, &rows); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for _, r := range rows {
			if err := r.Validate(); err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", path, err))
				continue
			}
			if seenIDs[r.ID] {
				errs = append(errs, fmt.Sprintf("%s: duplicate id %s", path, r.ID))
				continue
			}
			seenIDs[r.ID] = true
			reg.Rows = append(reg.Rows, r)
		}
		return nil
	}
	if err := filepath.WalkDir(dir, walk); err != nil {
		return nil, err
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return nil, fmt.Errorf("registry invalid:\n  %s", strings.Join(errs, "\n  "))
	}
	originRank := map[Origin]int{OriginNative: 0, OriginLHShim: 1, OriginLHAddition: 2}
	sort.SliceStable(reg.Rows, func(i, j int) bool {
		a, b := reg.Rows[i], reg.Rows[j]
		if originRank[a.Origin] != originRank[b.Origin] {
			return originRank[a.Origin] < originRank[b.Origin]
		}
		return a.ID < b.ID
	})
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	return reg, nil
}

// Native returns the rows with origin=native in report order.
func (reg *Registry) Native() []Row {
	var out []Row
	for _, r := range reg.Rows {
		if r.Origin == OriginNative {
			out = append(out, r)
		}
	}
	return out
}

// UpstreamKeys maps "<kind>:<name>" (see Upstream.Key) to the row ids covering it.
func (reg *Registry) UpstreamKeys() map[string][]string {
	out := map[string][]string{}
	for _, r := range reg.Rows {
		if r.Upstream != nil {
			k := r.Upstream.Key()
			out[k] = append(out[k], r.ID)
		}
	}
	return out
}
