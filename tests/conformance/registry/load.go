package registry

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// flatten converts multi-line error messages to single-line format for aggregation.
func flatten(err error) string {
	return strings.ReplaceAll(err.Error(), "\n", " ")
}

// LoadDir reads every *.yaml file under dir (recursively); each file holds one
// or more YAML documents, each a list of rows. It validates each row, rejects
// duplicate ids and unknown YAML keys, and returns rows sorted: native first,
// then lh-shim, then lh-addition, then by id — the report order. .yml files are
// ignored on purpose (CI globs **/*.yaml).
func LoadDir(dir string) (*Registry, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("registry dir %q: %w", dir, err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("registry dir %q: not a directory", dir)
	}
	reg := &Registry{ByID: map[string]*Row{}}
	var errs []string
	seenIDs := make(map[string]bool) // Track duplicates during loading
	walk := func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			// Returned as-is: the caller below wraps whatever WalkDir
			// returns with "registry dir %q: %w" exactly once. Wrapping
			// here too would double it to "registry dir %q: registry dir
			// %q: ...".
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, err))
			return nil
		}
		// Use strict decoder to reject unknown keys and handle multiple YAML documents
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		for {
			var doc []Row
			err := dec.Decode(&doc)
			if err == io.EOF {
				break
			}
			if err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", path, flatten(err)))
				break
			}
			for _, r := range doc {
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
		}
		return nil
	}
	if err := filepath.WalkDir(dir, walk); err != nil {
		return nil, fmt.Errorf("registry dir %q: %w", dir, err)
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
