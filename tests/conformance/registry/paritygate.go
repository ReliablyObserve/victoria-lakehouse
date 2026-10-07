package registry

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The parity-lock gate (cmd/testlinks -parity). Owner rule (2026-10-07): every
// parity fix ships LOCKS, and a lock is never weakened without the owner.
//
// A PR is a PARITY-FIX PR when it removes entries from the parity allowlist,
// flips a divergence in docs/parity-and-gaps.md to resolved, or flips a
// registry row from expect=differ (a known gap) to expect=pass. Such a PR must
// also change a test under tests/parity and add or update an exact-compare row
// that references it. Independently, ANY PR that adds an allowlist entry or
// weakens a row (pass to differ, exact-json to a looser compare, or deletes an
// exact row) is a weakening and fails.

// ParityExactCompare is the compare type that counts as an exact lock.
const ParityExactCompare = "exact-json"

// RowLite is the part of a registry row the gate reads, plus the whole decoded
// row for change detection.
type RowLite struct {
	ID      string
	Expect  string
	Compare string
	Tests   []string
	Whole   map[string]any
}

// Exact reports whether the row is a lock: expected to pass with an exact compare.
func (r RowLite) Exact() bool { return r.Expect == "pass" && r.Compare == ParityExactCompare }

// ParitySnapshot is the lock-relevant state of the tree at one revision.
type ParitySnapshot struct {
	Allowlist map[string]bool    // known_failures.txt entries
	Resolved  map[string]bool    // divergences marked resolved in docs/parity-and-gaps.md
	Rows      map[string]RowLite // registry rows by id
}

// ParseAllowlist returns the test paths listed in tests/parity/known_failures.txt:
// every non-comment line's first field.
func ParseAllowlist(src []byte) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(string(src), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if f := strings.Fields(line); len(f) > 0 {
			out[f[0]] = true
		}
	}
	return out
}

// ParseResolved returns the divergences docs/parity-and-gaps.md marks resolved:
// the first cell of every table row that has a cell equal to "Resolved"
// (bold or plain).
func ParseResolved(src []byte) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(string(src), "\n") {
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "| \t"), "|")
		if len(cells) < 2 {
			continue
		}
		id := strings.TrimSpace(strings.ReplaceAll(cells[0], "**", ""))
		for _, c := range cells[1:] {
			if strings.TrimSpace(strings.ReplaceAll(c, "**", "")) == "Resolved" {
				out[id] = true
				break
			}
		}
	}
	return out
}

// ParseRowsLenient decodes registry row files leniently (unknown keys kept in
// Whole), adding the rows to dst.
func ParseRowsLenient(src []byte, dst map[string]RowLite) error {
	var docs []map[string]any
	if err := yaml.Unmarshal(src, &docs); err != nil {
		return err
	}
	for _, d := range docs {
		id, _ := d["id"].(string)
		if id == "" {
			continue
		}
		r := RowLite{ID: id, Whole: d}
		r.Expect, _ = d["expect"].(string)
		if c, ok := d["compare"].(map[string]any); ok {
			r.Compare, _ = c["type"].(string)
		}
		if refs, ok := d["refs"].(map[string]any); ok {
			if ts, ok := refs["tests"].([]any); ok {
				for _, t := range ts {
					if s, ok := t.(string); ok {
						r.Tests = append(r.Tests, s)
					}
				}
			}
		}
		dst[id] = r
	}
	return nil
}

// ParityVerdict compares two snapshots. changedParityTests are the changed
// `tests/parity/*_test.go` files.
type ParityVerdict struct {
	Fix        bool
	Triggers   []string // why the PR is a parity fix
	Problems   []string // a parity fix is missing its locks
	Weakenings []string // a lock was weakened or an allowlist entry added
}

func ParityCheck(base, head ParitySnapshot, changedParityTests []string) ParityVerdict {
	var v ParityVerdict
	for _, e := range sortedKeys(base.Allowlist) {
		if !head.Allowlist[e] {
			v.Triggers = append(v.Triggers, "allowlist entry removed: "+e)
		}
	}
	removedTop := map[string]bool{}
	for _, e := range sortedKeys(base.Allowlist) {
		if !head.Allowlist[e] {
			removedTop[topLevelTest(e)] = true
		}
	}
	for _, e := range sortedKeys(head.Allowlist) {
		if !base.Allowlist[e] {
			msg := "allowlist entry added: " + e
			if removedTop[topLevelTest(e)] {
				msg += " (looks like a rename of a removed entry of the same test — needs owner review (registry-exempt) or keep the old entry name)"
			}
			v.Weakenings = append(v.Weakenings, msg)
		}
	}
	for _, id := range sortedKeys(head.Resolved) {
		if !base.Resolved[id] {
			v.Triggers = append(v.Triggers, "divergence marked resolved in docs/parity-and-gaps.md: "+id)
		}
	}
	for _, id := range sortedRowKeys(base.Rows) {
		b := base.Rows[id]
		h, ok := head.Rows[id]
		switch {
		case !ok:
			if b.Exact() {
				v.Weakenings = append(v.Weakenings, "exact row deleted: "+id)
			}
		case b.Expect == "differ" && h.Expect == "pass":
			v.Triggers = append(v.Triggers, "row flipped from known gap (differ) to pass: "+id)
		case b.Exact() && !h.Exact():
			v.Weakenings = append(v.Weakenings, fmt.Sprintf("row weakened from exact pass to expect=%s compare=%s: %s", h.Expect, h.Compare, id))
		}
	}
	v.Fix = len(v.Triggers) > 0
	if !v.Fix {
		return v
	}
	if len(changedParityTests) == 0 {
		v.Problems = append(v.Problems, "no test under tests/parity/ was added or changed: a parity fix needs a differential test against hot VL/VT (all layers, both signals, both tenant forms)")
	}
	linked := false
	for _, id := range sortedRowKeys(head.Rows) {
		h := head.Rows[id]
		if !h.Exact() {
			continue
		}
		if b, ok := base.Rows[id]; ok && reflect.DeepEqual(b.Whole, h.Whole) {
			continue // unchanged: not this PR's lock
		}
		for _, t := range h.Tests {
			path, _ := splitRef(t)
			for _, c := range changedParityTests {
				if path == c {
					linked = true
				}
			}
		}
	}
	if !linked {
		v.Problems = append(v.Problems, "no new or updated registry row with compare type exact-json and expect pass references a changed tests/parity test (refs.tests): the fix needs a named lock")
	}
	return v
}

var parityTestFileRe = regexp.MustCompile(`^tests/parity/[^/]+_test\.go$`)

// IsParityTestFile reports whether path is a Go test file of the parity suite.
func IsParityTestFile(path string) bool { return parityTestFileRe.MatchString(path) }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedRowKeys(m map[string]RowLite) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// topLevelTest is the top-level test of an allowlist path: the part before the
// first subtest separator.
func topLevelTest(path string) string {
	if i := strings.Index(path, "/"); i >= 0 {
		return path[:i]
	}
	return path
}
