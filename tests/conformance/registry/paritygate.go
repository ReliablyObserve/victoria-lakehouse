package registry

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// The parity-lock gate (cmd/testlinks). Owner rule (2026-10-07): every parity
// fix ships LOCKS, and a lock is never weakened without the owner.
//
// A PR is a PARITY-FIX PR when it removes entries from the parity allowlist,
// flips a divergence in docs/parity-and-gaps.md to resolved, or flips a
// registry row from expect=differ (a known gap) to expect=pass. Such a PR must
// also ship a LOCK: an expect=pass row with an exact-equivalent compare that
// references, as `file#Test`, a tests/parity test function the PR added or
// modified. For a differ-to-pass flip the flipped row itself must be that lock.
//
// Independently, ANY PR that weakens an existing lock fails: adds an allowlist
// entry, or changes an expect=pass row in any way that could loosen it (the
// row deleted, no longer pass, its compare type/options/project changed, or its
// request changed).

// RowLite is the part of a registry row the gate reads, plus the whole decoded
// row for change detection.
type RowLite struct {
	ID         string
	Expect     string
	Compare    string         // compare type
	CompareMap map[string]any // the whole compare block
	Request    any
	Tests      []string
	Whole      map[string]any
}

// exactEquivalent reports whether a compare block demands the same answer as
// hot, with no tolerance: exact-json, count, trace and ndjson-multiset always;
// values-with-hits with hits_tolerance 0 and series with rel_tolerance 0, both
// stated explicitly.
func exactEquivalent(c map[string]any) bool {
	t, _ := c["type"].(string)
	switch t {
	case "exact-json", "count", "trace", "ndjson-multiset":
		return true
	case "values-with-hits":
		return toleranceZero(c, "hits_tolerance")
	case "series":
		return toleranceZero(c, "rel_tolerance")
	}
	return false
}

func toleranceZero(c map[string]any, key string) bool {
	opts, _ := c["options"].(map[string]any)
	raw, ok := opts[key]
	if !ok {
		return false
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(raw)), 64)
	return err == nil && f == 0
}

// Exact reports whether the row is a lock: expected to pass with an
// exact-equivalent compare.
func (r RowLite) Exact() bool { return r.Expect == "pass" && exactEquivalent(r.CompareMap) }

// ParitySnapshot is the lock-relevant state of the tree at one revision.
type ParitySnapshot struct {
	Allowlist map[string]bool    // known_failures.txt entries
	Resolved  map[string]bool    // divergences marked resolved in docs/parity-and-gaps.md
	Rows      map[string]RowLite // registry rows by id
	// Floors is tests/parity/lock_cells.txt: the minimum number of cells each
	// lock test must report in a parity run (see parity_ratchet.py).
	Floors map[string]int
	// RefExists reports whether a "file#Test" reference still resolves in this
	// tree; nil means every reference does. It lets a test rename or move that
	// replaces a lock's reference pass, while dropping a reference does not.
	RefExists func(ref string) bool
}

// ParseAllowlist returns the test paths listed in the parity allowlist:
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

var allowlistArgRe = regexp.MustCompile(`--allowlist[ =]+(\S+)`)

// AllowlistPath returns the allowlist file the parity workflow hands to the
// ratchet (its --allowlist argument), or "" when it has none.
func AllowlistPath(workflow []byte) string {
	if m := allowlistArgRe.FindSubmatch(workflow); m != nil {
		return string(m[1])
	}
	return ""
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

// ParseEntries decodes a registry YAML file into id -> entry. It reads every
// document the way the loader does and rejects what the loader and the gate
// could read differently (see CheckCanonical).
func ParseEntries(src []byte, dst map[string]map[string]any) error {
	if err := CheckCanonical(src); err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(src))
	for {
		var docs []map[string]any
		if err := dec.Decode(&docs); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		for _, d := range docs {
			if id, _ := d["id"].(string); id != "" {
				dst[id] = d
			}
		}
	}
}

// ParseRowsLenient decodes registry row files leniently, adding the rows to dst.
func ParseRowsLenient(src []byte, dst map[string]RowLite) error {
	entries := map[string]map[string]any{}
	if err := ParseEntries(src, entries); err != nil {
		return err
	}
	for id, d := range entries {
		r := RowLite{ID: id, Whole: d, Request: d["request"]}
		r.Expect, _ = d["expect"].(string)
		if c, ok := d["compare"].(map[string]any); ok {
			r.CompareMap = c
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

// ChangedEntries compares two id -> entry sets: ids only in head (added), in
// both with a different entry (changed), and only in base (removed).
func ChangedEntries(base, head map[string]map[string]any) (added, changed, removed []string) {
	for id, h := range head {
		b, ok := base[id]
		switch {
		case !ok:
			added = append(added, id)
		case !reflect.DeepEqual(b, h):
			changed = append(changed, id)
		}
	}
	for id := range base {
		if _, ok := head[id]; !ok {
			removed = append(removed, id)
		}
	}
	sort.Strings(added)
	sort.Strings(changed)
	sort.Strings(removed)
	return
}

// proseKeys are the row fields that describe a row without changing what it
// checks; editing them never weakens a pass row.
var proseKeys = map[string]bool{"title": true, "notes": true, "description": true, "highlight": true, "differ_note": true}

// normalizedRow returns a row's fields minus prose, minus refs.doc, minus a
// false `pending`, and with refs.tests as a sorted set, so two rows compare
// equal exactly when they check the same thing.
func normalizedRow(whole map[string]any) (fields map[string]any, tests map[string]bool, pending bool) {
	fields = map[string]any{}
	tests = map[string]bool{}
	for k, v := range whole {
		switch {
		case proseKeys[k]:
		case k == "pending":
			pending, _ = v.(bool)
		case k == "refs":
			refs, _ := v.(map[string]any)
			rest := map[string]any{}
			for rk, rv := range refs {
				switch rk {
				case "doc":
				case "tests":
					if list, ok := rv.([]any); ok {
						for _, t := range list {
							if ts, ok := t.(string); ok {
								tests[ts] = true
							}
						}
					}
				default:
					rest[rk] = rv
				}
			}
			if len(rest) > 0 {
				fields[k] = rest
			}
		default:
			fields[k] = v
		}
	}
	return fields, tests, pending
}

// passRowChanges lists what a PR changed on an expect=pass row that could make
// it check less. Allowed: prose, refs.doc, more refs.tests, and `pending`
// going from true to false (the row starts executing). Everything else,
// including targets, seed, layers, upstream, compare and request, is a
// weakening. A test reference may be replaced only by a rename of a tests/parity
// lock whose cell floor the new name keeps (renamed returns the old names).
func passRowChanges(b, h RowLite, baseFloors, headFloors map[string]int, headExists func(string) bool) (out []string, renamed map[string]bool) {
	renamed = map[string]bool{}
	bf, bt, bp := normalizedRow(b.Whole)
	hf, ht, hp := normalizedRow(h.Whole)
	var keys []string
	for k := range bf {
		keys = append(keys, k)
	}
	for k := range hf {
		if _, ok := bf[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if reflect.DeepEqual(bf[k], hf[k]) {
			continue
		}
		switch k {
		case "compare":
			out = append(out, fmt.Sprintf("pass row's compare (type, options or project) changed: %s", b.ID))
		default:
			out = append(out, fmt.Sprintf("pass row's %s changed: %s", k, b.ID))
		}
	}
	var lost, added []string
	for t := range bt {
		if !ht[t] {
			lost = append(lost, t)
		}
	}
	for t := range ht {
		if !bt[t] {
			added = append(added, t)
		}
	}
	sort.Strings(lost)
	sort.Strings(added)
	used := map[string]bool{}
	for _, t := range lost {
		path, name := splitRef(t)
		floor := baseFloors[name]
		ok := false
		if IsParityTestFile(path) && name != "" && floor > 0 && headExists != nil && !headExists(t) {
			for _, a := range added {
				apath, aname := splitRef(a)
				if !used[a] && IsParityTestFile(apath) && aname != "" && headFloors[aname] >= floor {
					used[a], ok = true, true
					renamed[name] = true
					break
				}
			}
		}
		if !ok {
			out = append(out, fmt.Sprintf("pass row lost its test reference %s: %s (a reference may only be replaced by a rename of a tests/parity lock whose cell floor the new name keeps)", t, b.ID))
		}
	}
	if hp && !bp {
		out = append(out, fmt.Sprintf("pass row set to pending (it stops executing): %s", b.ID))
	}
	return out, renamed
}

// ParityVerdict is the outcome of ParityCheck.
type ParityVerdict struct {
	Fix        bool
	Triggers   []string // why the PR is a parity fix
	Problems   []string // a parity fix is missing its locks
	Weakenings []string // a lock was weakened or an allowlist entry added
}

// ParityCheck compares two snapshots. modified is the set of "path#TestName"
// tests/parity functions the PR added or modified.
func ParityCheck(base, head ParitySnapshot, modified map[string]bool) ParityVerdict {
	var v ParityVerdict
	removedTop := map[string]bool{}
	for _, e := range sortedKeys(base.Allowlist) {
		if !head.Allowlist[e] {
			v.Triggers = append(v.Triggers, "allowlist entry removed: "+e)
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
	var flipped []string
	renamed := map[string]bool{}
	for _, id := range sortedRowKeys(base.Rows) {
		b := base.Rows[id]
		h, ok := head.Rows[id]
		if b.Expect == "differ" && ok && h.Expect == "pass" {
			flipped = append(flipped, id)
			v.Triggers = append(v.Triggers, "row flipped from known gap (differ) to pass: "+id)
		}
		if b.Expect != "pass" {
			continue
		}
		switch {
		case !ok:
			v.Weakenings = append(v.Weakenings, "pass row deleted: "+id)
		case h.Expect != "pass":
			v.Weakenings = append(v.Weakenings, fmt.Sprintf("pass row weakened to expect=%s: %s", h.Expect, id))
		default:
			w, r := passRowChanges(b, h, base.Floors, head.Floors, head.RefExists)
			v.Weakenings = append(v.Weakenings, w...)
			for name := range r {
				renamed[name] = true
			}
		}
	}
	for _, name := range sortedFloorKeys(base.Floors) {
		if head.Floors[name] < base.Floors[name] && !renamed[name] {
			v.Weakenings = append(v.Weakenings, fmt.Sprintf("lock cell floor of %s removed or lowered (%d -> %d) in tests/parity/lock_cells.txt", name, base.Floors[name], head.Floors[name]))
		}
	}
	v.Fix = len(v.Triggers) > 0
	if !v.Fix {
		return v
	}
	if len(modified) == 0 {
		v.Problems = append(v.Problems, "no test function under tests/parity/ was added or modified: a parity fix needs a differential test against hot VL/VT (all layers, both signals, both tenant forms)")
	}
	// A lock may be a pending row (declared, executed by the parity job rather than
	// the row runner), but its test must have a cell floor: the ratchet then holds
	// the test to "ran, passed, compared at least that many cells".
	lockRef := func(t string) (string, bool) {
		_, name := splitRef(t)
		return name, name != "" && modified[t] && head.Floors[name] > 0
	}
	isLock := func(r RowLite) bool {
		if !r.Exact() {
			return false
		}
		for _, t := range r.Tests {
			if _, ok := lockRef(t); ok {
				return true
			}
		}
		return false
	}
	// lockNames: the tests a changed or new lock row names (as a modified file#Test).
	lockNames := map[string]bool{}
	for _, id := range sortedRowKeys(head.Rows) {
		h := head.Rows[id]
		if b, ok := base.Rows[id]; ok && reflect.DeepEqual(b.Whole, h.Whole) {
			continue // unchanged: not this PR's lock
		}
		if isLock(h) {
			for _, t := range h.Tests {
				if name, ok := lockRef(t); ok {
					lockNames[name] = true
				}
			}
		}
	}
	if len(flipped) > 0 {
		for _, id := range flipped {
			if !isLock(head.Rows[id]) {
				v.Problems = append(v.Problems, fmt.Sprintf("the flipped row %s must itself be a lock: expect pass, an exact-equivalent compare (exact-json, count, trace, ndjson-multiset, values-with-hits with hits_tolerance 0, series with rel_tolerance 0) and refs.tests naming, as file#Test, a tests/parity test this PR added or modified that has a cell floor in tests/parity/lock_cells.txt", id))
			}
		}
	} else if len(lockNames) == 0 {
		v.Problems = append(v.Problems, "no new or changed registry row is a lock: expect pass, an exact-equivalent compare (exact-json, count, trace, ndjson-multiset, values-with-hits with hits_tolerance 0, series with rel_tolerance 0) and refs.tests naming, as file#Test, a tests/parity test this PR added or modified that has a cell floor in tests/parity/lock_cells.txt")
	}
	// A removed allowlist entry is only locked by a row that names ITS test.
	for _, t := range sortedKeys(removedTop) {
		if !lockNames[t] {
			v.Problems = append(v.Problems, fmt.Sprintf("no lock row names %s, the test of a removed allowlist entry: the lock must reference file#%s", t, t))
		}
	}
	return v
}

// ModifiedParityTests returns the "path#Test" keys of the tests/parity test
// functions the PR added or modified, from the changed files and their sources
// at both revisions. Test files outside tests/parity never count.
func ModifiedParityTests(changed []string, baseSrc, headSrc func(string) []byte) (map[string]bool, error) {
	out := map[string]bool{}
	for _, c := range changed {
		if !IsParityTestFile(c) {
			continue
		}
		names, err := ModifiedTests(baseSrc(c), headSrc(c))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c, err)
		}
		for _, n := range names {
			out[c+"#"+n] = true
		}
	}
	return out, nil
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

// SubstantiveChanges returns the ids of registry entries a PR adds, removes or
// changes in a way that is more than prose: an entry whose only differences are
// title, notes, description, highlight, differ_note or refs.doc claims nothing
// new about behaviour.
func SubstantiveChanges(base, head map[string]map[string]any) []string {
	added, changed, removed := ChangedEntries(base, head)
	out := append(append([]string{}, added...), removed...)
	for _, id := range changed {
		bf, bt, bp := normalizedRow(base[id])
		hf, ht, hp := normalizedRow(head[id])
		if !reflect.DeepEqual(bf, hf) || !reflect.DeepEqual(bt, ht) || bp != hp {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// ChangeSet is the added, changed and removed entry ids of one registry kind.
type ChangeSet struct {
	Kind                    string // "rows" or "features"
	Added, Changed, Removed []string
}

// maxInlineIDs is how many ids a summary line lists before collapsing the rest
// into a details block.
const maxInlineIDs = 8

// SummarizeChanges renders the registry changes as Markdown for the job
// summary: counts and the first few ids per line, the full list in a details
// block when longer.
func SummarizeChanges(sets []ChangeSet) string {
	var sb strings.Builder
	sb.WriteString("### Registry changes in this PR\n\n")
	listed := false
	for _, cs := range sets {
		for _, g := range []struct {
			label string
			ids   []string
		}{{"added", cs.Added}, {"changed", cs.Changed}, {"removed", cs.Removed}} {
			if len(g.ids) == 0 {
				continue
			}
			listed = true
			if len(g.ids) <= maxInlineIDs {
				fmt.Fprintf(&sb, "- %s %s (%d): %s\n", cs.Kind, g.label, len(g.ids), strings.Join(g.ids, ", "))
				continue
			}
			fmt.Fprintf(&sb, "- %s %s (%d): %s, … and %d more\n", cs.Kind, g.label, len(g.ids),
				strings.Join(g.ids[:maxInlineIDs], ", "), len(g.ids)-maxInlineIDs)
			fmt.Fprintf(&sb, "  <details><summary>all %d %s %s</summary>\n\n", len(g.ids), cs.Kind, g.label)
			for _, id := range g.ids {
				fmt.Fprintf(&sb, "  - %s\n", id)
			}
			sb.WriteString("\n  </details>\n")
		}
	}
	if !listed {
		sb.WriteString("none\n")
	}
	return sb.String()
}

func sortedFloorKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var lockCellsNameRe = regexp.MustCompile(`^Test[A-Za-z0-9_]*$`)

// ParseLockCells reads tests/parity/lock_cells.txt: one `TestName  N  # why` per
// line, N the minimum number of cells the test must report.
func ParseLockCells(src []byte) (map[string]int, error) {
	out := map[string]int{}
	for i, raw := range strings.Split(string(src), "\n") {
		line := strings.TrimSpace(strings.SplitN(raw, "#", 2)[0])
		if line == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 || !lockCellsNameRe.MatchString(f[0]) {
			return nil, fmt.Errorf("line %d: want `TestName  minCells`, got %q", i+1, raw)
		}
		n, err := strconv.Atoi(f[1])
		if err != nil || n < 1 {
			return nil, fmt.Errorf("line %d: %s: the cell floor must be a positive integer, got %q", i+1, f[0], f[1])
		}
		if _, dup := out[f[0]]; dup {
			return nil, fmt.Errorf("line %d: duplicate entry %s", i+1, f[0])
		}
		out[f[0]] = n
	}
	return out, nil
}
