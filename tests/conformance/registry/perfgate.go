package registry

import (
	"fmt"
	"reflect"
	"sort"
)

// The perf-block rule of the parity-lock gate (Rule 3). The perf rows hold the exact, deterministic
// counters of generated Parquet files (S3 GETs and bytes, row groups, pages) next to a latency budget. A
// change that alters the bytes of the generated files (a nullable column's definition levels, a new
// footer key, a different encoding) moves s3_bytes by a few bytes, so the rule decides a perf-block change
// of an expect=pass row by CONTENT, per field, against the merge base:
//
//	counter or budget lower or equal      free
//	s3_bytes higher by at most 1.0 %      allowed, LISTED in the report (PerfRaise)
//	s3_bytes higher by more than 1.0 %    FAIL
//	s3_gets, row_groups, pages higher     FAIL (more requests or more work is never structural noise)
//	p50_ms / p90_ms higher                FAIL (latency is noise-bound, not a structural counter)
//	budget.valid weaker                   FAIL (it may stay, or become a stronger exact k/N)
//	cell, counters.path, any other key    FAIL (a different measurement: as every other field of a pass row)
//
// The 1.0 % allowance is taken against the merge base's value, so each PR gets it against main's current
// value; slow drift (+1 % per PR) is possible, and the owner's review of the listed raises is the control.

// perfBytesAllowancePct is the relative increase of s3_bytes (in percent) a PR may make and have it
// listed instead of failed. The boundary is inclusive and decided in integer arithmetic.
const perfBytesAllowancePct = 1

// perfCounters are the deterministic work counters of a perf row. The first two are always written by
// the generator; row_groups and pages are omitted when zero.
var perfCounters = []struct {
	key      string
	required bool // present in every row: a key that appears or disappears is a change
	tolerant bool // an increase within perfBytesAllowancePct is listed, not failed
}{
	{"s3_gets", true, false},
	{"s3_bytes", true, true},
	{"row_groups", false, false},
	{"pages", false, false},
}

// PerfRaise is one deterministic counter a PR raises within the allowance.
type PerfRaise struct {
	Row, File, Field string
	Old, New         int64
}

// Percent is the relative increase in percent.
func (p PerfRaise) Percent() float64 {
	if p.Old <= 0 {
		return 0
	}
	return float64(p.New-p.Old) * 100 / float64(p.Old)
}

// Annotation is the `::warning file=<rows file>::` line for the pull request's Files tab.
func (p PerfRaise) Annotation() string {
	return "::warning file=" + WorkflowProperty(p.File) + "::" + WorkflowData(p.Line())
}

// Line is the one-line description of the raise, for the annotation and the log.
func (p PerfRaise) Line() string {
	return fmt.Sprintf("perf counter raised: %s %s %d -> %d (+%.2f %%)", p.Row, p.Field, p.Old, p.New, p.Percent())
}

// PerfRaiseWarnings renders one annotation per raise, at most max, then one "... and N more" annotation.
func PerfRaiseWarnings(raises []PerfRaise, max int) []string {
	var out []string
	for i, r := range raises {
		if i == max {
			out = append(out, "::warning::"+WorkflowData(fmt.Sprintf("... and %d more perf counters raised within 1.0 %% (see the job summary)", len(raises)-max)))
			break
		}
		out = append(out, r.Annotation())
	}
	return out
}

// LargestPerfRaise returns the raise with the largest relative increase (the first on a tie, in order).
func LargestPerfRaise(raises []PerfRaise) (PerfRaise, bool) {
	var best PerfRaise
	found := false
	for _, r := range raises {
		if !found || r.Percent() > best.Percent() {
			best, found = r, true
		}
	}
	return best, found
}

// perfDiff is what a PR did to the perf block of one expect=pass row.
type perfDiff struct {
	Weak    []string    // identity of the measurement changed: a weakening as for any other field
	Regress []string    // a counter or a budget got worse beyond the allowance
	Raises  []PerfRaise // s3_bytes raised within the allowance
}

func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case uint64:
		if n > 1<<62 {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// comparePerf decides the change of one row's perf block. b and h are the decoded `perf` values.
func comparePerf(id, file string, b, h any) perfDiff {
	var d perfDiff
	if reflect.DeepEqual(b, h) {
		return d
	}
	bm, bok := b.(map[string]any)
	hm, hok := h.(map[string]any)
	if !bok || !hok {
		d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf changed: %s", id))
		return d
	}
	for _, k := range unionKeys(bm, hm) {
		if k == "budget" || k == "counters" {
			continue
		}
		if !reflect.DeepEqual(bm[k], hm[k]) {
			d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.%s changed: %s (a different measurement)", k, id))
		}
	}
	d.comparePerfBudget(id, bm["budget"], hm["budget"])
	d.comparePerfCounters(id, file, bm["counters"], hm["counters"])
	if len(d.Weak)+len(d.Regress) > 0 {
		d.Raises = nil // a failing row is reported by its failures only
	}
	return d
}

func unionKeys(a, b map[string]any) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range []map[string]any{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (d *perfDiff) comparePerfBudget(id string, b, h any) {
	if reflect.DeepEqual(b, h) {
		return
	}
	bm, bok := b.(map[string]any)
	hm, hok := h.(map[string]any)
	if !bok || !hok {
		d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.budget changed: %s (a pass row keeps a latency budget)", id))
		return
	}
	for _, k := range unionKeys(bm, hm) {
		bv, hv := bm[k], hm[k]
		if reflect.DeepEqual(bv, hv) {
			continue
		}
		switch k {
		case "p50_ms", "p90_ms":
			bf, ok1 := asFloat(bv)
			hf, ok2 := asFloat(hv)
			switch {
			case !ok1 || !ok2:
				d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.budget.%s changed to a non-number: %s", k, id))
			case hf > bf:
				d.Regress = append(d.Regress, fmt.Sprintf("perf budget raised: %s %s %g -> %g ms: latency budgets are noise-bound, so a pass row's budget may only stay or go down; restore the budget, or the owner approves the increase by merging with an admin bypass", id, k, bf, hf))
			}
		case "valid":
			if !validNotWeaker(bv, hv) {
				d.Regress = append(d.Regress, fmt.Sprintf("perf budget weakened: %s valid %v -> %v: the cell must stay exact in every iteration (k/k with at least as many iterations); restore the budget, or the owner approves the change by merging with an admin bypass", id, bv, hv))
			}
		default:
			d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.budget.%s changed: %s", k, id))
		}
	}
}

// validNotWeaker: budget.valid is "k/N" (k exact iterations of N). It may stay, or become exact in every
// iteration of at least as many iterations as before.
func validNotWeaker(b, h any) bool {
	bs, _ := b.(string)
	hs, _ := h.(string)
	var bk, bn, hk, hn int
	if n, _ := fmt.Sscanf(bs, "%d/%d", &bk, &bn); n != 2 {
		return false
	}
	if n, _ := fmt.Sscanf(hs, "%d/%d", &hk, &hn); n != 2 {
		return false
	}
	return bk == bn && hk == hn && hn >= bn
}

func (d *perfDiff) comparePerfCounters(id, file string, b, h any) {
	if reflect.DeepEqual(b, h) {
		return
	}
	bm, bok := b.(map[string]any)
	hm, hok := h.(map[string]any)
	if !bok || !hok {
		d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.counters changed: %s", id))
		return
	}
	known := map[string]bool{}
	for _, c := range perfCounters {
		known[c.key] = true
	}
	for _, k := range unionKeys(bm, hm) {
		if !known[k] && !reflect.DeepEqual(bm[k], hm[k]) {
			d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.counters.%s changed: %s (a different measurement)", k, id))
		}
	}
	for _, c := range perfCounters {
		bv, bhas := bm[c.key]
		hv, hhas := hm[c.key]
		if bhas && hhas && reflect.DeepEqual(bv, hv) {
			continue
		}
		if !bhas && !hhas {
			continue
		}
		if c.required && bhas != hhas {
			d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.counters.%s appeared or disappeared: %s", c.key, id))
			continue
		}
		bi, ok1 := asInt(bv)
		hi, ok2 := asInt(hv)
		if (bhas && !ok1) || (hhas && !ok2) {
			d.Weak = append(d.Weak, fmt.Sprintf("pass row's perf.counters.%s is not an integer: %s", c.key, id))
			continue
		}
		d.compareCounter(id, file, c.key, c.tolerant, bi, hi)
	}
}

func (d *perfDiff) compareCounter(id, file, field string, tolerant bool, old, now int64) {
	if now <= old {
		return
	}
	delta := now - old
	switch {
	case tolerant && delta <= old*perfBytesAllowancePct/100:
		d.Raises = append(d.Raises, PerfRaise{Row: id, File: file, Field: field, Old: old, New: now})
	case tolerant:
		r := PerfRaise{Row: id, Field: field, Old: old, New: now}
		d.Regress = append(d.Regress, fmt.Sprintf("perf counter raised beyond %d.0 %%: %s %s +%.1f %% (%d -> %d): the PR makes this read path slower or heavier; restore the performance, or the owner approves the increase by merging with an admin bypass", perfBytesAllowancePct, id, field, r.Percent(), old, now))
	default:
		d.Regress = append(d.Regress, fmt.Sprintf("perf counter raised: %s %s %d -> %d: any increase of the requests, row groups or pages a read touches is more work, not structural noise; restore the performance, or the owner approves the increase by merging with an admin bypass", id, field, old, now))
	}
}
