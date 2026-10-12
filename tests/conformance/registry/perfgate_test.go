package registry

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// perfSpec is the perf block of a test row; perfRowYAML renders it the way the generator writes the
// real rows (one flow-style line).
type perfSpec struct {
	cell, path        string
	gets, bytes       int64
	rowGroups, pages  int64
	p50, p90          float64
	valid             string
	expect, requestQ  string
	extraCounterKey   string
	extraBudgetKey    string
	extraPerfKey      string
	dropRowGroupsPage bool
}

func newSpec() perfSpec {
	return perfSpec{cell: "field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms", path: "scan",
		gets: 4, bytes: 1000000, rowGroups: 6, pages: 494, p50: 18.5, p90: 18.6, valid: "2/2", expect: "pass", requestQ: "*"}
}

func (s perfSpec) yaml(id string) string {
	counters := fmt.Sprintf("s3_gets: %d, s3_bytes: %d, row_groups: %d, pages: %d, path: %s", s.gets, s.bytes, s.rowGroups, s.pages, s.path)
	if s.dropRowGroupsPage {
		counters = fmt.Sprintf("s3_gets: %d, s3_bytes: %d, path: %s", s.gets, s.bytes, s.path)
	}
	if s.extraCounterKey != "" {
		counters += ", " + s.extraCounterKey + ": 1"
	}
	budget := fmt.Sprintf("p50_ms: %g, p90_ms: %g, valid: %q", s.p50, s.p90, s.valid)
	if s.extraBudgetKey != "" {
		budget += ", " + s.extraBudgetKey + ": 1"
	}
	perf := fmt.Sprintf("cell: %q, budget: { %s }, counters: { %s }", s.cell, budget, counters)
	if s.extraPerfKey != "" {
		perf += ", " + s.extraPerfKey + ": 1"
	}
	return fmt.Sprintf(`- { id: %q, surface: "vl", kind: "select", origin: "native", expect: %q, targets: ["cold"], seed: ["logs.fieldmeta"], upstream: { route: "/select/logsql/field_names" }, request: { method: "GET", path: "/select/logsql/field_names", params: { query: %q } }, compare: { type: "values-with-hits", options: { hits_tolerance: "0" } }, layers: ["perf"], pending: true, refs: { doc: "docs/x.md", tests: ["tests/parity/field_names_layers_parity_test.go#TestParity_FieldNames_Layers"] }, perf: { %s } }
`, id, s.expect, s.requestQ, perf)
}

func (s perfSpec) row(t *testing.T, id string) RowLite {
	t.Helper()
	r := wholeRow(t, s.yaml(id))
	r.File = "tests/conformance/registry/rows/perf/field_metadata.yaml"
	return r
}

func perfSnap(rows ...RowLite) ParitySnapshot {
	sn := ParitySnapshot{Rows: map[string]RowLite{}, Allowlist: map[string]bool{}, Resolved: map[string]bool{}, Floors: map[string]int{}}
	for _, r := range rows {
		sn.Rows[r.ID] = r
	}
	return sn
}

// Every case is one row, base spec -> head spec, decided by ParityCheck exactly as the gate does.
func TestPerfBlockRule(t *testing.T) {
	type outcome struct{ raises, perfFails, weak int }
	cases := []struct {
		name string
		edit func(*perfSpec)
		want outcome
	}{
		{"unchanged", func(s *perfSpec) {}, outcome{}},
		{"s3_bytes lower is free", func(s *perfSpec) { s.bytes -= 5000 }, outcome{}},
		{"s3_bytes far lower is free", func(s *perfSpec) { s.bytes /= 2 }, outcome{}},
		{"pages lower is free", func(s *perfSpec) { s.pages-- }, outcome{}},
		{"s3_gets lower is free", func(s *perfSpec) { s.gets-- }, outcome{}},
		{"row_groups lower is free", func(s *perfSpec) { s.rowGroups-- }, outcome{}},
		{"latency lower is free", func(s *perfSpec) { s.p50 -= 1; s.p90 -= 1 }, outcome{}},
		{"s3_bytes +17 bytes (0.001 %) is listed", func(s *perfSpec) { s.bytes += 17 }, outcome{raises: 1}},
		{"s3_bytes +0.5 % is listed", func(s *perfSpec) { s.bytes += s.bytes / 200 }, outcome{raises: 1}},
		{"s3_bytes exactly +1.0 % is listed (inclusive)", func(s *perfSpec) { s.bytes += 10000 }, outcome{raises: 1}},
		{"s3_bytes +1.01 % fails", func(s *perfSpec) { s.bytes += 10100 }, outcome{perfFails: 1}},
		{"s3_bytes +1 byte over the boundary fails", func(s *perfSpec) { s.bytes += 10001 }, outcome{perfFails: 1}},
		{"s3_bytes +20 % fails", func(s *perfSpec) { s.bytes += s.bytes / 5 }, outcome{perfFails: 1}},
		{"s3_gets +1 fails", func(s *perfSpec) { s.gets++ }, outcome{perfFails: 1}},
		{"pages +1 fails", func(s *perfSpec) { s.pages++ }, outcome{perfFails: 1}},
		{"row_groups +1 fails", func(s *perfSpec) { s.rowGroups++ }, outcome{perfFails: 1}},
		{"latency p50 up fails", func(s *perfSpec) { s.p50 += 0.001 }, outcome{perfFails: 1}},
		{"latency p90 up fails", func(s *perfSpec) { s.p90 += 0.5 }, outcome{perfFails: 1}},
		{"latency p50 up and p90 down fails", func(s *perfSpec) { s.p50 += 1; s.p90 -= 0.05 }, outcome{perfFails: 1}},
		{"bytes within allowance and latency up: the row fails, nothing is listed", func(s *perfSpec) { s.bytes += 10; s.p50 += 1 }, outcome{perfFails: 1}},
		{"bytes within allowance and gets up: the row fails, nothing is listed", func(s *perfSpec) { s.bytes += 10; s.gets++ }, outcome{perfFails: 1}},
		{"valid kept", func(s *perfSpec) { s.valid = "2/2" }, outcome{}},
		{"valid more iterations, all exact is free", func(s *perfSpec) { s.valid = "4/4" }, outcome{}},
		{"valid fewer iterations fails", func(s *perfSpec) { s.valid = "1/1" }, outcome{perfFails: 1}},
		{"valid not exact in every iteration fails", func(s *perfSpec) { s.valid = "3/4" }, outcome{perfFails: 1}},
		{"path changed is a different measurement", func(s *perfSpec) { s.path = "index"; s.bytes /= 100 }, outcome{weak: 1}},
		{"cell changed (layout/pmeta/window/filter identity) is a different measurement", func(s *perfSpec) {
			s.cell = strings.Replace(s.cell, "layout=compacted", "layout=flushed", 1)
			s.bytes -= 10
		}, outcome{weak: 1}},
		{"request changed", func(s *perfSpec) { s.requestQ = "error" }, outcome{weak: 1}},
		{"expect pass -> differ", func(s *perfSpec) { s.expect = "differ" }, outcome{weak: 1}},
		{"an unknown perf key", func(s *perfSpec) { s.extraPerfKey = "note" }, outcome{weak: 1}},
		{"an unknown counters key", func(s *perfSpec) { s.extraCounterKey = "cpu_ms" }, outcome{weak: 1}},
		{"an unknown budget key", func(s *perfSpec) { s.extraBudgetKey = "p99_ms" }, outcome{weak: 1}},
		{"row_groups and pages dropped (omitted means zero) is lower, free", func(s *perfSpec) { s.dropRowGroupsPage = true }, outcome{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base := newSpec()
			head := newSpec()
			c.edit(&head)
			v := ParityCheck(perfSnap(base.row(t, "r")), perfSnap(head.row(t, "r")), nil)
			got := outcome{len(v.PerfRaises), len(v.PerfFailures), len(v.Weakenings)}
			if got != c.want {
				t.Fatalf("raises/perfFails/weak = %+v, want %+v\nverdict: %+v", got, c.want, v)
			}
		})
	}
}

func TestPerfBlockRule_FromZeroCounters(t *testing.T) {
	zero := newSpec()
	zero.dropRowGroupsPage = true
	up := newSpec() // row_groups and pages appear: more work than the omitted zero
	v := ParityCheck(perfSnap(zero.row(t, "r")), perfSnap(up.row(t, "r")), nil)
	if len(v.PerfFailures) != 2 {
		t.Fatalf("a counter that appears from zero is a raise: %+v", v)
	}
}

func TestPerfBlockRule_RequiredCounterDisappears(t *testing.T) {
	b := wholeRow(t, newSpec().yaml("r"))
	h := wholeRow(t, strings.Replace(newSpec().yaml("r"), "s3_gets: 4, ", "", 1))
	v := ParityCheck(perfSnap(b), perfSnap(h), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "s3_gets") {
		t.Fatalf("a required counter may not disappear: %+v", v)
	}
}

func TestPerfBlockRule_PerfBlockAddedOrRemoved(t *testing.T) {
	with := newSpec().row(t, "r")
	// the same row without its perf block: every other field is identical
	src := newSpec().yaml("r")
	i := strings.Index(src, ", perf: {")
	without := wholeRow(t, src[:i]+" }\n")
	if _, ok := without.Whole["perf"]; ok || without.Whole["request"] == nil {
		t.Fatalf("fixture: %v", without.Whole)
	}
	v := ParityCheck(perfSnap(with), perfSnap(without), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "pass row's perf changed: r") {
		t.Fatalf("a perf block that vanishes from a pass row is a weakening: %+v", v)
	}
	// and the other way round: a pass row gaining a perf block is a different measurement too
	v = ParityCheck(perfSnap(without), perfSnap(with), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "pass row's perf changed: r") {
		t.Fatalf("a perf block that appears on a pass row is a weakening: %+v", v)
	}
}

func TestPerfBlockRule_NonNumbers(t *testing.T) {
	b := newSpec().row(t, "r")
	h := wholeRow(t, strings.Replace(newSpec().yaml("r"), "s3_bytes: 1000000", `s3_bytes: "big"`, 1))
	v := ParityCheck(perfSnap(b), perfSnap(h), nil)
	if len(v.Weakenings) != 1 || len(v.PerfRaises) != 0 {
		t.Fatalf("a non-integer counter is a hard change: %+v", v)
	}
	h = wholeRow(t, strings.Replace(newSpec().yaml("r"), "p50_ms: 18.5", `p50_ms: "fast"`, 1))
	v = ParityCheck(perfSnap(b), perfSnap(h), nil)
	if len(v.Weakenings) != 1 {
		t.Fatalf("a non-number budget is a hard change: %+v", v)
	}
}

func TestPerfBlockRule_AddedAndDeletedRows(t *testing.T) {
	a := newSpec().row(t, "a")
	// a new row (any numbers) is free; unchanged rows stay free
	v := ParityCheck(perfSnap(a), perfSnap(a, newSpec().row(t, "b")), nil)
	if len(v.Weakenings)+len(v.PerfFailures)+len(v.PerfRaises) != 0 {
		t.Fatalf("an added row is free: %+v", v)
	}
	// a deleted pass row fails as before
	v = ParityCheck(perfSnap(a, newSpec().row(t, "b")), perfSnap(a), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "pass row deleted: b") {
		t.Fatalf("a deleted pass row fails: %+v", v)
	}
	// a differ row's numbers are not protected (as for every field of a differ row)
	d := newSpec()
	d.expect = "differ"
	d2 := d
	d2.gets += 10
	v = ParityCheck(perfSnap(d.row(t, "d")), perfSnap(d2.row(t, "d")), nil)
	if len(v.Weakenings)+len(v.PerfFailures)+len(v.PerfRaises) != 0 {
		t.Fatalf("a differ row is not a protected row: %+v", v)
	}
}

// One PR, two rows: a listed raise on one and a real regression on the other fails as a whole, and the
// raise of the failing row is not listed.
func TestPerfBlockRule_MixedPR(t *testing.T) {
	okB, badB := newSpec(), newSpec()
	okH, badH := newSpec(), newSpec()
	okH.bytes += 100
	badH.gets++
	badH.bytes += 100
	v := ParityCheck(perfSnap(okB.row(t, "ok"), badB.row(t, "bad")), perfSnap(okH.row(t, "ok"), badH.row(t, "bad")), nil)
	if len(v.PerfFailures) != 1 || !strings.Contains(v.PerfFailures[0], "bad s3_gets") {
		t.Fatalf("the regressed row fails: %+v", v)
	}
	if len(v.PerfRaises) != 1 || v.PerfRaises[0].Row != "ok" || v.PerfRaises[0].Field != "s3_bytes" || v.PerfRaises[0].Old != 1000000 || v.PerfRaises[0].New != 1000100 {
		t.Fatalf("only the passing row's raise is listed: %+v", v.PerfRaises)
	}
	if v.PerfRaises[0].File != "tests/conformance/registry/rows/perf/field_metadata.yaml" {
		t.Errorf("the raise names its rows file: %+v", v.PerfRaises[0])
	}
}

// The whole real perf registry, every s3_bytes raised by 0.5 %: every counter is listed, none fails;
// raised by 2 %: every one fails.
func TestPerfBlockRule_RealRowsFile(t *testing.T) {
	const path = "rows/perf/field_metadata.yaml"
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the perf rows file is part of this test (a moved file must not skip it): %v", err)
	}
	re := regexp.MustCompile(`s3_bytes: (\d+)`)
	scale := func(num, den int64) []byte {
		return re.ReplaceAllFunc(src, func(m []byte) []byte {
			n, _ := strconv.ParseInt(string(re.FindSubmatch(m)[1]), 10, 64)
			return []byte(fmt.Sprintf("s3_bytes: %d", n+n*num/den))
		})
	}
	load := func(b []byte) ParitySnapshot {
		sn := perfSnap()
		if err := ParseRowsLenient(b, sn.Rows); err != nil {
			t.Fatal(err)
		}
		for id, r := range sn.Rows {
			r.File = path
			sn.Rows[id] = r
		}
		return sn
	}
	base := load(src)
	pass := 0
	for _, r := range base.Rows {
		if r.Expect == "pass" {
			pass++
		}
	}
	if pass == 0 {
		t.Fatal("the perf rows file holds no pass rows")
	}
	v := ParityCheck(base, load(scale(1, 200)), nil)
	if len(v.PerfFailures) != 0 || len(v.Weakenings) != 0 || len(v.PerfRaises) == 0 || len(v.PerfRaises) > pass {
		t.Fatalf("+0.5 %% on every row: %d raises, %d perf failures, %d weakenings (pass rows %d)", len(v.PerfRaises), len(v.PerfFailures), len(v.Weakenings), pass)
	}
	v = ParityCheck(base, load(scale(1, 50)), nil)
	if len(v.PerfRaises) != 0 || len(v.PerfFailures) == 0 {
		t.Fatalf("+2 %% on every row must fail: %d raises, %d failures", len(v.PerfRaises), len(v.PerfFailures))
	}
}

func TestPerfRaiseReporting(t *testing.T) {
	r := PerfRaise{Row: "vl.perf.x", File: "tests/conformance/registry/rows/perf/f.yaml", Field: "s3_bytes", Old: 10000, New: 10050}
	if got, want := r.Line(), "perf counter raised: vl.perf.x s3_bytes 10000 -> 10050 (+0.50 %)"; got != want {
		t.Errorf("Line = %q, want %q", got, want)
	}
	if got, want := r.Annotation(), "::warning file=tests/conformance/registry/rows/perf/f.yaml::perf counter raised: vl.perf.x s3_bytes 10000 -> 10050 (+0.50 %25)"; got != want {
		t.Errorf("Annotation = %q, want %q", got, want)
	}
	// an author-controlled row id cannot forge a workflow command or a second line
	evil := PerfRaise{Row: "x\n::error::forged", File: "a,b:c.yaml", Field: "s3_bytes", Old: 100, New: 101}
	a := evil.Annotation()
	if strings.Contains(a, "\n") || !strings.Contains(a, "file=a%2Cb%3Ac.yaml::") || !strings.Contains(a, "x%0A::error::forged") {
		t.Errorf("annotation not escaped: %q", a)
	}
	var many []PerfRaise
	for i := 0; i < 25; i++ {
		many = append(many, PerfRaise{Row: fmt.Sprintf("r%d", i), File: "f.yaml", Field: "s3_bytes", Old: 1000, New: 1000 + int64(i%7)})
	}
	w := PerfRaiseWarnings(many, 10)
	if len(w) != 11 || !strings.Contains(w[10], "and 15 more") {
		t.Errorf("annotations capped at 10 plus one summary: %d %q", len(w), w)
	}
	if len(PerfRaiseWarnings(many[:3], 10)) != 3 {
		t.Error("under the cap every raise is annotated")
	}
	big, ok := LargestPerfRaise(many)
	if !ok || big.Row != "r6" {
		t.Errorf("largest = %+v", big)
	}
	if _, ok := LargestPerfRaise(nil); ok {
		t.Error("no raises, no largest")
	}
	if (PerfRaise{Old: 0, New: 5}).Percent() != 0 {
		t.Error("Percent of a zero base is 0 (such a raise fails before it is listed)")
	}
}

// A relative allowance exists for s3_bytes only: one more request, row group or page is more work however
// large the counter already is (+1 of a million is 0.0001 %).
func TestPerfBlockRule_OnlyBytesHaveAnAllowance(t *testing.T) {
	base := newSpec()
	base.gets, base.rowGroups, base.pages = 1000000, 1000000, 1000000
	head := base
	head.gets++
	head.rowGroups++
	head.pages++
	v := ParityCheck(perfSnap(base.row(t, "r")), perfSnap(head.row(t, "r")), nil)
	if len(v.PerfFailures) != 3 || len(v.PerfRaises) != 0 {
		t.Fatalf("each of gets, row groups and pages fails: %+v", v)
	}
}
