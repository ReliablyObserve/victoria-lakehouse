package parquets3

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"
)

// Issue #689: upstream VictoriaLogs `stats sum()` and `avg()` give different
// answers for the same rows depending on how the rows are split into blocks.
//
// lib/logstorage/block_result.go, (*blockResultColumn).sumValues:
//   - a constant column (every row of the block holds the same value) parses
//     that value with tryParseFloat64: plain decimal digits, minus, one dot and
//     underscores; an integer part must fit uint64 and must not have a leading
//     zero; no exponent, no unit;
//   - a string or dictionary column parses each value with tryParseNumber,
//     which also accepts what tryParseFloat64 rejects: longer digit strings,
//     durations ("5s", "1.5ms"), byte sizes ("1KB") and other numeric forms.
//
// So a value in the class "tryParseNumber accepts it, tryParseFloat64 does
// not" is summed in one block shape and ignored in the other. Hot VictoriaLogs
// and the cold tier cut blocks differently (in-memory parts versus Parquet row
// groups), so the answers can differ for such values. This is upstream
// behaviour, identical in both VictoriaLogs pins (the code was diffed
// function by function), not a projection defect. Owner decision 2026-10-23
// (option A): the pins are not patched, the differential property test excludes
// exactly this class and everything else stays compared, and the difference is
// a documented known-difference row in the registry.
//
// The class is never hand-written here: sumAvgBlockShapeDiffers asks upstream's
// own engine, on hand-built blocks, whether a value is summed differently in a
// constant block and in a mixed block. If upstream changes tryParseNumber or
// tryParseFloat64 the class moves with it, and
// TestSumAvgBlockShape_ExclusionClassIsExact fails and says so.

// sumAvgTail is the final `stats sum(X)` / `stats avg(X)` pipe of a query.
type sumAvgTail struct {
	prefix string // the query without that pipe
	field  string // X as written in the query (may be quoted)
}

var sumAvgTailRe = regexp.MustCompile(`^stats (?:sum|avg)\(("[^"]*"|[A-Za-z_][A-Za-z0-9_.:\-]*)\)(?: as [A-Za-z_][A-Za-z0-9_]*)?$`)

// parseSumAvgTail recognises a query whose last pipe is a single-field sum or
// avg. It deliberately matches nothing else: the exclusion applies to no other
// pipe.
func parseSumAvgTail(query string) (sumAvgTail, bool) {
	i := strings.LastIndex(query, " | ")
	if i < 0 {
		return sumAvgTail{}, false
	}
	m := sumAvgTailRe.FindStringSubmatch(query[i+len(" | "):])
	if m == nil {
		return sumAvgTail{}, false
	}
	return sumAvgTail{prefix: query[:i], field: m[1]}, true
}

func (a sumAvgTail) fieldName() string { return strings.Trim(a.field, `"`) }

// sumAvgInputValues returns the values of the aggregated field exactly as the
// aggregate sees them: the rows of the query up to the aggregate, every column
// read.
func sumAvgInputValues(t testing.TB, s *Storage, f *parquet.File, base time.Time, a sumAvgTail) []string {
	t.Helper()
	rows, ok := runProjectedRows(t, s, f, base, a.prefix+" | fields "+a.field, false)
	if !ok {
		t.Fatalf("aggregate input query %q does not run", a.prefix+" | fields "+a.field)
	}
	var vals []string
	for _, r := range rows {
		if v := r[a.fieldName()]; v != "" {
			vals = append(vals, v)
		}
	}
	return vals
}

// sumAvgBlockShapeSensitive reports whether any value the aggregate reads is in
// the #689 class.
func sumAvgBlockShapeSensitive(t testing.TB, s *Storage, f *parquet.File, base time.Time, a sumAvgTail) bool {
	t.Helper()
	for _, v := range sumAvgInputValues(t, s, f, base, a) {
		if sumAvgBlockShapeDiffers(t, v) {
			return true
		}
	}
	return false
}

// sumAvgInputRowsDiff compares, on both read paths, the rows that feed the
// aggregate and returns a description of the difference ("" when equal). This
// is the part of the property the exclusion does not weaken: the same rows must
// reach sum()/avg() with and without the projection.
func sumAvgInputRowsDiff(t testing.TB, s *Storage, f *parquet.File, base time.Time, a sumAvgTail, breakProjection ...func(map[string]bool)) string {
	t.Helper()
	q := a.prefix + " | fields " + a.field
	want, ok := runProjectedQuery(t, s, f, base, q, false)
	if !ok {
		t.Fatalf("aggregate input query %q does not run with every column", q)
	}
	gotRows, ok := runProjectedRows(t, s, f, base, q, true, breakProjection...)
	if !ok {
		t.Fatalf("aggregate input query %q runs with every column but fails with the projection", q)
	}
	if got := canonRows(gotRows); got != want {
		return fmt.Sprintf("projection changed the rows that feed the aggregate of %q\n  projected:\n%s\n  all columns:\n%s", q, got, want)
	}
	return ""
}

func checkSumAvgInputRowsEqual(t testing.TB, s *Storage, f *parquet.File, base time.Time, a sumAvgTail) {
	t.Helper()
	if d := sumAvgInputRowsDiff(t, s, f, base, a); d != "" {
		t.Fatal(d)
	}
}

// statsOverBlocks runs `stats sum(x)` (or avg) over hand-built blocks, one column
// x, through upstream's pipe machinery, and returns the single answer. Each
// element of blocks is one block, so the caller controls the block shape.
func statsOverBlocks(t testing.TB, fn string, blocks [][]string) string {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp("* | stats "+fn+"(x) as n", time.Now().UnixNano())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	search := func(wb logstorage.WriteDataBlockFunc) error {
		for _, vals := range blocks {
			db := &logstorage.DataBlock{}
			db.SetColumns([]logstorage.BlockColumn{{Name: "x", Values: vals}})
			wb(0, db)
		}
		return nil
	}
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
	var mu sync.Mutex
	var out []map[string]string
	err = logstorage.RunQueryExternal(qctx, search, func(_ uint, db *logstorage.DataBlock) {
		rows := blockRowFields([]*logstorage.DataBlock{db})
		mu.Lock()
		out = append(out, rows...)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("stats %s over %v: want one row, got %v", fn, blocks, out)
	}
	return out[0]["n"]
}

// sumAvgBlockShapeDiffers asks upstream whether sum() treats the value v the
// same in a constant block and in a block that also holds a different,
// non-numeric value. The filler is non-numeric so that it adds nothing to the
// sum or the count on any path; the two blocks therefore differ only in block
// shape (constant column versus string column).
func sumAvgBlockShapeDiffers(t testing.TB, v string) bool {
	t.Helper()
	constant := statsOverBlocks(t, "sum", [][]string{{v, v}})
	mixed := statsOverBlocks(t, "sum", [][]string{{v, v, "filler-689"}})
	return constant != mixed
}

// TestSumAvgBlockShape_UpstreamDependsOnBlockShape pins the upstream behaviour
// itself, on hand-built blocks with no Lakehouse read code. It fails the day
// upstream makes the two paths agree; then the exclusion, the registry rows and
// the docs section are to be removed (#689).
func TestSumAvgBlockShape_UpstreamDependsOnBlockShape(t *testing.T) {
	long1 := "00000000000000000000000000000020" // 32 digits: longer than tryParseFloat64's 27
	long2 := "00000000000000000000000000000022"

	cases := []struct {
		name                string
		fn                  string
		oneBlock, twoBlocks [][]string
		wantOne, wantTwo    string
	}{
		{"sum of long digit strings", "sum",
			[][]string{{long1, long2}}, [][]string{{long1}, {long2}}, "42", "NaN"},
		{"avg of long digit strings", "avg",
			[][]string{{long1, long2}}, [][]string{{long1}, {long2}}, "21", "NaN"},
		{"sum of a duration and a number", "sum",
			[][]string{{"5s", "1"}}, [][]string{{"5s"}, {"1"}}, "5000000001", "1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := statsOverBlocks(t, c.fn, c.oneBlock); got != c.wantOne {
				t.Fatalf("%s over one block %v = %q, want %q", c.fn, c.oneBlock, got, c.wantOne)
			}
			if got := statsOverBlocks(t, c.fn, c.twoBlocks); got != c.wantTwo {
				t.Fatalf("%s over two blocks %v = %q, want %q (upstream no longer depends on block shape: remove the #689 exclusion, rows and docs)", c.fn, c.twoBlocks, got, c.wantTwo)
			}
		})
	}

	// min and max are not affected (the issue's observation), and a plain
	// number is summed the same in every shape.
	if a, b := statsOverBlocks(t, "min", [][]string{{long1, long2}}), statsOverBlocks(t, "min", [][]string{{long1}, {long2}}); a != b {
		t.Fatalf("min depends on block shape too: %q vs %q", a, b)
	}
	if a, b := statsOverBlocks(t, "sum", [][]string{{"20", "22"}}), statsOverBlocks(t, "sum", [][]string{{"20"}, {"22"}}); a != "42" || b != "42" {
		t.Fatalf("a plain number must be summed the same in every block shape: %q vs %q", a, b)
	}
}

// TestSumAvgBlockShape_ExclusionClassIsExact is the guard that the exclusion
// cannot silently widen or narrow. Every row is asserted twice: against the
// documented class (the "in" column, written from reading
// tryParseFloat64, tryParseUint64 and tryParseNumber in both pins) and against upstream's own
// engine through sumAvgBlockShapeDiffers. A value outside the class is compared
// in full by the property test; a value inside it is the only thing the
// exclusion lets through.
func TestSumAvgBlockShape_ExclusionClassIsExact(t *testing.T) {
	cases := []struct {
		value string
		in    bool
		why   string
	}{
		// In the class: tryParseNumber accepts, tryParseFloat64 rejects.
		{"00000000000000000000000000000020", true, "32 digits with leading zeros (the fuzz crasher: a %032x trace id)"},
		{"0000000000000010", true, "leading zeros: tryParseUint64 rejects an integer part starting with 0 (VictoriaMetrics#8361)"},
		{"007", true, "leading zeros, short"},
		{"01.5", true, "leading zero in the integer part of a decimal"},
		{"123456789012345678901234567", true, "27 digits: overflows uint64, so tryParseFloat64 rejects it"},
		{"18446744073709551616", true, "above uint64: tryParseUint64 overflows, ParseFloat takes it"},
		{"5s", true, "duration"},
		{"1.5ms", true, "duration with a fraction"},
		{"1KB", true, "byte size"},
		{"1.5GiB", true, "byte size with a fraction"},
		{"1e3", true, "scientific notation is rejected by tryParseFloat64"},
		{"0x10", true, "hex via ParseInt base 0"},
		{"1.", true, "trailing dot is rejected by tryParseFloat64"},
		// Outside the class: both parse, or neither does.
		{"20", false, "plain integer"},
		{"-20", false, "negative integer"},
		{"1.25", false, "decimal"},
		{"0", false, "a single zero is not a leading zero"},
		{"0.5", false, "zero integer part of a decimal"},
		{"1.007", false, "zeros after the dot are skipped"},
		{"18446744073709551615", false, "the largest uint64"},
		{"1_000", false, "underscores are accepted by tryParseFloat64 (tryParseUint64)"},
		{"abc", false, "not a number on any path"},
		{"host-gamma", false, "not a number on any path"},
		{"0000000000000000000000000000000a", false, "hex-looking id with a letter: octal parse fails, so neither path takes it"},
		{"trace-alpha-1", false, "not a number on any path"},
		{"1.2.3.4", false, "IP address: isLikelyNumber refuses it"},
		{"2026-05-10T14:00:00Z", false, "timestamp: isLikelyNumber refuses it"},
	}
	for _, c := range cases {
		t.Run(c.value, func(t *testing.T) {
			if got := sumAvgBlockShapeDiffers(t, c.value); got != c.in {
				t.Fatalf("value %q: upstream block-shape dependence = %v, documented class says %v (%s). If upstream changed tryParseNumber or tryParseFloat64, update the class, the registry differ_note and docs/parity-and-gaps.md together", c.value, got, c.in, c.why)
			}
		})
	}
}

// TestProjectionEquivalence_SumAvgCrasher689 runs the crasher found by the
// nightly fuzz job (fuzz-logs-18, seed "1\x030ba17"), which is also committed
// under testdata/fuzz/FuzzProjectionEquivalence.
func TestProjectionEquivalence_SumAvgCrasher689(t *testing.T) {
	f, base := projectionFixture(t)
	s := testStorage()
	const query = sumAvgCrasherQuery

	// Without the exclusion the two read paths do disagree: this is what made the
	// fuzz job red, and it is upstream's doing, so it must stay observable.
	want, ok := runProjectedQuery(t, s, f, base, query, false)
	got, ok2 := runProjectedQuery(t, s, f, base, query, true)
	if !ok || !ok2 {
		t.Fatalf("crasher query does not run (all columns %v, projected %v)", ok, ok2)
	}
	if got == want {
		t.Fatalf("the crasher no longer shows the upstream block-shape dependence (projected %q, all columns %q): upstream changed, remove the #689 exclusion", got, want)
	}

	// With it, the aggregate is not compared but the rows feeding it are.
	if out := checkProjectionEquivalence(t, s, f, base, query); out != projSumAvgExcluded {
		t.Fatalf("outcome = %v, want the sum/avg exclusion to fire", out)
	}
	a, ok := parseSumAvgTail(query)
	if !ok || !sumAvgBlockShapeSensitive(t, s, f, base, a) {
		t.Fatalf("the crasher's aggregate input must hold a value of the documented class")
	}
}

const sumAvgCrasherQuery = `NOT "MARKER request 1" (host.name:=host-gamma) | sort by (_time, _msg) | rename trace_id as severity_number | stats sum(severity_number) as n`

// TestSumAvgInputRowsComparison_CatchesBrokenProjection proves the part of the
// property that survives the exclusion is real: with a projection that drops the
// column the aggregate reads (the defect class the property exists for), the
// rows feeding the aggregate differ and are reported, while the intact
// projection reports none.
func TestSumAvgInputRowsComparison_CatchesBrokenProjection(t *testing.T) {
	f, base := projectionFixture(t)
	s := testStorage()
	a, ok := parseSumAvgTail(sumAvgCrasherQuery)
	if !ok {
		t.Fatal("crasher query is not a sum/avg tail")
	}
	if d := sumAvgInputRowsDiff(t, s, f, base, a); d != "" {
		t.Fatalf("intact projection reported a difference: %s", d)
	}
	dropTrace := func(cols map[string]bool) { delete(cols, "trace_id") }
	if d := sumAvgInputRowsDiff(t, s, f, base, a, dropTrace); d == "" {
		t.Fatal("a projection that drops the aggregated column was not detected in the rows that feed the aggregate")
	}
}

// TestProjectionEquivalence_SumAvgStillCompared proves the exclusion is narrow:
// a sum()/avg() over values outside the class is compared in full, also when a
// pipe renamed a string field into the aggregated one.
func TestProjectionEquivalence_SumAvgStillCompared(t *testing.T) {
	f, base := projectionFixture(t)
	s := testStorage()
	for _, q := range []string{
		`* | stats sum(severity_number) as n`,
		`* | stats avg(severity_number) as n`,
		`level:=ERROR | stats by (service.name) sum(severity_number) as n`,
		`* | unpack_json from _msg | rename k as severity_number | stats sum(severity_number) as n`,
		`* | copy repro_layer as severity_number | stats sum(severity_number) as n`, // not numeric on any path: still equal
	} {
		if out := checkProjectionEquivalence(t, s, f, base, q); out != projCompared {
			t.Errorf("%q: outcome = %v, want the whole answer compared", q, out)
		}
	}
}

// TestParseSumAvgTail_OnlySingleFieldSumAvg guards the matcher: the exclusion
// can only ever apply to a final single-field sum() or avg(), never to another
// pipe, another function or a query with several aggregates.
func TestParseSumAvgTail_OnlySingleFieldSumAvg(t *testing.T) {
	yes := map[string]string{
		`* | stats sum(severity_number) as n`:         "severity_number",
		`* | sort by (_time) | stats avg(x)`:          "x",
		`* | stats sum("resource_attr:service.name")`: "resource_attr:service.name",
	}
	for q, field := range yes {
		a, ok := parseSumAvgTail(q)
		if !ok || a.fieldName() != field {
			t.Errorf("%q: parsed (%+v, %v), want field %q", q, a, ok, field)
		}
	}
	no := []string{
		`* | stats count() as n`,
		`* | stats sum(a), sum(b)`,
		`* | stats sum(a) as x, count() as y`,
		`* | stats min(a)`,
		`* | stats max(a) as m`,
		`* | stats sum(a) | fields n`,
		`* | stats sum(a) if (b:=1) as n`,
		`* | stats by (a, b) sum(x)`, // grouping: a projection defect in a by field must not hide here
		`* | stats by (a) avg(x) as n`,
		`* | math a + 1 as m`,
		`* | stats sum(a + b)`,
		`stats sum(a)`,
		`sum(a)`,
	}
	for _, q := range no {
		if a, ok := parseSumAvgTail(q); ok {
			t.Errorf("%q must not be recognised as a sum/avg tail, got %+v", q, a)
		}
	}
}
