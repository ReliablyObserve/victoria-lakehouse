package delete

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// referenceKeep is the per-row implementation the block matcher replaced.
func referenceKeep(columns []logstorage.BlockColumn, rows int, tss []Tombstone) []bool {
	tsIdx := -1
	for i, c := range columns {
		if c.Name == "_time" || c.Name == "timestamp_unix_nano" {
			tsIdx = i
			break
		}
	}
	keep := make([]bool, rows)
	for r := 0; r < rows; r++ {
		row := make(map[string]string, len(columns))
		for _, c := range columns {
			row[c.Name] = c.Values[r]
		}
		var ns int64
		if tsIdx >= 0 {
			v := columns[tsIdx].Values[r]
			if t, ok := logstorage.TryParseTimestampRFC3339Nano(v); ok {
				ns = t
			} else {
				ns, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		keep[r] = true
		for i := range tss {
			if tss[i].MatchesRow(row, ns) {
				keep[r] = false
				break
			}
		}
	}
	return keep
}

// TestKeepRows_EqualsPerRowReference: the per-block matcher keeps exactly the
// rows the per-row MatchesRow loop kept, over random blocks with duplicate
// column names, a missing or unparsable timestamp, empty and "*" and
// unparsable queries, and range boundaries.
func TestKeepRows_EqualsPerRowReference(t *testing.T) {
	rng := rand.New(rand.NewSource(612))
	names := []string{"a", "b", "level", "_time", "timestamp_unix_nano", "service.name", "a"} // "a" twice: a duplicate name
	vals := []string{"", "x", "y", "ERROR", "alpha", "1500", "2026-06-01T10:00:00.000000001Z", "2026-06-01T10:00:00Z", "junk"}
	queries := []string{"", "*", `a:="x"`, `a:in("x","y")`, `b:~"y.*"`, `NOT a:="x"`, `a:="x" OR b:="y"`, `level:="ERROR" a:="x"`, `a:=`, `service.name:="alpha"`, `a:=""`, `_time:>1`}
	boundary := []int64{-1 << 63, 0, 999, 1000, 1499, 1500, 1501, 1<<63 - 1,
		time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC).UnixNano(), time.Date(2026, 6, 1, 10, 0, 0, 1, time.UTC).UnixNano()}
	for iter := 0; iter < 400; iter++ {
		nCols, rows := 1+rng.Intn(len(names)), 1+rng.Intn(8)
		cols := make([]logstorage.BlockColumn, nCols)
		for c := range cols {
			cols[c].Name = names[rng.Intn(len(names))]
			cols[c].Values = make([]string, rows)
			for r := range cols[c].Values {
				cols[c].Values[r] = vals[rng.Intn(len(vals))]
			}
		}
		tss := make([]Tombstone, 1+rng.Intn(4))
		for i := range tss {
			lo, hi := boundary[rng.Intn(len(boundary))], boundary[rng.Intn(len(boundary))]
			tss[i] = Tombstone{ID: strconv.Itoa(i), Query: queries[rng.Intn(len(queries))], StartNs: lo, EndNs: hi}
		}
		want := referenceKeep(cols, rows, tss)
		got, kept := KeepRows(cols, rows, tss)
		n := 0
		for r := range want {
			if want[r] {
				n++
			}
			if got[r] != want[r] {
				t.Fatalf("iter %d row %d: keep=%v want %v\ncols=%v\ntombstones=%+v", iter, r, got[r], want[r], cols, tss)
			}
		}
		if kept != n {
			t.Fatalf("iter %d: kept=%d, want %d", iter, kept, n)
		}
	}
}

// TestCompileProofs_Terms pins what the strict extractor reads from a filter:
// only exact and plain in() conjuncts of the top-level AND on a resolved field.
// Everything else gives no term, so the proof falls back to the time range.
func TestCompileProofs_Terms(t *testing.T) {
	resolve := func(field string) (string, bool, bool) {
		switch field {
		case "svc":
			return "service.name", true, true
		case "level":
			return "severity_text", false, true
		}
		return "", false, false
	}
	type term = ProofTerm
	cases := []struct {
		query string
		want  []term
	}{
		{`svc:="a"`, []term{{"service.name", []string{"a"}, true}}},
		{`svc:in("a","b")`, []term{{"service.name", []string{"a", "b"}, true}}},
		{`svc:="a" level:="E"`, []term{{"service.name", []string{"a"}, true}, {"severity_text", []string{"E"}, false}}},
		{`svc:="a" (level:="E" other:="x")`, []term{{"service.name", []string{"a"}, true}, {"severity_text", []string{"E"}, false}}},
		{`svc:="a" _time:5m`, []term{{"service.name", []string{"a"}, true}}},
		// no term
		{`other:="a"`, nil},
		{`svc:=""`, nil},
		{`svc:in("","b")`, nil},
		{`svc:in()`, nil},
		{`svc:="a"*`, nil},
		{`svc:~"a.*"`, nil},
		{`svc:a`, nil},
		{`svc:"a b"`, nil},
		{`NOT svc:="a"`, nil},
		{`-svc:="a"`, nil},
		{`svc:="a" OR svc:="b"`, nil},
		{`(svc:="a" OR level:="E") level:="E"`, []term{{"severity_text", []string{"E"}, false}}},
		{`svc*:="a"`, nil},
		{`svc:in(error | fields svc)`, nil},
		{`_stream:{svc="a"}`, nil},
		{`*`, nil},
	}
	for _, c := range cases {
		ps := CompileProofs([]Tombstone{{ID: "t", Query: c.query}}, resolve)
		var got []ProofTerm
		for _, fp := range ps {
			got = fp.Terms
		}
		if len(got) != len(c.want) {
			t.Errorf("%s: terms %+v, want %+v", c.query, got, c.want)
			continue
		}
		for i := range got {
			if got[i].Column != c.want[i].Column || got[i].Bloom != c.want[i].Bloom || len(got[i].Values) != len(c.want[i].Values) {
				t.Errorf("%s: term %d = %+v, want %+v", c.query, i, got[i], c.want[i])
			}
		}
	}
}

// TestFileProof_CannotMatch pins the proof itself, independent of any registry
// or storage: a "cannot" answer needs complete evidence, every doubt is "may".
func TestFileProof_CannotMatch(t *testing.T) {
	const cap = 100
	labels := map[string][]string{"service.name": {"alpha", "beta"}}
	full := make([]string, cap)
	for i := range full {
		full[i] = "s" + strconv.Itoa(i)
	}
	absent := func(string, string) (bool, bool) { return false, true }
	present := func(string, string) (bool, bool) { return true, true }
	unusable := func(string, string) (bool, bool) { return false, false }
	byValue := func(held ...string) func(string, string) (bool, bool) {
		return func(_, v string) (bool, bool) {
			for _, h := range held {
				if v == h {
					return true, true
				}
			}
			return false, true
		}
	}
	term := func(bloom bool, values ...string) []ProofTerm {
		return []ProofTerm{{Column: "service.name", Values: values, Bloom: bloom}}
	}
	cases := []struct {
		name        string
		p           FileProof
		min, max    int64
		labels      map[string][]string
		bloom       func(string, string) (bool, bool)
		cannotMatch bool
	}{
		{"no terms, overlapping range", FileProof{Start: 0, End: 100}, 10, 20, labels, nil, false},
		{"range ends before the file", FileProof{Start: 0, End: 9}, 10, 20, labels, nil, true},
		{"range starts after the file", FileProof{Start: 21, End: 30}, 10, 20, labels, nil, true},
		{"range touches the first row", FileProof{Start: 0, End: 10}, 10, 20, labels, nil, false},
		{"range touches the last row", FileProof{Start: 20, End: 30}, 10, 20, labels, nil, false},
		{"empty range", FileProof{Start: 5, End: 4}, 0, 0, nil, nil, true},
		{"unknown bounds", FileProof{Start: 0, End: 9}, 0, 0, labels, nil, false},
		{"label set lacks every value", FileProof{Start: 0, End: 100, Terms: term(false, "x", "y")}, 10, 20, labels, nil, true},
		{"label set holds one value", FileProof{Start: 0, End: 100, Terms: term(false, "x", "beta")}, 10, 20, labels, nil, false},
		{"label set missing", FileProof{Start: 0, End: 100, Terms: term(false, "x")}, 10, 20, nil, nil, false},
		{"label set at the cap", FileProof{Start: 0, End: 100, Terms: term(false, "x")}, 10, 20, map[string][]string{"service.name": full}, nil, false},
		{"bloom: every value absent", FileProof{Start: 0, End: 100, Terms: term(true, "x", "y")}, 10, 20, nil, absent, true},
		{"bloom: one value present", FileProof{Start: 0, End: 100, Terms: term(true, "x", "y")}, 10, 20, nil, byValue("y"), false},
		{"bloom: present", FileProof{Start: 0, End: 100, Terms: term(true, "x")}, 10, 20, nil, present, false},
		{"bloom: cannot answer", FileProof{Start: 0, End: 100, Terms: term(true, "x")}, 10, 20, nil, unusable, false},
		{"bloom: no facet", FileProof{Start: 0, End: 100, Terms: term(true, "x")}, 10, 20, nil, nil, false},
		{"bloom: column without one", FileProof{Start: 0, End: 100, Terms: term(false, "x")}, 10, 20, nil, absent, false},
	}
	for _, c := range cases {
		if got := c.p.CannotMatch(c.min, c.max, c.labels, cap, c.bloom); got != c.cannotMatch {
			t.Errorf("%s: CannotMatch = %v, want %v", c.name, got, c.cannotMatch)
		}
	}

	// AnyMayMatch: one tombstone that may match decides; an uncompiled one may.
	resolve := func(f string) (string, bool, bool) { return f, false, f == "service.name" }
	gone := Tombstone{ID: "a", Query: `service.name:="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	beta := Tombstone{ID: "b", Query: `service.name:="beta"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	ps := CompileProofs([]Tombstone{gone, beta}, resolve)
	if ps.AnyMayMatch([]Tombstone{gone}, 10, 20, labels, cap, nil) {
		t.Error("a value the file does not hold should prove clean")
	}
	if !ps.AnyMayMatch([]Tombstone{gone, beta}, 10, 20, labels, cap, nil) {
		t.Error("one tombstone on a held value must decide")
	}
	if !ps.AnyMayMatch([]Tombstone{{ID: "c", Query: `service.name:="gone"`}}, 10, 20, labels, cap, nil) {
		t.Error("a tombstone that was not compiled must count as may-match")
	}
	if Proofs(nil).AnyMayMatch(nil, 10, 20, labels, cap, nil) {
		t.Error("no tombstone: nothing may match")
	}
}
