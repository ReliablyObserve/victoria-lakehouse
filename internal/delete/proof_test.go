package delete

import (
	"fmt"
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
	absent := func(string, string, string) (bool, bool) { return false, true }
	present := func(string, string, string) (bool, bool) { return true, true }
	unusable := func(string, string, string) (bool, bool) { return false, false }
	byValue := func(held ...string) func(string, string, string) (bool, bool) {
		return func(_, _, v string) (bool, bool) {
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
		bloom       func(string, string, string) (bool, bool)
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
		if got := c.p.CannotMatch("k", c.min, c.max, c.labels, cap, c.bloom); got != c.cannotMatch {
			t.Errorf("%s: CannotMatch = %v, want %v", c.name, got, c.cannotMatch)
		}
	}

	// AnyMayMatch: one tombstone that may match decides; an uncompiled one may.
	resolve := func(f string) (string, bool, bool) { return f, false, f == "service.name" }
	gone := Tombstone{ID: "a", Query: `service.name:="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	beta := Tombstone{ID: "b", Query: `service.name:="beta"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	ps := CompileProofs([]Tombstone{gone, beta}, resolve)
	if ps.AnyMayMatch([]Tombstone{gone}, "k", 10, 20, labels, cap, nil) {
		t.Error("a value the file does not hold should prove clean")
	}
	if !ps.AnyMayMatch([]Tombstone{gone, beta}, "k", 10, 20, labels, cap, nil) {
		t.Error("one tombstone on a held value must decide")
	}
	if !ps.AnyMayMatch([]Tombstone{{ID: "c", Query: `service.name:="gone"`}}, "k", 10, 20, labels, cap, nil) {
		t.Error("a tombstone that was not compiled must count as may-match")
	}
	if Proofs(nil).AnyMayMatch(nil, "k", 10, 20, labels, cap, nil) {
		t.Error("no tombstone: nothing may match")
	}
}

// TestCompileProofs_TermsAreImpliedByAMatch is the soundness property of the
// extractor: over random filters (AND, OR, NOT, nested, exact, in(), empty
// value, prefix, regex) and random rows, whenever the filter matches a row,
// every term it yields holds for that row (the field equals one of the term's
// values). A term that did not follow from a match would let the proof clear a
// file that holds a matching row.
func TestCompileProofs_TermsAreImpliedByAMatch(t *testing.T) {
	rng := rand.New(rand.NewSource(612))
	vals := []string{"", "v", "w", "x", "vv"}
	fields := []string{"a", "b", "c"}
	resolve := func(f string) (string, bool, bool) { return f, false, f != "c" } // c is not a registered column
	var gen func(depth int) string
	gen = func(depth int) string {
		f, v := fields[rng.Intn(len(fields))], vals[rng.Intn(len(vals))]
		if depth == 0 || rng.Intn(3) == 0 {
			switch rng.Intn(6) {
			case 0:
				return fmt.Sprintf(`%s:=%q`, f, v)
			case 1:
				return fmt.Sprintf(`%s:in(%q,%q)`, f, v, vals[rng.Intn(len(vals))])
			case 2:
				return fmt.Sprintf(`%s:=%q*`, f, v)
			case 3:
				return fmt.Sprintf(`%s:~%q`, f, v+".*")
			case 4:
				return fmt.Sprintf(`%s:%q`, f, v)
			}
			return fmt.Sprintf(`%s:=%q`, f, vals[rng.Intn(len(vals))])
		}
		a, b := gen(depth-1), gen(depth-1)
		switch rng.Intn(4) {
		case 0:
			return "(" + a + " OR " + b + ")"
		case 1:
			return "NOT (" + a + ")"
		case 2:
			return "(" + a + " AND " + b + ")"
		}
		return a + " " + b
	}
	matches := 0
	for iter := 0; iter < 600; iter++ {
		q := gen(3)
		ts := Tombstone{ID: "t", Query: q, StartNs: -1 << 63, EndNs: 1<<63 - 1}
		f := ts.Filter()
		if f == nil {
			continue
		}
		var terms []ProofTerm
		for _, fp := range CompileProofs([]Tombstone{ts}, resolve) {
			terms = fp.Terms
		}
		for r := 0; r < 20; r++ {
			row := map[string]string{}
			var fs []logstorage.Field
			for _, fld := range fields {
				row[fld] = vals[rng.Intn(len(vals))]
				fs = append(fs, logstorage.Field{Name: fld, Value: row[fld]})
			}
			if !f.MatchRow(fs) {
				continue
			}
			matches++
			for _, term := range terms {
				ok := false
				for _, v := range term.Values {
					ok = ok || row[term.Column] == v
				}
				if !ok {
					t.Fatalf("filter %s matches %v but the term %+v does not hold", q, row, term)
				}
			}
		}
	}
	if matches == 0 {
		t.Fatal("the generator never produced a matching row")
	}
}
