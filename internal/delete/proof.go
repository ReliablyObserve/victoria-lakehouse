package delete

import (
	"reflect"
	"strconv"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Per-file proof that a tombstone cannot match: lets the read path keep the
// metadata fast paths for the files a tombstone provably does not touch (see
// parquets3/tombstone_proof.go). The proof never reads S3 and defaults to "may
// match".

// ProofTerm is one exact-value conjunct of a tombstone's filter, resolved to
// the Parquet column that holds the field: the field must equal one of Values
// (never the empty string) for a row to match.
type ProofTerm struct {
	Column string
	Values []string
	Bloom  bool // the column has a bloom filter in pmeta
}

// FileProof is the evidence a tombstone gives about a file without reading it.
type FileProof struct {
	Start, End int64 // the tombstone's time range, inclusive
	Terms      []ProofTerm
}

// Resolve maps a filter's field name to the promoted column holding it, and
// whether that column has a bloom. ok is false for any other field (a map
// attribute has no label set or bloom to prove anything with).
type Resolve func(field string) (column string, bloom, ok bool)

// Bloom asks the bloom of the file at key whether column may hold value: present
// is false only when it proves the value absent, usable is false when it cannot
// tell. One Bloom serves a whole request, so asking costs nothing per file.
type Bloom func(key, column, value string) (present, usable bool)

type proofKey struct {
	id, query      string
	start, end, at int64
}

// Proofs holds the compiled FileProof of each tombstone of one request.
type Proofs map[proofKey]FileProof

// CompileProofs compiles tss, once per request. Only the top-level AND of a
// filter is read, so a term under NOT or OR is never used. A value that is
// empty (rows without the field match it), an `in(<subquery>)`, an exact
// prefix and a wildcard field name give no term.
func CompileProofs(tss []Tombstone, resolve Resolve) Proofs {
	if len(tss) == 0 {
		return nil
	}
	p := make(Proofs, len(tss))
	for i := range tss {
		fp := FileProof{Start: tss[i].StartNs, End: tss[i].EndNs}
		if f := tss[i].Filter(); f != nil {
			fp.Terms = proofTerms(reflect.ValueOf(f).Elem().Field(0), resolve)
		}
		p[keyOf(&tss[i])] = fp
	}
	return p
}

func keyOf(t *Tombstone) proofKey {
	return proofKey{t.ID, t.Query, t.StartNs, t.EndNs, t.FilterAt}
}

// AnyMayMatch reports whether any of tss may match a row of a file with these
// time bounds and label sets (see CannotMatch). A tombstone that was not
// compiled may.
func (p Proofs) AnyMayMatch(tss []Tombstone, key string, minNs, maxNs int64, labels map[string][]string, labelCap int, bloom Bloom) bool {
	for i := range tss {
		fp, ok := p[keyOf(&tss[i])]
		if !ok || !fp.CannotMatch(key, minNs, maxNs, labels, labelCap, bloom) {
			return true
		}
	}
	return false
}

func node(v reflect.Value) (reflect.Value, string) {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return v, ""
	}
	return v, v.Type().Name()
}

func str(v reflect.Value, name string) string {
	if f := v.FieldByName(name); f.IsValid() && f.Kind() == reflect.String {
		return f.String()
	}
	return ""
}

// proofTerms reads the exact-value conjuncts (`field:=v`, `field:in(a,b)`) of
// a filter node.
func proofTerms(v reflect.Value, resolve Resolve) []ProofTerm {
	v, name := node(v)
	switch name {
	case "filterAnd":
		var terms []ProofTerm
		if fs := v.FieldByName("filters"); fs.IsValid() && fs.Kind() == reflect.Slice {
			for i := 0; i < fs.Len(); i++ {
				terms = append(terms, proofTerms(fs.Index(i), resolve)...)
			}
		}
		return terms
	case "filterGeneric":
		if w := v.FieldByName("isWildcard"); !w.IsValid() || w.Bool() {
			return nil
		}
		column, bloom, ok := resolve(str(v, "fieldName"))
		if vals := exactValues(v.FieldByName("f")); ok && len(vals) > 0 {
			return []ProofTerm{{Column: column, Values: vals, Bloom: bloom}}
		}
	}
	return nil
}

// exactValues is the value list of a filterExact or a plain filterIn, or nil
// (also when any value is empty).
func exactValues(f reflect.Value) []string {
	f, name := node(f)
	var vals []string
	switch name {
	case "filterExact":
		vals = []string{str(f, "value")}
	case "filterIn":
		in, _ := node(f.FieldByName("values"))
		if !in.IsValid() || in.Kind() != reflect.Struct {
			return nil
		}
		if q := in.FieldByName("q"); !q.IsValid() || q.Kind() != reflect.Pointer || !q.IsNil() {
			return nil
		}
		if arr := in.FieldByName("values"); arr.IsValid() && arr.Kind() == reflect.Slice {
			for i := 0; i < arr.Len(); i++ {
				vals = append(vals, arr.Index(i).String())
			}
		}
	}
	for _, v := range vals {
		if v == "" {
			return nil
		}
	}
	return vals
}

// CannotMatch reports whether no row of a file can match the tombstone. It is
// true only when the file's own metadata proves it, and any doubt answers
// false, which is the read path before the proof existed:
//   - time: the file's [minNs, maxNs] (both known) misses the tombstone's range;
//   - labels: a complete label set (below labelCap values, the empty value is
//     never listed) holds none of a term's values;
//   - bloom: the column's bloom, from memory only, holds none of them. bloom
//     answers for the file at key with (present, usable) and is nil when there
//     is no bloom facet.
func (p *FileProof) CannotMatch(key string, minNs, maxNs int64, labels map[string][]string, labelCap int, bloom Bloom) bool {
	if p.Start > p.End || (minNs != 0 && maxNs != 0 && (maxNs < p.Start || minNs > p.End)) {
		return true
	}
	for i := range p.Terms {
		t := &p.Terms[i]
		if have := labels[t.Column]; len(have) > 0 && len(have) < labelCap && !anyOf(t.Values, have) {
			return true
		}
		if t.Bloom && bloom != nil && allAbsent(t, key, bloom) {
			return true
		}
	}
	return false
}

func anyOf(values, have []string) bool {
	for _, v := range values {
		for _, h := range have {
			if v == h {
				return true
			}
		}
	}
	return false
}

func allAbsent(t *ProofTerm, key string, bloom Bloom) bool {
	for _, v := range t.Values {
		if present, usable := bloom(key, t.Column, v); present || !usable {
			return false
		}
	}
	return true
}

// KeepRows reports which rows of a block (rows of them, in columns) no
// tombstone matches, and how many. Each tombstone's time range and filter are
// resolved once for the block, one field slice is reused for every row, and
// the time range is checked before the filter, which is the shape of
// upstream's per-row drop-filter. A tombstone matches a row exactly as
// Tombstone.MatchesRow does: the row's time is in range and its query is
// empty or "*", or its filter matches; a query that does not parse matches
// nothing. For a name carried by several columns the last one is the row's
// value.
func KeepRows(columns []logstorage.BlockColumn, rows int, tss []Tombstone) ([]bool, int) {
	type matcher struct {
		lo, hi int64
		all    bool
		f      *logstorage.Filter
	}
	ms := make([]matcher, len(tss))
	for i := range tss {
		ms[i] = matcher{tss[i].StartNs, tss[i].EndNs, tss[i].Query == "" || tss[i].Query == "*", tss[i].Filter()}
	}
	tsCol, last := -1, make(map[string]int, len(columns))
	for i, c := range columns {
		if tsCol < 0 && (c.Name == "_time" || c.Name == "timestamp_unix_nano") {
			tsCol = i
		}
		last[c.Name] = i
	}
	fieldCols := make([]int, 0, len(last))
	for i, c := range columns {
		if last[c.Name] == i {
			fieldCols = append(fieldCols, i)
		}
	}
	row := make([]logstorage.Field, len(fieldCols))
	keep, kept := make([]bool, rows), 0
	for r := 0; r < rows; r++ {
		var ns int64
		if tsCol >= 0 {
			v := columns[tsCol].Values[r]
			if t, ok := logstorage.TryParseTimestampRFC3339Nano(v); ok {
				ns = t
			} else {
				ns, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		filled, matched := false, false
		for i := range ms {
			m := &ms[i]
			if ns < m.lo || ns > m.hi {
				continue
			}
			if m.all {
				matched = true
				break
			}
			if m.f == nil {
				continue
			}
			if !filled {
				for j, c := range fieldCols {
					row[j] = logstorage.Field{Name: columns[c].Name, Value: columns[c].Values[r]}
				}
				filled = true
			}
			if m.f.MatchRow(row) {
				matched = true
				break
			}
		}
		if !matched {
			keep[r] = true
			kept++
		}
	}
	return keep, kept
}
