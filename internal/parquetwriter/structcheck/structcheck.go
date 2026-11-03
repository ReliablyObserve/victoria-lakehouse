// Package structcheck decodes the footer of a Lakehouse Parquet object into a
// comparable description of its pruning structures: the footer key-value
// entries, the split-block bloom columns, the column statistics, the page index
// and the sort order. Two objects written from the same rows have the same
// Structure whichever path wrote them; Diff names every difference. The golden
// tests of "flushed versus compacted" (MAIN RULE 4, #344) compare Structures
// rather than bytes, so they say WHICH structure a writer left out.
package structcheck

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// Structure is what a footer says about an object.
type Structure struct {
	CreatedBy string
	NumRows   int64
	// KeyValues maps each footer key-value name to a short digest of its value:
	// equal rows give equal digests, and a value never has to be printed.
	KeyValues map[string]string
	RowGroups []RowGroup
}

// RowGroup is one row group's structure.
type RowGroup struct {
	NumRows int64
	// Sorting lists the declared sort order ("col" or "col desc"), in order.
	Sorting []string
	Columns []Column
}

// Column is one column chunk's structure.
type Column struct {
	Path      string
	Type      string
	Codec     string
	Encodings []string
	// Statistics: the digest of min and max, and the null count.
	MinMax    string
	NullCount int64
	// BloomBytes is the size of the column's split-block bloom filter; 0 = none.
	BloomBytes int32
	// ColumnIndex and OffsetIndex are the page index; false = absent.
	ColumnIndex bool
	OffsetIndex bool
}

// Describe decodes the footer of data.
func Describe(data []byte) (*Structure, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet footer: %w", err)
	}
	md := f.Metadata()
	if md == nil {
		return nil, fmt.Errorf("parquet footer has no metadata")
	}
	s := &Structure{CreatedBy: md.CreatedBy, NumRows: md.NumRows, KeyValues: map[string]string{}}
	for _, kv := range md.KeyValueMetadata {
		s.KeyValues[kv.Key] = digest([]byte(kv.Value))
	}
	for i := range md.RowGroups {
		rg := &md.RowGroups[i]
		out := RowGroup{NumRows: rg.NumRows}
		for _, sc := range rg.SortingColumns {
			name := fmt.Sprintf("#%d", sc.ColumnIdx)
			if int(sc.ColumnIdx) < len(rg.Columns) {
				name = strings.Join(rg.Columns[sc.ColumnIdx].MetaData.PathInSchema, ".")
			}
			if sc.Descending {
				name += " desc"
			}
			out.Sorting = append(out.Sorting, name)
		}
		for j := range rg.Columns {
			out.Columns = append(out.Columns, describeColumn(&rg.Columns[j]))
		}
		s.RowGroups = append(s.RowGroups, out)
	}
	return s, nil
}

func describeColumn(cc *format.ColumnChunk) Column {
	cm := &cc.MetaData
	enc := make([]string, 0, len(cm.Encoding))
	for _, e := range cm.Encoding {
		enc = append(enc, fmt.Sprint(e))
	}
	sort.Strings(enc)
	st := &cm.Statistics
	minMax := ""
	if len(st.MinValue) > 0 || len(st.MaxValue) > 0 || len(st.Min) > 0 || len(st.Max) > 0 {
		minMax = digest(st.MinValue, st.MaxValue, st.Min, st.Max)
	}
	return Column{
		Path:        strings.Join(cm.PathInSchema, "."),
		Type:        cm.Type.String(),
		Codec:       fmt.Sprint(cm.Codec),
		Encodings:   enc,
		MinMax:      minMax,
		NullCount:   st.NullCount,
		BloomBytes:  cm.BloomFilterLength,
		ColumnIndex: cc.ColumnIndexLength > 0,
		OffsetIndex: cc.OffsetIndexLength > 0,
	}
}

func digest(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		var n [8]byte
		l := len(p)
		for i := 0; i < 8; i++ {
			n[i] = byte(l >> (8 * i))
		}
		h.Write(n[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// KeyNames returns the footer key-value names, sorted.
func (s *Structure) KeyNames() []string {
	names := make([]string, 0, len(s.KeyValues))
	for k := range s.KeyValues {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// BloomColumns returns, sorted, the columns that carry a split-block bloom in
// every row group; a column with a bloom in only some row groups is listed with
// a "(partial)" suffix.
func (s *Structure) BloomColumns() []string {
	count := map[string]int{}
	for _, rg := range s.RowGroups {
		for _, c := range rg.Columns {
			if c.BloomBytes > 0 {
				count[c.Path]++
			}
		}
	}
	var out []string
	for p, n := range count {
		if n == len(s.RowGroups) {
			out = append(out, p)
		} else {
			out = append(out, p+" (partial)")
		}
	}
	sort.Strings(out)
	return out
}

// Diff returns one line per difference between want and got, empty when the two
// structures are equal. Each line names what differs (the footer key, the row
// group and column), so a failing golden says which structure a writer dropped.
func Diff(want, got *Structure) []string {
	var d []string
	if want.CreatedBy != got.CreatedBy {
		d = append(d, fmt.Sprintf("created_by: %q -> %q", want.CreatedBy, got.CreatedBy))
	}
	if want.NumRows != got.NumRows {
		d = append(d, fmt.Sprintf("num_rows: %d -> %d", want.NumRows, got.NumRows))
	}
	for _, k := range union(want.KeyValues, got.KeyValues) {
		w, wok := want.KeyValues[k]
		g, gok := got.KeyValues[k]
		switch {
		case wok && !gok:
			d = append(d, fmt.Sprintf("footer key %q: present -> MISSING", k))
		case !wok && gok:
			d = append(d, fmt.Sprintf("footer key %q: absent -> present", k))
		case w != g:
			d = append(d, fmt.Sprintf("footer key %q: value differs (%s -> %s)", k, w, g))
		}
	}
	if len(want.RowGroups) != len(got.RowGroups) {
		d = append(d, fmt.Sprintf("row groups: %d -> %d", len(want.RowGroups), len(got.RowGroups)))
		return d
	}
	for i := range want.RowGroups {
		w, g := &want.RowGroups[i], &got.RowGroups[i]
		at := fmt.Sprintf("row group %d", i)
		if w.NumRows != g.NumRows {
			d = append(d, fmt.Sprintf("%s: rows %d -> %d", at, w.NumRows, g.NumRows))
		}
		if strings.Join(w.Sorting, ",") != strings.Join(g.Sorting, ",") {
			d = append(d, fmt.Sprintf("%s: sorting columns %v -> %v", at, w.Sorting, g.Sorting))
		}
		if len(w.Columns) != len(g.Columns) {
			d = append(d, fmt.Sprintf("%s: columns %d -> %d", at, len(w.Columns), len(g.Columns)))
			continue
		}
		for j := range w.Columns {
			d = append(d, diffColumn(at, &w.Columns[j], &g.Columns[j])...)
		}
	}
	return d
}

func diffColumn(at string, w, g *Column) []string {
	var d []string
	c := fmt.Sprintf("%s column %q", at, w.Path)
	if w.Path != g.Path {
		return []string{fmt.Sprintf("%s: path %q -> %q", at, w.Path, g.Path)}
	}
	if w.Type != g.Type || w.Codec != g.Codec {
		d = append(d, fmt.Sprintf("%s: type/codec %s/%s -> %s/%s", c, w.Type, w.Codec, g.Type, g.Codec))
	}
	if strings.Join(w.Encodings, ",") != strings.Join(g.Encodings, ",") {
		d = append(d, fmt.Sprintf("%s: encodings %v -> %v", c, w.Encodings, g.Encodings))
	}
	if w.MinMax != g.MinMax || w.NullCount != g.NullCount {
		d = append(d, fmt.Sprintf("%s: statistics %q/%d nulls -> %q/%d nulls", c, w.MinMax, w.NullCount, g.MinMax, g.NullCount))
	}
	if w.BloomBytes != g.BloomBytes {
		d = append(d, fmt.Sprintf("%s: split-block bloom %d bytes -> %d bytes", c, w.BloomBytes, g.BloomBytes))
	}
	if w.ColumnIndex != g.ColumnIndex || w.OffsetIndex != g.OffsetIndex {
		d = append(d, fmt.Sprintf("%s: page index column=%v offset=%v -> column=%v offset=%v", c, w.ColumnIndex, w.OffsetIndex, g.ColumnIndex, g.OffsetIndex))
	}
	return d
}

func union(a, b map[string]string) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
