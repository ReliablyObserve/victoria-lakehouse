package tracesidx

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestKeyHelpers(t *testing.T) {
	for key, want := range map[string]bool{
		"0/0/traces_idx/dt=2026-10-13/hour=10/a.parquet": true,
		"traces_idx/dt=2026-10-13/hour=10/a.parquet":     true,
		"0/0/traces/dt=2026-10-13/hour=10/a.parquet":     false,
		"0/0/logs/dt=2026-10-13/hour=10/a.parquet":       false,
		"0/0/traces_idx_x/a.parquet":                     false,
	} {
		if got := IsKey(key); got != want {
			t.Errorf("IsKey(%q) = %v, want %v", key, got, want)
		}
	}
	for in, want := range map[string]string{
		"0/0/traces/":   "0/0/traces_idx/",
		"traces/":       "traces_idx/",
		"static":        "static/traces_idx/",
		"static/prefix": "static/prefix/traces_idx/",
		"":              "traces_idx/",
	} {
		if got := PrefixFor(in); got != want {
			t.Errorf("PrefixFor(%q) = %q, want %q", in, got, want)
		}
	}
	if StreamOf("a7afa54ca4b8ac6ad227620053ce32f8") != 206 {
		t.Error("StreamOf differs from the stream hot VictoriaTraces gave this trace (206, measured)")
	}
	if FilterBytes(0) != 32 || FilterBytes(1000) != 2048 || FilterBytes(250_000) != 524288 {
		t.Errorf("FilterBytes: %d %d %d", FilterBytes(0), FilterBytes(1000), FilterBytes(250_000))
	}
}

func TestMarshalLevelsAndEmpty(t *testing.T) {
	rows := randRows(rand.New(rand.NewSource(1)), 500)
	var sizes []int
	for _, lvl := range []int{0, 1, 3, 7, 15} {
		data, _, err := Marshal(append([]Row(nil), rows...), lvl)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadAll(data)
		if err != nil || len(got) != len(rows) {
			t.Fatalf("level %d: %d rows, %v", lvl, len(got), err)
		}
		sizes = append(sizes, len(data))
	}
	if sizes[0] != sizes[1] || sizes[2] != sizes[3] {
		t.Logf("sizes by level: %v", sizes) // 0 and 1 are the fastest codec, 3 and 7 the default
	}
	data, info, err := Marshal(nil, 3) // the compaction of an object whose rows were all deleted
	if err != nil || info.Rows != 0 || info.Groups != 0 {
		t.Fatalf("empty object: %+v %v", info, err)
	}
	if got, err := ReadAll(data); err != nil || len(got) != 0 {
		t.Fatalf("empty object read: %v %v", got, err)
	}
	if got, _, err := NewLocator(1).Lookup(context.Background(), newCountSource("e", data), "x", WantAll); err != nil || len(got) != 0 {
		t.Fatalf("lookup in an empty object: %v %v", got, err)
	}
	if _, err := ReadAll([]byte("not parquet")); err == nil {
		t.Fatal("ReadAll accepted garbage")
	}
	// ties sort deterministically
	tie := []Row{{TraceID: "a", Time: 2, Start: 1, End: 1}, {TraceID: "a", Time: 1, Start: 2, End: 1}, {TraceID: "a", Time: 1, Start: 1, End: 2}, {TraceID: "a", Time: 1, Start: 1, End: 1, Stream: 2}, {TraceID: "a", Time: 1, Start: 1, End: 1}}
	Sort(tie)
	if tie[0].Time != 1 || tie[0].Stream != 0 || tie[len(tie)-1].Time != 2 {
		t.Fatalf("tie order: %+v", tie)
	}
}

// ValidateThrift accepts what the library writes (the fuzz seeds are real index slices)
// and rejects every malformation by name.
func TestValidateThriftCases(t *testing.T) {
	ok := [][]byte{
		{0x00},                               // empty struct
		{0x11, 0x00},                         // field 1 bool true
		{0x12, 0x00},                         // field 1 bool false
		{0x13, 0x07, 0x00},                   // byte
		{0x14, 0x02, 0x00},                   // i16
		{0x15, 0x96, 0x01, 0x00},             // i32 varint
		{0x16, 0x96, 0x01, 0x00},             // i64
		{0x17, 1, 2, 3, 4, 5, 6, 7, 8, 0x00}, // double
		{0x18, 0x03, 'a', 'b', 'c', 0x00},    // binary
		{0x19, 0x25, 1, 2, 0x00},             // list<i32>, 2 elements
		{0x19, 0x22, 1, 2, 0x00},             // list<bool>, 2 elements
		{0x1a, 0x15, 1, 0x00},                // set<i32>
		{0x1b, 0x00, 0x00},                   // empty map
		{0x1b, 0x01, 0x55, 1, 2, 0x00},       // map<i32,i32>, one entry
		{0x1c, 0x11, 0x00, 0x00},             // nested struct
		{0x19, 0xf5, 0x02, 1, 2, 0x00},       // list with the long-form size
		{0x05, 0x04, 0x02, 0x00},             // long-form field id (delta 0, zigzag id), i32
	}
	for i, b := range ok {
		if err := ValidateThrift(b); err != nil {
			t.Errorf("valid case %d rejected: %v", i, err)
		}
	}
	bad := map[string][]byte{
		"truncated":        {0x15},
		"no stop":          {0x11},
		"trailing":         {0x00, 0x00},
		"unknown type":     {0x1d, 0x00},
		"binary lies":      {0x18, 0x40, 'a', 0x00},
		"list lies":        {0x19, 0xf5, 0xff, 0xff, 0xff, 0x0f, 0x00},
		"map lies":         {0x1b, 0x7f, 0x55, 0x00},
		"varint too long":  append(append([]byte{0x15}, bytes.Repeat([]byte{0xff}, 11)...), 0x00),
		"nesting too deep": append(bytes.Repeat([]byte{0x1c}, 20), 0x00),
		"double short":     {0x17, 1, 2},
		"empty":            {},
	}
	for name, b := range bad {
		if err := ValidateThrift(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

type failSource struct {
	*countSource
	failKind ReadKind
	after    int
	short    bool
	n        int
}

func (f *failSource) ReadRange(ctx context.Context, k ReadKind, off, n int64) ([]byte, error) {
	if k == f.failKind {
		f.n++
		if f.n > f.after {
			if f.short {
				b, err := f.countSource.ReadRange(ctx, k, off, n)
				return b[:len(b)/2], err
			}
			return nil, errors.New("injected")
		}
	}
	return f.countSource.ReadRange(ctx, k, off, n)
}

func TestLocatorErrorPaths(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	rows := randRows(rng, 3000)
	data, _, _ := Marshal(append([]Row(nil), rows...), 3)
	id := rows[100].TraceID
	for _, kind := range []ReadKind{KindFooter, KindIndex, KindPage} {
		for _, short := range []bool{false, true} {
			src := &failSource{countSource: newCountSource("f", data), failKind: kind, short: short}
			if _, _, err := NewLocator(1).Lookup(context.Background(), src, id, WantAll); err == nil {
				t.Errorf("a failing %s read (short=%v) did not fail the lookup", kind, short)
			}
		}
	}
	// objects that are not idx tables
	var plain bytes.Buffer
	w := parquet.NewGenericWriter[Row](&plain) // right columns, no format entry
	_, _ = w.Write(rows)
	_ = w.Close()
	var other bytes.Buffer
	w2 := parquet.NewGenericWriter[struct{ A int64 }](&other)
	_, _ = w2.Write([]struct{ A int64 }{{1}, {2}})
	_ = w2.Close()
	bad := map[string][]byte{
		"garbage":         bytes.Repeat([]byte("x"), 200),
		"tiny":            []byte("PAR1"),
		"no format entry": plain.Bytes(),
		"other table":     other.Bytes(),
		"truncated":       data[:len(data)/2],
		"bad footer len":  append(append([]byte(nil), data[:len(data)-8]...), 0xff, 0xff, 0xff, 0x7f, 'P', 'A', 'R', '1'),
		"zero footer len": append(append([]byte(nil), data[:len(data)-8]...), 0, 0, 0, 0, 'P', 'A', 'R', '1'),
		"wrong trailer":   append(append([]byte(nil), data[:len(data)-4]...), 'X', 'X', 'X', 'X'),
	}
	for name, b := range bad {
		_, _, err := NewLocator(1).Lookup(context.Background(), newCountSource(name, b), id, WantAll)
		if err == nil {
			t.Errorf("%s: lookup accepted the object", name)
		} else if !errors.Is(err, ErrNotIdxObject) && name != "truncated" {
			t.Logf("%s: %v", name, err)
		}
	}
	if _, _, err := NewLocator(1).Fences(context.Background(), newCountSource("g", bad["garbage"])); err == nil {
		t.Error("Fences accepted garbage")
	}
	lo, hi, err := NewLocator(1).Fences(context.Background(), newCountSource("ok", data))
	if err != nil || lo > hi || lo == "" {
		t.Fatalf("fences %q %q %v", lo, hi, err)
	}
	// Forget and the LRU
	loc := NewLocator(2)
	for _, k := range []string{"a", "b", "c"} {
		if _, _, err := loc.Lookup(context.Background(), newCountSource(k, data), id, WantAll); err != nil {
			t.Fatal(err)
		}
	}
	if loc.cached("a", int64(len(data))) != nil {
		t.Error("the oldest entry survived the bound")
	}
	loc.Forget("c")
	if loc.cached("c", int64(len(data))) != nil {
		t.Error("Forget kept the entry")
	}
	// an object whose size changed under the same key is not served from the cache
	if loc.cached("b", int64(len(data))+1) != nil {
		t.Error("a cached footer was used for an object of another size")
	}
}

// An object written by another writer (the library's own filter, whose size is not a power
// of two) has filters the cache does not trust; the lookups are still exact.
func TestLocatorIgnoresForeignFilters(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	rows := randRows(rng, 3000)
	Sort(rows)
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf, writerOptions(3)...)
	_, _ = w.Write(rows)
	_ = w.Close()
	// stock filter: re-encode with the library's own SplitBlockFilter(10)
	var buf2 bytes.Buffer
	opts := append(writerOptions(3), parquet.BloomFilters(parquet.SplitBlockFilter(10, IDField)))
	w2 := parquet.NewGenericWriter[Row](&buf2, opts...)
	_, _ = w2.Write(rows)
	_ = w2.Close()
	for name, data := range map[string][]byte{"no filters": buf.Bytes(), "stock filters": buf2.Bytes()} {
		loc := NewLocator(1)
		src := newCountSource(name, data)
		for i := 0; i < 50; i++ {
			r := rows[rng.Intn(len(rows))]
			got, _, err := loc.Lookup(context.Background(), src, r.TraceID, WantAll)
			if err != nil || len(got) != 1 || got[0] != r {
				t.Fatalf("%s: %v %v", name, got, err)
			}
		}
		e, _, _ := loc.open(context.Background(), src)
		if name == "no filters" && e.filters != nil {
			t.Error("filters invented for an object without any")
		}
	}
	// WantTime only reads fewer ranges than WantAll
	data, _, _ := Marshal(append([]Row(nil), rows...), 3)
	a, b := newCountSource("a", data), newCountSource("b", data)
	loc := NewLocator(2)
	_, _, _ = loc.Lookup(context.Background(), a, rows[1].TraceID, WantTime)
	_, _, _ = loc.Lookup(context.Background(), b, rows[1].TraceID, WantAll)
	if a.reads >= b.reads {
		t.Errorf("WantTime read %d ranges, WantAll %d", a.reads, b.reads)
	}
	if !strings.Contains(KindBloom.String()+KindFooter.String()+KindIndex.String()+KindPage.String()+ReadKind(9).String(), "unknown") {
		t.Error("ReadKind.String")
	}
}

func TestOverlayErrors(t *testing.T) {
	o := &overlay{size: 100}
	o.add(10, []byte("0123456789"))
	buf := make([]byte, 4)
	if n, err := o.ReadAt(buf, 12); err != nil || n != 4 || string(buf) != "2345" {
		t.Fatalf("%d %v %q", n, err, buf)
	}
	if n, err := o.ReadAt(buf, 18); err == nil || n != 2 {
		t.Fatalf("a read past the window must return what it has and an error: %d %v", n, err)
	}
	if _, err := o.ReadAt(buf, 50); err == nil {
		t.Fatal("an unplanned read must fail")
	}
	if _, err := o.ReadAt(buf, 100); err == nil {
		t.Fatal("a read at the end must fail")
	}
}

func TestValidateThriftNestedContainers(t *testing.T) {
	ok := [][]byte{
		{0x19, 0x1c, 0x11, 0x00, 0x00}, // list<struct> of one struct {bool}
		{0x19, 0x18, 0x02, 'h', 'i', 0x00},
		{0x1a, 0x18, 0x01, 'x', 0x00},                   // set<binary>
		{0x1b, 0x01, 0x8c, 0x01, 'a', 0x11, 0x00, 0x00}, // map<binary,struct>
		{0x1b, 0x01, 0x11, 0x01, 0x02, 0x00},            // map<bool,bool> (one byte each)
		{0x19, 0x1b, 0x00, 0x00},                        // list<map> of one empty map
		{0x19, 0x19, 0x19, 0x15, 5, 0x00},               // list<list<i32>>
		{0x19, 0x17, 1, 2, 3, 4, 5, 6, 7, 8, 0x00},      // list<double>
		{0x19, 0x13, 9, 0x00},                           // list<byte>
		{0x19, 0x14, 2, 0x00},                           // list<i16>
		{0x19, 0x16, 2, 0x00},                           // list<i64>
	}
	for i, b := range ok {
		if err := ValidateThrift(b); err != nil {
			t.Errorf("case %d: %v", i, err)
		}
	}
	for i, b := range [][]byte{
		{0x19, 0x1c},                   // list<struct> missing its element
		{0x19, 0x28, 0x05},             // list<binary> whose binary lies
		{0x1b, 0x01, 0x8c, 0x01, 'a'},  // map<binary,struct> without the struct
		{0x1b, 0x01},                   // map without its type byte
		{0x19, 0x1b, 0x01, 0x55},       // list<map> truncated
		{0x1a},                         // set without header
		{0x19, 0x1d, 0x00},             // list of an unknown type
		{0x1b, 0x01, 0x1d, 0x00},       // map of an unknown type
		{0x19, 0x15, 0xff, 0xff, 0xff}, // list<i32> with a truncated varint
		{0x14},                         // i16 without its varint
		{0x1c, 0x1c},                   // nested struct without stop
	} {
		if err := ValidateThrift(b); err == nil {
			t.Errorf("bad case %d accepted", i)
		}
	}
}

func TestLocatorFetchLimitsAndPageChecks(t *testing.T) {
	data, _, _ := Marshal(randRows(rand.New(rand.NewSource(5)), 1000), 3)
	src := newCountSource("lim", data)
	loc := NewLocator(1)
	ov := &overlay{size: src.Size()}
	st := &LookupStats{}
	size := src.Size()
	for name, r := range map[string]rng{"zero offset": {0, 10}, "empty": {10, 0}, "past the end": {size - 5, 10}, "negative": {-1, 5}} {
		if err := loc.fetch(context.Background(), src, ov, st, KindPage, 0, []rng{r}); err == nil {
			t.Errorf("%s: range accepted", name)
		}
	}
	if err := loc.fetch(context.Background(), src, ov, st, KindIndex, 0, []rng{{4, maxIndexBytes + 1}}); err == nil {
		t.Error("an index slice over the limit was read")
	}
	if err := loc.fetch(context.Background(), src, ov, st, KindIndex, 0, nil); err != nil {
		t.Error(err)
	}
	// a slice that is not a Thrift struct is refused before the library decodes it
	garbage := append([]byte(nil), data...)
	for i := 100; i < 300; i++ {
		garbage[i] = 0xff
	}
	gsrc := newCountSource("garb", garbage)
	if err := loc.fetch(context.Background(), gsrc, &overlay{size: size}, st, KindIndex, 0, []rng{{100, 200}}); err == nil {
		t.Error("a non-Thrift index slice was accepted")
	}
	// page locations from a hostile offset index
	if _, err := pagesOf(fakeOffsets{{off: 4, size: 10, first: 0}, {off: 20, size: 10, first: 0}}, 1000); err == nil {
		t.Error("page rows not increasing were accepted")
	}
	for name, p := range map[string]fakeOffsets{
		"past the end": {{off: 995, size: 10, first: 0}},
		"zero size":    {{off: 4, size: 0, first: 0}},
		"huge page":    {{off: 4, size: maxPageBytes + 1, first: 0}},
		"negative row": {{off: 4, size: 10, first: -1}},
		"negative off": {{off: -4, size: 10, first: 0}},
	} {
		if _, err := pagesOf(p, 1000); err == nil {
			t.Errorf("%s: page accepted", name)
		}
	}
	if pg, err := pagesOf(fakeOffsets{{off: 4, size: 10, first: 0}, {off: 14, size: 10, first: 5}}, 1000); err != nil || len(pg) != 2 || pageRows(pg, 0, 9) != 5 || pageRows(pg, 1, 9) != 4 || pageAt(pg, 7) != 1 {
		t.Errorf("pages %v %v", pg, err)
	}
}

type fakeOffsets []pageRef

func (f fakeOffsets) NumPages() int                  { return len(f) }
func (f fakeOffsets) Offset(i int) int64             { return f[i].off }
func (f fakeOffsets) CompressedPageSize(i int) int64 { return f[i].size }
func (f fakeOffsets) FirstRowIndex(i int) int64      { return f[i].first }

// Filters whose section is not a header plus a power-of-two bitset, or lies outside the
// object, are not kept; a lookup stays exact without them.
func TestLoadFiltersRefusesWhatItDoesNotKnow(t *testing.T) {
	data, _, _ := Marshal(randRows(rand.New(rand.NewSource(6)), 2000), 3)
	src := newCountSource("lf", data)
	loc := NewLocator(1)
	e, st, err := loc.open(context.Background(), src)
	if err != nil || e.filters == nil {
		t.Fatalf("open: %v filters=%v", err, e.filters != nil)
	}
	idCol := e.cols[IDField]
	md := &e.meta.RowGroups[0].Columns[idCol].MetaData
	saved := *md
	for name, mut := range map[string]func(){
		"no offset":    func() { md.BloomFilterOffset = 0 },
		"past the end": func() { md.BloomFilterOffset = e.size - 4 },
		"huge":         func() { md.BloomFilterLength = maxCachedBloomBytes + 1 },
		"bad header":   func() { md.BloomFilterLength = saved.BloomFilterLength - 5 },
	} {
		*md = saved
		mut()
		e.filters = nil
		_ = loc.loadFilters(context.Background(), src, e, &overlay{size: e.size}, st)
		if e.filters != nil {
			t.Errorf("%s: filters kept", name)
		}
	}
	*md = saved
	if !e.mayContain(0, "anything") && e.filters != nil {
		t.Error("mayContain without filters must say yes")
	}
	e.filters = nil
	if !e.mayContain(0, "x") {
		t.Error("an object without filters may hold anything")
	}
}
