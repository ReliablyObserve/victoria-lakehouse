package parquets3

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// footerAllocBound is the regression bound the tests hold a footer parse to
// for an input of n bytes: a fixed floor for parquet-go's read buffers plus
// 200 bytes per input byte (the largest decoded list element is a 560-byte
// ColumnChunk for at least 3 encoded bytes, ~190x). It is a test threshold for
// the rejected shapes and for real footers, not a proof: the path strings of a
// deep schema stay on parquet-go (see TestParseFooterFromBytes_DeepSchemaResidual).
// TotalAlloc is process-wide; the tests that use it run sequentially and the
// 32 MiB floor is far above what a background goroutine allocates in one parse.
func footerAllocBound(n int) uint64 { return 32<<20 + 200*uint64(n) }

// allocatedBy returns the bytes fn allocated (process-wide TotalAlloc delta).
func allocatedBy(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// thriftUvarint appends v as a thrift compact varint.
func thriftUvarint(b []byte, v uint64) []byte {
	return binary.AppendUvarint(b, v)
}

// footerWithMeta wraps raw FileMetaData bytes the way the file tail carries
// them: meta, little-endian length, "PAR1".
func footerWithMeta(meta []byte) []byte {
	out := append([]byte(nil), meta...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(meta)))
	return append(out, "PAR1"...)
}

// declaredList is FileMetaData{version=1, <field> = list<struct> of n}: a
// footer that is a handful of bytes yet declares n list elements. field is
// the thrift field id (2 schema, 4 row_groups, 5 key_value_metadata).
func declaredList(field byte, n uint64) []byte {
	meta := []byte{0x15, 0x02}               // field 1 (version) i32 = 1
	meta = append(meta, (field-1)<<4|tcList) // compact field header: id delta from field 1
	if n < 15 {
		meta = append(meta, byte(n)<<4|tcStruct)
	} else {
		meta = append(meta, 0xF0|tcStruct)
		meta = thriftUvarint(meta, n)
	}
	return append(meta, tcStop)
}

// TestParseFooterFromBytes_BoundsDeclaredListSize pins the fix for the
// FuzzParseFooterBytes / FuzzCacheFooterFromTail "fuzzing process hung or
// terminated unexpectedly: exit status 2" reported by the 2026-10-22 nightly
// (#724). parquet-go v0.32.0 allocates each thrift list with
// reflect.MakeSlice(size, size) from the size the footer declares, before it
// reads an element, so an 850-byte footer made OpenFile allocate 26 GiB (a
// 100 KB one 146 GiB) and the fuzz worker died with no panic to recover.
func TestParseFooterFromBytes_BoundsDeclaredListSize(t *testing.T) {
	cases := []struct {
		name  string
		field byte
		n     uint64
	}{
		// 20M RowGroups x 88 B = 1.7 GiB on the unguarded decoder.
		{"row_groups", 4, 20_000_000},
		// 20M SchemaElements x 96 B = 1.9 GiB.
		{"schema", 2, 20_000_000},
		// 20M KeyValues x 32 B = 640 MB.
		{"key_value_metadata", 5, 20_000_000},
		// MaxInt32 entries: the largest size the compact reader accepts.
		{"row_groups_maxint32", 4, 1<<31 - 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			footer := footerWithMeta(declaredList(tc.field, tc.n))
			var err error
			alloc := allocatedBy(func() {
				_, _, err = ParseFooterFromBytes("crafted.parquet", footer, 1<<20)
			})
			if err == nil {
				t.Fatal("a footer declaring more list elements than it has bytes must be rejected")
			}
			if bound := footerAllocBound(len(footer)); alloc > bound {
				t.Fatalf("parse of a %d-byte footer allocated %d bytes (bound %d)", len(footer), alloc, bound)
			}
		})
	}
}

// TestCheckFooterThriftBounds_AcceptsRealFooters: the guard must never reject
// a file a standard writer produced (open Parquet): single and multi row
// group, wide and narrow.
func TestCheckFooterThriftBounds_AcceptsRealFooters(t *testing.T) {
	for _, tc := range []struct{ rows, rg int }{{10, 10}, {3000, 1000}, {9000, 500}, {20000, 100}} {
		obj := logsObject(t, tc.rows, tc.rg)
		n := footerTotal(obj)
		meta := obj[len(obj)-n : len(obj)-8]
		if err := checkFooterThriftBounds(meta); err != nil {
			t.Fatalf("rows=%d rg=%d: valid footer (%d bytes) rejected: %v", tc.rows, tc.rg, len(meta), err)
		}
		if _, _, err := ParseFooterFromBytes("real.parquet", obj[len(obj)-n:], int64(len(obj))); err != nil {
			t.Fatalf("rows=%d rg=%d: valid footer no longer parses: %v", tc.rows, tc.rg, err)
		}
	}
}

// TestCheckFooterThriftBounds_Structure exercises every compact type the
// walker skips, so a bound check cannot hide a decoder drift.
func TestCheckFooterThriftBounds_Structure(t *testing.T) {
	ok := [][]byte{
		{tcStop},
		{0x11, tcStop},                         // bool true field
		{0x12, tcStop},                         // bool false field
		{0x13, 0x07, tcStop},                   // byte
		{0x14, 0x02, tcStop},                   // i16
		{0x15, 0x80, 0x01, tcStop},             // i32, multi-byte varint
		{0x06, 0x04, 0x09, tcStop},             // i64 with its field id spelled out (delta 0)
		{0x29, 0x1C, 0x51, tcStop, tcStop},     // a SchemaElement whose field 5 is a bool, not num_children
		{0x17, 1, 2, 3, 4, 5, 6, 7, 8, tcStop}, // double
		{0x18, 0x03, 'a', 'b', 'c', tcStop},    // binary
		{0x19, 0x35, 1, 2, 3, tcStop},          // list<i32> of 3
		{0x19, 0x31, 1, 2, 1, tcStop},          // list<bool> of 3: one byte each
		{0x19, 0x3C, 0x15, 0x02, tcStop, 0x15, 0x02, tcStop, 0x15, 0x02, tcStop, tcStop}, // list<struct> of 3
		{0x1C, 0x15, 0x02, tcStop, tcStop},                                               // nested struct
		{0x1B, 0x01, 0x88, 0x01, 'k', 0x01, 'v', tcStop},                                 // map<binary,binary>, 1 entry
		{0x1B, 0x00, tcStop}, // empty map
	}
	for i, b := range ok {
		if err := checkFooterThriftBounds(b); err != nil {
			t.Fatalf("ok[%d] %x: %v", i, b, err)
		}
	}
	bad := map[string][]byte{
		"empty":              {},
		"no stop":            {0x15, 0x02},
		"short binary":       {0x18, 0x09, 'a', tcStop},
		"short double":       {0x17, 1, 2, tcStop},
		"huge short list":    {0x19, 0xF5, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, tcStop},
		"struct list budget": {0x19, 0x9C, 1, 1, 1, tcStop}, // 9 structs x 3 bytes > 6 bytes
		"varint overflow":    {0x15, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, tcStop, tcStop},
		"unknown type":       {0x1E, tcStop},
	}
	// Two lists of 6 empty structs: each fits the budget alone (18 of 29), the
	// pair does not (36), so only a budget shared across the footer rejects it.
	pad := append([]byte{0x18, 0x0A}, make([]byte, 10)...)
	twoLists := append(append([]byte(nil), pad...), 0x19, 0x6C, 0, 0, 0, 0, 0, 0, 0x19, 0x6C, 0, 0, 0, 0, 0, 0, tcStop)
	bad["footer-wide budget"] = twoLists
	// Empty structs are one byte each on the wire but never occur in parquet;
	// a list of them is how a small footer would claim a large allocation.
	bad["empty struct elements"] = []byte{0x19, 0x6C, 0, 0, 0, 0, 0, 0, tcStop}
	// A binary length of 2^64-1 must not wrap the read position.
	bad["binary length wraps"] = []byte{0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, tcStop, tcStop}
	for name, b := range bad {
		if err := checkFooterThriftBounds(b); err == nil {
			t.Fatalf("%s %x: want an error", name, b)
		}
	}
	// Nesting past the depth cap must error, not overflow the stack. The
	// nesting is well formed (every struct is closed), so only the cap rejects it.
	var deep []byte
	for i := 0; i < footerThriftMaxDepth+8; i++ {
		deep = append(deep, 0x1C)
	}
	for i := 0; i < footerThriftMaxDepth+8; i++ {
		deep = append(deep, tcStop)
	}
	deep = append(deep, tcStop)
	if err := checkFooterThriftBounds(deep); err == nil {
		t.Fatal("over-deep nesting must be rejected")
	}
}

// TestParseFooterFromBytes_MutatedFootersStayBounded is the property the
// fuzzers check at scale, run deterministically: whatever a flipped, grown or
// shrunk length field does to a real footer, parsing stays within the linear
// allocation bound and never panics.
func TestParseFooterFromBytes_MutatedFootersStayBounded(t *testing.T) {
	obj := logsObject(t, 3000, 1000)
	n := footerTotal(obj)
	footer := append([]byte(nil), obj[len(obj)-n:]...)
	rng := rand.New(rand.NewSource(724))
	iters := 3000
	if testing.Short() {
		iters = 600
	}
	for i := 0; i < iters; i++ {
		m := append([]byte(nil), footer...)
		for k := 0; k <= rng.Intn(3); k++ {
			pos := rng.Intn(len(m) - 8) // keep the length+magic suffix intact
			switch rng.Intn(3) {
			case 0:
				m[pos] = byte(rng.Intn(256))
			case 1:
				m[pos] = 0xFF // pushes a varint towards a huge size
			default:
				m[pos] ^= 1 << uint(rng.Intn(8))
			}
		}
		alloc := allocatedBy(func() { _, _, _ = ParseFooterFromBytes("mut.parquet", m, int64(len(obj))) })
		if bound := footerAllocBound(len(m)); alloc > bound {
			t.Fatalf("iter %d: mutated footer allocated %d bytes (bound %d): %x", i, alloc, bound, m)
		}
	}
}

// TestParseFooterFromBytes_ScalarFieldsDoNotDriveAllocation re-encodes a real
// footer with its integer fields pushed to extremes (the ones parquet-go sizes
// slices or reads from) and requires every variant to stay inside the linear
// bound, so a size that is not a list length cannot reintroduce the nightly
// out-of-memory exit.
func TestParseFooterFromBytes_ScalarFieldsDoNotDriveAllocation(t *testing.T) {
	obj := logsObject(t, 3000, 1000)
	n := footerTotal(obj)
	orig, _, err := ParseFooterFromBytes("orig.parquet", obj[len(obj)-n:], int64(len(obj)))
	if err != nil {
		t.Fatal(err)
	}
	base := orig.File.Metadata()

	mutations := map[string]func(m *format.FileMetaData){
		"root num_children max": func(m *format.FileMetaData) {
			m.Schema[0].NumChildren = thrift.New[int32](math.MaxInt32)
		},
		"root num_children negative": func(m *format.FileMetaData) {
			m.Schema[0].NumChildren = thrift.New[int32](-1)
		},
		"leaf num_children max": func(m *format.FileMetaData) {
			m.Schema[len(m.Schema)-1].NumChildren = thrift.New[int32](math.MaxInt32)
		},
		"num_rows max": func(m *format.FileMetaData) { m.NumRows = math.MaxInt64 },
		"row group num_rows max": func(m *format.FileMetaData) {
			m.RowGroups[0].NumRows = math.MaxInt64
		},
		"column num_values max": func(m *format.FileMetaData) {
			m.RowGroups[0].Columns[0].MetaData.NumValues = math.MaxInt64
		},
		"column sizes max": func(m *format.FileMetaData) {
			c := &m.RowGroups[0].Columns[0].MetaData
			c.TotalCompressedSize, c.TotalUncompressedSize = math.MaxInt64, math.MaxInt64
			c.DataPageOffset, c.IndexPageOffset = math.MaxInt64, math.MaxInt64
		},
		"column bloom length max": func(m *format.FileMetaData) {
			c := &m.RowGroups[0].Columns[0].MetaData
			c.BloomFilterOffset, c.BloomFilterLength = 1, math.MaxInt32
		},
		"column index length max": func(m *format.FileMetaData) {
			c := &m.RowGroups[0].Columns[0]
			c.ColumnIndexOffset, c.ColumnIndexLength = 1, math.MaxInt32
			c.OffsetIndexOffset, c.OffsetIndexLength = 1, math.MaxInt32
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			md := *base // shallow copy; mutate deep copies of what is touched
			md.Schema = append([]format.SchemaElement(nil), base.Schema...)
			md.RowGroups = append([]format.RowGroup(nil), base.RowGroups...)
			for i := range md.RowGroups {
				md.RowGroups[i].Columns = append([]format.ColumnChunk(nil), base.RowGroups[i].Columns...)
			}
			mutate(&md)
			meta, err := thrift.Marshal(new(thrift.CompactProtocol), &md)
			if err != nil {
				t.Fatal(err)
			}
			footer := footerWithMeta(meta)
			alloc := allocatedBy(func() {
				_, _, _ = ParseFooterFromBytes("scalar.parquet", footer, int64(len(obj)))
			})
			if bound := footerAllocBound(len(footer)); alloc > bound {
				t.Fatalf("footer of %d bytes allocated %d bytes (bound %d)", len(footer), alloc, bound)
			}
		})
	}
}

// BenchmarkParseFooterBytes is the per-file cost of the guarded footer parse
// (the prefetch path every cold first read takes). "guard" is the thrift size
// walk alone; "parse" is ParseFooterFromBytes end to end, so main vs branch
// shows what the walk adds to a parse. Compare with
//
//	go test -run '^$' -bench ParseFooterBytes -benchmem -count=10 | benchstat
func BenchmarkParseFooterBytes(b *testing.B) {
	for _, tc := range []struct {
		name     string
		rows, rg int
	}{{"small_1rg", 9000, 9000}, {"mid_3rg", 9000, 3000}, {"large_400rg", 20000, 50}} {
		obj := logsObject(b, tc.rows, tc.rg)
		n := footerTotal(obj)
		footer := obj[len(obj)-n:]
		b.Run(tc.name+"/parse", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				if _, _, err := ParseFooterFromBytes("k", footer, int64(len(obj))); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/guard", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			meta := footer[:n-8]
			for i := 0; i < b.N; i++ {
				if err := checkFooterThriftBounds(meta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// nestedFooter returns n nested values of one compact type as a footer body,
// every one of them well formed (n containers deep, the innermost empty). nest = "list" | "map" | "struct".
func nestedFooter(nest string, n int) []byte {
	var b []byte
	switch nest {
	case "list": // n lists, each of size 1 holding the next; the innermost empty
		b = append(b, 0x19) // field: list
		for i := 0; i < n; i++ {
			b = append(b, 0x19) // size 1, elements are lists
		}
		b[len(b)-1] = 0x05 // innermost: size 0, elements i32
	case "map": // n maps, each of size 1 with an i32 key and the next map as value
		b = append(b, 0x1B) // field: map
		for i := 1; i < n; i++ {
			b = append(b, 0x01, 0x5B, 0x00) // size 1, key i32 / value map, key 0, then the value
		}
		b = append(b, 0x00) // innermost: size 0
	case "mapkey": // n maps nested through their keys (value i32), the innermost empty
		b = append(b, 0x1B)
		for i := 1; i < n; i++ {
			b = append(b, 0x01, 0xB5) // size 1, key map / value i32
		}
		b = append(b, 0x00)
		b = append(b, make([]byte, n-1)...) // the i32 value of every entry
	case "struct":
		for i := 0; i < n; i++ {
			b = append(b, 0x1C)
		}
		for i := 0; i < n; i++ {
			b = append(b, tcStop)
		}
	}
	return append(b, tcStop)
}

// TestCheckFooterThriftBounds_NestingCap pins the nesting cap for every
// container kind: a value at footerThriftMaxDepth is accepted, one level more
// is rejected. Lists and maps used to recurse without counting, so a footer of
// repeated list headers ran the decoder out of stack (a Go fatal error, exit 2).
func TestCheckFooterThriftBounds_NestingCap(t *testing.T) {
	// The contract is the literal 32 / 33, not the constant: a change of the
	// cap must change this test on purpose.
	for _, nest := range []string{"list", "map", "mapkey", "struct"} {
		if err := checkFooterThriftBounds(nestedFooter(nest, 32)); err != nil {
			t.Fatalf("%s nested 32 deep (the cap) must be accepted: %v", nest, err)
		}
		if err := checkFooterThriftBounds(nestedFooter(nest, 33)); err == nil {
			t.Fatalf("%s nested 33 deep (cap+1) must be rejected", nest)
		}
	}
	// A SchemaElement is walked by its own function: its fields count from the
	// element's level (top struct 0, schema list 1, element 2, first field 3).
	inElement := func(k int) []byte {
		b := []byte{0x29, 0x1C, 0x89} // field 2 list of 1 struct; its field 8: a list
		for i := 1; i < k; i++ {
			b = append(b, 0x19)
		}
		return append(b, 0x05, tcStop, tcStop) // innermost list of i32, element stop, top stop
	}
	if err := checkFooterThriftBounds(inElement(30)); err != nil {
		t.Fatalf("30 lists inside a SchemaElement (level 32) must be accepted: %v", err)
	}
	if err := checkFooterThriftBounds(inElement(31)); err == nil {
		t.Fatal("31 lists inside a SchemaElement (level 33) must be rejected")
	}
}

// TestParseFooterFromBytes_RejectsDeepNesting feeds footers of 1 MiB and 8 MiB
// of nested container headers through the public entry point. Before the cap
// covered lists and maps the 8 MiB footer ended the process with
// "fatal error: stack overflow" in (*footerGuard).value (1 MiB survived).
func TestParseFooterFromBytes_RejectsDeepNesting(t *testing.T) {
	for _, nest := range []string{"list", "map", "mapkey", "struct"} {
		for _, size := range []int{1 << 20, 8 << 20} {
			meta := nestedFooter(nest, size/2)
			footer := footerWithMeta(meta)
			var err error
			alloc := allocatedBy(func() { _, _, err = ParseFooterFromBytes("deep.parquet", footer, int64(len(footer))+1<<20) })
			if err == nil || !strings.Contains(err.Error(), "nesting too deep") {
				t.Fatalf("%s nesting of %d bytes must be rejected as too deep, got %v", nest, len(meta), err)
			}
			if bound := footerAllocBound(len(footer)); alloc > bound {
				t.Fatalf("%s nesting of %d bytes allocated %d (bound %d)", nest, len(meta), alloc, bound)
			}
		}
	}
}

// fmdField appends a compact field header for id after prev (ids ascend).
func fmdField(b []byte, prev, id int, typ byte) []byte {
	return append(b, byte(id-prev)<<4|typ)
}

// budgetFooter builds a FileMetaData body of n empty struct elements in field
// 1 (each one byte on the wire, charged 3 against the budget) plus a binary
// padding field so the whole body is exactly total bytes.
func budgetFooter(t *testing.T, n, total int) []byte {
	t.Helper()
	b := fmdField(nil, 0, 1, tcList)
	b = append(b, byte(n)<<4|tcStruct) // n < 15
	b = append(b, make([]byte, n)...)
	for pad := 0; pad < 400; pad++ { // find the padding that makes the body total bytes
		c := fmdField(append([]byte(nil), b...), 1, 2, tcBinary)
		c = thriftUvarint(c, uint64(pad))
		c = append(c, make([]byte, pad)...)
		c = append(c, tcStop)
		if len(c) == total {
			return c
		}
	}
	t.Fatalf("no padding gives a %d byte body for %d elements", total, n)
	return nil
}

// TestCheckFooterThriftBounds_BudgetBoundary pins the budget at its edge: n
// struct elements are charged 3 bytes each, so a body of exactly 3n bytes
// holds them and a body one byte shorter does not.
func TestCheckFooterThriftBounds_BudgetBoundary(t *testing.T) {
	for _, n := range []int{3, 7, 14} {
		if err := checkFooterThriftBounds(budgetFooter(t, n, 3*n)); err != nil {
			t.Fatalf("n=%d: exactly the budget's worth (%d bytes) must pass: %v", n, 3*n, err)
		}
		if err := checkFooterThriftBounds(budgetFooter(t, n, 3*n-1)); err == nil {
			t.Fatalf("n=%d: one byte under the budget (%d bytes) must fail", n, 3*n-1)
		}
	}
}

// metaOf builds FileMetaData with a root group over leaves int64 columns and
// rowGroups row groups that each carry chunks column chunks.
func metaOf(leaves, rowGroups, chunks int) *format.FileMetaData {
	m := &format.FileMetaData{Version: 1}
	m.Schema = append(m.Schema, format.SchemaElement{Name: "root", NumChildren: thrift.New[int32](int32(leaves))})
	for i := 0; i < leaves; i++ {
		m.Schema = append(m.Schema, format.SchemaElement{
			Type: thrift.New(format.Int64), RepetitionType: thrift.New(format.Required), Name: "c",
		})
	}
	for r := 0; r < rowGroups; r++ {
		m.RowGroups = append(m.RowGroups, format.RowGroup{Columns: make([]format.ColumnChunk, chunks)})
	}
	return m
}

func footerOfMeta(t *testing.T, m *format.FileMetaData) []byte {
	t.Helper()
	meta, err := thrift.Marshal(new(thrift.CompactProtocol), m)
	if err != nil {
		t.Fatal(err)
	}
	return footerWithMeta(meta)
}

// TestParseFooterFromBytes_RejectsQuadraticColumnAllocation: columnLoader.reserve
// allocates one chunk pointer per (leaf x row group) and one path string per
// schema element and ancestor, before validating anything. Measured on the
// previous guard (list sizes only), which accepted all of these: 3000 leaves x
// 3000 row groups (a 45 KB footer) 73.5 MB, 6000 x 6000 (90 KB) 291 MB, a
// schema chain of 8000 groups (48 KB) 515 MB.
func TestParseFooterFromBytes_RejectsQuadraticColumnAllocation(t *testing.T) {
	chain := &format.FileMetaData{Version: 1}
	for i := 0; i < 8000; i++ {
		chain.Schema = append(chain.Schema, format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](1)})
	}
	chain.Schema = append(chain.Schema, format.SchemaElement{Type: thrift.New(format.Int64), Name: "leaf"})
	for _, tc := range []struct {
		name string
		m    *format.FileMetaData
	}{
		{"3000x3000", metaOf(3000, 3000, 0)},
		{"6000x6000", metaOf(6000, 6000, 0)},
		{"schema chain 8000", chain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			footer := footerOfMeta(t, tc.m)
			var err error
			alloc := allocatedBy(func() { _, _, err = ParseFooterFromBytes("quad.parquet", footer, int64(len(footer))+1<<20) })
			if err == nil {
				t.Fatalf("a %d-byte footer with a shape parquet-go cannot hold must be rejected", len(footer))
			}
			t.Logf("%s: %d-byte footer, rejected, %d bytes allocated", tc.name, len(footer), alloc)
			if bound := footerAllocBound(len(footer)); alloc > bound {
				t.Fatalf("allocated %d bytes (bound %d)", alloc, bound)
			}
		})
	}
}

// TestCheckFooterThriftBounds_SchemaAndRowGroupBoundaries pins the two shape
// checks at their edges. Leaves x row groups x 3 may equal the footer length
// and not exceed it; a schema may nest parquet.MaxColumnDepth levels and not one
// more.
func TestCheckFooterThriftBounds_SchemaAndRowGroupBoundaries(t *testing.T) {
	// 10 leaves (3 bytes each: type + stop... see below) and 10 row groups.
	build := func(total int) []byte {
		b := fmdField(nil, 0, 2, tcList)
		b = append(b, 10<<4|tcStruct)
		for i := 0; i < 10; i++ {
			b = append(b, 0x15, 0x04, tcStop) // SchemaElement{type: 2}: a leaf
		}
		b = fmdField(b, 2, 4, tcList)
		b = append(b, 10<<4|tcStruct)
		b = append(b, make([]byte, 10)...) // ten empty RowGroups
		for pad := 0; pad < 400; pad++ {
			c := fmdField(append([]byte(nil), b...), 4, 6, tcBinary)
			c = thriftUvarint(c, uint64(pad))
			c = append(c, make([]byte, pad)...)
			c = append(c, tcStop)
			if len(c) == total {
				return c
			}
		}
		t.Fatalf("no padding for %d", total)
		return nil
	}
	if err := checkFooterThriftBounds(build(10 * 10 * 3)); err != nil {
		t.Fatalf("10 leaves x 10 row groups in exactly 300 bytes must pass: %v", err)
	}
	if err := checkFooterThriftBounds(build(10*10*3 - 1)); err == nil {
		t.Fatal("10 leaves x 10 row groups in 299 bytes must fail")
	}

	// Field 1 is a leaf marker only as an i32: with another type parquet-go
	// skips it, the element is a group, and no chunks are expected for it.
	notLeaf := fmdField(nil, 0, 2, tcList)
	notLeaf = append(notLeaf, 10<<4|tcStruct)
	for i := 0; i < 10; i++ {
		notLeaf = append(notLeaf, 0x16, 0x04, tcStop) // field 1 as i64
	}
	notLeaf = fmdField(notLeaf, 2, 4, tcList)
	notLeaf = append(notLeaf, 10<<4|tcStruct)
	notLeaf = append(notLeaf, make([]byte, 10)...)
	notLeaf = fmdField(notLeaf, 4, 6, tcBinary)
	notLeaf = append(thriftUvarint(notLeaf, 60), make([]byte, 60)...) // room for the budget
	notLeaf = append(notLeaf, tcStop)
	if err := checkFooterThriftBounds(notLeaf); err != nil {
		t.Fatalf("elements whose field 1 is not an i32 are not leaves: %v", err)
	}

	// row_groups given twice (explicit field ids): the larger list counts.
	dup := fmdField(nil, 0, 2, tcList)
	dup = append(dup, 10<<4|tcStruct)
	for i := 0; i < 10; i++ {
		dup = append(dup, 0x15, 0x04, tcStop)
	}
	dup = fmdField(dup, 2, 4, tcList)
	dup = append(dup, 10<<4|tcStruct)
	dup = append(dup, make([]byte, 10)...)
	dup = append(dup, 0x09, 0x08, 0x0C) // explicit id 4 again: list<struct> of 0
	dup = fmdField(dup, 4, 6, tcBinary)
	dup = append(thriftUvarint(dup, 60), make([]byte, 60)...) // room for the budget, still under 300 bytes
	dup = append(dup, tcStop)
	if err := checkFooterThriftBounds(dup); err == nil {
		t.Fatal("10 leaves x 10 row groups in a short footer must fail even when a later empty row_groups follows")
	}

	// Field ids spelled out (delta 0 form) are the same fields.
	explicit := []byte{0x09, 0x04, 10<<4 | tcStruct} // field 2: list of 10 structs
	for i := 0; i < 10; i++ {
		explicit = append(explicit, 0x15, 0x04, tcStop)
	}
	explicit = append(explicit, 0x09, 0x08, 10<<4|tcStruct) // field 4: list of 10 structs
	explicit = append(explicit, make([]byte, 10)...)
	explicit = append(explicit, tcStop)
	if err := checkFooterThriftBounds(explicit); err == nil {
		t.Fatal("10 leaves x 10 row groups must fail when their field ids are spelled out too")
	}

	// A chain of groups, each with one child, ending in a leaf: depth is the
	// number of open ancestors of the last element.
	chain := func(levels int) []byte {
		m := &format.FileMetaData{Version: 1}
		for i := 0; i < levels; i++ {
			m.Schema = append(m.Schema, format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](1)})
		}
		m.Schema = append(m.Schema, format.SchemaElement{Type: thrift.New(format.Int64), Name: "leaf"})
		meta, err := thrift.Marshal(new(thrift.CompactProtocol), m)
		if err != nil {
			t.Fatal(err)
		}
		return meta
	}
	if err := checkFooterThriftBounds(chain(parquet.MaxColumnDepth)); err != nil {
		t.Fatalf("a schema nested %d levels (parquet-go's limit) must pass: %v", parquet.MaxColumnDepth, err)
	}
	if err := checkFooterThriftBounds(chain(parquet.MaxColumnDepth + 1)); err == nil {
		t.Fatalf("a schema nested %d levels must fail", parquet.MaxColumnDepth+1)
	}

	// A second branch is as deep as its own chain plus the root: the first
	// branch closing must not reset the depth of what follows.
	branches := func(levels int) []byte {
		m := &format.FileMetaData{Version: 1}
		m.Schema = append(m.Schema, format.SchemaElement{Name: "root", NumChildren: thrift.New[int32](2)})
		m.Schema = append(m.Schema, format.SchemaElement{Name: "a", NumChildren: thrift.New[int32](1)})
		m.Schema = append(m.Schema, format.SchemaElement{Type: thrift.New(format.Int64), Name: "x"})
		for i := 0; i < levels; i++ {
			m.Schema = append(m.Schema, format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](1)})
		}
		m.Schema = append(m.Schema, format.SchemaElement{Type: thrift.New(format.Int64), Name: "y"})
		meta, err := thrift.Marshal(new(thrift.CompactProtocol), m)
		if err != nil {
			t.Fatal(err)
		}
		return meta
	}
	if err := checkFooterThriftBounds(branches(parquet.MaxColumnDepth - 1)); err != nil {
		t.Fatalf("a second branch whose leaf is at depth %d must pass: %v", parquet.MaxColumnDepth, err)
	}
	if err := checkFooterThriftBounds(branches(parquet.MaxColumnDepth)); err == nil {
		t.Fatalf("a second branch whose leaf is at depth %d must fail", parquet.MaxColumnDepth+1)
	}
}

// TestCheckFooterThriftBounds_AcceptsEdgeShapedRealFiles: files a standard
// writer produces whose footers sit at the edges of the shape checks must keep
// passing, and parse: no row groups with a schema, a deeply nested schema,
// many leaf columns in few row groups, many row groups with few columns.
func TestCheckFooterThriftBounds_AcceptsEdgeShapedRealFiles(t *testing.T) {
	type l3 struct{ V int64 }
	type l2 struct{ C l3 }
	type l1 struct{ B l2 }
	type nested struct {
		A  l1
		L  []l2
		M  map[string]l3
		TS int64
	}
	write := func(schema *parquet.Schema, rows []parquet.Row, flushEvery int) []byte {
		var buf bytes.Buffer
		w := parquet.NewWriter(&buf, schema)
		for i, r := range rows {
			if _, err := w.WriteRows([]parquet.Row{r}); err != nil {
				t.Fatal(err)
			}
			if flushEvery > 0 && (i+1)%flushEvery == 0 {
				if err := w.Flush(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	wide := parquet.Group{}
	for i := 0; i < 300; i++ {
		wide[string(rune('a'+i%26))+string(rune('a'+(i/26)%26))+string(rune('a'+(i/676)%26))] = parquet.Leaf(parquet.Int64Type)
	}
	wideSchema := parquet.NewSchema("wide", wide)
	wideRow := func() parquet.Row {
		r := make(parquet.Row, 0, len(wide))
		for i := range wideSchema.Columns() {
			r = append(r, parquet.Int64Value(int64(i)).Level(0, 0, i))
		}
		return r
	}()
	groups := parquet.Group{}
	for i := 0; i < 300; i++ {
		groups[string(rune('a'+i%26))+string(rune('a'+(i/26)%26))] = parquet.Group{"x": parquet.Leaf(parquet.Int64Type)}
	}
	groupsSchema := parquet.NewSchema("groups", groups)
	narrow := parquet.NewSchema("narrow", parquet.Group{"x": parquet.Leaf(parquet.Int64Type)})
	narrowRows := make([]parquet.Row, 300)
	for i := range narrowRows {
		narrowRows[i] = parquet.Row{parquet.Int64Value(int64(i)).Level(0, 0, 0)}
	}
	nestedSchema := parquet.SchemaOf(new(nested))
	cases := map[string][]byte{
		"schema only, zero row groups": write(wideSchema, nil, 0),
		"300 columns, 1 row group":     write(wideSchema, []parquet.Row{wideRow}, 0),
		"300 columns, 40 row groups":   write(wideSchema, repeatRow(wideRow, 40), 1),
		"1 column, 300 row groups":     write(narrow, narrowRows, 1),
		"nested, 12 levels":            write(nestedSchema, nil, 0),
		"300 sibling groups of 1 leaf": write(groupsSchema, nil, 0),
	}
	for name, obj := range cases {
		n := footerTotal(obj)
		meta := obj[len(obj)-n : len(obj)-8]
		if err := checkFooterThriftBounds(meta); err != nil {
			t.Fatalf("%s: valid footer (%d bytes) rejected: %v", name, len(meta), err)
		}
		if _, _, err := ParseFooterFromBytes(name, obj[len(obj)-n:], int64(len(obj))); err != nil {
			t.Fatalf("%s: valid footer no longer parses: %v", name, err)
		}
	}
}

func repeatRow(r parquet.Row, n int) []parquet.Row {
	out := make([]parquet.Row, n)
	for i := range out {
		out[i] = r
	}
	return out
}

// TestParseFooterFromBytes_DeepSchemaResidual documents what the guard leaves to
// parquet-go: a schema that nests at most parquet.MaxColumnDepth levels still
// makes columnLoader.reserve allocate one string header per ancestor of each
// element. It measures the worst shape the guard accepts (a 255-deep chain
// with leaves at the bottom) so the PR numbers are reproducible; it asserts a
// ceiling of 1 KiB of allocation per footer byte, well above what is measured.
func TestParseFooterFromBytes_DeepSchemaResidual(t *testing.T) {
	m := &format.FileMetaData{Version: 1}
	const leaves = 4000
	for i := 0; i < parquet.MaxColumnDepth-1; i++ {
		m.Schema = append(m.Schema, format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](1)})
	}
	m.Schema = append(m.Schema, format.SchemaElement{Name: "g", NumChildren: thrift.New[int32](leaves)})
	for i := 0; i < leaves; i++ {
		m.Schema = append(m.Schema, format.SchemaElement{Type: thrift.New(format.Int64), Name: "c"})
	}
	footer := footerOfMeta(t, m)
	var err error
	alloc := allocatedBy(func() { _, _, err = ParseFooterFromBytes("deep.parquet", footer, int64(len(footer))+1<<20) })
	t.Logf("255-deep schema, %d leaves: %d-byte footer, err=%v, %d bytes allocated (%.0fx)", leaves, len(footer), err, alloc, float64(alloc)/float64(len(footer)))
	if alloc > 1024*uint64(len(footer))+footerAllocBound(0) {
		t.Fatalf("allocated %d bytes for a %d-byte footer", alloc, len(footer))
	}
}
