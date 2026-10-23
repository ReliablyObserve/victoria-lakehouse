package parquets3

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"testing"
)

// footerAllocBound is the most a footer parse may allocate for an input of n
// bytes: a fixed floor for parquet-go's read buffers plus the linear
// amplification checkFooterThriftBounds guarantees (the largest decoded
// element is a 560-byte ColumnChunk for at least 3 encoded bytes, ~190x).
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
			if tc.n > 100_000_000 && testing.Short() {
				t.Skip("declares 2^31 elements: the unguarded decoder would exhaust memory")
			}
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
		"varint overflow":    {0x15, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, tcStop},
		"unknown type":       {0x1E, tcStop},
	}
	for name, b := range bad {
		if err := checkFooterThriftBounds(b); err == nil {
			t.Fatalf("%s %x: want an error", name, b)
		}
	}
	// Nesting past the depth cap must error, not overflow the stack.
	deep := []byte{}
	for i := 0; i < footerThriftMaxDepth+8; i++ {
		deep = append(deep, 0x1C)
	}
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
