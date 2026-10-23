package parquets3

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// craftedTraceIndex returns a version-1 `_trace_idx` value whose declared
// entry count is count and which carries payload bytes after the header.
func craftedTraceIndex(count uint32, payload int) []byte {
	b := make([]byte, 5+payload)
	b[0] = 1
	binary.LittleEndian.PutUint32(b[1:5], count)
	return b
}

// totalAllocDelta reports the bytes allocated while f runs.
func totalAllocDelta(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestUnmarshalTraceIndex_DeclaredCountBounded pins issue #745: the
// `_trace_idx` footer value is untrusted bucket data, and a 5-byte value
// declaring 268 M entries used to preallocate 10.7 GB. The decoder must
// reject it without allocating in proportion to the declared count.
func TestUnmarshalTraceIndex_DeclaredCountBounded(t *testing.T) {
	const ceiling = 4 << 20 // 4 MiB, generous for background-goroutine noise (TotalAlloc is process-wide); the crasher allocated 10.7 GB
	cases := map[string][]byte{
		"5 bytes, count 268M":      {1, 0xF0, 0xFF, 0xFF, 0x0F},
		"5 bytes, count 2^32-1":    {1, 0xFF, 0xFF, 0xFF, 0xFF},
		"5 bytes, count at 1<<20":  craftedTraceIndex(1<<20, 0),
		"5 bytes, count 1<<20 - 1": craftedTraceIndex(1<<20-1, 0),
		"count exceeds payload":    craftedTraceIndex(1000, 19*1000),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			var (
				entries []TraceIndexEntry
				err     error
			)
			alloc := totalAllocDelta(func() { entries, err = unmarshalTraceIndex(data) })
			if err == nil {
				t.Fatalf("expected an error, got %d entries", len(entries))
			}
			if alloc > ceiling {
				t.Fatalf("decoder allocated %d bytes for a %d-byte value (ceiling %d)", alloc, len(data), ceiling)
			}
		})
	}
}

// TestUnmarshalTraceIndex_RoundTripUnchanged: valid indexes decode to
// exactly what was marshaled (the bound must not change valid decoding).
func TestUnmarshalTraceIndex_RoundTripUnchanged(t *testing.T) {
	for _, n := range []int{0, 1, 2, 100, 5000} {
		in := make([]TraceIndexEntry, n)
		for i := range in {
			id := string(rune('a'+i%26)) + "-trace"
			if i%7 == 0 {
				id = "" // empty trace id: minimum-size element
			}
			in[i] = TraceIndexEntry{TraceID: id, Partition: traceIDPartition(id), StartNs: int64(i), EndNs: int64(i) * 3}
		}
		got, err := unmarshalTraceIndex(marshalTraceIndex(in))
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(got) != n {
			t.Fatalf("n=%d: got %d entries", n, len(got))
		}
		for i := range in {
			if got[i] != in[i] {
				t.Fatalf("n=%d entry %d: got %+v want %+v", n, i, got[i], in[i])
			}
		}
	}
}
