package traceindex

import (
	"encoding/binary"
	"runtime"
	"strings"
	"testing"
)

func allocDelta(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func crafted(count uint32, payload int) []byte {
	b := make([]byte, 5+payload)
	b[0] = Version
	binary.LittleEndian.PutUint32(b[1:5], count)
	return b
}

// TestUnmarshal_DeclaredCountBoundedByPayload pins issue #745. The
// `_trace_idx` value is untrusted bucket data: a 5-byte value declaring N
// entries must be rejected without allocating in proportion to N. The
// maxEntries cap alone still allowed ~42 MB for a 5-byte value.
func TestUnmarshal_DeclaredCountBoundedByPayload(t *testing.T) {
	const ceiling = 4 << 20 // 4 MiB (assumed generous: TotalAlloc is process-wide; the crasher is 10.7 GB uncapped, 42 MB at the cap)
	cases := map[string][]byte{
		"count 268M":        {Version, 0xF0, 0xFF, 0xFF, 0x0F},
		"count 2^32-1":      {Version, 0xFF, 0xFF, 0xFF, 0xFF},
		"count at cap":      crafted(maxEntriesForTest, 0),
		"count 1":           crafted(1, 0),
		"count exceeds fit": crafted(1000, 19*1000),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			a := allocDelta(func() { _, err = Unmarshal(data) })
			if err == nil {
				t.Fatal("expected an error")
			}
			if a > ceiling {
				t.Fatalf("allocated %d bytes for a %d-byte value (ceiling %d)", a, len(data), ceiling)
			}
		})
	}
}

// testMinEntrySize is the smallest encoded entry (empty trace ID).
const testMinEntrySize = 2 + 2 + 8 + 8

// maxEntriesForTest mirrors the decoder's maxEntries cap (1<<20).
const maxEntriesForTest = 1 << 20

// TestUnmarshal_CapRejectsOversizedCount: an index whose payload really
// does hold more than maxEntries entries is still refused by the cap
// (the byte bound alone would let it through). The input itself is ~21 MB,
// so no allocation ceiling applies here.
func TestUnmarshal_CapRejectsOversizedCount(t *testing.T) {
	const n = maxEntriesForTest + 1
	_, err := Unmarshal(crafted(n, n*testMinEntrySize))
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected the count-cap error, got %v", err)
	}
}

// TestUnmarshal_ExactFitAccepted: a payload with exactly count x
// testMinEntrySize bytes (all-empty trace IDs) is the tightest valid input
// and must still decode.
func TestUnmarshal_ExactFitAccepted(t *testing.T) {
	const n = 50
	in := make([]Entry, n)
	for i := range in {
		in[i] = Entry{Partition: uint16(i), StartNs: int64(i), EndNs: int64(i) + 1}
	}
	raw := Marshal(in)
	if len(raw) != 5+n*testMinEntrySize {
		t.Fatalf("unexpected encoded size %d", len(raw))
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("got %d entries want %d", len(got), n)
	}
	for i := range in {
		if got[i] != in[i] {
			t.Fatalf("entry %d: got %+v want %+v", i, got[i], in[i])
		}
	}
	// One byte short of the declared count must be rejected.
	if _, err := Unmarshal(raw[:len(raw)-1]); err == nil {
		t.Fatal("truncated payload accepted")
	}
}

// TestUnmarshal_BoundIsExact: the bound is count x minEntrySize against the
// bytes after the 5-byte header, not an approximation. 999 minimum-size
// entries fit in 19,999 bytes and 1000 do not.
func TestUnmarshal_BoundIsExact(t *testing.T) {
	_, err := Unmarshal(crafted(1000, 1000*testMinEntrySize-1))
	if err == nil || !strings.Contains(err.Error(), "exceeds payload") {
		t.Fatalf("expected the payload-bound error, got %v", err)
	}
	// Exactly enough bytes passes the bound (the zero payload then decodes
	// as 1000 empty entries).
	got, err := Unmarshal(crafted(1000, 1000*testMinEntrySize))
	if err != nil || len(got) != 1000 {
		t.Fatalf("exact fit: %d entries, err %v", len(got), err)
	}
}

// FuzzUnmarshalTraceIndex asserts the decoder stays bounded by its input:
// no panic, no allocation out of proportion to len(data), and a successful
// decode never yields more entries than the bytes can hold.
func FuzzUnmarshalTraceIndex(f *testing.F) {
	f.Add([]byte{Version, 0xF0, 0xFF, 0xFF, 0x0F}) // #745 crasher
	f.Add([]byte{Version, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(crafted(1<<20, 0))
	f.Add(Marshal([]Entry{{TraceID: "t1", Partition: 3, StartNs: 1, EndNs: 2}, {Partition: 9}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		var (
			entries []Entry
			err     error
		)
		a := allocDelta(func() { entries, err = Unmarshal(data) })
		// Generous: one Entry (40 B) per testMinEntrySize input bytes plus
		// the decoded trace IDs, plus fixed slack for runtime noise.
		if limit := uint64(8*len(data)) + 4<<20; a > limit {
			t.Fatalf("allocated %d bytes for %d input bytes (limit %d)", a, len(data), limit)
		}
		if err != nil {
			return
		}
		if len(entries) > (len(data)-5)/testMinEntrySize {
			t.Fatalf("%d entries from %d bytes", len(entries), len(data))
		}
		// Re-encoding the decoded entries is stable.
		again, err := Unmarshal(Marshal(entries))
		if err != nil || len(again) != len(entries) {
			t.Fatalf("re-decode: %v (%d vs %d)", err, len(again), len(entries))
		}
	})
}

// BenchmarkUnmarshalValid decodes a realistic valid index (10k traces,
// 32-hex IDs) to show the bound adds no measurable cost on valid input.
func BenchmarkUnmarshalValid(b *testing.B) {
	in := make([]Entry, 10_000)
	for i := range in {
		id := make([]byte, 32)
		for j := range id {
			id[j] = "0123456789abcdef"[(i*7+j*3)%16]
		}
		in[i] = Entry{TraceID: string(id), Partition: uint16(i % 1024), StartNs: int64(i), EndNs: int64(i) + 5}
	}
	raw := Marshal(in)
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Unmarshal(raw); err != nil {
			b.Fatal(err)
		}
	}
}
