package traceindex

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
)

// FuzzUnmarshalIndex feeds Unmarshal arbitrary bytes; it must never panic,
// must not allocate out of proportion to its input (#745) and must not
// return more entries than the bytes can hold.
// Returning an error is allowed (and expected for malformed inputs); the
// trace-index codec contract (commit 5bab58f — _trace_idx footer KV) only
// guarantees a non-panicking decode.
//
// API under test (internal/traceindex/traceindex.go):
//
//	func Unmarshal(data []byte) ([]Entry, error)
func FuzzUnmarshalIndex(f *testing.F) {
	// Seed: a valid marshaled index from a small set of entries.
	good := Marshal([]Entry{
		{TraceID: "trace-a", Partition: 1, StartNs: 10, EndNs: 20},
		{TraceID: "trace-b-longer-id", Partition: 999, StartNs: -5, EndNs: 50000},
	})
	f.Add(good)

	// Seed: truncated copies of a valid payload.
	for i := 1; i < len(good); i++ {
		f.Add(good[:i])
	}

	// Seed: single-bit-flipped copies (cover header version, count, entry len, payload).
	flip := func(b []byte, off int, mask byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		if off < len(out) {
			out[off] ^= mask
		}
		return out
	}
	f.Add(flip(good, 0, 0xFF)) // bad version
	f.Add(flip(good, 1, 0x01)) // perturb count LSB
	f.Add(flip(good, 4, 0x80)) // perturb count MSB (huge count)
	f.Add(flip(good, 5, 0xFF)) // perturb first traceIDLen

	// Seed: empty buffer.
	f.Add([]byte{})

	// Seed: 1 MB of random bytes (deterministic seed for reproducibility).
	rng := rand.New(rand.NewSource(0xC0FFEE))
	big := make([]byte, 1<<20)
	_, _ = rng.Read(big)
	f.Add(big)

	// Seed: length-prefix lying about content size.
	// Version=1, count claims 1_000_000 entries but no entry payload follows.
	bigCount := make([]byte, 5)
	bigCount[0] = Version
	binary.LittleEndian.PutUint32(bigCount[1:5], 1_000_000)
	f.Add(bigCount)

	// Seed: version=1, count=1, then a tidLen claiming 0xFFFF bytes with no payload.
	lyingLen := []byte{Version, 1, 0, 0, 0, 0xFF, 0xFF}
	f.Add(lyingLen)

	// Seeds for #745: a 5-byte value declaring 268 M entries, and the
	// largest count the cap lets through with no payload at all.
	f.Add([]byte{Version, 0xF0, 0xFF, 0xFF, 0x0F})
	f.Add([]byte{Version, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add(crafted(1<<20, 0))

	// Seeds for #768: indexes written before the fix with a wrapped trace
	// ID length (the decoder must refuse them), and payloads whose first
	// bytes pick a trace ID length around the uint16 limit.
	f.Add(legacyMarshal(longThenNormal(1<<16 + 5)))
	f.Add(legacyMarshal(longThenNormal(1 << 17)))
	f.Add(append(append([]byte{}, good...), 0))
	f.Add([]byte{0xFF, 0xFF, 0x00, 0x00, 'a'})
	f.Add([]byte{0x00, 0x00, 0x01, 0x00, 'a'})
	f.Add([]byte{0x01, 0x00, 0x01, 0x00, 'a'})

	f.Fuzz(func(t *testing.T, data []byte) {
		checkRoundTripOrOmitted(t, data)
		// Must never panic. Error or success both fine. The allocation
		// must stay in proportion to the input (generous: one 40-byte
		// Entry per 20 input bytes plus the decoded IDs, plus slack for
		// the process-wide TotalAlloc counter).
		var (
			entries []Entry
			err     error
		)
		a := allocDelta(func() { entries, err = Unmarshal(data) })
		if limit := uint64(8*len(data)) + 4<<20; a > limit {
			t.Fatalf("allocated %d bytes for %d input bytes (limit %d)", a, len(data), limit)
		}
		if err != nil {
			return
		}
		if len(entries) > (len(data)-5)/testMinEntrySize {
			t.Fatalf("%d entries from %d bytes", len(entries), len(data))
		}
		// On success, every entry's TraceID must be a valid (possibly
		// empty) string and Partition must be a uint16 — both are
		// type-enforced, so just touch them to ensure no aliasing
		// surprises with sliced backing memory.
		for _, e := range entries {
			_ = len(e.TraceID)
			_ = e.Partition
			_ = e.StartNs
			_ = e.EndNs
		}
		// #768: a payload that decodes is exactly what Marshal writes for
		// those entries (the decoder consumed all of it), so no garbage can
		// pass as an index.
		if re := Marshal(entries); !bytes.Equal(re, data) {
			t.Fatalf("decoded %d entries from %d bytes but Marshal gives %d bytes", len(entries), len(data), len(re))
		}
	})
}

// roundTripIndexIDLen picks a trace ID length from the fuzz input, covering
// the uint16 boundary and far above it, bounded to 3 x 65536 bytes.
func roundTripIndexIDLen(data []byte) int {
	if len(data) < 4 {
		return len(data)
	}
	return int(binary.LittleEndian.Uint32(data[:4])) % (3 << 16)
}

// checkRoundTripOrOmitted is the round-trip half of FuzzUnmarshalIndex (#768): entries
// built from the fuzz input with a trace ID of any length up to 3 x 65536
// bytes either round-trip exactly or are not indexed at all (Marshal returns
// nil, which the writers treat as "no index"). A wrapped index that decodes
// into other entries is never produced.
func checkRoundTripOrOmitted(t *testing.T, data []byte) {
	idLen := roundTripIndexIDLen(data)
	id := make([]byte, idLen)
	for i := range id {
		if len(data) > 0 {
			id[i] = data[i%len(data)]
		}
	}
	in := []Entry{
		{TraceID: string(id), Partition: 1, StartNs: 10, EndNs: 20},
		{TraceID: "normal-trace-id", Partition: 2, StartNs: 30, EndNs: 40},
	}
	raw := Marshal(in)
	if idLen > MaxTraceIDLen {
		if raw != nil {
			t.Fatalf("a %d-byte trace ID was indexed (%d bytes)", idLen, len(raw))
		}
		return
	}
	if raw == nil {
		t.Fatalf("a %d-byte trace ID is representable but Marshal omitted the index", idLen)
	}
	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal of self-Marshal failed: %v", err)
	}
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("round-trip mismatch for a %d-byte trace ID", idLen)
	}
}

// FuzzRoundTripIndex feeds randomly-shaped Entry slices through
// Marshal → Unmarshal and asserts round-trip equality. The codec doesn't
// validate semantics (StartNs > EndNs is allowed), only round-trip
// fidelity — that's what this fuzz pins down.
//
// API under test:
//
//	func Marshal(entries []Entry) []byte
//	func Unmarshal(data []byte) ([]Entry, error)
func FuzzRoundTripIndex(f *testing.F) {
	// Seed: empty.
	f.Add(uint8(0), []byte{}, int64(0), int64(0))
	// Seed: one entry.
	f.Add(uint8(1), []byte("trace-1"), int64(100), int64(200))
	// Seed: duplicate TraceIDs (n=4 entries, all sharing one ID).
	f.Add(uint8(4), []byte("dup"), int64(0), int64(0))
	// Seed: StartNs > EndNs.
	f.Add(uint8(1), []byte("t"), int64(1_000_000), int64(1))
	// Seed: extreme int64 values.
	f.Add(uint8(2), []byte("x"), int64(-9223372036854775808), int64(9223372036854775807))
	// Seed: empty TraceID (the codec allows it).
	f.Add(uint8(3), []byte(""), int64(7), int64(7))

	f.Fuzz(func(t *testing.T, n uint8, tidSeed []byte, startNs, endNs int64) {
		// Bound the entry count so the fuzzer doesn't allocate gigabytes.
		count := int(n)
		if count > 32 {
			count = 32
		}
		// TraceIDs can be arbitrary bytes (the wire format is length-
		// prefixed); we cap length to keep the corpus small.
		if len(tidSeed) > 256 {
			tidSeed = tidSeed[:256]
		}

		entries := make([]Entry, count)
		for i := 0; i < count; i++ {
			// Mix in i so we cover both identical-TraceID and varying-TraceID shapes.
			tid := append([]byte{}, tidSeed...)
			if i%2 == 1 {
				tid = append(tid, byte(i))
			}
			entries[i] = Entry{
				TraceID:   string(tid),
				Partition: uint16(i),
				StartNs:   startNs + int64(i),
				EndNs:     endNs + int64(i),
			}
		}

		raw := Marshal(entries)
		got, err := Unmarshal(raw)
		if err != nil {
			t.Fatalf("Unmarshal of self-Marshal failed: %v", err)
		}
		// Normalise nil vs empty so DeepEqual is well-behaved for the
		// zero-entry case.
		if len(entries) == 0 && len(got) == 0 {
			return
		}
		if !reflect.DeepEqual(got, entries) {
			t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", got, entries)
		}
	})
}
