package traceindex

import (
	"encoding/binary"
	"math/rand"
	"reflect"
	"testing"
)

// FuzzUnmarshalIndex feeds Unmarshal arbitrary bytes; it must never panic.
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

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic. Error or success both fine.
		entries, err := Unmarshal(data)
		if err != nil {
			return
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
	})
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
