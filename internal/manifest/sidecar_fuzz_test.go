package manifest

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

// validSidecarBytes builds a real sidecar via the writer at fuzz-time so we
// don't hard-code wire bytes. Keeps the seed independent of JSON formatting.
func validSidecarBytes(tb testing.TB) []byte {
	tb.Helper()
	sc := &FileMetaSidecar{
		Files: map[string]FileMeta{
			"dt=2026-05-20/hour=11/abc.parquet": {
				RowCount:          1000,
				MinTimeNs:         1716000000000000000,
				MaxTimeNs:         1716003600000000000,
				RawBytes:          100000,
				SchemaFingerprint: "sha256abc",
				Labels:            map[string][]string{"level": {"INFO", "ERROR"}},
			},
			"dt=2026-05-20/hour=11/def.parquet": {
				RowCount:  2000,
				MinTimeNs: 1716003600000000000,
				MaxTimeNs: 1716007200000000000,
			},
		},
	}
	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		tb.Fatalf("marshal seed: %v", err)
	}
	return data
}

// flipBit returns a copy of data with one bit flipped at byte offset off.
// If off is out of range, returns data unchanged.
func flipBit(data []byte, off int) []byte {
	if off < 0 || off >= len(data) {
		out := make([]byte, len(data))
		copy(out, data)
		return out
	}
	out := make([]byte, len(data))
	copy(out, data)
	out[off] ^= 0x01
	return out
}

// truncate returns the first len(data)-n bytes of data.
func truncate(data []byte, n int) []byte {
	if n >= len(data) {
		return []byte{}
	}
	out := make([]byte, len(data)-n)
	copy(out, data[:len(data)-n])
	return out
}

func FuzzUnmarshalFileMetaSidecar(f *testing.F) {
	valid := validSidecarBytes(f)

	// Valid baseline.
	f.Add(valid)

	// Truncated valid sidecar.
	f.Add(truncate(valid, 1))
	f.Add(truncate(valid, 10))
	f.Add(truncate(valid, 100))

	// Bit-flipped variants.
	f.Add(flipBit(valid, 0))
	f.Add(flipBit(valid, 16))
	f.Add(flipBit(valid, 64))
	if len(valid) > 0 {
		f.Add(flipBit(valid, len(valid)/2))
		f.Add(flipBit(valid, len(valid)-1))
	}

	// Empty.
	f.Add([]byte{})

	// Large random buffer (1 MiB).
	rng := rand.New(rand.NewSource(0xDEADBEEF))
	big := make([]byte, 1<<20)
	_, _ = rng.Read(big)
	f.Add(big)

	// JSON-shaped with type mismatches.
	f.Add([]byte(`{"f":"not-a-map"}`))
	f.Add([]byte(`{"f":{"key":"not-a-meta-object"}}`))
	f.Add([]byte(`{"f":{"key":{"rc":"not-an-int","mn":1,"mx":2}}}`))
	f.Add([]byte(`{"f":{"key":{"rc":1,"lb":"not-a-map"}}}`))
	f.Add([]byte(`{"f":{"key":{"rc":1,"lb":{"k":"not-a-slice"}}}}`))
	f.Add([]byte(`{"f":null}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`"string-not-object"`))
	f.Add([]byte(`{"f":{"":{}}}`))
	f.Add([]byte(`{"f":{"k":{"rc":-9223372036854775808,"mn":-1,"mx":-1}}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Determinism: two calls on the same input must yield the same
		// (result, error) — no shared mutable state between calls.
		a, errA := UnmarshalFileMetaSidecar(data)
		b, errB := UnmarshalFileMetaSidecar(data)

		if (errA == nil) != (errB == nil) {
			t.Fatalf("non-deterministic error: %v vs %v", errA, errB)
		}
		if errA != nil {
			// Errored both times — fine.
			if a != nil || b != nil {
				t.Fatalf("error path returned non-nil result: a=%v b=%v", a, b)
			}
			return
		}

		// Success path invariants.
		if a == nil || b == nil {
			t.Fatalf("nil result on success: a=%v b=%v", a, b)
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("non-deterministic result on success")
		}

		// Self-consistency: caller dereferences a.Files entries and their
		// Labels map values; ensure none of those operations panic on the
		// decoded struct.
		for key, fm := range a.Files {
			_ = key
			_ = fm.RowCount
			_ = fm.MinTimeNs
			_ = fm.MaxTimeNs
			_ = fm.RawBytes
			_ = fm.SchemaFingerprint
			for lbKey, lbVals := range fm.Labels {
				_ = lbKey
				for _, v := range lbVals {
					_ = v
				}
			}

			// Round-trip: a successfully parsed sidecar must re-marshal.
			fi := FileInfo{}
			fm.ApplyTo(&fi)
		}

		// Round-trip the whole sidecar.
		remarshalled, err := MarshalFileMetaSidecar(a)
		if err != nil {
			t.Fatalf("re-marshal of successfully parsed sidecar failed: %v", err)
		}
		if !bytes.Contains(remarshalled, []byte(`"f":`)) && len(a.Files) > 0 {
			t.Fatalf("re-marshalled sidecar missing files envelope")
		}
	})
}
