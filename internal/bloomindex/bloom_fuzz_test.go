package bloomindex

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func FuzzBloomMarshalUnmarshal(f *testing.F) {
	f.Add("hello")
	f.Add("trace-abc123")
	f.Add("")
	f.Add("a")
	f.Add("very-long-trace-id-with-lots-of-characters-1234567890abcdef")

	f.Fuzz(func(t *testing.T, value string) {
		filter := NewFilter(100, 0.01)
		filter.Add(value)

		data := filter.Marshal()
		restored, err := UnmarshalFilter(data)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if !restored.MayContain(value) {
			t.Errorf("roundtrip lost value %q", value)
		}
	})
}

func FuzzBloomCorruptInput(f *testing.F) {
	filter := NewFilter(50, 0.01)
	filter.Add("seed-value")
	validData := filter.Marshal()

	f.Add(validData)
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add([]byte{2, 0, 0, 0, 0})
	f.Add([]byte{255, 255, 255, 255})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic — error return is fine
		_, _ = UnmarshalFilter(data)
	})
}

func FuzzIndexMarshalUnmarshal(f *testing.F) {
	idx := New()
	idx.Add("file1", "trace_id", filterWith("abc"))
	validData := idx.Marshal()

	f.Add(validData)
	f.Add([]byte{2, 0, 0, 0, 0})
	// #763: 5-byte values declaring 1,000,000 entries (v1 and v2) used to
	// preallocate ~56 MB each; and a v2 entry declaring 65535 columns.
	f.Add([]byte{1, 0x40, 0x42, 0x0F, 0x00})
	f.Add([]byte{2, 0x40, 0x42, 0x0F, 0x00})
	f.Add([]byte{2, 1, 0, 0, 0, 0, 0, 0xFF, 0xFF})
	f.Add([]byte{2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF}) // column count declared by a LATER entry

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic. The allocation must stay in proportion to the
		// input (#763): the index is untrusted bucket data and a declared
		// count must not drive a preallocation the bytes cannot back.
		// runtime.ReadMemStats stops the world, so it is only sampled for
		// inputs that declare a large count or a large column count, the
		// shapes that used to allocate out of proportion.
		var (
			restored *Index
			err      error
		)
		if declaresLargeCount(data) {
			a := allocDelta(func() { restored, err = Unmarshal(data) })
			// Linear in the input plus 4 MiB of slack for the process-wide
			// TotalAlloc counter (assumed generous; the crashers allocated
			// ~56 MB for 5 bytes). The coefficient is 64, not the 8 used for
			// the flat-struct trace index: every bloom entry allocates a map
			// and a Filter, and a VALID index of minimum-size entries was
			// measured at 26x (v2, 4 B/entry) and 44x (v1, 8 B/entry) its
			// size at the 1,000,000-entry cap.
			if limit := uint64(64*len(data)) + 4<<20; a > limit {
				t.Fatalf("allocated %d bytes for %d input bytes (limit %d)", a, len(data), limit)
			}
		} else {
			restored, err = Unmarshal(data)
		}
		if err != nil {
			return
		}
		// If unmarshal succeeded, re-marshal should also work
		redata := restored.Marshal()
		_ = redata
	})
}

// declaresLargeCount reports whether data is a v1/v2 value whose header
// declares more than 1024 entries, or (v2) in which any entry declares more
// than 1024 columns: the shapes whose preallocation was not bounded by the
// payload. It walks the entries as the decoder does, so it is bounded by
// len(data), and stops at the first byte it cannot follow.
func declaresLargeCount(data []byte) bool {
	if len(data) < 5 || (data[0] != 1 && data[0] != 2) {
		return false
	}
	count := binary.LittleEndian.Uint32(data[1:5])
	if count > 1024 {
		return true
	}
	if data[0] != 2 {
		return false
	}
	pos := 5
	for i := uint32(0); i < count; i++ {
		if pos+2 > len(data) {
			return false
		}
		pos += 2 + int(binary.LittleEndian.Uint16(data[pos:]))
		if pos+2 > len(data) {
			return false
		}
		cols := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if cols > 1024 {
			return true
		}
		for c := 0; c < cols; c++ {
			if pos+1 > len(data) {
				return false
			}
			pos += 1 + int(data[pos])
			if pos+4 > len(data) {
				return false
			}
			pos += 4 + int(binary.LittleEndian.Uint32(data[pos:]))
			if pos < 0 || pos > len(data) {
				return false
			}
		}
	}
	return false
}

func FuzzChecksumTamper(f *testing.F) {
	idx := New()
	for i := 0; i < 5; i++ {
		idx.Add(fmt.Sprintf("file%d", i), "trace_id", filterWith(fmt.Sprintf("val%d", i)))
	}
	validData := MarshalWithChecksum(idx)

	f.Add(validData)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic
		_, _ = UnmarshalWithChecksum(data)
	})
}
