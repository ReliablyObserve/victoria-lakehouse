package bloomindex

import (
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

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic
		restored, err := Unmarshal(data)
		if err != nil {
			return
		}
		// If unmarshal succeeded, re-marshal should also work
		redata := restored.Marshal()
		_ = redata
	})
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
