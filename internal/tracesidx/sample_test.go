package tracesidx

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteSampleObjects writes objects for the external-reader proof when
// T6_SAMPLE_DIR is set (it writes nothing otherwise): a small one, a one-group one
// whose row count is not a power of two, and a multi-group one.
func TestWriteSampleObjects(t *testing.T) {
	dir := os.Getenv("T6_SAMPLE_DIR")
	if dir == "" {
		t.Skip("T6_SAMPLE_DIR is not set")
	}
	for name, n := range map[string]int{"small": 6553, "multi": 3*RowsPerGroup + 12345, "odd": 100003} {
		rows := randRows(rand.New(rand.NewSource(int64(n))), n)
		data, info, err := Marshal(rows, 3)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".parquet"), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".ids"), []byte(rows[n/2].TraceID+"\n"+rows[0].TraceID+"\n"+rows[n-1].TraceID+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d rows %d bytes %d groups bloom sizes %v", name, n, len(data), info.Groups, info.BloomSize)
	}
}
