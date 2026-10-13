package tracesidx

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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
		// 200 stored ids (first, last and random) and 20 absent ones; the truth, one row per
		// line, for every object row so external readers can be compared value by value.
		var ids, truth strings.Builder
		rng := rand.New(rand.NewSource(int64(n) + 1))
		ids.WriteString(rows[0].TraceID + "\n" + rows[n-1].TraceID + "\n")
		for i := 0; i < 198; i++ {
			ids.WriteString(rows[rng.Intn(n)].TraceID + "\n")
		}
		for i := 0; i < 20; i++ {
			fmt.Fprintf(&ids, "%032x\n", uint64(i)*0x1234567+1)
		}
		for _, r := range rows {
			fmt.Fprintf(&truth, "%s,%d,%d,%d,%d,%d\n", r.TraceID, r.Time, r.Stream, r.Start, r.End, r.Duration)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".ids"), []byte(ids.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".truth.csv"), []byte(truth.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d rows %d bytes %d groups bloom sizes %v", name, n, len(data), info.Groups, info.BloomSize)
	}
}
