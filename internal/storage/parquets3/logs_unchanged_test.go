package parquets3

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// The trace_id_idx table is a traces-binary feature: a logs object is the same bytes
// as before it existed. The hash was taken from the writer of main for the fixture
// below (goldenLogRows, golden row-group size and compression level); the writer here
// must still produce exactly that object.
func TestLogsObjectBytesUnchangedByTheIdxTable(t *testing.T) {
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })
	res, err := writeLogsParquet(goldenLogRows(2*goldenRowGroupSize+321), goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatal(err)
	}
	const want = "1d8a0b7dc2a5d792b7b91caee7cd7f4bff6a8136df29046aba3e46bb472a2ab6"
	if got := fmt.Sprintf("%x", sha256.Sum256(res.Data)); got != want {
		t.Fatalf("a logs object changed: sha256 %s (%d bytes), want %s", got, len(res.Data), want)
	}
}
