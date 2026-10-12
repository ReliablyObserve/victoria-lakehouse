package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/compaction"
	lhdelete "github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
)

// TestFieldPresence_WritesObjectsForExternalReaders writes a flushed, a compacted
// and a delete-rewritten object, each with the facet, to FP_EXTERNAL_DIR together
// with the facet this code decoded (expected.json), for scripts/readers-style
// checks: pyarrow and DuckDB must open every object and read its rows, and an
// independent (Python) decoder must read the same facet from the footer.
// Skipped unless FP_EXTERNAL_DIR is set.
func TestFieldPresence_WritesObjectsForExternalReaders(t *testing.T) {
	dir := os.Getenv("FP_EXTERNAL_DIR")
	if dir == "" {
		t.Skip("set FP_EXTERNAL_DIR to write the objects for the external reader check")
	}
	installPresenceComputers(t)
	rows := genPresenceTraceRows(rand.New(rand.NewSource(9)), 600)
	for i := range rows {
		if i%2 == 0 { // the traces-style shape the delete below removes
			rows[i].ServiceName = "leaky"
		}
	}
	sorted := sortedTraceRows(rows)
	expected := map[string]any{}
	write := func(name string, data []byte, nRows int) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		f, overCap := facetOfParquet(t, data)
		var entries []map[string]any
		if f != nil {
			for _, e := range f.Entries {
				names := f.NamesOf(e)
				sort.Strings(names)
				entries = append(entries, map[string]any{"day": e.Day, "kind": string(e.Kind), "key_hex": fmt.Sprintf("%x", e.Key), "rows": e.Rows, "names": names})
			}
		}
		expected[name] = map[string]any{"rows": nRows, "over_cap": overCap, "entries": entries}
	}
	flushed, err := writeTracesParquet(sorted, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	write("flushed.parquet", flushed.Data, len(sorted))
	compacted, err := compaction.WriteTraces(sorted, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	write("compacted.parquet", compacted, len(sorted))

	pool := &presenceMemPool{objects: map[string][]byte{"traces/src.parquet": flushed.Data}}
	rw := lhdelete.NewRewriter(pool, "traces/", 100, "traces", lhdelete.WithParquetWriters(lhdelete.ParquetWriters{Logs: compaction.WriteLogs, Traces: compaction.WriteTraces, CompressionLevel: 3}))
	res, err := rw.RewriteFile(context.Background(), "traces/src.parquet", []lhdelete.Tombstone{{ID: "t", Query: `service.name:="leaky"`, StartNs: 0, EndNs: 1 << 62}})
	if err != nil {
		t.Fatal(err)
	}
	write("rewritten.parquet", pool.objects[res.NewKey], int(res.RowsKept))

	b, err := json.MarshalIndent(expected, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "expected.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}
