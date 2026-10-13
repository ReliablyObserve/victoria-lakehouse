package compaction

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

func idxTestRows(rng *rand.Rand, n int, dup []string) []tracesidx.Row {
	rows := make([]tracesidx.Row, 0, n+len(dup))
	for i := 0; i < n; i++ {
		var id [16]byte
		rng.Read(id[:])
		h := hex.EncodeToString(id[:])
		s := int64(1_800_000_000_000_000_000) + rng.Int63n(3_600e9)
		d := rng.Int63n(5e9)
		rows = append(rows, tracesidx.Row{Time: s, Stream: tracesidx.StreamOf(h), TraceID: h, Start: s, End: s + d, Duration: d})
	}
	for _, h := range dup { // index rows of the same trace written in two index windows
		s := int64(1_800_000_000_000_000_000) + rng.Int63n(3_600e9)
		rows = append(rows, tracesidx.Row{Time: s, Stream: tracesidx.StreamOf(h), TraceID: h, Start: s, End: s + 10, Duration: 10})
	}
	return rows
}

// structure is everything about an idx object that prunability depends on, and nothing
// that depends on the data: the golden comparison of a flushed and a compacted object.
func structure(t *testing.T, data []byte) string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, c := range f.Schema().Columns() {
		fmt.Fprintf(&b, "col %v\n", c)
	}
	for _, kv := range f.Metadata().KeyValueMetadata {
		if kv.Key == tracesidx.FormatKey {
			fmt.Fprintf(&b, "kv %s=%s\n", kv.Key, kv.Value)
		}
	}
	for i, g := range f.Metadata().RowGroups {
		for j, c := range g.Columns {
			fmt.Fprintf(&b, "rg%d col%d codec=%v hasColIdx=%v hasOffIdx=%v bloom=%v\n", i, j, c.MetaData.Codec, c.ColumnIndexLength > 0, c.OffsetIndexLength > 0, c.MetaData.BloomFilterLength > 0)
			if c.MetaData.BloomFilterLength > 0 {
				// header (a few bytes of Thrift) + a power-of-two bitset
				length, ok := int(c.MetaData.BloomFilterLength), false
				for p := 32; p <= length; p <<= 1 {
					if length-p >= 0 && length-p < 24 {
						ok = true
					}
				}
				if !ok {
					t.Errorf("rg%d: bloom filter section of %d bytes is not a header and a power-of-two bitset", i, length)
				}
			}
		}
	}
	return b.String()
}

func TestIdxCompaction_GoldenFlushedVersusCompacted(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	a := idxTestRows(rng, 3000, nil)
	b := idxTestRows(rng, 2000, []string{a[5].TraceID, a[17].TraceID})
	c := idxTestRows(rng, 1000, []string{a[5].TraceID})
	pool := newMockPool()
	m := manifest.New("bucket", "0/0/traces/")
	const partition = "dt=2026-10-13/hour=10"
	const prefix = "0/0/traces_idx/" + partition + "/"
	var files []manifest.FileInfo
	var all []tracesidx.Row
	for i, rows := range [][]tracesidx.Row{a, b, c} {
		all = append(all, rows...)
		data, info, err := tracesidx.Marshal(append([]tracesidx.Row(nil), rows...), 7)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s%d-0.parquet", prefix, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: info.Rows, MinTimeNs: info.MinTime, MaxTimeNs: info.MaxTime, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}
	comp := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "0/0/traces/", Mode: config.ModeTraces, RowGroupSize: 1000, CompressionLevel: 7})
	res, err := comp.Compact(context.Background(), partition, files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.RowsMerged != int64(len(all)) {
		t.Fatalf("merged %d rows, want %d: compaction must not merge or drop index rows", res.RowsMerged, len(all))
	}
	if !strings.HasPrefix(res.OutputFile, "0/0/traces_idx/") {
		t.Fatalf("the output %q left the idx table's directory", res.OutputFile)
	}
	pool.mu.Lock()
	compacted := pool.uploaded[res.OutputFile]
	pool.mu.Unlock()

	flushed, _, err := tracesidx.Marshal(append([]tracesidx.Row(nil), all...), 7)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := structure(t, compacted), structure(t, flushed); got != want {
		t.Fatalf("compacted structure differs from a flush of the same rows:\n%s\nvs\n%s", got, want)
	}
	// With the same compression level the two objects are the same bytes: the compaction
	// and the flush are one function.
	if !bytes.Equal(compacted, flushed) {
		t.Fatal("the compacted object is not byte-identical to a flush of the same rows at the same level")
	}
	rows, err := tracesidx.ReadAll(compacted)
	if err != nil || len(rows) != len(all) {
		t.Fatalf("read back %d rows (%v), want %d", len(rows), err, len(all))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].TraceID > rows[i].TraceID {
			t.Fatalf("rows %d and %d are out of order", i-1, i)
		}
	}
	// The duplicated trace keeps all its rows (hot VictoriaTraces counts them).
	n := 0
	for _, r := range rows {
		if r.TraceID == a[5].TraceID {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("trace %s has %d rows after compaction, want 3", a[5].TraceID, n)
	}
	// Source objects are gone, the manifest has the output only.
	if got := m.FilesForPartition(partition); len(got) != 1 || got[0].Key != res.OutputFile {
		t.Fatalf("manifest after compaction: %v", got)
	}
}

// Other compression levels change the codec's effort, never the structure.
func TestIdxCompaction_StructureIndependentOfLevel(t *testing.T) {
	rows := idxTestRows(rand.New(rand.NewSource(8)), 4000, nil)
	a, _, _ := tracesidx.Marshal(append([]tracesidx.Row(nil), rows...), 1)
	b, _, _ := tracesidx.Marshal(append([]tracesidx.Row(nil), rows...), 15)
	if structure(t, a) != structure(t, b) {
		t.Fatal("the structure depends on the compression level")
	}
}
