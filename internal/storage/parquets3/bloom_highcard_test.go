package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestBloomFooter_HighCardContainsValue locks that the FOOTER bloom written with
// the production bloom config contains the high-cardinality trace_id values it was
// built from — a present value must never bloom-skip its own row group.
func TestBloomFooter_HighCardContainsValue(t *testing.T) {
	const known = "f23c3b4540b0d9c0deaa84147180052b"
	rows := []schema.LogRow{
		{TimestampUnixNano: 1, TraceID: known, ServiceName: "api-gateway", Body: "x"},
		{TimestampUnixNano: 2, TraceID: "aaaa3b4540b0d9c0deaa84147180052b", ServiceName: "worker", Body: "y"},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf, parquet.BloomFilters(bloomFilters(schema.LogBloomColumns())...))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	idx := findColumnIndex(f.Root(), "trace_id")
	if idx < 0 {
		t.Fatal("trace_id column not found")
	}
	for _, rg := range f.RowGroups() {
		bf := rg.ColumnChunks()[idx].BloomFilter()
		if bf == nil {
			t.Fatal("trace_id has no footer bloom")
		}
		found, err := bf.Check(parquet.ValueOf(known))
		if err != nil || !found {
			t.Errorf("footer bloom FALSE-NEGATIVE on present trace_id %q (found=%v err=%v)", known, found, err)
		}
	}
}

// TestBloomIndex_HighCardMayContain locks that the pmeta bloom index does not
// false-negative as cardinality grows — a present value is always reported present.
func TestBloomIndex_HighCardMayContain(t *testing.T) {
	for _, n := range []int{2, 1000, 5000, 20000} {
		vals := make([]string, n)
		for i := range vals {
			vals[i] = fmt.Sprintf("%032x", i*2654435761)
		}
		idx := bloomindex.New()
		idx.AddColumns("file1", bloomindex.BuildFileColumns(map[string][]string{"trace_id": vals}, 0.01))
		probe := vals[n/2]
		if got := idx.MayContain([]string{"file1"}, "trace_id", probe); len(got) != 1 {
			t.Errorf("n=%d: MayContain(present %q) = %v, want [file1]", n, probe, got)
		}
	}
}

// TestBloomFilterFiles_ExactHighCard locks the file-level pmeta-bloom pre-filter:
// an exact-match query for a high-cardinality trace_id present in one of many files
// (each holding thousands of distinct trace_ids) must KEEP that file, not prune it.
func TestBloomFilterFiles_ExactHighCard(t *testing.T) {
	s := testStorage()
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	partition := "dt=2026-06-01/hour=00"
	const nFiles, perFile = 10, 2000
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	var probe string
	for i := 0; i < nFiles; i++ {
		key := fmt.Sprintf("%s/file%02d.parquet", partition, i)
		vals := make([]string, perFile)
		for j := range vals {
			vals[j] = fmt.Sprintf("%032x", (i*perFile+j)*2654435761)
		}
		if i == 5 {
			probe = vals[1000]
		}
		pi.AddFile(partition, key, map[string][]string{"trace_id": vals})
		files[i] = manifest.FileInfo{Key: key, Size: 1024, MinTimeNs: now.Add(-time.Hour).UnixNano(), MaxTimeNs: now.UnixNano()}
	}
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, func(_ context.Context, p string) (*bloomindex.Index, error) {
		return pi.GetPartition(p), nil
	})
	result := s.bloomFilterFiles(context.Background(), files, fmt.Sprintf(`trace_id:=%q`, probe))
	want := fmt.Sprintf("%s/file05.parquet", partition)
	for _, f := range result {
		if f.Key == want {
			return // kept — correct
		}
	}
	t.Errorf("file with present trace_id %q was wrongly pruned (kept %d/%d files)", probe, len(result), nFiles)
}

// TestBloomFilterFiles_FacetHighCard exercises the FACET file-level bloom path
// (the one the pmeta-enabled live instance uses), not the legacy bloomCache path:
// a high-cardinality trace_id present in one file must keep that file.
func TestBloomFilterFiles_FacetHighCard(t *testing.T) {
	s := testStorage()
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, func(_ context.Context, _ string) (*bloomindex.Index, error) {
		return nil, nil
	})
	partition := "dt=2026-06-01/hour=00"
	const nFiles, perFile = 10, 2000
	files := make([]manifest.FileInfo, nFiles)
	now := time.Now()
	var probe string
	for i := 0; i < nFiles; i++ {
		key := "logs/" + partition + fmt.Sprintf("/file%02d.parquet", i)
		vals := make([]string, perFile)
		for j := range vals {
			vals[j] = fmt.Sprintf("%032x", (i*perFile+j)*2654435761)
		}
		if i == 5 {
			probe = vals[1000]
		}
		s.catalog.OnFileFlush(pmeta.FileContribution{Partition: partition, FileKey: key, BloomValues: map[string][]string{"trace_id": vals}})
		files[i] = manifest.FileInfo{Key: key, Size: 1024, MinTimeNs: now.Add(-time.Hour).UnixNano(), MaxTimeNs: now.UnixNano()}
	}
	result := s.bloomFilterFiles(context.Background(), files, fmt.Sprintf(`trace_id:=%q`, probe))
	want := "logs/" + partition + "/file05.parquet"
	for _, f := range result {
		if f.Key == want {
			return
		}
	}
	t.Errorf("FACET path false-negative: file with present trace_id %q wrongly pruned (kept %d/%d)", probe, len(result), nFiles)
}
