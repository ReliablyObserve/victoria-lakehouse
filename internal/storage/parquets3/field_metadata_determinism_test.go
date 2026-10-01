package parquets3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fmParquetDigests returns, per partition directory, the sorted SHA-256 of
// every Parquet object the mock holds. Object names carry a random suffix,
// so the comparison is by partition and content, not by key.
func fmParquetDigests(tb testing.TB, m *fmS3) map[string][]string {
	tb.Helper()
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string][]string)
	for key, data := range m.files {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		sum := sha256.Sum256(data)
		dir := path.Dir(key)
		out[dir] = append(out[dir], hex.EncodeToString(sum[:]))
	}
	for dir := range out {
		sort.Strings(out[dir])
	}
	return out
}

// TestFieldMetadataCompactedLayoutIsByteReproducible builds the compacted
// deployment of the field-metadata dataset twice and requires byte-identical
// Parquet objects in every partition, written with the Lakehouse created_by
// rather than one derived from the Go build information. The perf gate holds
// the compacted cells' s3_bytes exactly, so any run-to-run or
// toolchain-to-toolchain variation in these bytes would fail it at random.
func TestFieldMetadataCompactedLayoutIsByteReproducible(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two compacted deployments of 48k rows")
	}
	a := buildFmEnv(t, "compacted", true)
	b := buildFmEnv(t, "compacted", true)
	da, db := fmParquetDigests(t, a.mock), fmParquetDigests(t, b.mock)
	if len(da) != 2 {
		t.Fatalf("want 2 compacted partitions, got %d: %v", len(da), da)
	}
	for dir, sums := range da {
		if len(sums) != 1 {
			t.Errorf("%s: want one compacted object, got %d", dir, len(sums))
		}
		if strings.Join(sums, ",") != strings.Join(db[dir], ",") {
			t.Errorf("%s: compacted bytes differ between two builds of the same rows: %v vs %v", dir, sums, db[dir])
		}
	}

	a.mock.mu.RLock()
	defer a.mock.mu.RUnlock()
	for key, data := range a.mock.files {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if got, want := f.Metadata().CreatedBy, schema.ParquetWriterApp+" version "; !strings.HasPrefix(got, want) {
			t.Errorf("%s: created_by = %q, want prefix %q", key, got, want)
		}
	}
}

// TestFieldMetadataCompactedCountersAreDeterministic runs every compacted
// cell several times against cold caches and requires the same S3 GETs,
// bytes, row groups and pages each time. Concurrent page readers sharing one
// read-ahead window made these depend on goroutine scheduling (the
// fv_level/compacted/filter=svc cells read 32-36 GETs from identical files).
func TestFieldMetadataCompactedCountersAreDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two compacted deployments of 48k rows")
	}
	type counters struct{ gets, bytes, rgs, pages int64 }
	for _, pm := range []bool{true, false} {
		e := buildFmEnv(t, "compacted", pm)
		for _, ep := range fmEndpoints {
			for _, w := range fmWindows() {
				for _, f := range fmFilters {
					name := fmCellName(ep, e, w, f, 0)
					var first counters
					for i := 0; i < 6; i++ {
						r := e.run(t, ep, f, w, 0)
						c := counters{r.gets, r.bytes, r.rowGroups, r.pages}
						if i == 0 {
							first = c
							continue
						}
						if c != first {
							t.Errorf("%s: iteration %d read %+v, iteration 0 read %+v", name, i, c, first)
							break
						}
					}
				}
			}
		}
	}
}
