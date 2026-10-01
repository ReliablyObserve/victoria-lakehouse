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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fmtParquetDigests returns, per partition directory, the sorted SHA-256 of
// every Parquet object the mock holds. Object names carry a random suffix,
// so the comparison is by partition and content, not by key.
func fmtParquetDigests(m *fmtS3) map[string][]string {
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

// TestFieldMetadataTracesCompactedLayoutIsByteReproducible is the traces twin
// of the logs test: the compacted deployment built twice from the same spans
// must hold byte-identical Parquet objects. The `_trace_idx` footer value was
// serialised in Go map order, so compacted trace files differed run to run;
// created_by came from the Go build information, so they differed between
// toolchains.
func TestFieldMetadataTracesCompactedLayoutIsByteReproducible(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two compacted deployments")
	}
	a := buildFmtEnv(t, "compacted", true)
	b := buildFmtEnv(t, "compacted", true)
	da, db := fmtParquetDigests(a.mock), fmtParquetDigests(b.mock)
	if len(da) != 2 {
		t.Fatalf("want 2 compacted partitions, got %d: %v", len(da), da)
	}
	for dir, sums := range da {
		if len(sums) != 1 {
			t.Errorf("%s: want one compacted object, got %d", dir, len(sums))
		}
		if strings.Join(sums, ",") != strings.Join(db[dir], ",") {
			t.Errorf("%s: compacted bytes differ between two builds of the same spans: %v vs %v", dir, sums, db[dir])
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

// TestFieldMetadataTracesCompactedCountersAreDeterministic runs every
// compacted traces cell several times against cold caches and requires the
// same S3 GETs and bytes each time. No traces cell reads several columns
// through one read-ahead window today (the traces matrix reads the same counters
// in sync and async mode), so this guards against a future read path that
// does, rather than reproducing the logs failure.
func TestFieldMetadataTracesCompactedCountersAreDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("builds two compacted deployments")
	}
	for _, pm := range []bool{true, false} {
		e := buildFmtEnv(t, "compacted", pm)
		for _, ep := range fmtEndpoints {
			for _, w := range fmtWindows() {
				for _, f := range []string{"none", "svc"} {
					name := fmtCellName(ep, e, w, f, 0)
					first := e.run(t, ep, f, w, 0)
					for i := 1; i < 6; i++ {
						r := e.run(t, ep, f, w, 0)
						if r.Gets != first.Gets || r.Bytes != first.Bytes {
							t.Errorf("%s: iteration %d read %d GETs / %d B, iteration 0 read %d GETs / %d B",
								name, i, r.Gets, r.Bytes, first.Gets, first.Bytes)
							break
						}
					}
				}
			}
		}
	}
}

// TestRangedOpenOptions_ReadMode pins which parquet-go page read mode a
// ranged open gets: sync by default (and for an unset mode), async only when
// s3.parquet_read_mode asks for it.
func TestRangedOpenOptions_ReadMode(t *testing.T) {
	s := &Storage{cfg: testConfig()}
	fi := manifest.FileInfo{Key: "k", Size: 1 << 20}
	for _, tc := range []struct {
		mode string
		want parquet.ReadMode
	}{
		{testConfig().S3.ParquetReadMode, parquet.ReadModeSync},
		{"", parquet.ReadModeSync},
		{"sync", parquet.ReadModeSync},
		{"async", parquet.ReadModeAsync},
	} {
		s.cfg.S3.ParquetReadMode = tc.mode
		cfg := parquet.DefaultFileConfig()
		cfg.Apply(s.rangedOpenOptions(fi, nil)...)
		if cfg.ReadMode != tc.want {
			t.Errorf("parquet_read_mode %q: read mode %v, want %v", tc.mode, cfg.ReadMode, tc.want)
		}
	}
}
