package parquets3

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// Objects whose time range the manifest only inferred (learned from the bucket
// listing, or from a snapshot an older release wrote) are never answered from
// metadata, and the inferred range is never persisted as if it were exact.
//
// Twin of internal/storage/parquets3/inferred_bounds_test.go (the logs module).

// No metadata-only answer may use an object whose bounds are still inferred.
func TestResolveFaults_InferredObjectIsNotAnsweredFromMetadata(t *testing.T) {
	inferred := manifest.FileInfo{Key: "k", RowCount: 3, MinTimeNs: rwHour.UnixNano(), MaxTimeNs: rwHour.Add(time.Hour).UnixNano() - 1, BoundsInferred: true}
	exact := inferred
	exact.BoundsInferred = false
	from, to := rwHour.Add(-time.Hour).UnixNano(), rwHour.Add(2*time.Hour).UnixNano()
	if fileFullyInRange(inferred, from, to) {
		t.Error("fileFullyInRange accepted an object with inferred bounds")
	}
	if !fileFullyInRange(exact, from, to) {
		t.Error("fileFullyInRange rejected the same object with exact bounds")
	}
	if fileWithinWindow(inferred, from, to) {
		t.Error("fileWithinWindow accepted an object with inferred bounds")
	}
	if !fileWithinWindow(exact, from, to) {
		t.Error("fileWithinWindow rejected the same object with exact bounds")
	}
	// handle404Recovery must not serve the retired object from its row count.
	s := &Storage{}
	ctx := withMetadataOnlyPlan(storage.WithTimestampOnlyHint(context.Background()), planMetadataOnly(mustParseQuery(t, "* | stats count() n")))
	emitted := 0
	s.handle404Recovery(ctx, inferred, nil, false, func(uint, *logstorage.DataBlock) { emitted++ })
	if emitted != 0 {
		t.Errorf("handle404Recovery served %d blocks for an object with inferred bounds", emitted)
	}
}

// The count pushdown (label aggregates) must not answer for an inferred object
// either, and does for the same object once its bounds are exact.
func TestResolveFaults_CountPushdownSkipsInferredObjects(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	mk := func(inferred bool) manifest.FileInfo {
		return manifest.FileInfo{
			Key: "k", RowCount: 3, MinTimeNs: rwHour.UnixNano(), MaxTimeNs: rwHour.Add(time.Hour).UnixNano() - 1, BoundsInferred: inferred,
			LabelAggregates: map[string]map[string]int64{"service.name": {"svc": 3}},
		}
	}
	from, to := rwHour.Add(-time.Hour).UnixNano(), rwHour.Add(2*time.Hour).UnixNano()
	emitted := 0
	write := func(_ uint, db *logstorage.DataBlock) { emitted += db.RowsCount() }
	if rem := s.manifestCountFastPath([]manifest.FileInfo{mk(true)}, from, to, "service.name", write); len(rem) != 1 || emitted != 0 {
		t.Errorf("inferred object: remaining=%d emitted=%d, want it left for the scan (1, 0)", len(rem), emitted)
	}
	if rem := s.manifestCountFastPath([]manifest.FileInfo{mk(false)}, from, to, "service.name", write); len(rem) != 0 || emitted != 3 {
		t.Errorf("exact object: remaining=%d emitted=%d, want it answered from metadata (0, 3)", len(rem), emitted)
	}
}

// Inferred bounds are never exported to the persisted file-metadata cache or to
// the pmeta facet, whichever way the facet is filled.
func TestResolveFaults_InferredBoundsAreNotExported(t *testing.T) {
	r := newRestartEnv(t)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false, true) // the object is learned by listing
	key := r.s.manifest.GetFilesForRange(0, 1<<62)[0].Key
	r.s.manifest.EnrichFileMetadata(key, 2, 0, 0) // rows known, bounds still inferred
	if fi, _ := r.s.manifest.GetFileByKey(key); !fi.BoundsInferred || fi.RowCount != 2 {
		t.Fatalf("precondition: %+v", fi)
	}

	t.Run("file-metadata cache", func(t *testing.T) {
		p, err := cache.NewPersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		r.s.persister = p
		r.s.saveFileMetadataToDisk()
		got, err := p.LoadFileMetadata()
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Entries) != 1 || got.Entries[0].MinTimeNs != 0 || got.Entries[0].MaxTimeNs != 0 {
			t.Errorf("persisted entries = %+v, want one entry without time bounds", got.Entries)
		}
	})

	facetBounds := func(t *testing.T) (int64, int64, bool) {
		v, ok := r.s.catalog.FileMeta(manifest.ExtractTenantPartition(key), key)
		return v.MinTimeNs, v.MaxTimeNs, ok
	}
	r.s.cfg.Pmeta.Enabled = true
	t.Run("catalog replay from the manifest", func(t *testing.T) {
		r.s.catalog = newCatalogStore(r.s.cfg.Pmeta, "logs/")
		r.s.WarmCatalog(context.Background())
		mn, mx, ok := facetBounds(t)
		if !ok || mn != 0 || mx != 0 {
			t.Errorf("facet entry after WarmCatalog = [%d, %d] ok=%v, want present without bounds", mn, mx, ok)
		}
	})
	t.Run("catalog rebuild after a lost bundle", func(t *testing.T) {
		r.s.catalog = newCatalogStore(r.s.cfg.Pmeta, "logs/")
		r.s.WarmCatalogFromS3(context.Background())
		mn, mx, ok := facetBounds(t)
		if !ok || mn != 0 || mx != 0 {
			t.Errorf("facet entry after WarmCatalogFromS3 = [%d, %d] ok=%v, want present without bounds", mn, mx, ok)
		}
	})
}

type rr3Row struct {
	T int64  `parquet:"_time"`
	M string `parquet:"_msg"`
}

// rr3File writes one row group per value (so every group has its own page index).
func rr3File(t *testing.T, vals ...int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[rr3Row](&buf)
	for _, v := range vals {
		if _, err := w.Write([]rr3Row{{v, "x"}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// rr3DropIndex rewrites the footer of data so row group rg has no column index.
func rr3DropIndex(t *testing.T, data []byte, rg int) []byte {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	md := f.Metadata()
	idx := findColumnIndex(f.Root(), "_time")
	md.RowGroups[rg].Columns[idx].ColumnIndexOffset = 0
	md.RowGroups[rg].Columns[idx].ColumnIndexLength = 0
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), md)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), data[:len(data)-8-footerLen]...)
	out = append(out, footer...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
	out = append(out, l[:]...)
	return append(out, []byte("PAR1")...)
}

// A row group without a page index may hold the newest row: the range must be
// reported as unknown, not as the range of the groups that have one.
func TestRR3_PartialPageIndexGivesNoBounds(t *testing.T) {
	data := rr3File(t, 100, 900, 500)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if rows, lo, hi := pageIndexTimeBounds(f, "_time"); rows != 3 || lo != 100 || hi != 900 {
		t.Fatalf("complete index: rows=%d [%d, %d], want 3 [100, 900]", rows, lo, hi)
	}
	partial := rr3DropIndex(t, data, 1) // the group with the newest row
	// Lazy index loading: parquet-go reads each group's index on first use, so a
	// group whose index offset is gone reports an error instead of panicking.
	pf, err := parquet.OpenFile(bytes.NewReader(partial), int64(len(partial)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatal(err)
	}
	rows, lo, hi := pageIndexTimeBounds(pf, "_time")
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}
	if lo != 0 || hi != 0 {
		t.Errorf("a partial page index reported [%d, %d]: an understated range would let a metadata-only answer miss rows", lo, hi)
	}
	if rows, lo, hi := pageIndexTimeBounds(f, "nope"); rows != 3 || lo != 0 || hi != 0 {
		t.Errorf("missing column: rows=%d [%d, %d]", rows, lo, hi)
	}
}
