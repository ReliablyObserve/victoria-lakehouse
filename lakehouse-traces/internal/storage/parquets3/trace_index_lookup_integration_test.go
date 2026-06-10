package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// writeParquetWithTraceIndex builds a parquet file in memory whose footer
// carries a `_trace_idx` KV entry for the given index entries. extraRows
// pads the file with incompressible data so tests can force the
// footer-range-read path (file size >= minFileSizeForPrefetch).
func writeParquetWithTraceIndex(t *testing.T, entries []TraceIndexEntry, extraRows int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	rows := make([]logRow, 0, extraRows+1)
	now := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i <= extraRows; i++ {
		pad := make([]byte, 64)
		for j := range pad {
			pad[j] = byte('a' + rng.Intn(26))
		}
		rows = append(rows, logRow{
			TimestampUnixNano: now + int64(i),
			Body:              fmt.Sprintf("%s-%d", pad, rng.Int63()),
			SeverityText:      "INFO",
			ServiceName:       "svc",
		})
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf,
		parquet.Compression(&parquet.Uncompressed),
		parquet.KeyValueMetadata(traceIndexMetadataKey, string(marshalTraceIndex(entries))),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestLookupTraceIndex_AggregatesAcrossFiles is the end-to-end footer
// path: two files both index the trace; the lookup must return the
// (min start, max end) union without touching row data.
func TestLookupTraceIndex_AggregatesAcrossFiles(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data1 := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-xyz", StartNs: 1_000, EndNs: 5_000},
		{TraceID: "trace-other", StartNs: 9_000, EndNs: 9_500},
	}, 4)
	data2 := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-xyz", StartNs: 500, EndNs: 3_000},
	}, 4)
	registerFileInMockS3(t, s, mock, "logs/dt=2026-06-01/hour=10/f1.parquet", data1, base)
	registerFileInMockS3(t, s, mock, "logs/dt=2026-06-01/hour=10/f2.parquet", data2, base)

	startNs, endNs, found, err := s.LookupTraceIndex(context.Background(), "trace-xyz")
	if err != nil {
		t.Fatalf("LookupTraceIndex: %v", err)
	}
	if !found {
		t.Fatal("expected found=true for indexed trace")
	}
	if startNs != 500 || endNs != 5_000 {
		t.Errorf("aggregated range = (%d, %d), want (500, 5000) = (min start, max end)", startNs, endNs)
	}
}

// TestLookupTraceIndex_MissIsNotAuthoritative: a trace absent from every
// footer index reports found=false WITHOUT error — the adapter must fall
// through to the span-scan rewrite (footer indexes can lag ingest).
func TestLookupTraceIndex_MissIsNotAuthoritative(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-other", StartNs: 1, EndNs: 2},
	}, 2)
	registerFileInMockS3(t, s, mock, "logs/dt=2026-06-01/hour=10/f1.parquet", data, base)

	_, _, found, err := s.LookupTraceIndex(context.Background(), "trace-not-here")
	if err != nil {
		t.Fatalf("a clean miss must not error, got %v", err)
	}
	if found {
		t.Fatal("expected found=false for unindexed trace")
	}
}

func TestLookupTraceIndex_EmptyTraceIDAndEmptyManifest(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	if _, _, found, err := s.LookupTraceIndex(context.Background(), ""); err != nil || found {
		t.Errorf("empty trace ID must be a clean miss, got found=%v err=%v", found, err)
	}
	if _, _, found, err := s.LookupTraceIndex(context.Background(), "any"); err != nil || found {
		t.Errorf("empty manifest must be a clean miss, got found=%v err=%v", found, err)
	}
}

// TestLookupTraceIndex_FooterErrorsSwallowed: a manifest entry whose S3
// object is gone (404) must not fail the lookup — errors are logged,
// the metric carries the signal, and the caller falls back to the scan.
func TestLookupTraceIndex_FooterErrorsSwallowed(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/ghost.parquet",
		Size:      512 * 1024, // big enough for the footer range-read path
		MinTimeNs: base.UnixNano(),
		MaxTimeNs: base.Add(time.Minute).UnixNano(),
	})

	_, _, found, err := s.LookupTraceIndex(context.Background(), "trace-xyz")
	if err != nil {
		t.Fatalf("footer errors must be swallowed (fallback to scan), got %v", err)
	}
	if found {
		t.Fatal("expected found=false when the only file's footer is unreadable")
	}
}

// TestLookupTraceIndex_ErrorDoesNotMaskHit: one unreadable file plus one
// good file with the trace — the hit must win over the error.
func TestLookupTraceIndex_ErrorDoesNotMaskHit(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	good := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-xyz", StartNs: 100, EndNs: 200},
	}, 2)
	registerFileInMockS3(t, s, mock, "logs/dt=2026-06-01/hour=10/good.parquet", good, base)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/ghost.parquet",
		Size:      512 * 1024,
		MinTimeNs: base.UnixNano(),
		MaxTimeNs: base.Add(time.Minute).UnixNano(),
	})

	startNs, endNs, found, err := s.LookupTraceIndex(context.Background(), "trace-xyz")
	if err != nil {
		t.Fatalf("LookupTraceIndex: %v", err)
	}
	if !found || startNs != 100 || endNs != 200 {
		t.Errorf("hit must win over a sibling file's footer error: found=%v range=(%d,%d)", found, startNs, endNs)
	}
}

// TestLookupTraceIndex_CancelledContext: a cancelled context must abort
// the fan-out cleanly (no goroutine leak, no panic) and report a miss.
func TestLookupTraceIndex_CancelledContext(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-xyz", StartNs: 1, EndNs: 2},
	}, 2)
	for i := 0; i < 40; i++ {
		registerFileInMockS3(t, s, mock,
			fmt.Sprintf("logs/dt=2026-06-01/hour=10/f%02d.parquet", i), data, base)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, found, err := s.LookupTraceIndex(ctx, "trace-xyz")
	if err != nil {
		t.Fatalf("cancelled lookup must not return an error, got %v", err)
	}
	_ = found // hit-or-miss depends on how far the fan-out got; no-panic + clean return is the contract
}

// TestLookupTraceIndex_LargeFileFooterRangeRead drives the footer
// range-read path of fetchFooterFile (file >= minFileSizeForPrefetch):
// only the footer tail is fetched, and the trace is still found.
func TestLookupTraceIndex_LargeFileFooterRangeRead(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := writeParquetWithTraceIndex(t, []TraceIndexEntry{
		{TraceID: "trace-big", StartNs: 7, EndNs: 9},
	}, 3000) // ~>200KB of low-compressibility rows
	if int64(len(data)) < minFileSizeForPrefetch {
		t.Fatalf("fixture too small for the range-read path: %d < %d", len(data), minFileSizeForPrefetch)
	}
	registerFileInMockS3(t, s, mock, "logs/dt=2026-06-01/hour=10/big.parquet", data, base)

	startNs, endNs, found, err := s.LookupTraceIndex(context.Background(), "trace-big")
	if err != nil {
		t.Fatalf("LookupTraceIndex: %v", err)
	}
	if !found || startNs != 7 || endNs != 9 {
		t.Errorf("range-read footer lookup failed: found=%v range=(%d,%d), want (7,9)", found, startNs, endNs)
	}
	// Footer must now be cached for subsequent lookups.
	if _, ok := s.footerCache.Get("logs/dt=2026-06-01/hour=10/big.parquet"); !ok {
		t.Error("footer must be cached after a range-read fetch")
	}
}
