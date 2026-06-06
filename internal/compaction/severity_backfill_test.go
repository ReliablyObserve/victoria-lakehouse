package compaction

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestCompactor_BackfillsSeverityTextFromStreamTag pins the historical-
// data healing path: when a parquet was written before the insert-time
// severity fallback existed (so its rows have empty SeverityText), the
// compactor's mergeLogFiles must re-derive the value from the stream
// tag's `level` label. Without this, cold-tier rows pre-dating the
// fallback stay `level=""` forever and Grafana keeps showing them as
// "unknown" until retention rolls them off — at PB scale that's
// 7+ days of unfixable historical data per ingest pipeline change.
func TestCompactor_BackfillsSeverityTextFromStreamTag(t *testing.T) {
	// Build a parquet file with rows that have stream-tag level=WARN
	// but empty SeverityText — exactly the shape historical files
	// take.
	rows := []schema.LogRow{
		{
			AccountID:         0,
			ProjectID:         0,
			TimestampUnixNano: 1_000_000_000,
			Body:              "msg-1",
			Stream:            `{level="WARN",service.name="api-gateway"}`,
		},
		{
			AccountID:         0,
			ProjectID:         0,
			TimestampUnixNano: 2_000_000_000,
			Body:              "msg-2",
			Stream:            `{level="ERROR",service.name="api-gateway"}`,
		},
		{
			// Row that already has SeverityText: must not be touched.
			AccountID:         0,
			ProjectID:         0,
			TimestampUnixNano: 3_000_000_000,
			Body:              "msg-3",
			SeverityText:      "INFO",
			Stream:            `{level="DEBUG",service.name="api-gateway"}`, // would mismatch — keep INFO
		},
	}

	data, err := writeCompactedLogs(rows, 4, 1)
	if err != nil {
		t.Fatalf("write source parquet: %v", err)
	}

	c := &Compactor{}
	merged, err := c.mergeLogFiles([][]byte{data})
	if err != nil {
		t.Fatalf("mergeLogFiles: %v", err)
	}

	// Group merged rows by Body so the test is robust to compactor
	// sort order (which sorts by timestamp then service name).
	byBody := map[string]schema.LogRow{}
	for _, r := range merged {
		byBody[r.Body] = r
	}
	if got := byBody["msg-1"].SeverityText; got != "WARN" {
		t.Errorf("msg-1 SeverityText = %q, want WARN (lifted from stream tag)", got)
	}
	if got := byBody["msg-2"].SeverityText; got != "ERROR" {
		t.Errorf("msg-2 SeverityText = %q, want ERROR (lifted from stream tag)", got)
	}
	if got := byBody["msg-3"].SeverityText; got != "INFO" {
		t.Errorf("msg-3 SeverityText = %q, want INFO (explicit text must survive)", got)
	}
}

// TestCompactor_BackfillsSeverityTextFromSeverityNumber pins the
// second derivation path: when neither SeverityText nor the stream
// tag's level is set, the compactor falls back to VL upstream's
// FormatSeverity on the row's severity_number column.
func TestCompactor_BackfillsSeverityTextFromSeverityNumber(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "msg-info",
			SeverityNumber:    9, // Info
			Stream:            `{service.name="foo"}`,
		},
		{
			TimestampUnixNano: 2_000_000_000,
			Body:              "msg-error",
			SeverityNumber:    17, // Error
			Stream:            `{service.name="foo"}`,
		},
	}

	data, err := writeCompactedLogs(rows, 4, 1)
	if err != nil {
		t.Fatalf("write source parquet: %v", err)
	}

	c := &Compactor{}
	merged, err := c.mergeLogFiles([][]byte{data})
	if err != nil {
		t.Fatalf("mergeLogFiles: %v", err)
	}

	byBody := map[string]schema.LogRow{}
	for _, r := range merged {
		byBody[r.Body] = r
	}
	if got := byBody["msg-info"].SeverityText; got != "Info" {
		t.Errorf("msg-info SeverityText = %q, want Info", got)
	}
	if got := byBody["msg-error"].SeverityText; got != "Error" {
		t.Errorf("msg-error SeverityText = %q, want Error", got)
	}
}

// TestCompactor_BackfillLeavesEmptyWhenNoSource verifies the no-op
// path. Rows with neither severity_text, severity_number, nor a
// stream-tag level (legitimate raw-stdout / syslog-only lines)
// stay empty after compaction — the backfill must not invent
// information.
func TestCompactor_BackfillLeavesEmptyWhenNoSource(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "naked-1",
			Stream:            `{service.name="foo"}`, // no level tag, no severity_number
		},
	}

	data, err := writeCompactedLogs(rows, 4, 1)
	if err != nil {
		t.Fatalf("write source parquet: %v", err)
	}

	c := &Compactor{}
	merged, err := c.mergeLogFiles([][]byte{data})
	if err != nil {
		t.Fatalf("mergeLogFiles: %v", err)
	}
	if len(merged) != 1 {
		t.Fatalf("merged len=%d, want 1", len(merged))
	}
	if merged[0].SeverityText != "" {
		t.Errorf("SeverityText = %q, want empty (no source severity)", merged[0].SeverityText)
	}
}

// Sanity: confirm the parquet round-trip preserves the Stream field
// when we read back — the backfill code assumes Stream is intact on
// the merged rows.
func TestCompactor_BackfillRoundTrip_StreamPreserved(t *testing.T) {
	src := schema.LogRow{
		TimestampUnixNano: 1_000_000_000,
		Body:              "x",
		Stream:            `{level="WARN",service.name="api"}`,
	}
	data, err := writeCompactedLogs([]schema.LogRow{src}, 1, 1)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	got := make([]schema.LogRow, 1)
	if _, err := r.Read(got); err != nil && err.Error() != "EOF" {
		t.Fatalf("read: %v", err)
	}
	if got[0].Stream != src.Stream {
		t.Errorf("Stream after round-trip = %q, want %q", got[0].Stream, src.Stream)
	}
}
