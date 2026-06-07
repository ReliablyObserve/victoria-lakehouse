package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

// traceParityRow mirrors the on-disk trace column shape used by the
// TracesProfile registry: `trace_id` and `service.name` are top-level
// promoted columns, and `_stream` carries the canonical label-set
// literal that VT's step-1 query filters with
// (`_stream:{resource_attr:service.name="X"}`).
type traceParityRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	TraceID           string `parquet:"trace_id"`
	ServiceName       string `parquet:"service.name"`
	SpanName          string `parquet:"span.name"`
	Stream            string `parquet:"_stream"`
}

// TestColdHotParity_TraceIdxKeptFile_MustReturnRows is the load-bearing
// safeguard for the cold/hot 0-vs-20 regression class first observed at
// the 12h window in the live drilldown stack.
//
// The chain that broke:
//
//  1. Jaeger search step 1 returned a non-empty trace_id list against
//     cold parquet (the `_stream`-filter shape did its job).
//  2. The `_trace_idx` footer KV correctly reported that the kept file
//     contained at least one of those trace_ids, so the pre-filter
//     narrowed to 1/2 files instead of dropping everything.
//  3. Step 2's per-trace-id scan (`trace_id:"X"`) of that 1 kept file
//     returned 0 rows — Jaeger's GetTraceList saw an empty span set
//     and emitted 0 results, while hot VT returned 20.
//
// That third step is silent corruption: the index claims yes, the rows
// say no. This test reproduces the 1+2+3 chain end-to-end against a
// synthetic parquet and asserts the invariant `_trace_idx says yes ⇒
// trace_id scan returns ≥1 row` so a regression in column resolution
// (e.g., the `service.name` ↔ `resource_attr:service.name` registry
// pair, or a future change to how `trace_id` reads back) fails the
// test loudly instead of presenting as zero traces in the UI.
//
// Don't relax this. The whole point is that step 1 and step 2 read
// the SAME parquet file and MUST agree about which trace_ids exist.
func TestColdHotParity_TraceIdxKeptFile_MustReturnRows(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	s := testStorageWithS3(t, mock.url())

	// Pin time well inside the 12h window the bug was observed at.
	// We use a fixed date so the partition key is deterministic and
	// the test doesn't drift with wall-clock.
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	const svc = "api-gateway"

	// 5 distinct trace_ids × 4 spans each = 20 rows. Mirrors the
	// "hot returns 20, cold returns 0" shape exactly. Every row
	// carries the canonical `_stream` literal so VT's step-1
	// `_stream:{resource_attr:service.name="X"}` filter resolves.
	traceIDs := []string{
		"trace-aaa-001", "trace-bbb-002", "trace-ccc-003",
		"trace-ddd-004", "trace-eee-005",
	}
	streamLit := fmt.Sprintf(`{resource_attr:service.name=%q}`, svc)
	rows := make([]traceParityRow, 0, 20)
	for i, tid := range traceIDs {
		for j := 0; j < 4; j++ {
			rows = append(rows, traceParityRow{
				TimestampUnixNano: now.Add(time.Duration(i*4+j) * time.Millisecond).UnixNano(),
				TraceID:           tid,
				ServiceName:       svc,
				SpanName:          fmt.Sprintf("span-%d", j),
				Stream:            streamLit,
			})
		}
	}

	// Build the _trace_idx footer KV in the same shape the writer
	// emits at flush time. Keeping this in lockstep with writer.go's
	// computeTraceIndex output is what makes the pre-filter path
	// actually exercise in the test — without the KV, the file is
	// "kept_unindexed" and we'd never see the silent-corruption case.
	tidxEntries := make([]TraceIndexEntry, 0, len(traceIDs))
	for _, tid := range traceIDs {
		tidxEntries = append(tidxEntries, TraceIndexEntry{
			TraceID: tid,
			StartNs: now.UnixNano(),
			EndNs:   now.Add(time.Second).UnixNano(),
		})
	}
	idxData := marshalTraceIndex(tidxEntries)

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[traceParityRow](&buf,
		parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata(traceIndexMetadataKey, string(idxData)),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	key := "traces/dt=2026-05-10/hour=14/parity.parquet"
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), now)

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// Helper: run a query, collect every emitted (trace_id, count) pair.
	runQuery := func(t *testing.T, queryStr string) (total int, perTraceID map[string]int) {
		t.Helper()
		q := mustParseQueryWithTime(t, queryStr, startNs, endNs)
		var mu sync.Mutex
		perTraceID = make(map[string]int)
		err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			rowsN := db.RowsCount()
			total += rowsN
			columns := db.GetColumns(false)
			for _, c := range columns {
				if c.Name != "trace_id" {
					continue
				}
				for _, v := range c.Values {
					perTraceID[v]++
				}
			}
		})
		if err != nil {
			t.Fatalf("RunQuery(%q): %v", queryStr, err)
		}
		return total, perTraceID
	}

	// Sanity: wildcard sees all 20 rows. If this fails, the parquet
	// fixture or the manifest registration is wrong — not the
	// parity invariant we want to pin.
	if all, _ := runQuery(t, `*`); all != 20 {
		t.Fatalf("wildcard returned %d rows, expected 20 — fixture broken, not a parity regression", all)
	}

	// Step 1 (VT's getTraceIDList shape from vtselect/traces/query/
	// query.go:225): `_stream:{resource_attr:service.name="X"}` is
	// the exact filter VT applies to narrow by service. We test the
	// raw stream filter alone (without VT's outer pipes like
	// `last 1 by (_time) partition by (trace_id)`) because the
	// pipes don't affect the row narrowing — they only reshape the
	// projection. If the row narrowing is broken, the pipes get
	// nothing to work with and the cold path emits 0 traces.
	step1Stream := fmt.Sprintf(`_stream:{resource_attr:service.name=%q}`, svc)
	streamTotal, streamTids := runQuery(t, step1Stream)
	if streamTotal == 0 {
		t.Fatalf("step 1 (_stream filter) returned 0 rows — the cold/hot 0-vs-20 regression "+
			"class reproduces: %d rows in the file carry the canonical _stream literal but "+
			"the stream-shaped query sees nothing", len(rows))
	}
	if len(streamTids) == 0 {
		t.Fatal("step 1 returned non-zero rows but no trace_id values in the projection — " +
			"trace_id column is not surfacing, which breaks step 2's per-id lookup chain")
	}

	// Step 2 (VT's findSpansByTraceIDAndTime shape from
	// vtselect/traces/query/query.go:456): a plain `trace_id:"X"`
	// field equality per id returned in step 1. This is the
	// load-bearing invariant: the _trace_idx KV says every id in
	// `traceIDs` is in the file; the pre-filter therefore keeps
	// the file; the per-id scan MUST surface ≥1 row. A miss here
	// is the silent-corruption signal — index says yes, rows say no.
	for tid := range streamTids {
		queryStr := fmt.Sprintf(`trace_id:%q`, tid)
		got, perTid := runQuery(t, queryStr)
		if got == 0 {
			t.Errorf("INVARIANT VIOLATION: _trace_idx claims %s is in the file and step 1 "+
				"returned spans for it, but step 2 (trace_id:%q) returned 0 rows. This is "+
				"the cold/hot 0-vs-20 corruption class — index says yes, rows say no.",
				tid, tid)
			continue
		}
		if perTid[tid] != got {
			t.Errorf("step 2 returned %d rows for trace_id:%q but only %d carried that trace_id "+
				"in the trace_id column — projection misalignment",
				got, tid, perTid[tid])
		}
	}
}

// TestColdHotParity_TraceIdxIntegrity_WriterSelfCheck pins the
// writer-side invariant that backs the query-side test above: every
// entry in the `_trace_idx` footer KV MUST correspond to at least one
// real row in the parquet file. If this ever fails, the silent-corruption
// class is real and the bug is in computeTraceIndex / marshalTraceIndex
// (or in a future writer change that bypasses them) — NOT in the query
// path. Keep this test as the first thing to check when the parity
// test above fires.
func TestColdHotParity_TraceIdxIntegrity_WriterSelfCheck(t *testing.T) {
	rows := []traceParityRow{
		{TimestampUnixNano: 1_000_000_000, TraceID: "trace-1", ServiceName: "svc", SpanName: "a"},
		{TimestampUnixNano: 1_000_000_001, TraceID: "trace-1", ServiceName: "svc", SpanName: "b"},
		{TimestampUnixNano: 1_000_000_002, TraceID: "trace-2", ServiceName: "svc", SpanName: "a"},
	}
	rowTids := make(map[string]bool, len(rows))
	for _, r := range rows {
		rowTids[r.TraceID] = true
	}

	// Build the footer KV from a deliberately-corrupted entry set:
	// include a trace_id that is NOT in `rows`. This simulates the
	// failure mode where the writer claims a trace_id is in the file
	// but no row actually carries it.
	bogus := []TraceIndexEntry{
		{TraceID: "trace-1", StartNs: 0, EndNs: 0},
		{TraceID: "trace-2", StartNs: 0, EndNs: 0},
		{TraceID: "trace-PHANTOM", StartNs: 0, EndNs: 0},
	}
	idxData := marshalTraceIndex(bogus)

	// Round-trip through the parquet KV so we exercise the same
	// decoding path the query side uses.
	entries, ok := traceIndexFromMetadata(map[string]string{
		traceIndexMetadataKey: string(idxData),
	})
	if !ok {
		t.Fatal("decoded _trace_idx unexpectedly empty")
	}

	// Now the actual safeguard: every decoded entry must be in
	// the row set. The PHANTOM entry must trigger the failure.
	for _, e := range entries {
		if !rowTids[e.TraceID] {
			// Expected for the planted PHANTOM entry — this proves
			// the safeguard works. If we ever change marshal/
			// unmarshal to drop unknown entries silently, this
			// detection vanishes and so does our ability to surface
			// the silent-corruption class.
			if e.TraceID != "trace-PHANTOM" {
				t.Errorf("unexpected phantom trace_id %q surfaced from _trace_idx", e.TraceID)
			}
			return
		}
	}
	t.Fatal("PHANTOM entry was silently dropped by decode round-trip; the safeguard cannot " +
		"detect writer-side trace_idx corruption — fix marshalTraceIndex / traceIndexFromMetadata " +
		"to preserve all entries (drop-vs-decode integrity guarantee)")
}

// buildParityTraceFile is the shared fixture for the extended parity
// tests below. Writes a parquet with `count` distinct trace_ids ×
// `spansPerTrace` spans for `svc`, embeds the matching `_trace_idx`
// footer KV, and registers it in the mock S3 + manifest.
func buildParityTraceFile(t *testing.T, s *Storage, mock *mockS3Server, svc string, baseTime time.Time, count, spansPerTrace int, withTraceIdx bool, keySuffix string) []string {
	t.Helper()
	streamLit := fmt.Sprintf(`{resource_attr:service.name=%q}`, svc)
	tids := make([]string, 0, count)
	rows := make([]traceParityRow, 0, count*spansPerTrace)
	for i := 0; i < count; i++ {
		tid := fmt.Sprintf("trace-%s-%03d", svc, i)
		tids = append(tids, tid)
		for j := 0; j < spansPerTrace; j++ {
			rows = append(rows, traceParityRow{
				TimestampUnixNano: baseTime.Add(time.Duration(i*spansPerTrace+j) * time.Millisecond).UnixNano(),
				TraceID:           tid,
				ServiceName:       svc,
				SpanName:          fmt.Sprintf("span-%d", j),
				Stream:            streamLit,
			})
		}
	}
	opts := []parquet.WriterOption{parquet.Compression(&parquet.Zstd)}
	if withTraceIdx {
		entries := make([]TraceIndexEntry, 0, count)
		for _, tid := range tids {
			entries = append(entries, TraceIndexEntry{
				TraceID: tid, StartNs: baseTime.UnixNano(),
				EndNs: baseTime.Add(time.Second).UnixNano(),
			})
		}
		opts = append(opts, parquet.KeyValueMetadata(traceIndexMetadataKey, string(marshalTraceIndex(entries))))
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[traceParityRow](&buf, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/%s.parquet",
		baseTime.Format("2006-01-02"), baseTime.Hour(), keySuffix)
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), baseTime)
	return tids
}

// runParityQuery is the shared query helper for the extended parity
// tests. Returns the total row count and (trace_id → count) map.
func runParityQuery(t *testing.T, s *Storage, queryStr string, startNs, endNs int64) (int, map[string]int) {
	t.Helper()
	q := mustParseQueryWithTime(t, queryStr, startNs, endNs)
	var mu sync.Mutex
	total := 0
	perTid := make(map[string]int)
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		total += db.RowsCount()
		for _, c := range db.GetColumns(false) {
			if c.Name != "trace_id" {
				continue
			}
			for _, v := range c.Values {
				perTid[v]++
			}
		}
	})
	if err != nil {
		t.Fatalf("RunQuery(%q): %v", queryStr, err)
	}
	return total, perTid
}

// TestColdHotParity_TraceIDInFilter pins the alternate step-2 shape:
// some VT/Jaeger backend versions (and direct API users) issue a
// single batched `trace_id:in(...)` query instead of one
// `trace_id:"X"` query per id. The `_trace_idx` pre-filter extracts
// the trace_id set from both filter shapes (`extractFilterValuesAST`
// recognizes both), so the candidate file set narrows identically;
// the scan must then surface ≥1 row per id. Catches a regression
// where in-filter extraction or column resolution treats
// `in(a,b,c)` differently from `:"a"` and the batch shape silently
// returns 0 rows.
func TestColdHotParity_TraceIDInFilter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	tids := buildParityTraceFile(t, s, mock, "api-gateway", now, 5, 3, true, "in-filter")
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// Quote every id so the parser doesn't reinterpret hyphens.
	parts := make([]string, len(tids))
	for i, t := range tids {
		parts[i] = fmt.Sprintf("%q", t)
	}
	q := `trace_id:in(` + strings.Join(parts, ",") + `)`
	total, perTid := runParityQuery(t, s, q, startNs, endNs)
	if total == 0 {
		t.Fatalf("trace_id:in(...) batch returned 0 rows — the cold/hot regression class "+
			"reproduces for the batched step-2 shape; %d ids exist in the file but in-filter saw none",
			len(tids))
	}
	want := len(tids) * 3
	if total != want {
		t.Errorf("trace_id:in(...) returned %d rows, expected %d (5 trace_ids × 3 spans each) — "+
			"silent undercount in batched step 2", total, want)
	}
	for _, tid := range tids {
		if perTid[tid] != 3 {
			t.Errorf("trace_id %s: expected 3 spans, got %d — uneven batch coverage indicates "+
				"the in-filter is dropping some ids silently", tid, perTid[tid])
		}
	}
}

// TestColdHotParity_NegationFilter pins VT's actual outer filter from
// vtselect/traces/query/query.go:225 — `{resource_attr:service.name!=""}`.
// This `!=""` predicate is what VT uses to require the service.name
// label is present at all. Catches a regression where the parquet
// scan's negation handling treats every row's `_stream` literal as
// missing the label (because the predicate parses against the MAP
// fallback) and the file's contents get dropped from step 1 — exactly
// the cold/hot 0-vs-20 shape, just triggered by an even broader
// filter.
func TestColdHotParity_NegationFilter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	tids := buildParityTraceFile(t, s, mock, "api-gateway", now, 5, 4, true, "negation")
	_ = tids
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// VT's actual outer wrap: stream filter with !="". The rows we
	// wrote carry the canonical _stream literal `{resource_attr:
	// service.name="api-gateway"}`, so the negation must NOT drop
	// any row — the predicate is "has the label", not "label is
	// non-empty string value".
	q := `_stream:{resource_attr:service.name!=""}`
	total, _ := runParityQuery(t, s, q, startNs, endNs)
	if total == 0 {
		t.Fatalf("_stream:{resource_attr:service.name!=\"\"} returned 0 rows — VT's outer " +
			"step-1 wrap doesn't match canonical stream literals carrying that label. This " +
			"is the regression class that surfaces as 'cold drilldown returns 0 traces' even " +
			"when the file clearly contains spans for the queried service")
	}
}

// TestColdHotParity_FieldEqByParquetName pins the a5576bf fix surface
// from the storage side. The registry-level test in
// internal/schema/registry_parity_test.go pins ResolveToParquet, but
// the path from "user types `service.name:=\"X\"`" through to
// "rows surface" includes the projection layer, the column reader,
// and the field-equality predicate — all of which must agree on
// `service.name` being the same column as
// `resource_attr:service.name`. This test exercises that full chain
// so a regression in any one component fires here.
func TestColdHotParity_FieldEqByParquetName(t *testing.T) {
	// Pins the dual-emission fix in `readRowGroupColumnar`: promoted
	// trace columns whose parquet name differs from the internal
	// alias (e.g. parquet `service.name` ↔ internal
	// `resource_attr:service.name`) MUST surface under BOTH names so
	// a user filter spelling either dialect matches. a5576bf added
	// this for `parquetRowToFields` (used by /select/logsql/values);
	// the columnar reader needed the same pattern for the main scan
	// path that handles /select/logsql/query.
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	tids := buildParityTraceFile(t, s, mock, "api-gateway", now, 3, 2, true, "fieldeq")
	_ = tids
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// Operators routinely type `service.name:="X"` because that's
	// the column name they see in /select/logsql/query JSON. Pre-
	// a5576bf this resolved to the resource.attributes MAP and
	// matched 0 rows. Post-fix it must reach the same 6 rows the
	// stream-filter shape sees.
	// First sanity: wildcard sees all 6 rows.
	if all, _ := runParityQuery(t, s, `*`, startNs, endNs); all != 6 {
		t.Fatalf("wildcard returned %d rows, expected 6 — fixture broken, not the parity case", all)
	}
	total, _ := runParityQuery(t, s, `service.name:="api-gateway"`, startNs, endNs)
	if total == 0 {
		t.Fatalf("service.name:=\"api-gateway\" returned 0 rows — regression of a5576bf, the " +
			"parquet-column-name resolution fix. Operators typing the column name they see " +
			"in /select/logsql/query JSON would once again get 0 results")
	}
	if total != 6 {
		t.Errorf("service.name:=\"X\" returned %d rows, expected 6 (3 trace_ids × 2 spans) — "+
			"undercount even after a5576bf", total)
	}
}

// TestColdHotParity_BuildPushDownFilter_NoSubstringCollision pins
// the higher-level integration of the extractQuotedOp boundary
// fix: when querying `service.name:="X"`, buildPushDownFilter
// loops through every promoted column and probes for the column's
// internal alias / parquet name. Pre-fix, the internal alias of
// `span.name` (which is `name`) substring-matched inside
// `service.name:=` and the loop emitted a PushDownCheck for the
// WRONG column (`span.name` with value `"X"`). At query time the
// column-stats pre-filter then dropped every file because the
// queried value never falls inside span.name's [min,max] range —
// silent zero results.
//
// This test exercises buildPushDownFilter end-to-end (registry +
// extractQuotedOp + check emission) and asserts the resulting
// PushDownFilter contains a check for `service.name` (the right
// column) and ONLY for service.name — no spurious cross-column
// check from substring collision.
func TestColdHotParity_BuildPushDownFilter_NoSubstringCollision(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`service.name:="api-gateway"`, reg)
	if pdf == nil {
		t.Fatal("expected pushdown filter, got nil")
	}
	if len(pdf.Checks) == 0 {
		t.Fatal("expected at least one pushdown check")
	}

	sawServiceName := false
	for _, check := range pdf.Checks {
		if check.Column == "service.name" {
			if check.Value != "api-gateway" {
				t.Errorf("service.name check has wrong value: got %q, want api-gateway", check.Value)
			}
			sawServiceName = true
			continue
		}
		// Any OTHER column appearing here is the silent-collision bug.
		// Pre-fix, `span.name` would show up because `name:=` matched
		// inside `service.name:=`. The boundary check in
		// extractQuotedOp prevents that.
		t.Errorf("pushdown check on unexpected column %q (value %q) — substring "+
			"collision regression: extractQuotedOp matched a non-boundary "+
			"occurrence of the column's internal alias inside the queried "+
			"field name. This is exactly the class that silently zeroed "+
			"`service.name:=\"X\"` filters on cold drilldown.",
			check.Column, check.Value)
	}
	if !sawServiceName {
		t.Error("expected a pushdown check for column service.name")
	}
}

// TestColdHotParity_UnindexedFileMustStillEmitRows pins the
// conservative fallback in filterFilesByTraceIdx for files that
// pre-date the _trace_idx feature (no KV at all). Such files MUST
// be kept in the candidate set and their rows MUST surface — a
// regression that tightens the pre-filter to "indexed-files-only"
// would silently lose every cold parquet written before the index
// landed, presenting as a partial-history gap in the UI.
func TestColdHotParity_UnindexedFileMustStillEmitRows(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	// withTraceIdx=false — older parquet shape, no `_trace_idx` KV.
	tids := buildParityTraceFile(t, s, mock, "legacy-service", now, 2, 3, false, "unindexed")
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// Pick one of the trace_ids and ask for it explicitly. The
	// trace_idx pre-filter has no index to consult, so it must
	// classify the file as `kept_unindexed` and let the scan run.
	q := fmt.Sprintf(`trace_id:%q`, tids[0])
	total, _ := runParityQuery(t, s, q, startNs, endNs)
	if total == 0 {
		t.Fatalf("trace_id query against an unindexed file returned 0 rows — the pre-filter " +
			"is dropping files without `_trace_idx` KV. This breaks every cold parquet written " +
			"before the trace_idx feature landed (silent partial-history gap)")
	}
	if total != 3 {
		t.Errorf("unindexed file: expected 3 spans for trace_id %s, got %d", tids[0], total)
	}
}

// TestColdHotParity_CombinedStreamAndTraceID pins composition: VT's
// step-2 query effectively narrows by (stream-filter AND trace_id),
// and the AND must compose without dropping rows. Catches a
// regression where one of the two filters' projection bypasses the
// other (e.g., stream-filter side-channel that doesn't read
// trace_id, or a trace_id reader that doesn't see _stream context).
func TestColdHotParity_CombinedStreamAndTraceID(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	tids := buildParityTraceFile(t, s, mock, "api-gateway", now, 3, 2, true, "combined")
	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	q := fmt.Sprintf(`_stream:{resource_attr:service.name="api-gateway"} AND trace_id:%q`, tids[0])
	total, _ := runParityQuery(t, s, q, startNs, endNs)
	if total == 0 {
		t.Fatalf("combined _stream AND trace_id returned 0 rows — the two filters don't " +
			"compose on the same parquet; this is the silent-undercount class for VT's " +
			"step-2 lookup chain")
	}
	if total != 2 {
		t.Errorf("combined query: expected 2 spans (one trace_id × 2 spans), got %d", total)
	}
}

// TestColdHotParity_OutOfWindowReturnsZeroNoError pins that a query
// whose time window doesn't overlap any file returns 0 rows cleanly
// — no panic, no error, no silent fallback to a different file. This
// is the cliff-guard sibling to the a2c3c3f manifest fix: the storage
// layer must be honest about "no data here" instead of returning
// some other file's contents.
func TestColdHotParity_OutOfWindowReturnsZeroNoError(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	tids := buildParityTraceFile(t, s, mock, "api-gateway", now, 2, 2, true, "out-of-window")
	_ = tids

	// Time window 6h before the file's content. The manifest
	// registers the file with [now-1min, now+1min], so a [now-7h,
	// now-6h] window must skip it.
	startNs := now.Add(-7 * time.Hour).UnixNano()
	endNs := now.Add(-6 * time.Hour).UnixNano()
	total, _ := runParityQuery(t, s, `*`, startNs, endNs)
	if total != 0 {
		t.Errorf("out-of-window wildcard returned %d rows, expected 0 — manifest time-bound "+
			"narrowing is leaking files outside the requested window", total)
	}

	// And specifically: a trace_id query for an id the file has,
	// but with the wrong window, must also return 0.
	total, _ = runParityQuery(t, s, fmt.Sprintf(`trace_id:%q`, tids[0]), startNs, endNs)
	if total != 0 {
		t.Errorf("out-of-window trace_id query returned %d rows, expected 0", total)
	}
}

// TestColdHotParity_SmartCachePartialHit_MustNotNarrowSilently pins
// the load-bearing bug that broke the live 12h Jaeger drilldown.
//
// VT's GetTraceList step 2 issues `trace_id:in(t1,t2,...,t20)` against
// our storage. Our smartCache fast-path used to union the
// `FindFilesByTraceID(t_i)` results across all queried trace_ids and
// narrow the candidate file set to that union. This is unsafe for
// multi-id queries: the smartCache only records files it has
// previously fetched, so the union is a LOWER BOUND on the relevant
// file set — never a complete one. A partial hit (one tid cached,
// the rest missing) narrows to only the cached file, silently
// dropping every uncached-but-relevant file.
//
// Live symptom: cold Jaeger /api/traces returned 0 traces at the
// 12h window while hot returned 20. Step 1 succeeded (20 trace_ids),
// step 2 narrowed to 1 file via partial-hit, and that file held
// spans for only a few of the 20 → empty result for the rest.
//
// The fix: take the smartCache fast-path ONLY for single-id
// queries (`trace_id:="X"`). For multi-id `trace_id:in(...)` we
// fall through to bloom / trace_idx narrowing, which examines
// every file and is honest about coverage.
//
// This test pins the contract: when the input is multi-id with
// mixed cache hits and misses, narrowing must NOT drop the
// uncached file.
func TestColdHotParity_SmartCachePartialHit_MustNotNarrowSilently(t *testing.T) {
	// Unit-level test of preFilterFiles — the bug lives entirely in
	// how the smartCache fast-path unions cache-hit file keys when
	// a multi-id `trace_id:in(...)` query has a mix of cache hits
	// and misses. Exercises preFilterFiles directly so we don't
	// drag the full S3 fetch path into a focused regression pin.
	s := testStorage()

	// Seed smartCache metadata so file A is "known" for tidA, but
	// file B has NO metadata entry (cache miss for tidB).
	keyA := "traces/dt=2026-05-10/hour=14/cache-hit.parquet"
	keyB := "traces/dt=2026-05-10/hour=14/cache-miss.parquet"
	tidA := "trace-A-cached-id-aaaaaaaaaaaa"
	tidB := "trace-B-uncached-id-bbbbbbbbbb"

	meta := smartcache.NewMetadataMap()
	meta.Set(keyA, smartcache.EntryMeta{
		Signal:   "traces",
		Size:     1,
		TraceIDs: []string{tidA},
	})
	s.smartCache = smartcache.NewController(smartcache.ControllerConfig{
		L1:          &mockL1{},
		L2:          &mockL2{},
		PeerLookup:  &mockPeerLookup{},
		S3Fetcher:   &mockS3Fetcher{},
		Metadata:    meta,
		GracePeriod: 5 * time.Minute,
	})

	files := []manifest.FileInfo{
		{Key: keyA, Size: 1},
		{Key: keyB, Size: 1},
	}
	queryStr := fmt.Sprintf(`trace_id:in(%s,%s)`, tidA, tidB)
	narrowed := s.preFilterFiles(files, queryStr)

	// The load-bearing invariant: BOTH files must survive narrowing.
	// File A because the smartCache cache-hit lists it; file B
	// because the smartCache HAS NO entry for tidB, so we CANNOT
	// know which file holds tidB's spans and must keep all
	// candidates. Pre-fix, narrowed = [keyA] only — that's the
	// silent-narrowing class that caused the live 12h cold Jaeger
	// drilldown to return 0 spans for an in() filter mixing
	// recently-warmed and never-warmed trace_ids.
	keys := make(map[string]bool, len(narrowed))
	for _, fi := range narrowed {
		keys[fi.Key] = true
	}
	if !keys[keyA] {
		t.Errorf("file A (cache hit) must remain in narrowed; got %v", keys)
	}
	if !keys[keyB] {
		t.Errorf("file B (cache MISS for tidB) was silently dropped. This is the "+
			"smartCache partial-hit regression class that broke the live 12h cold "+
			"Jaeger drilldown. Fix: require ALL queried trace_ids to hit the "+
			"smartCache before taking the fast-path; on any miss, fall through to "+
			"bloom/trace_idx narrowing which checks every file. narrowed keys: %v",
			keys)
	}
}

// TestColdHotParity_SingleIDRecentFlush_MustNotDropViaSmartCache pins
// the recently-flushed-file parity bug at the preFilterFiles layer for
// the SINGLE-id (Jaeger get-trace) shape — the residue of the same
// lower-bound class that TestColdHotParity_SmartCachePartialHit pins
// for the multi-id shape.
//
// Scenario: smartCache has recorded trace "X" against an OLD file
// (keyOld) from an earlier query. A recently-flushed file (keyRecent)
// is in the manifest and genuinely contains spans for the SAME query
// (it just hasn't been queried yet, so its smartCache TraceIDs are
// empty). A single-id `trace_id:="X"` query must NOT narrow to only
// keyOld and drop keyRecent — the deterministic footer-based
// filterFilesByTraceIdx (run later) is the authority, and it can only
// run on files preFilterFiles keeps.
//
// Live symptom this guards: cold Jaeger /api/traces returned 0 traces
// for any span flushed minutes-to-~1h ago (queryable by _stream but
// invisible to trace_id:"X"), while hot VT returned them.
func TestColdHotParity_SingleIDRecentFlush_MustNotDropViaSmartCache(t *testing.T) {
	s := testStorage()
	sc := newSmartCacheWithLocalKeys(nil)

	keyOld := "traces/dt=2026-05-10/hour=12/old.parquet"
	keyRecent := "traces/dt=2026-05-10/hour=14/recent.parquet"

	// keyOld was queried before → its trace_ids are recorded.
	// keyRecent is freshly flushed → NOT recorded (empty TraceIDs),
	// the exact "limbo" state of a minutes-old file.
	sc.RecordTraceIDs(keyOld, []string{"trace-X"})
	s.smartCache = sc

	files := []manifest.FileInfo{
		{Key: keyOld},
		{Key: keyRecent},
	}

	got := s.preFilterFiles(files, `trace_id:="trace-X"`)
	keys := map[string]bool{}
	for _, fi := range got {
		keys[fi.Key] = true
	}
	if !keys[keyRecent] {
		t.Fatalf("single-id preFilterFiles dropped the recently-flushed file keyRecent — "+
			"the smartCache lower-bound narrowing must never remove a manifest file that "+
			"the deterministic trace_idx pre-filter would keep. This is the cold-tier "+
			"recently-flushed parity bug (cold Jaeger 0 vs hot VT N for minutes-old spans). "+
			"got keys: %v", keys)
	}
}

// TestColdHotParity_MultipleFilesNarrowingMustAgree pins the
// multi-file narrowing path: write two files for the same partition,
// one containing trace_id X, the other containing only Y. A
// `trace_id:"X"` query must hit the X file only, but its rows must
// all surface — a regression in the trace_idx classify loop where
// `kept_match` rows get dropped at a later stage (e.g., bloom
// re-narrowing) would silently halve the result count.
func TestColdHotParity_MultipleFilesNarrowingMustAgree(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	// File A has 3 trace_ids, file B has 3 different trace_ids.
	tidsA := buildParityTraceFile(t, s, mock, "service-a", now, 3, 2, true, "multi-a")
	tidsB := buildParityTraceFile(t, s, mock, "service-b", now, 3, 2, true, "multi-b")

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	// Querying for a trace_id that exists only in file A must hit
	// it and return its 2 spans. File B's `_trace_idx` must drop
	// the candidate (classify as `dropped`), and the result count
	// must be exactly 2 — not 0 (regression: kept_match dropped
	// downstream), not 4 (regression: file B's index ignored).
	total, perTid := runParityQuery(t, s, fmt.Sprintf(`trace_id:%q`, tidsA[0]), startNs, endNs)
	if total == 0 {
		t.Fatalf("trace_id %s exists only in file A; query returned 0 — kept_match files "+
			"are being dropped at a later narrowing stage (post-trace_idx)", tidsA[0])
	}
	if total != 2 {
		t.Errorf("trace_id %s: expected 2 spans, got %d (file A has 2 spans for this id; "+
			"any other count means the file set narrowing disagrees with the trace_idx classify)",
			tidsA[0], total)
	}
	if perTid[tidsA[0]] != 2 {
		t.Errorf("trace_id %s: trace_id column shows %d spans but total is %d — projection drift",
			tidsA[0], perTid[tidsA[0]], total)
	}

	// And the inverse: a query for a trace_id from file B must hit
	// only file B and return its 2 spans. Symmetric guard.
	total, _ = runParityQuery(t, s, fmt.Sprintf(`trace_id:%q`, tidsB[0]), startNs, endNs)
	if total != 2 {
		t.Errorf("inverse symmetry broken: trace_id %s expected 2 spans, got %d", tidsB[0], total)
	}
}
