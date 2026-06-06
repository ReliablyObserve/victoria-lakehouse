package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// parityRow mirrors logRow but also carries a _stream column so the
// stream-filter ({service.name="…"}) shape has a concrete column to
// match against. VL's stream filter doesn't read service.name directly —
// it reads _stream and tests whether the stream's label set contains
// service.name=<value>. Without _stream, the stream-filter path returns
// 0 unconditionally and the parity assertion becomes vacuous.
type parityRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
	Stream            string `parquet:"_stream"`
}

// TestFieldEqualityAndStreamFilter_ReturnSameRows pins the diagnostic
// asymmetry that exposed the be8c126 compactor-labels / unindexed-file
// inclusion bug.
//
// Field-equality syntax  : service.name:="api-gateway"
// Stream-filter syntax   : {service.name="api-gateway"}
//
// Both should ultimately match the same rows. A regression that tightens
// filterByLabelIndex back to "indexed-files-only" silently undercounts
// the stream-filter shape (~80% loss observed during the be8c126
// investigation) while leaving the field-equality path intact.
//
// We write a small parquet with rows split evenly across 4 services and
// assert the two query shapes return the same row count for the same
// service. This is the load-bearing parity invariant — keep it.
func TestFieldEqualityAndStreamFilter_ReturnSameRows(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()

	// Use LogsProfile here: the parquet fixture uses flat `service.name`
	// schema (logRow), matching VL's logs column shape. TracesProfile
	// would route resolution through `resource_attr:service.name`, which
	// is a separate concern from the field-eq vs stream-eq parser parity.
	pool := testPool(t, mock.url())
	cfg := testConfig()
	cfg.S3.ReadAheadBytes = 4096
	cfg.S3.CoalesceGapBytes = 1024
	s := &Storage{
		cfg:         cfg,
		pool:        pool,
		manifest:    manifest.New("test-bucket", "logs/"),
		registry:    schema.NewRegistry(schema.LogsProfile),
		memCache:    cache.NewLRU(64 * 1024 * 1024),
		sfGroup:     cache.NewGroup(),
		labelIndex:  cache.NewLabelIndex(),
		discovery:   discovery.New("", nil, "", "", "9428", 5*time.Second),
		footerCache: NewFooterCache(1000),
		dlSem:       make(chan struct{}, 4),
	}

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	services := []string{"api-gateway", "payment-service", "notification-service", "order-service"}

	// 100 rows split evenly: 25 per service. _stream is a label-set
	// literal so VL's stream filter resolves `{service.name="…"}`
	// against it the same way it would against an indexed stream.
	rows := make([]parityRow, 0, 100)
	for i := 0; i < 100; i++ {
		svc := services[i%len(services)]
		rows = append(rows, parityRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              "msg",
			SeverityText:      "INFO",
			ServiceName:       svc,
			Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
		})
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[parityRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	key := "logs/dt=2026-05-10/hour=14/parity.parquet"
	registerFileInMockS3(t, s, mock, key, data, now)

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()

	count := func(t *testing.T, queryStr string) int {
		t.Helper()
		q := mustParseQueryWithTime(t, queryStr, startNs, endNs)
		var mu sync.Mutex
		total := 0
		err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			total += db.RowsCount()
			mu.Unlock()
		})
		if err != nil {
			t.Fatalf("RunQuery(%q): %v", queryStr, err)
		}
		return total
	}

	// Sanity: wildcard must return all 100 rows. If this fails, the manifest
	// registration or time range is wrong — not the parser asymmetry we're
	// pinning.
	if all := count(t, `*`); all != 100 {
		t.Fatalf("wildcard returned %d rows, expected 100 — test fixture broken, not a parser regression", all)
	}

	fieldEq := count(t, `service.name:="api-gateway"`)
	streamEq := count(t, `{service.name="api-gateway"}`)

	if fieldEq != streamEq {
		t.Fatalf("parity regression: field-equality returned %d rows, stream-filter returned %d rows — "+
			"this is the ~80%% undercount class from be8c126. Likely cause: filterByLabelIndex re-tightened "+
			"to indexed-files-only, dropping unindexed files for the stream-filter shape.",
			fieldEq, streamEq)
	}

	// Sanity: we wrote 25 rows for api-gateway. Both paths must hit them.
	if fieldEq == 0 {
		t.Fatalf("both queries returned 0 rows; expected ~25 for api-gateway out of 100 total")
	}
}
