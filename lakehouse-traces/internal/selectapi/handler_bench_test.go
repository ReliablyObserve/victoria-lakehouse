package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// The served LogsQL path, end to end through the select handler, on a tier that
// holds benchBlocks blocks of benchRowsPerBlock rows, every one older than the
// latency offset (so the offset filters nothing out and only its cost shows).
// docs/perf/vt-v0.12.0-bump.md records the numbers for this benchmark on main
// (VictoriaLogs' handlers, no offset) and on the VictoriaTraces v0.12.0 branch
// (VictoriaTraces' handlers, offset on by default), and with
// disable_latency_offset=true.
const (
	benchBlocks       = 100
	benchRowsPerBlock = 1000
)

// benchTier serves fixed blocks; it prunes by block time range (as the cold
// tier prunes by row-group statistics) and does not evaluate row filters.
type benchTier struct {
	mockStore
	blocks []benchBlock
}

type benchBlock struct {
	minNs, maxNs int64
	cols         []logstorage.BlockColumn
}

func newBenchTier(now time.Time) *benchTier {
	t := &benchTier{}
	base := now.Add(-3 * time.Minute)
	for b := 0; b < benchBlocks; b++ {
		times := make([]string, benchRowsPerBlock)
		msgs := make([]string, benchRowsPerBlock)
		lvls := make([]string, benchRowsPerBlock)
		start := base.Add(-time.Duration(b) * time.Minute)
		for i := range times {
			ts := start.Add(-time.Duration(i) * time.Millisecond)
			times[i] = ts.UTC().Format(time.RFC3339Nano)
			msgs[i] = "message " + strconv.Itoa(b*benchRowsPerBlock+i)
			lvls[i] = []string{"info", "warn", "error"}[i%3]
		}
		t.blocks = append(t.blocks, benchBlock{
			minNs: start.Add(-time.Duration(benchRowsPerBlock) * time.Millisecond).UnixNano(),
			maxNs: start.UnixNano(),
			cols: []logstorage.BlockColumn{
				{Name: "_time", Values: times},
				{Name: "_msg", Values: msgs},
				{Name: "level", Values: lvls},
			},
		})
	}
	return t
}

func (s *benchTier) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	start, end := q.GetFilterTimeRange()
	for _, b := range s.blocks {
		if b.maxNs < start || b.minNs > end {
			continue
		}
		var db logstorage.DataBlock
		db.SetColumns(b.cols)
		writeBlock(0, &db)
	}
	return nil
}

func BenchmarkServedLogsQL(b *testing.B) {
	st := newBenchTier(time.Now())
	vtstorageadapter.Init(st)
	mux := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces)).Register(mux)

	cases := []struct {
		name string
		path string
		args url.Values
	}{
		{"query_limit100", "/select/logsql/query", url.Values{"query": {"* | limit 100"}, "start": {"-2h"}}},
		{"query_stats_count", "/select/logsql/query", url.Values{"query": {"* | stats count() n"}, "start": {"-2h"}}},
		{"query_stats_by_level", "/select/logsql/query", url.Values{"query": {"* | stats by (level) count() n"}, "start": {"-2h"}}},
		{"stats_query", "/select/logsql/stats_query", url.Values{"query": {"* | stats count() n"}, "start": {"-2h"}}},
		{"hits_5m", "/select/logsql/hits", url.Values{"query": {"*"}, "start": {"-2h"}, "step": {"5m"}}},
	}
	for _, c := range cases {
		for _, mode := range []string{"offset_on", "offset_off"} {
			args := url.Values{}
			for k, v := range c.args {
				args[k] = v
			}
			if mode == "offset_off" {
				args.Set("disable_latency_offset", "true")
			}
			target := c.path + "?" + args.Encode()
			b.Run(c.name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
					if rec.Code != http.StatusOK {
						b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}
