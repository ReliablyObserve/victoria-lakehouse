package selectapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// spanStore answers every query with the same two spans of one trace; the
// adapter runs the query's pipes over them, exactly as it does over cold rows.
type spanStore struct {
	mockStore
	start, end time.Time
}

var _ storage.Storage = (*spanStore)(nil)

func (s *spanStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	ns := func(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{s.start.UTC().Format(time.RFC3339Nano), s.start.Add(time.Second).UTC().Format(time.RFC3339Nano)}},
		{Name: "trace_id", Values: []string{"0af7651916cd43dd8448eb211c80319c", "0af7651916cd43dd8448eb211c80319c"}},
		{Name: "span_id", Values: []string{"b7ad6b7169203331", "00f067aa0ba902b7"}},
		{Name: "parent_span_id", Values: []string{"", "b7ad6b7169203331"}},
		{Name: "name", Values: []string{"GET /orders", "SELECT orders"}},
		{Name: "resource_attr:service.name", Values: []string{"checkout", "checkout"}},
		{Name: "start_time_unix_nano", Values: []string{ns(s.start), ns(s.start.Add(time.Second))}},
		{Name: "end_time_unix_nano", Values: []string{ns(s.end), ns(s.end.Add(-time.Second))}},
	})
	writeBlock(0, &db)
	return nil
}

// VictoriaTraces v0.12.0 returns startTimeUnixNano as a JSON string in the
// Tempo /api/search response (a JSON number broke clients that decode it as a
// string). The traces binary serves that endpoint through VictoriaTraces' own
// Tempo handler, so it inherits the fix; this pins that a Lakehouse-served
// search response carries the string type, for the trace summary and for every
// span in its span set.
func TestTempoSearch_StartTimeUnixNanoIsAString(t *testing.T) {
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	st := &spanStore{start: start, end: start.Add(2 * time.Second)}
	vtstorageadapter.Init(st)

	mux := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces)).Register(mux)

	args := url.Values{
		"q":     {"{}"},
		"start": {strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)},
		"end":   {strconv.FormatInt(time.Now().Unix(), 10)},
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/tempo/api/search?"+args.Encode(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Traces []struct {
			TraceID           string          `json:"traceID"`
			StartTimeUnixNano json.RawMessage `json:"startTimeUnixNano"`
			SpanSets          []struct {
				Spans []struct {
					StartTimeUnixNano json.RawMessage `json:"startTimeUnixNano"`
				} `json:"spans"`
			} `json:"spanSets"`
		} `json:"traces"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	if len(resp.Traces) != 1 {
		t.Fatalf("want one trace, got %d: %s", len(resp.Traces), rec.Body.String())
	}
	tr := resp.Traces[0]
	wantStart := strconv.FormatInt(start.UnixNano(), 10)
	if got := string(tr.StartTimeUnixNano); got != strconv.Quote(wantStart) {
		t.Errorf("trace startTimeUnixNano = %s, want the JSON string %q", got, wantStart)
	}
	spans := 0
	for _, set := range tr.SpanSets {
		for _, sp := range set.Spans {
			spans++
			if s := string(sp.StartTimeUnixNano); len(s) == 0 || s[0] != '"' {
				t.Errorf("span startTimeUnixNano = %s, want a JSON string", s)
			}
		}
	}
	if spans == 0 {
		t.Errorf("the response carries no spans to check: %s", rec.Body.String())
	}
}
