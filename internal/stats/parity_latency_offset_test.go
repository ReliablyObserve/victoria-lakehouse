package stats

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The parity check loops back to the process's own /select/logsql/stats_query
// with end = now. On the traces binary that endpoint hides the newest
// -search.latencyOffset of data (VictoriaTraces v0.12.0) unless the request says
// disable_latency_offset=true, so the check must say it.
func TestVLStatsCountAdapter_DisablesTheLatencyOffset(t *testing.T) {
	var got http.Header
	var args map[string][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header
		args = r.URL.Query()
		_, _ = w.Write([]byte(`{"data":{"result":[{"value":[0,"7"]}]}}`))
	}))
	defer srv.Close()

	n, err := NewLocalVLQuerier(srv.URL).StatsCountAll(context.Background(), 1_000_000_000, 2_000_000_000)
	if err != nil || n != 7 {
		t.Fatalf("StatsCountAll = %d, %v; want 7, nil", n, err)
	}
	_ = got
	if v := args["disable_latency_offset"]; len(v) != 1 || v[0] != "true" {
		t.Errorf("disable_latency_offset = %v, want [true]: the check would miss the newest rows on the traces binary", v)
	}
}
