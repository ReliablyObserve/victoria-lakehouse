package selectapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// rangeStore records the time range of the queries the upstream handlers send to the storage.
type rangeStore struct {
	mockStore
	mu     sync.Mutex
	ranges [][2]int64
}

func (s *rangeStore) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	start, end := q.GetFilterTimeRange()
	s.mu.Lock()
	s.ranges = append(s.ranges, [2]int64{start, end})
	s.mu.Unlock()
	return nil
}

func (s *rangeStore) last(t *testing.T) (int64, int64) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ranges) == 0 {
		t.Fatal("the storage was not asked for anything")
	}
	r := s.ranges[len(s.ranges)-1]
	return r[0], r[1]
}

// The start and end arguments reach the storage exactly as upstream parses them: an integer
// argument is read as seconds, milliseconds, microseconds or nanoseconds by its magnitude, to
// the unit's own precision, and the end argument is exclusive (the handler turns it into an
// inclusive bound by subtracting one nanosecond). The handler used to divide an integer above
// 1e12 by 1000 before upstream saw it: a nanosecond bound was cut to microseconds (a row
// exactly at the end, or inside the last microsecond, was lost) and a millisecond bound to
// seconds (up to 999 ms lost).
func TestWrapVL_TimeRangeReachesTheStorageAtTheArgumentsPrecision(t *testing.T) {
	st := &rangeStore{}
	vlstorage.SetStorage(st, nil)
	h := NewHandler(st, testConfig(config.ModeLogs))
	mux := http.NewServeMux()
	h.Register(mux)

	const (
		startNs = int64(1790838001234567123) // not aligned to a microsecond, a millisecond or a second
		endNs   = int64(1790838002345678456)
	)
	for _, tc := range []struct {
		name       string
		start, end string
		wantStart  int64
		wantEnd    int64 // the inclusive end: the argument minus one nanosecond
	}{
		{"nanoseconds", fmt.Sprint(startNs), fmt.Sprint(endNs), startNs, endNs - 1},
		{"microseconds", fmt.Sprint(startNs / 1e3), fmt.Sprint(endNs / 1e3), startNs / 1e3 * 1e3, endNs/1e3*1e3 - 1},
		{"milliseconds", fmt.Sprint(startNs / 1e6), fmt.Sprint(endNs / 1e6), startNs / 1e6 * 1e6, endNs/1e6*1e6 - 1},
		{"seconds", fmt.Sprint(startNs / 1e9), fmt.Sprint(endNs / 1e9), startNs / 1e9 * 1e9, endNs/1e9*1e9 - 1},
		{"fractional seconds", "1790838001.234567123", "1790838002.345678456", startNs, endNs - 1},
		{"RFC3339 with nanoseconds", "2026-10-01T07:00:01.234567123Z", "2026-10-01T07:00:02.345678456Z", startNs, endNs - 1},
	} {
		for _, path := range []string{"/select/logsql/query", "/select/logsql/stats_query"} {
			t.Run(tc.name+path, func(t *testing.T) {
				q := url.Values{"query": {"* | stats count() rows"}, "start": {tc.start}, "end": {tc.end}}
				if path == "/select/logsql/query" {
					q.Set("query", "*")
				}
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest("GET", path+"?"+q.Encode(), nil))
				if rec.Code != http.StatusOK {
					t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
				}
				gotStart, gotEnd := st.last(t)
				if gotStart != tc.wantStart || gotEnd != tc.wantEnd {
					t.Errorf("the storage was asked for [%d, %d], want [%d, %d]", gotStart, gotEnd, tc.wantStart, tc.wantEnd)
				}
			})
		}
	}
}

// The wrapper leaves every time argument as it received it.
func TestWrapVL_LeavesTheTimeArgumentsAlone(t *testing.T) {
	h := NewHandler(mockStore{}, testConfig(config.ModeLogs))
	var got url.Values
	wrapped := h.wrapVL(func(_ context.Context, _ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.Form
	})
	const q = "start=1790838001234567123&end=1790838002345678456&time=1790838001234&query=*"
	wrapped(httptest.NewRecorder(), httptest.NewRequest("GET", "/select/logsql/hits?"+q, nil))
	want, _ := url.ParseQuery(q)
	for k, v := range want {
		if got.Get(k) != v[0] {
			t.Errorf("%s = %q, want %q", k, got.Get(k), v[0])
		}
	}
}
