package parquets3

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selectapi"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// rangeCounter serves the real handlers over e's Storage and returns a function that asks
// stats_query for count() of [start, end) and reads the count out of the answer.
func rangeCounter(t *testing.T, e *restartEnv) func(t *testing.T, query, start, end string) int {
	t.Helper()
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.Query.Timeout = 30 * time.Second
	cfg.Query.MaxConcurrent = 4
	vlstorage.SetStorage(e.s, nil)
	mux := http.NewServeMux()
	selectapi.NewHandler(e.s, cfg).Register(mux)

	return func(t *testing.T, query, start, end string) int {
		t.Helper()
		q := url.Values{"query": {query}, "start": {start}, "end": {end}}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", "/select/logsql/stats_query?"+q.Encode(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Data struct {
				Result []struct {
					Value []any `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%v: %s", err, rec.Body.String())
		}
		if len(body.Data.Result) == 0 {
			return 0
		}
		n, err := strconv.Atoi(fmt.Sprint(body.Data.Result[0].Value[1]))
		if err != nil {
			t.Fatalf("%v: %s", err, rec.Body.String())
		}
		return n
	}
}

func rangeNs(v int64) string { return strconv.FormatInt(v, 10) }

// A row in the first nanosecond of an hour belongs to that hour's partition, which starts at
// the end of a window that ends there: the partition walk used to stop before it.
func TestHTTPRange_RowAtTheFirstNanosecondOfAnHourIsRead(t *testing.T) {
	e := newRestartEnv(t)
	hour := rwHour.Add(time.Hour)
	e.ingest("info", rwHour.Add(30*time.Minute), hour, hour.Add(time.Second))
	e.flush()
	e.reap()
	count := rangeCounter(t, e)
	h := hour.UnixNano()
	for _, tc := range []struct {
		name       string
		start, end int64
		want       int
	}{
		{"[earlier hour, hour+1) holds the row at the hour start", rwHour.UnixNano(), h + 1, 2},
		{"[earlier hour, hour) stops before it", rwHour.UnixNano(), h, 1},
		{"[hour, hour+1)", h, h + 1, 1},
		{"[hour-1, hour+1)", h - 1, h + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := count(t, "* | stats count() rows", rangeNs(tc.start), rangeNs(tc.end)); got != tc.want {
				t.Errorf("count() over [%d, %d) = %d, want %d", tc.start, tc.end, got, tc.want)
			}
		})
	}
}

// The parity cases of TestParity_TimeRange (boundary_ns, boundary_ns_start_inclusive), asked of
// the real handlers over a real Storage with the rows in Parquet: the HTTP end argument is
// exclusive, the start inclusive, both to the nanosecond, and a count of the window is exact.
func TestHTTPRange_BoundaryNanosecondsAreExactOnParquet(t *testing.T) {
	e := newRestartEnv(t)
	rows := rangeEndTimes()
	e.ingest("info", rows...)
	e.flush()
	e.reap()
	count := rangeCounter(t, e)

	first := rows[0].UnixNano()
	ns := rangeNs
	for _, tc := range []struct {
		name       string
		start, end string
		want       int
	}{
		{"boundary_ns: [first-1, first) is empty", ns(first - 1), ns(first), 0},
		{"boundary_ns_start_inclusive: [first, first+1) holds the first row", ns(first), ns(first + 1), 1},
		{"the end is exclusive one row later", ns(first - 1), ns(first + 1), 1},
		{"[first-1h, first+1) holds the first row", ns(first - int64(time.Hour)), ns(first + 1), 1},
		{"[first+1, first+2) is empty", ns(first + 1), ns(first + 2), 0},
		{"all rows", ns(first), ns(rows[2].UnixNano() + 1), 3},
		{"all rows but the last", ns(first), ns(rows[2].UnixNano()), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := count(t, "* | stats count() rows", tc.start, tc.end); got != tc.want {
				t.Errorf("count() over [%s, %s) = %d, want %d", tc.start, tc.end, got, tc.want)
			}
		})
	}
	// Millisecond and microsecond arguments keep their unit's precision: the first row is 123 ns
	// into its microsecond (rangeEndTimes), 0.000123 ms into its millisecond.
	t.Run("millisecond and microsecond arguments", func(t *testing.T) {
		ms, us := first/1e6, first/1e3
		for _, c := range []struct {
			name       string
			start, end int64
			want       int
		}{
			{"the millisecond holding the first row", ms, ms + 1, 1},
			{"the millisecond before it", ms - 1, ms, 0},
			{"the microsecond holding the first row", us, us + 1, 1},
			{"the microsecond before it", us - 1, us, 0},
		} {
			if got := count(t, "* | stats count() rows", ns(c.start), ns(c.end)); got != c.want {
				t.Errorf("%s: count() = %d, want %d", c.name, got, c.want)
			}
		}
	})
}
