//go:build parity

package parity

// #274 on the traces side: a service-graph edge row is not a span, so it carries
// none of the numeric span fields. Hot VictoriaTraces returns it with only its
// edge fields; Lakehouse used to give it kind, status_code, duration and
// start_time_unix_nano "0" because those Parquet columns were required. Both
// tiers run the service-graph task (docker-compose.yml), so each holds edge rows.
// The case reads them from both and requires that no row carries a numeric span
// field. Which edges the two tiers computed, and their counts, are not compared
// here (TestServiceGraphParity_DependenciesAPI does that).

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestParity_ServiceGraph_RowsCarryNoSpanNumerics(t *testing.T) {
	cells := 0
	for _, tier := range []struct{ name, base string }{{"hot", vtBaseURL}, {"cold", lhtBaseURL}} {
		var rows []map[string]any
		deadline := time.Now().Add(serviceGraphFirstTickTimeout)
		for {
			p := url.Values{"query": {"parent:* | limit 20"}, "disable_latency_offset": {"true"},
				"start": {fmt.Sprintf("%d", time.Now().Add(-48*time.Hour).UnixNano())},
				"end":   {fmt.Sprintf("%d", time.Now().Add(time.Hour).UnixNano())}}
			r := fetch(t, tier.base, "/select/logsql/query", p)
			if r.StatusCode == 200 {
				rows = parseNDJSON(r.Body)
			}
			if len(rows) > 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Second)
		}
		if len(rows) == 0 {
			t.Fatalf("%s holds no service-graph edge row after %s", tier.name, serviceGraphFirstTickTimeout)
		}
		for i, row := range rows {
			for _, k := range []string{"kind", "status_code", "duration", "start_time_unix_nano"} {
				if v, ok := row[k]; ok {
					t.Errorf("%s: service-graph row %d carries %s=%v; hot returns only the edge fields (#274)", tier.name, i, k, v)
				}
			}
			cells++
		}
	}
	reportLockCells(t, cells)
}
