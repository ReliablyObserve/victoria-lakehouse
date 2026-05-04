//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestPerf_ManifestFastPath(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	future := time.Now().Add(365 * 24 * time.Hour)
	params := url.Values{
		"query": {"*"},
		"start": {fmt.Sprintf("%d", future.UnixNano())},
		"end":   {fmt.Sprintf("%d", future.Add(time.Hour).UnixNano())},
	}

	start := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("manifest fast path took %s, expected <50ms (target <1ms, allowing network overhead)", elapsed)
	}
	t.Logf("manifest fast path: %s", elapsed)
}

func TestPerf_BloomPointQuery(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", `trace_id:="0000000000000001"`)
	params.Set("limit", "1")

	start := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("bloom point query took %s, expected <500ms (target <100ms, allowing E2E overhead)", elapsed)
	}
	t.Logf("bloom point query: %s", elapsed)
}

func TestPerf_TimeRangeScan(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	end := time.Now()
	rangeStart := end.Add(-1 * time.Hour)
	params := url.Values{
		"query": {"*"},
		"start": {fmt.Sprintf("%d", rangeStart.UnixNano())},
		"end":   {fmt.Sprintf("%d", end.UnixNano())},
		"limit": {"100"},
	}

	t0 := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(t0)

	if elapsed > 2*time.Second {
		t.Errorf("time range scan took %s, expected <2s (target <500ms, allowing E2E overhead)", elapsed)
	}
	t.Logf("time range scan (1h): %s", elapsed)
}

func TestPerf_FieldNames(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	params := defaultTimeParams()

	start := time.Now()
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Errorf("field_names took %s, expected <200ms (target <1ms, allowing E2E overhead)", elapsed)
	}
	if len(body) == 0 {
		t.Error("field_names returned empty response")
	}
	t.Logf("field_names: %s (response: %d bytes)", elapsed, len(body))
}
