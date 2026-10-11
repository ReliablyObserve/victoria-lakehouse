//go:build parity

package parity

// A stream that reached hot in batches of different shapes is one block only
// once hot has merged its parts (TestParity_FieldNames_Layers writes such a
// stream and asks hot to merge). The merge is asynchronous and the time hot takes
// to converge on its own varies, so the test forces it and waits for it.

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// forceMergeHot merges the parts of the partition of at on the hot instance and
// returns once the merge is done: the endpoint answers at once and merges in the
// background, so the instance's active-merges gauge is polled until it has been
// zero twice in a row.
func forceMergeHot(t *testing.T, hot, mode string, at time.Time) {
	t.Helper()
	gauge := "vl_active_force_merges"
	if mode == "traces" {
		gauge = "vt_active_force_merges"
	}
	post(t, hot+"/internal/force_flush", "text/plain", nil, http.Header{})
	post(t, hot+"/internal/force_merge?partition_prefix="+at.UTC().Format("20060102"), "text/plain", nil, http.Header{})
	deadline := time.Now().Add(2 * time.Minute)
	time.Sleep(500 * time.Millisecond) // the gauge is raised by the merge goroutine, not by the request
	for idle := 0; idle < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("%s: the forced merge on %s did not finish in 2 minutes", gauge, hot)
		}
		if n := hotGaugeValue(t, hot, gauge); n == 0 {
			idle++
		} else {
			idle = 0
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// hotGaugeValue reads one unlabelled metric from the instance's /metrics.
func hotGaugeValue(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := httpClient.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET %s/metrics: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s/metrics: %v", base, err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatalf("metric %s: %q: %v", name, line, err)
			}
			return v
		}
	}
	// The gauge is exposed from start-up with value 0, so a missing name is a wrong
	// name, not a merge that has not begun: fail instead of "waiting" on nothing.
	t.Fatalf("metric %s is not exposed by %s/metrics", name, base)
	return 0
}
