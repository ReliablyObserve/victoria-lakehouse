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

// forceMergeStartWait bounds how long forceMergeHot waits for the merge it asked
// for to begin.
const forceMergeStartWait = 10 * time.Second

// forceMergeHot merges the parts of the partition of at on the hot instance and
// returns once the merge is done. The endpoint answers at once and merges in a
// goroutine that raises the instance's active-merges gauge only when it runs, so
// the wait has two phases: first (bounded by forceMergeStartWait) until the merge
// is seen to have begun, that is the gauge is above zero or the instance's part
// count changed, then until the gauge has been zero twice in a row. A merge that
// never shows (no gauge, no change in the parts for the whole bound) is a hot
// instance with nothing to merge, which is logged; a merge that does not finish
// in 2 minutes fails the test.
func forceMergeHot(t *testing.T, hot, mode string, at time.Time) {
	t.Helper()
	prefix := "vl"
	if mode == "traces" {
		prefix = "vt"
	}
	gauge := prefix + "_active_force_merges"
	post(t, hot+"/internal/force_flush", "text/plain", nil, http.Header{})
	partsBefore := hotPartCount(t, hot, prefix) // after the flush: the parts the merge starts from
	post(t, hot+"/internal/force_merge?partition_prefix="+at.UTC().Format("20060102"), "text/plain", nil, http.Header{})

	started := false
	for deadline := time.Now().Add(forceMergeStartWait); !started && time.Now().Before(deadline); {
		started = hotGaugeValue(t, hot, gauge) > 0 || hotPartCount(t, hot, prefix) != partsBefore
		if !started {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !started {
		t.Logf("forceMergeHot(%s): no merge began within %s (parts %.0f before and after): hot had nothing to merge", hot, forceMergeStartWait, partsBefore)
	}

	deadline := time.Now().Add(2 * time.Minute)
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

// hotMetrics reads the instance's /metrics.
func hotMetrics(t *testing.T, base string) []string {
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
	return strings.Split(string(body), "\n")
}

// hotPartCount is the number of storage parts the instance holds (the sum of
// <prefix>_storage_parts over its types), or -1 when it exposes none.
func hotPartCount(t *testing.T, base, prefix string) float64 {
	t.Helper()
	total, found := 0.0, false
	for _, line := range hotMetrics(t, base) {
		rest, ok := strings.CutPrefix(line, prefix+"_storage_parts{")
		if !ok {
			continue
		}
		i := strings.LastIndex(rest, " ")
		if i < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(rest[i+1:]), 64)
		if err != nil {
			t.Fatalf("metric %s_storage_parts: %q: %v", prefix, line, err)
		}
		total += v
		found = true
	}
	if !found {
		return -1
	}
	return total
}

// hotGaugeValue reads one unlabelled metric from the instance's /metrics.
func hotGaugeValue(t *testing.T, base, name string) float64 {
	t.Helper()
	for _, line := range hotMetrics(t, base) {
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
