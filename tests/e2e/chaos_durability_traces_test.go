//go:build e2e && chaos

package e2e

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The traces binary's twins of the logs chaos tests: a span acknowledged before
// a graceful restart or a SIGKILL of the traces container is returned exactly
// once afterwards, restored from the insert buffer's segments on the pod's
// persistent volume.

const chaosTracesContainer = "victoria-lakehouse-lakehouse-traces-1"

// ingestSpanMarker acknowledges one span whose name is a unique marker.
func ingestSpanMarker(t *testing.T, prefix string) (string, int64) {
	t.Helper()
	marker := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	start := time.Now().Add(-time.Second).UnixNano()
	end := time.Now().UnixNano()
	body := fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"chaos-svc"}}]},`+
		`"scopeSpans":[{"scope":{"name":"chaos"},"spans":[{"traceId":"%032x","spanId":"%016x","name":%q,"kind":2,`+
		`"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, end, end, marker, start, end)
	resp := httpPost(t, tracesBaseURL, "/insert/opentelemetry/v1/traces", "application/json", []byte(body))
	if resp.StatusCode/100 != 2 {
		t.Fatalf("ingest span marker: status %d", resp.StatusCode)
	}
	resp.Body.Close()
	return marker, end
}

// spanMarkerCount returns how many spans named marker the traces select path
// returns (VictoriaTraces' latency offset disabled: the span is seconds old).
func spanMarkerCount(t *testing.T, marker string, nowNs int64) string {
	t.Helper()
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`name:=%q | stats count() as n`, marker))
	q.Set("start", fmt.Sprintf("%d", nowNs-int64(time.Minute)))
	q.Set("end", fmt.Sprintf("%d", time.Now().Add(time.Minute).UnixNano()))
	q.Set("disable_latency_offset", "true")
	return string(httpGetBody(t, tracesBaseURL, "/select/logsql/query", q))
}

// waitForSpanOnce polls until the span is returned, then requires exactly one.
func waitForSpanOnce(t *testing.T, marker string, nowNs int64, what string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		body := spanMarkerCount(t, marker, nowNs)
		if strings.Contains(body, `"n":"1"`) {
			return
		}
		if !strings.Contains(body, `"n":"0"`) && strings.Contains(body, `"n":`) {
			t.Fatalf("span %q is not returned exactly once %s: %s", marker, what, body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("span %q NOT found %s — a span acknowledged before it was lost (durability regression): %s", marker, what, body)
		}
		time.Sleep(3 * time.Second)
	}
}

func TestChaos_TracesRestartRestoresTheBuffer(t *testing.T) {
	marker, nowNs := ingestSpanMarker(t, "chaos-traces-restart")
	restartContainer(t, chaosTracesContainer, tracesBaseURL)
	waitForSpanOnce(t, marker, nowNs, "after a restart")
}

func TestChaos_TracesKill9LosesNothingBeyondTheUpstreamWindow(t *testing.T) {
	marker, nowNs := ingestSpanMarker(t, "chaos-traces-kill9")
	time.Sleep(8 * time.Second) // past upstream's flush interval: the part is on disk
	killContainer(t, chaosTracesContainer, tracesBaseURL)
	waitForSpanOnce(t, marker, nowNs, "after kill -9")
}
