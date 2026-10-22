//go:build parity

package parity

// A parity test that finds no data to compare has not passed and has not been
// skipped for a good reason: TestMain holds the suite until the seeded corpus
// is in Parquet on both Lakehouse binaries and both manifests record rows, so
// an empty hot or cold answer afterwards is a seed, ingestion or tier defect.
// A skip there turned a cold-tier failure into a green run. The helpers below
// give the data a short, bounded time to appear (a restart or a flush can
// still be settling) and then fail with the evidence.

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

const (
	// dataWaitBudget is how long one wait gives data the seeded corpus must
	// already hold. It is short on purpose: a stack with a real defect has
	// several waits to fail, and they must end before the suite's -timeout.
	dataWaitBudget = 30 * time.Second
	dataWaitStep   = 3 * time.Second
)

// requireEventually calls probe until it reports true. When dataWaitBudget has
// passed it fails the test with evidence (called once, at the end) instead of
// skipping it.
func requireEventually(t *testing.T, what string, probe func() bool, evidence func() string) {
	t.Helper()
	deadline := time.Now().Add(dataWaitBudget)
	for !probe() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: nothing after %s (%s) — the seeded corpus must hold it, so this is a seed, "+
				"ingestion or tier defect, not a reason to skip", what, dataWaitBudget, evidence())
		}
		time.Sleep(dataWaitStep)
	}
}

// probeStatus describes a GET for a failure message: the status and size, or
// the error text fetch kept in the body.
func probeStatus(t *testing.T, base, path string, params url.Values) string {
	t.Helper()
	r := fetch(t, base, path, params)
	body := string(r.Body)
	if len(body) > 200 {
		body = body[:200] + "..."
	}
	return fmt.Sprintf("GET %s -> %d, %d bytes: %s", path, r.StatusCode, len(r.Body), body)
}

// requireHotServices waits for hot VictoriaTraces to list at least one service
// on its Jaeger API and returns them.
func requireHotServices(t *testing.T, why string) []string {
	t.Helper()
	var hot []string
	requireEventually(t, "hot Jaeger services ("+why+")", func() bool {
		hot = jaegerServices(t, vtBaseURL)
		return len(hot) > 0
	}, func() string { return probeStatus(t, vtBaseURL, "/select/jaeger/api/services", nil) })
	return hot
}
