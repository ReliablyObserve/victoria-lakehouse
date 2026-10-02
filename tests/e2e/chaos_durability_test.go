//go:build e2e && chaos

// Package e2e chaos suite — data-survival edge cases (docs/durability.md §8).
//
// These tests KILL/RESTART running containers, so they are gated behind the
// `chaos` build tag (in addition to `e2e`) and are NOT part of the normal `e2e`
// run. Run with:
//
//	docker compose -f deployment/docker/docker-compose-e2e.yml up -d
//	go test -tags 'e2e chaos' ./tests/e2e/ -run Chaos -v -count=1
//
// They validate the durability claim of the insert buffer (docs/durability.md):
// every acknowledged row is in an upstream logstorage segment on the pod's
// persistent volume, so a restart or a kill -9 loses at most upstream's own
// in-memory window (5 s) — the same crash-loss window as hot VL/VT — and the
// restarted pod restores its segments and writes them to object storage.
package e2e

import (
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const chaosContainer = "victoria-lakehouse-lakehouse-logs-1"

// docker runs a docker command and fails the test on error.
func docker(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %s: %v (%s)", strings.Join(args, " "), err, out)
	}
}

// restartContainer stops a compose container gracefully and starts it again,
// then waits for the cold tier to report healthy.
func restartContainer(t *testing.T, name, baseURL string) {
	t.Helper()
	docker(t, "restart", name)
	waitForHealth(t, baseURL, 90*time.Second)
}

// killContainer SIGKILLs a compose container (no graceful close of the insert
// buffer), starts it again, then waits for the cold tier to report healthy.
func killContainer(t *testing.T, name, baseURL string) {
	t.Helper()
	docker(t, "kill", "-s", "KILL", name)
	docker(t, "start", name)
	waitForHealth(t, baseURL, 90*time.Second)
}

// ingestMarker acknowledges one uniquely marked row and returns the marker and
// its timestamp.
func ingestMarker(t *testing.T, prefix string) (string, int64) {
	t.Helper()
	marker := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	nowNs := time.Now().UnixNano()
	body := fmt.Sprintf(
		`{"_time":"%s","_msg":"%s","service.name":"chaos-svc"}`+"\n",
		time.Unix(0, nowNs).UTC().Format(time.RFC3339Nano), marker,
	)
	resp := httpPost(t, logsBaseURL, "/insert/jsonline", "application/x-ndjson", []byte(body))
	if resp.StatusCode/100 != 2 {
		t.Fatalf("ingest marker: status %d", resp.StatusCode)
	}
	resp.Body.Close()
	return marker, nowNs
}

// waitForMarker polls the select path until the marker is returned.
func waitForMarker(t *testing.T, marker string, nowNs int64, what string) {
	t.Helper()
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`_msg:%q`, marker))
	q.Set("start", fmt.Sprintf("%d", nowNs-int64(time.Minute)))
	q.Set("end", fmt.Sprintf("%d", time.Now().Add(time.Minute).UnixNano()))
	deadline := time.Now().Add(60 * time.Second)
	for {
		body := httpGetBody(t, logsBaseURL, "/select/logsql/query", q)
		if strings.Contains(string(body), marker) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("marker %q NOT found %s — a row acknowledged before the restart was lost (durability regression)", marker, what)
		}
		time.Sleep(3 * time.Second)
	}
}

// TestChaos_RestartRestoresTheBuffer proves the insert buffer survives a
// graceful cold-tier container restart: a row acknowledged moments before the
// restart — still unflushed — is queryable afterwards, served from the restored
// segments, and is neither lost nor shown twice.
func TestChaos_RestartRestoresTheBuffer(t *testing.T) {
	marker, nowNs := ingestMarker(t, "chaos-restart")
	restartContainer(t, chaosContainer, logsBaseURL)
	waitForMarker(t, marker, nowNs, "after a restart")
	assertMarkerOnce(t, marker, nowNs)
}

// TestChaos_Kill9LosesNothingBeyondTheUpstreamWindow proves the crash claim: a
// row that has been in the buffer for longer than upstream's in-memory window
// (5 s, after which its part is fsynced) survives a SIGKILL; the restarted pod
// restores its segments and drains them to object storage.
func TestChaos_Kill9LosesNothingBeyondTheUpstreamWindow(t *testing.T) {
	marker, nowNs := ingestMarker(t, "chaos-kill9")
	time.Sleep(8 * time.Second) // past upstream's flush interval: the part is on disk
	killContainer(t, chaosContainer, logsBaseURL)
	waitForMarker(t, marker, nowNs, "after kill -9")
	assertMarkerOnce(t, marker, nowNs)
}

// assertMarkerOnce checks the row is returned exactly once (the segments and the
// objects written from them are never both read).
func assertMarkerOnce(t *testing.T, marker string, nowNs int64) {
	t.Helper()
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`_msg:%q | stats count() as n`, marker))
	q.Set("start", fmt.Sprintf("%d", nowNs-int64(time.Minute)))
	q.Set("end", fmt.Sprintf("%d", time.Now().Add(time.Minute).UnixNano()))
	body := string(httpGetBody(t, logsBaseURL, "/select/logsql/query", q))
	if !strings.Contains(body, `"n":"1"`) {
		t.Errorf("marker %q is not counted exactly once: %s", marker, body)
	}
}
