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
// They validate the no-WAL durability claim: with buffer_engine=logstore the
// insert buffer persists rows to on-disk parts and restores them on open, so a
// container restart loses at most the buffer flush interval — the same crash-loss
// window as hot VL/VT.
package e2e

import (
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// restartContainer hard-restarts a compose container and waits for the cold tier
// to report healthy again.
func restartContainer(t *testing.T, name, baseURL string) {
	t.Helper()
	out, err := exec.Command("docker", "restart", name).CombinedOutput()
	if err != nil {
		t.Fatalf("docker restart %s: %v (%s)", name, err, out)
	}
	waitForHealth(t, baseURL, 90*time.Second)
}

// TestChaos_BufferRestoreOnRestart proves the logstore buffer survives a cold-tier
// container restart: data ingested moments before the restart — still inside the
// buffer's flush window — must be queryable afterward, served from the restored
// on-disk parts. This is the concrete, end-to-end form of TestBufferFlusher_
// CrashRecovery and the buffer-restore-on-restart row in docs/durability.md §8.
func TestChaos_BufferRestoreOnRestart(t *testing.T) {
	const container = "victoria-lakehouse-lakehouse-logs-1"

	// Unique marker so the assertion can't be satisfied by pre-existing data.
	marker := fmt.Sprintf("chaos-restore-%d", time.Now().UnixNano())
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

	// Give the buffer a moment to persist the row to an on-disk part (the
	// logstore flush interval), then restart WITHOUT a graceful flush window.
	time.Sleep(8 * time.Second)
	restartContainer(t, container, logsBaseURL)

	// After restart the row must be queryable from the restored buffer (or S3 if
	// it had already flushed). Poll briefly to let warmup/restore settle.
	q := url.Values{}
	q.Set("query", fmt.Sprintf(`_msg:%q`, marker))
	q.Set("start", fmt.Sprintf("%d", nowNs-int64(time.Minute)))
	q.Set("end", fmt.Sprintf("%d", time.Now().UnixNano()))

	deadline := time.Now().Add(60 * time.Second)
	for {
		body := httpGetBody(t, logsBaseURL, "/select/logsql/query", q)
		if strings.Contains(string(body), marker) {
			return // survived the restart — durability holds
		}
		if time.Now().After(deadline) {
			t.Fatalf("marker %q NOT found after restart — data ingested before the restart was lost (no-WAL durability regression)", marker)
		}
		time.Sleep(3 * time.Second)
	}
}
