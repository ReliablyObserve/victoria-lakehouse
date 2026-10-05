//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// shared state populated by TestMain for use in other tests
var (
	// dataMinTime and dataMaxTime are the time range of seeded data,
	// retrieved from /manifest/range during startup.
	dataMinTime int64
	dataMaxTime int64
)

func TestMain(m *testing.M) {
	// Phase 1: Wait for both services to become healthy.
	fmt.Println("e2e: waiting for lakehouse-logs health...")
	waitForHealthFatal(logsBaseURL, 180*time.Second)
	fmt.Println("e2e: lakehouse-logs is healthy")

	fmt.Println("e2e: waiting for lakehouse-traces health...")
	waitForHealthFatal(tracesBaseURL, 180*time.Second)
	fmt.Println("e2e: lakehouse-traces is healthy")

	// Phase 2: Verify manifest has data.
	fmt.Println("e2e: verifying manifest/range on logs...")
	verifyManifest(logsBaseURL, "logs")

	fmt.Println("e2e: verifying manifest/range on traces...")
	verifyManifest(tracesBaseURL, "traces")

	// Phase 2b: datagen-continuous keeps inserting, but those rows reach S3 one
	// flush interval after they arrive. Tests that query the last 30 minutes
	// (defaultTimeParams) need Parquet holding such rows, so wait for it.
	fmt.Println("e2e: waiting for recent Parquet on logs...")
	waitRecentData(logsBaseURL, "logs")
	fmt.Println("e2e: waiting for recent Parquet on traces...")
	waitRecentData(tracesBaseURL, "traces")

	// Phase 2c: an object in the manifest is not yet READ from Parquet. A
	// committed segment keeps answering its rows from the buffer, and its
	// objects stay out of the scan, for its grace period (90 s here). Tests that
	// must read Parquet (cache recovery, cold rows) need a segment past that
	// point, so wait until each binary has removed one.
	waitSegmentReaped(logsBaseURL, "logs")
	waitSegmentReaped(tracesBaseURL, "traces")

	// Phase 3: Store the data time range for use in other tests.
	storeTimeRange()

	fmt.Printf("e2e: data range: %s to %s\n",
		time.Unix(0, dataMinTime).UTC().Format(time.RFC3339),
		time.Unix(0, dataMaxTime).UTC().Format(time.RFC3339),
	)

	// Phase 4: Warm the smart cache so individual tests don't each pay cold-start cost.
	fmt.Println("e2e: warming smart cache (logs)...")
	warmCache(logsBaseURL)
	fmt.Println("e2e: warming smart cache (traces)...")
	warmCache(tracesBaseURL)
	fmt.Println("e2e: cache warm-up complete")

	os.Exit(m.Run())
}

func waitForHealthFatal(baseURL string, timeout time.Duration) {
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(os.Stderr, "FATAL: health check at %s did not respond within %s\n", baseURL, timeout)
	os.Exit(1)
}

// manifestWait bounds how long the stack may take to write its first Parquet
// object. The insert buffer is durable by default: seeded rows sit in an
// upstream storage segment and reach S3 only when it is sealed, at most
// insert.buffer_flush_interval (2m in lakehouse-e2e-config.yml) after it opened,
// so the manifest is legitimately empty until then. Wait for real Parquet
// (totalFiles > 0) rather than treating an empty manifest as a failure.
const manifestWait = 6 * time.Minute

func verifyManifest(baseURL string, label string) {
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(manifestWait)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/manifest/range")
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		totalFiles, _ := result["totalFiles"].(float64)
		if totalFiles > 0 {
			fmt.Printf("e2e: %s manifest has %.0f files\n", label, totalFiles)
			return
		}

		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s manifest/range returned 0 files after %s\n", label, manifestWait)
	os.Exit(1)
}

// waitSegmentReaped waits until the binary has removed at least one committed
// insert-buffer segment: lakehouse_buffer_segments_committed_total exceeds the
// segments still kept (state committed, or retired and still held by a query),
// so a whole segment's rows are answered from Parquet. It fails the run when
// none is removed within manifestWait.
func waitSegmentReaped(baseURL, label string) {
	client := &http.Client{Timeout: 10 * time.Second}
	start := time.Now()
	last := "no answer"
	for time.Since(start) < manifestWait {
		if total, kept, err := segmentCounts(client, baseURL); err == nil {
			if total-kept >= 1 {
				fmt.Printf("e2e: %s has read-from-Parquet segments (%.0f removed from the buffer) after %s\n", label, total-kept, time.Since(start).Round(time.Second))
				return
			}
			last = fmt.Sprintf("committed_total=%.0f still kept=%.0f", total, kept)
		} else {
			last = err.Error()
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s removed no committed insert-buffer segment within %s (%s): no row is answered from Parquet yet\n", label, manifestWait, last)
	os.Exit(1)
}

// segmentCounts reads lakehouse_buffer_segments_committed_total and the
// segments still kept (state committed or retired) from /metrics.
func segmentCounts(client *http.Client, baseURL string) (total, kept float64, err error) {
	resp, err := client.Get(baseURL + "/metrics")
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}
	m := parsePrometheusText(string(body))
	for _, l := range m["lakehouse_buffer_segments_committed_total"] {
		total += l.value
	}
	for _, l := range m["lakehouse_buffer_segments"] {
		if st := l.labels["state"]; st == "committed" || st == "retired" {
			kept += l.value
		}
	}
	return total, kept, nil
}

// waitRecentData blocks until /manifest/range reports data newer than 10
// minutes, i.e. until the continuous generator's rows have been flushed to
// Parquet. It fails the run when none arrives within manifestWait.
func waitRecentData(baseURL string, label string) {
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(manifestWait)
	for time.Now().Before(deadline) {
		if resp, err := client.Get(baseURL + "/manifest/range"); err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var result map[string]any
			if json.Unmarshal(body, &result) == nil {
				if maxT, _ := result["maxTime"].(float64); maxT > 0 &&
					time.Since(time.Unix(0, int64(maxT))) < 10*time.Minute {
					fmt.Printf("e2e: %s has recent Parquet\n", label)
					return
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s manifest has no data newer than 10m after %s\n", label, manifestWait)
	os.Exit(1)
}

func storeTimeRange() {
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(logsBaseURL + "/manifest/range")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot get manifest/range: %v\n", err)
		os.Exit(1)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot parse manifest/range: %v\n", err)
		os.Exit(1)
	}

	if minT, ok := result["minTime"].(float64); ok {
		dataMinTime = int64(minT)
	}
	if maxT, ok := result["maxTime"].(float64); ok {
		dataMaxTime = int64(maxT)
	}

	if dataMinTime == 0 || dataMaxTime == 0 {
		// Fall back to a wide window
		now := time.Now()
		dataMinTime = now.Add(-72 * time.Hour).UnixNano()
		dataMaxTime = now.UnixNano()
	}
}

// warmCache fires a wildcard query against each service to populate the smart
// cache. Subsequent tests hit warm cache instead of scanning S3 from scratch.
func warmCache(baseURL string) {
	client := &http.Client{Timeout: 120 * time.Second}
	now := time.Now()
	params := fmt.Sprintf("query=*&limit=1&start=%d&end=%d",
		now.Add(-30*time.Minute).UnixNano(), now.UnixNano())
	resp, err := client.Get(baseURL + "/select/logsql/query?" + params)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: cache warm-up failed for %s: %v\n", baseURL, err)
		return
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
}
