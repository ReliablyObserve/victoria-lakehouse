//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

	// Phases 2b and 2c wait for the real stack's insert buffer to reach
	// Parquet. A child process started by a test with its own mock servers
	// (TestIngestMatrix_EstablishedSampleRejectsDip) has no buffer to wait
	// for, and its mock manifest is deliberately old.
	if os.Getenv("LH_INGEST_SAMPLE_CHILD") != "1" {
		// Phase 2b: datagen-continuous keeps inserting, but those rows reach S3 one
		// flush interval after they arrive. Tests that query the last 30 minutes
		// (defaultTimeParams) need Parquet holding such rows, so wait for it.
		fmt.Println("e2e: waiting for recent Parquet on logs...")
		waitRecentData(logsBaseURL, "logs")
		fmt.Println("e2e: waiting for recent Parquet on traces...")
		waitRecentData(tracesBaseURL, "traces")

		// Phase 2c: an object in the manifest is not yet READ from Parquet. While a
		// row is in the insert buffer (an open, draining or committed-in-grace
		// segment) a query answers it from the buffer and leaves that segment's
		// objects out of the scan. Tests that must read Parquet use windows ending
		// seededBefore ago (datagen-continuous only writes the last hour), so wait,
		// as the parity settle step does, until no buffered row is that old.
		waitBufferOlderThanDrained(logsBaseURL, "logs")
		waitBufferOlderThanDrained(tracesBaseURL, "traces")
	}

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

// seededBefore is how far back the windows that must read Parquet end: older
// than anything datagen-continuous writes (--hours-back=1).
const seededBefore = 2 * time.Hour

// waitBufferOlderThanDrained waits until the binary's insert buffer holds no
// row older than seededBefore, read through the endpoint select pods read it
// with (/internal/buffer/query, every tenant, with the peer key): from then on such rows are
// answered from Parquet only. It fails the run when they are still buffered
// after manifestWait.
func waitBufferOlderThanDrained(baseURL, label string) {
	client := &http.Client{Timeout: 30 * time.Second}
	mode := "logs"
	if baseURL == tracesBaseURL {
		mode = "traces"
	}
	start := time.Now()
	last := "no answer"
	for time.Since(start) < manifestWait {
		end := time.Now().Add(-seededBefore).UnixNano()
		// every tenant at once is served only to a caller presenting the
		// stack's peer key (docs/security.md)
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/internal/buffer/query?start=0&end=%d&mode=%s&tenant_scope=v1&all_tenants=true", baseURL, end, mode), nil)
		var resp *http.Response
		if err == nil {
			resp, err = client.Do(withPeerKey(req))
		}
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				rows := 0
				for _, line := range strings.Split(string(body), "\n") {
					if strings.TrimSpace(line) != "" {
						rows++
					}
				}
				if rows == 0 {
					fmt.Printf("e2e: %s buffers no row older than %s after %s\n", label, seededBefore, time.Since(start).Round(time.Second))
					return
				}
				last = fmt.Sprintf("%d such rows still buffered", rows)
			} else {
				last = fmt.Sprintf("status %d", resp.StatusCode)
			}
		} else {
			last = err.Error()
		}
		time.Sleep(3 * time.Second)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s still buffers rows older than %s after %s (%s): they are not read from Parquet yet\n", label, seededBefore, manifestWait, last)
	os.Exit(1)
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
