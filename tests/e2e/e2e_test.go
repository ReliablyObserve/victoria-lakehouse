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

	// Phase 3: Store the data time range for use in other tests.
	storeTimeRange()

	fmt.Printf("e2e: data range: %s to %s\n",
		time.Unix(0, dataMinTime).UTC().Format(time.RFC3339),
		time.Unix(0, dataMaxTime).UTC().Format(time.RFC3339),
	)

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

func verifyManifest(baseURL string, label string) {
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
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
	fmt.Fprintf(os.Stderr, "FATAL: %s manifest/range returned 0 files after 60s\n", label)
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
