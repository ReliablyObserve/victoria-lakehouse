//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestDelete_TombstoneAndQuery(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	// Query for service.name:="nginx" data — skip if no data.
	params := defaultTimeParams()
	params.Set("query", `service.name:="nginx"`)
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Skip("no nginx data available — skipping delete test")
	}

	// Create a tombstone via POST /delete/logsql/delete.
	startNs := fmt.Sprintf("%d", dataMinTime)
	endNs := fmt.Sprintf("%d", dataMaxTime)
	deleteURL := fmt.Sprintf("/delete/logsql/delete?query=%s&start=%s&end=%s",
		url.QueryEscape(`service.name:="nginx"`), startNs, endNs)

	resp := httpPost(t, logsBaseURL, deleteURL, "application/x-www-form-urlencoded", nil)
	defer _ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST delete returned status %d: %s", resp.StatusCode, string(respBody))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading delete response: %v", err)
	}

	result := mustParseJSON(t, respBody)
	tombstoneID, ok := result["tombstone_id"].(string)
	if !ok || tombstoneID == "" {
		t.Fatalf("expected tombstone_id in response, got: %s", string(respBody))
	}
	t.Logf("created tombstone: %s", tombstoneID)

	// Query again — nginx data should be suppressed by the tombstone.
	body = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines = assertValidNDJSON(t, body)
	for i, line := range lines {
		svc, _ := line["service.name"].(string)
		if svc == "nginx" {
			t.Errorf("line %d: expected nginx to be suppressed by tombstone, but found it", i)
		}
	}

	// Remove the tombstone via DELETE /delete/logsql/tombstone/{id}.
	httpDoDelete(t, logsBaseURL, "/delete/logsql/tombstone/"+tombstoneID)

	// Query again — nginx data should be back.
	body = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines = assertValidNDJSON(t, body)
	foundNginx := false
	for _, line := range lines {
		svc, _ := line["service.name"].(string)
		if svc == "nginx" {
			foundNginx = true
			break
		}
	}
	if !foundNginx {
		t.Error("expected nginx data to return after tombstone removal, but got none")
	}
}

func TestDelete_Estimate(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	startNs := fmt.Sprintf("%d", dataMinTime)
	endNs := fmt.Sprintf("%d", dataMaxTime)
	estimateURL := fmt.Sprintf("/delete/logsql/estimate?query=%s&start=%s&end=%s",
		url.QueryEscape("*"), startNs, endNs)

	resp := httpPost(t, logsBaseURL, estimateURL, "application/x-www-form-urlencoded", nil)
	defer _ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST estimate returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading estimate response: %v", err)
	}

	result := mustParseJSON(t, body)
	affectedFiles, _ := result["affected_files"].(float64)
	if affectedFiles <= 0 {
		t.Fatalf("expected affected_files > 0, got %.0f", affectedFiles)
	}
	t.Logf("estimate: affected_files=%.0f", affectedFiles)
}

func TestDelete_Verify(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	startNs := fmt.Sprintf("%d", dataMinTime)
	endNs := fmt.Sprintf("%d", dataMaxTime)

	// Create a tombstone first.
	deleteURL := fmt.Sprintf("/delete/logsql/delete?query=%s&start=%s&end=%s",
		url.QueryEscape("*"), startNs, endNs)

	resp := httpPost(t, logsBaseURL, deleteURL, "application/x-www-form-urlencoded", nil)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST delete returned status %d: %s", resp.StatusCode, string(respBody))
	}

	result := mustParseJSON(t, respBody)
	tombstoneID, ok := result["tombstone_id"].(string)
	if !ok || tombstoneID == "" {
		t.Fatalf("expected tombstone_id in response, got: %s", string(respBody))
	}

	// Verify the tombstone covers the range.
	verifyURL := fmt.Sprintf("/delete/logsql/verify?query=%s&start=%s&end=%s",
		url.QueryEscape("*"), startNs, endNs)

	resp = httpPost(t, logsBaseURL, verifyURL, "application/x-www-form-urlencoded", nil)
	verifyBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST verify returned status %d: %s", resp.StatusCode, string(verifyBody))
	}

	verifyResult := mustParseJSON(t, verifyBody)
	verified, _ := verifyResult["verified"].(bool)
	if !verified {
		t.Fatalf("expected verified=true, got: %s", string(verifyBody))
	}

	// Clean up: remove tombstone.
	httpDoDelete(t, logsBaseURL, "/delete/logsql/tombstone/"+tombstoneID)
}

func TestDelete_ListTombstones(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	resp := httpGet(t, logsBaseURL, "/delete/logsql/tombstones", nil)
	defer _ = resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading tombstones response: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse JSON: %v\nraw: %s", err, string(body))
	}

	// Must have a count field.
	if _, ok := result["count"]; !ok {
		t.Fatalf("expected 'count' field in response, got keys: %v", keys(result))
	}

	// count should be a number >= 0.
	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("expected count to be a number, got %T", result["count"])
	}
	t.Logf("tombstones list: count=%.0f", count)
}

// httpDoDelete performs an HTTP DELETE request and verifies a 200 response.
func httpDoDelete(t *testing.T, baseURL, path string) {
	t.Helper()

	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest(http.MethodDelete, baseURL+path, nil)
	if err != nil {
		t.Fatalf("creating DELETE request for %s%s: %v", baseURL, path, err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s%s failed: %v", baseURL, path, err)
	}
	defer _ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE %s%s returned status %d: %s", baseURL, path, resp.StatusCode, string(body))
	}
}

// keys returns the keys of a map for diagnostic output.
func keys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
