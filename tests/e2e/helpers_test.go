//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	logsBaseURL   = envOrDefault("LOGS_BASE_URL", "http://localhost:19428")
	tracesBaseURL = envOrDefault("TRACES_BASE_URL", "http://localhost:20428")
	lokiProxyURL  = envOrDefault("LOKI_PROXY_URL", "http://localhost:3100")
	vlselectURL   = envOrDefault("VLSELECT_URL", "http://localhost:9471")
)

// httpGet performs an HTTP GET and returns the response, failing the test on error.
func httpGet(t *testing.T, baseURL, path string, params url.Values) *http.Response {
	t.Helper()

	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("GET %s returned status %d: %s", u, resp.StatusCode, string(body))
	}

	return resp
}

// httpGetBody performs an HTTP GET and returns the response body, failing on error.
func httpGetBody(t *testing.T, baseURL, path string, params url.Values) []byte {
	t.Helper()
	resp := httpGet(t, baseURL, path, params)
	defer _ = resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body from %s%s: %v", baseURL, path, err)
	}
	return body
}

// httpGetAllowStatus performs an HTTP GET and returns the response, allowing
// any of the given status codes. Fails if the status code is not in the list.
func httpGetAllowStatus(t *testing.T, baseURL, path string, params url.Values, allowedStatuses ...int) *http.Response {
	t.Helper()

	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(u)
	if err != nil {
		t.Fatalf("GET %s failed: %v", u, err)
	}

	for _, s := range allowedStatuses {
		if resp.StatusCode == s {
			return resp
		}
	}

	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	t.Fatalf("GET %s returned unexpected status %d (allowed: %v): %s", u, resp.StatusCode, allowedStatuses, string(body))
	return nil
}

// httpPost performs an HTTP POST and returns the response, failing the test on error.
func httpPost(t *testing.T, baseURL, path string, contentType string, body []byte) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Post(baseURL+path, contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s%s failed: %v", baseURL, path, err)
	}
	return resp
}

// mustParseJSON parses JSON data into a map, failing the test on error.
func mustParseJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to parse JSON: %v\nraw: %s", err, string(data))
	}
	return result
}

// waitForHealth polls the /health endpoint until it returns 200 or the timeout expires.
func waitForHealth(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}

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
	t.Fatalf("health check at %s did not become healthy within %s", baseURL, timeout)
}

// defaultTimeParams returns url.Values with start/end covering the last 72 hours
// to encompass all datagen data (48h window + margin).
func defaultTimeParams() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-72*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}
