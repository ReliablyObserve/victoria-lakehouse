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
	"strings"
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
	logsBaseURL   = envOrDefault("LOGS_BASE_URL", "http://localhost:29428")
	tracesBaseURL = envOrDefault("TRACES_BASE_URL", "http://localhost:20428")
	lokiProxyURL  = envOrDefault("LOKI_PROXY_URL", "http://localhost:23100")
	vlselectURL   = envOrDefault("VLSELECT_URL", "http://localhost:29471")
	// peerAuthKey is the stack's peer.auth_key (the e2e stack sets one in
	// deployment/docker/lakehouse-e2e-config.yml); the pods require it on
	// their /internal/* peer endpoints.
	peerAuthKey = envOrDefault("LH_PEER_AUTH_KEY", "")
)

// bearer is the Authorization value carrying the stack's peer key, or "".
func bearer() string {
	if peerAuthKey == "" {
		return ""
	}
	return "Bearer " + peerAuthKey
}

// withPeerKey presents the peer key on requests to the endpoints peer.auth_key
// guards: the pods' /internal/* endpoints (as the pods themselves do) and the
// alias admin API (/lakehouse/api/v1/tenants/aliases, POST and DELETE).
func withPeerKey(req *http.Request) *http.Request {
	guarded := strings.HasPrefix(req.URL.Path, "/internal/") || strings.HasPrefix(req.URL.Path, "/lakehouse/api/v1/tenants/aliases")
	if b := bearer(); b != "" && guarded && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", b)
	}
	return req
}

// e2eParams adds disable_latency_offset=true to a LogsQL request against the
// traces binary. VictoriaTraces v0.12.0 hides the newest -search.latencyOffset
// (30s) of data from LogsQL by default, and the traces binary mirrors that; these
// tests ingest and query straight away and want to see everything, so they opt
// out. The offset itself is covered by lakehouse-traces/internal/selectapi and
// the conformance rows vt.select.logsql_query.latency_offset*.
func e2eParams(baseURL, path string, params url.Values) url.Values {
	if baseURL != tracesBaseURL || !strings.HasPrefix(path, "/select/logsql/") || params.Has("disable_latency_offset") {
		return params
	}
	out := url.Values{}
	for k, v := range params {
		out[k] = v
	}
	out.Set("disable_latency_offset", "true")
	return out
}

// httpGet performs an HTTP GET and returns the response, failing the test on error.
func httpGet(t *testing.T, baseURL, path string, params url.Values) *http.Response {
	t.Helper()

	params = e2eParams(baseURL, path, params)
	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	resp, err := client.Do(withPeerKey(req))
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
	defer func() { _ = resp.Body.Close() }()

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

	params = e2eParams(baseURL, path, params)
	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	client := &http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	resp, err := client.Do(withPeerKey(req))
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
	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s%s: %v", baseURL, path, err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.Do(withPeerKey(req))
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

// defaultTimeParams returns url.Values with start/end covering the last 30 minutes.
// This keeps queries fast (~25-50 files). Tests that need the full datagen
// range should use wideTimeParams().
func defaultTimeParams() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-30*time.Minute).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}

func wideTimeParams() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-72*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}
