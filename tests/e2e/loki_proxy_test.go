//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestLokiProxy_QueryRange(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	end := time.Now()
	start := end.Add(-72 * time.Hour)

	params := url.Values{
		"query":     {`{service_name=~".+"}`},
		"start":     {fmt.Sprintf("%d", start.UnixNano())},
		"end":       {fmt.Sprintf("%d", end.UnixNano())},
		"limit":     {"100"},
		"direction": {"backward"},
	}

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/query_range", params)

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v\nraw: %s", err, string(body))
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data.Result) == 0 {
		t.Fatal("expected at least one stream in results")
	}

	totalLines := 0
	for _, stream := range result.Data.Result {
		totalLines += len(stream.Values)
	}
	if totalLines == 0 {
		t.Fatal("expected at least one log line")
	}
	t.Logf("query_range returned %d streams, %d total lines", len(result.Data.Result), totalLines)
}

func TestLokiProxy_Labels(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/labels", nil)

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data) == 0 {
		t.Fatal("expected at least one label")
	}
	t.Logf("labels returned: %v", result.Data)
}

func TestLokiProxy_LabelValues(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/label/service_name/values", nil)

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data) == 0 {
		t.Fatal("expected at least one service name value")
	}
	t.Logf("service_name values: %v", result.Data)
}
