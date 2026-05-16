//go:build e2e

package e2e

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

func TestAZ_CacheStatsIncludesAZ(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/internal/cache/stats", nil)

	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("decode cache stats: %v", err)
	}

	az, ok := stats["az"]
	if !ok {
		t.Fatal("cache stats should include 'az' field")
	}

	azStr, ok := az.(string)
	if !ok {
		t.Fatalf("az field should be string, got %T", az)
	}
	if azStr != "az-a" {
		t.Errorf("expected AZ=az-a (from LAKEHOUSE_AZ env), got %q", azStr)
	}
}

func TestAZ_HealthAndReadyWork(t *testing.T) {
	_ = httpGetBody(t, logsBaseURL, "/health", nil)
	_ = httpGetBody(t, logsBaseURL, "/ready", nil)
}

func TestAZ_QueriesStillWorkWithAZ(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
		"limit": {"5"},
	}
	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	if len(body) == 0 {
		t.Error("query should return data even with AZ-aware routing")
	}
}

func TestAZ_TracesCacheStatsIncludesAZ(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/internal/cache/stats", nil)

	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("decode traces cache stats: %v", err)
	}

	az, ok := stats["az"]
	if !ok {
		t.Fatal("traces cache stats should include 'az' field")
	}

	azStr, ok := az.(string)
	if !ok {
		t.Fatalf("az field should be string, got %T", az)
	}
	if azStr != "az-a" {
		t.Errorf("expected AZ=az-a (from LAKEHOUSE_AZ env), got %q", azStr)
	}
}

func TestAZ_TracesQueriesWorkWithAZ(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
		"limit": {"5"},
	}
	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	if len(body) == 0 {
		t.Error("traces query should return data with AZ-aware routing")
	}
}

func TestAZ_TracesStreamsWork(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
	}
	body := httpGetBody(t, tracesBaseURL, "/select/logsql/streams", params)
	if len(body) == 0 {
		t.Error("traces streams should return data")
	}
}

func TestAZ_TracesFieldNamesWork(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
	}
	body := httpGetBody(t, tracesBaseURL, "/select/logsql/field_names", params)
	if len(body) == 0 {
		t.Error("traces field_names should return data")
	}
}

func TestAZ_LogsStreamsWork(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
	}
	body := httpGetBody(t, logsBaseURL, "/select/logsql/streams", params)
	if len(body) == 0 {
		t.Error("logs streams should return data")
	}
}

func TestAZ_LogsStreamIDsWork(t *testing.T) {
	params := url.Values{
		"query": {"*"},
		"start": {time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339)},
		"end":   {time.Now().UTC().Format(time.RFC3339)},
	}
	body := httpGetBody(t, logsBaseURL, "/select/logsql/stream_ids", params)
	if len(body) == 0 {
		t.Error("logs stream_ids should return data")
	}
}
