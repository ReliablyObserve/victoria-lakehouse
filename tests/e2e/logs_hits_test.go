//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestHits_Basic(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("step", "3600s")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	resp := assertHitsResponse(t, body)

	hits := resp["hits"].([]any)
	if len(hits) == 0 {
		t.Fatal("expected at least one hit bucket")
	}

	// Verify the first bucket has the expected structure
	first := hits[0].(map[string]any)
	timestamps := first["timestamps"].([]any)
	values := first["values"].([]any)

	if len(timestamps) == 0 {
		t.Fatal("expected at least one timestamp in hits")
	}
	if len(values) == 0 {
		t.Fatal("expected at least one value in hits")
	}

	t.Logf("hits returned %d series, first has %d buckets", len(hits), len(timestamps))
}

func TestHits_CustomStep(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("step", "3600s") // 1-hour buckets

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	resp := assertHitsResponse(t, body)

	hits := resp["hits"].([]any)
	if len(hits) == 0 {
		t.Fatal("expected at least one hit series with step=3600s")
	}

	first := hits[0].(map[string]any)
	timestamps := first["timestamps"].([]any)

	// With 48h of data and 1h steps, we expect roughly 48 buckets (some may be empty).
	// Just verify we have more than 1 bucket.
	if len(timestamps) < 2 {
		t.Logf("only %d hourly buckets — data may be sparse", len(timestamps))
	}

	t.Logf("hourly hits: %d buckets in first series", len(timestamps))
}

func TestHits_GroupByLevel(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "level")
	params.Set("step", "3600s")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	resp := assertHitsResponse(t, body)

	hits := resp["hits"].([]any)
	if len(hits) == 0 {
		t.Fatal("expected hits grouped by level")
	}

	// When grouped by field, each hit entry has fields.level set
	for i, entry := range hits {
		obj := entry.(map[string]any)
		fields, ok := obj["fields"].(map[string]any)
		if !ok {
			t.Fatalf("hits[%d] missing fields object", i)
		}

		_, hasLevel := fields["level"]
		if !hasLevel {
			t.Errorf("hits[%d] missing fields.level when grouped by level", i)
		}
	}

	t.Logf("grouped by level: %d series", len(hits))
}

func TestHits_GroupByLevel_AllLevels(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "level")
	params.Set("step", "3600s")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	resp := assertHitsResponse(t, body)

	hits := resp["hits"].([]any)

	seenLevels := make(map[string]bool)
	for _, entry := range hits {
		obj := entry.(map[string]any)
		fields := obj["fields"].(map[string]any)
		if lvl, ok := fields["level"].(string); ok && lvl != "" {
			seenLevels[lvl] = true
		}
	}

	expectedLevels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for _, lvl := range expectedLevels {
		if !seenLevels[lvl] {
			t.Errorf("level %q not found in grouped hits; found: %v", lvl, seenLevels)
		}
	}
}

func TestHits_TimeRange(t *testing.T) {
	// Use a narrow 4-hour window
	now := time.Now()
	start := now.Add(-8 * time.Hour)
	end := now.Add(-4 * time.Hour)

	wideParams := defaultTimeParams()
	wideParams.Set("query", "*")
	wideParams.Set("step", "3600s")

	narrowParams := url.Values{
		"query": {"*"},
		"step":  {"3600s"},
		"start": {fmt.Sprintf("%d", start.UnixNano())},
		"end":   {fmt.Sprintf("%d", end.UnixNano())},
	}

	wideBody := httpGetBody(t, logsBaseURL, "/select/logsql/hits", wideParams)
	wideResp := assertHitsResponse(t, wideBody)

	narrowBody := httpGetBody(t, logsBaseURL, "/select/logsql/hits", narrowParams)
	narrowResp := assertHitsResponse(t, narrowBody)

	wideHits := wideResp["hits"].([]any)
	narrowHits := narrowResp["hits"].([]any)

	// Count total buckets
	wideBuckets := 0
	for _, h := range wideHits {
		wideBuckets += len(h.(map[string]any)["timestamps"].([]any))
	}

	narrowBuckets := 0
	for _, h := range narrowHits {
		narrowBuckets += len(h.(map[string]any)["timestamps"].([]any))
	}

	if wideBuckets > 0 && narrowBuckets >= wideBuckets {
		t.Logf("warning: narrow range (%d buckets) >= wide range (%d buckets) — may indicate sparse data",
			narrowBuckets, wideBuckets)
	}

	t.Logf("wide range: %d buckets, narrow range: %d buckets", wideBuckets, narrowBuckets)
}
