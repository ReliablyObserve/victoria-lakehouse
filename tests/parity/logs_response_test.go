//go:build parity

package parity

import (
	"testing"
)

func TestParity_Response(t *testing.T) {
	t.Run("jsonl_structure", func(t *testing.T) {
		pc := ParityCase{Name: "jsonl_structure", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "10"}, Compare: RowsMatch}
		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		if len(refRows) == 0 {
			t.Fatal("reference returned 0 JSONL rows")
		}
		if len(sutRows) == 0 {
			t.Fatal("SUT returned 0 JSONL rows")
		}
		compareParity(t, pc, ref, sut)
	})

	t.Run("limit_respected", func(t *testing.T) {
		pc := ParityCase{Name: "limit_respected", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "5"}, Compare: RowsMatch}
		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		if len(refRows) > 5 {
			t.Errorf("reference returned %d rows, expected <= 5", len(refRows))
		}
		if len(sutRows) > 5 {
			t.Errorf("SUT returned %d rows, expected <= 5", len(sutRows))
		}
		t.Logf("limit=5: ref=%d sut=%d", len(refRows), len(sutRows))
	})

	t.Run("limit_zero", func(t *testing.T) {
		pc := ParityCase{Name: "limit_zero", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_bucket_keys", func(t *testing.T) {
		pc := ParityCase{Name: "hits_bucket_keys", Endpoint: hitsEndpoint(), Params: map[string]string{"query": "*", "step": "1800s"}, Compare: BucketMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_sum_equals_count", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "*")
		params.Set("step", "3600s")
		hitsRef := fetch(t, vlBaseURL, hitsEndpoint(), params)
		_, refCounts := extractHitsBuckets(hitsRef.Body)
		totalHits := 0.0
		for _, c := range refCounts {
			totalHits += c
		}

		statsParams := fullRangeParams()
		statsParams.Set("query", "* | stats count() rows")
		statsRef := fetch(t, vlBaseURL, statsEndpoint(), statsParams)
		statsCount, err := extractVectorCount(statsRef.Body)
		if err != nil {
			t.Fatalf("extract stats count: %v", err)
		}
		diff := totalHits - statsCount
		if diff < 0 {
			diff = -diff
		}
		pct := 0.0
		if statsCount > 0 {
			pct = diff / statsCount
		}
		if pct > 0.02 {
			t.Errorf("hits sum (%.0f) != stats count (%.0f), diff=%.1f%%", totalHits, statsCount, pct*100)
		}
		t.Logf("hits_sum=%.0f stats_count=%.0f diff=%.1f%%", totalHits, statsCount, pct*100)
	})

	t.Run("stats_vector_format", func(t *testing.T) {
		pc := ParityCase{Name: "stats_vector_format", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: StructureMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("stats_range_matrix", func(t *testing.T) {
		pc := ParityCase{Name: "stats_range_matrix", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "3600s"}, Compare: StructureMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("field_names_jsonl", func(t *testing.T) {
		pc := ParityCase{Name: "field_names_jsonl", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("field_values_jsonl", func(t *testing.T) {
		pc := ParityCase{Name: "field_values_jsonl", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "level"}, Compare: SetEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("large_limit", func(t *testing.T) {
		pc := ParityCase{Name: "large_limit", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "100000"}, Compare: NonEmpty}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})
}
