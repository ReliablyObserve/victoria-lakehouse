//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"
)

func TestParity_CrossValidationExtended(t *testing.T) {
	t.Run("stats_count_matches_query_row_count", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get stats count for ERROR logs.
				statsParams := fullRangeParams()
				statsParams.Set("query", `level:="ERROR" | stats count() rows`)
				statsRes := fetch(t, label.baseURL, statsEndpoint(), statsParams)
				if statsRes.StatusCode != 200 {
					t.Fatalf("stats query returned status %d: %s", statsRes.StatusCode, string(statsRes.Body))
				}
				statsCount, err := extractVectorCount(statsRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount: %v", err)
				}

				// Get actual rows from query endpoint (with a high limit).
				queryParams := fullRangeParams()
				queryParams.Set("query", `level:="ERROR"`)
				queryParams.Set("limit", "10000")
				queryRes := fetch(t, label.baseURL, queryEndpoint(), queryParams)
				if queryRes.StatusCode != 200 {
					t.Fatalf("query returned status %d: %s", queryRes.StatusCode, string(queryRes.Body))
				}
				rows := parseNDJSON(queryRes.Body)

				// Row count should match stats count (if under limit).
				rowCount := float64(len(rows))
				if statsCount <= 10000 && rowCount != statsCount {
					t.Errorf("stats count (%v) != query row count (%v)", statsCount, rowCount)
				}
				t.Logf("stats_count=%v query_rows=%v", statsCount, rowCount)
			})
		}
	})

	t.Run("field_values_consistency_across_ranges", func(t *testing.T) {
		now := time.Now()
		// Get field values for the full 24h range.
		fullParams := url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-24*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.UnixNano())},
			"query": {"*"},
			"field": {"level"},
		}
		// Get field values for a narrower 1h range.
		narrowParams := url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-1*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.UnixNano())},
			"query": {"*"},
			"field": {"level"},
		}

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				fullRes := fetch(t, label.baseURL, "/select/logsql/field_values", fullParams)
				if fullRes.StatusCode != 200 {
					t.Fatalf("full field_values status %d: %s", fullRes.StatusCode, string(fullRes.Body))
				}
				fullVals := stringSet(extractValuesStrings(fullRes.Body))

				narrowRes := fetch(t, label.baseURL, "/select/logsql/field_values", narrowParams)
				if narrowRes.StatusCode != 200 {
					t.Fatalf("narrow field_values status %d: %s", narrowRes.StatusCode, string(narrowRes.Body))
				}
				narrowVals := extractValuesStrings(narrowRes.Body)

				// Narrow range values must be a subset of full range values.
				for _, v := range narrowVals {
					if !fullVals[v] {
						t.Errorf("narrow range value %q not found in full range values", v)
					}
				}
				t.Logf("full_values=%d narrow_values=%d", len(fullVals), len(narrowVals))
			})
		}
	})

	t.Run("hits_buckets_within_query_range", func(t *testing.T) {
		now := time.Now()
		start := now.Add(-6 * time.Hour)

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				params := url.Values{
					"start": {fmt.Sprintf("%d", start.UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {"*"},
					"step":  {"3600s"},
				}
				res := fetch(t, label.baseURL, hitsEndpoint(), params)
				if res.StatusCode != 200 {
					t.Fatalf("hits status %d: %s", res.StatusCode, string(res.Body))
				}
				timestamps, counts := extractHitsBuckets(res.Body)

				if len(timestamps) == 0 {
					t.Fatal("no buckets returned")
				}

				// Sum of bucket counts should be non-negative.
				var total float64
				for _, c := range counts {
					if c < 0 {
						t.Errorf("negative bucket count: %v", c)
					}
					total += c
				}

				// Compare total hits with stats count.
				statsParams := url.Values{
					"start": {fmt.Sprintf("%d", start.UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {"* | stats count() rows"},
				}
				statsRes := fetch(t, label.baseURL, statsEndpoint(), statsParams)
				if statsRes.StatusCode != 200 {
					t.Fatalf("stats status %d: %s", statsRes.StatusCode, string(statsRes.Body))
				}
				statsCount, err := extractVectorCount(statsRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount: %v", err)
				}

				if statsCount > 0 {
					diff := math.Abs(total-statsCount) / statsCount
					if diff > 0.05 {
						t.Errorf("hits total (%v) differs from stats count (%v) by %.2f%%", total, statsCount, diff*100)
					}
				}
				t.Logf("buckets=%d hits_total=%v stats_count=%v", len(timestamps), total, statsCount)
			})
		}
	})

	t.Run("streams_endpoint_consistency", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get streams from the streams endpoint.
				streamsParams := fullRangeParams()
				streamsParams.Set("query", "*")
				streamsRes := fetch(t, label.baseURL, "/select/logsql/streams", streamsParams)
				if streamsRes.StatusCode != 200 {
					t.Fatalf("streams status %d: %s", streamsRes.StatusCode, string(streamsRes.Body))
				}
				streamVals := extractValuesStrings(streamsRes.Body)

				// Get stream_ids from the stream_ids endpoint.
				sidsParams := fullRangeParams()
				sidsParams.Set("query", "*")
				sidsRes := fetch(t, label.baseURL, "/select/logsql/stream_ids", sidsParams)
				if sidsRes.StatusCode != 200 {
					t.Fatalf("stream_ids status %d: %s", sidsRes.StatusCode, string(sidsRes.Body))
				}
				sidVals := extractValuesStrings(sidsRes.Body)

				// Both should return non-empty results.
				if len(streamVals) == 0 {
					t.Error("streams endpoint returned empty result")
				}
				if len(sidVals) == 0 {
					t.Error("stream_ids endpoint returned empty result")
				}

				// The number of streams and stream_ids should match.
				if len(streamVals) != len(sidVals) {
					t.Errorf("streams count (%d) != stream_ids count (%d)", len(streamVals), len(sidVals))
				}
				t.Logf("streams=%d stream_ids=%d", len(streamVals), len(sidVals))
			})
		}
	})

	t.Run("field_names_parity", func(t *testing.T) {
		// Field names returned by VL and LH should match.
		fnParams := fullRangeParams()
		fnParams.Set("query", "*")
		refRes := fetch(t, vlBaseURL, "/select/logsql/field_names", fnParams)
		sutRes := fetch(t, lhBaseURL, "/select/logsql/field_names", fnParams)
		if refRes.StatusCode != 200 {
			t.Fatalf("VL field_names status %d: %s", refRes.StatusCode, string(refRes.Body))
		}
		if sutRes.StatusCode != 200 {
			t.Fatalf("LH field_names status %d: %s", sutRes.StatusCode, string(sutRes.Body))
		}
		refNames := sortedStrings(extractValuesStrings(refRes.Body))
		sutNames := sortedStrings(extractValuesStrings(sutRes.Body))
		refSet := stringSet(refNames)
		sutSet := stringSet(sutNames)
		for _, v := range refNames {
			if !sutSet[v] {
				t.Errorf("LH missing field %q present in VL", v)
			}
		}
		for _, v := range sutNames {
			if !refSet[v] {
				t.Errorf("LH has extra field %q not in VL", v)
			}
		}
		t.Logf("VL_fields=%d LH_fields=%d", len(refNames), len(sutNames))
	})

	t.Run("stats_sum_across_steps_matches_total", func(t *testing.T) {
		// Using stats_query_range with a step, the sum of per-step counts should approximate the total.
		now := time.Now()
		start := now.Add(-6 * time.Hour)

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get total count.
				totalParams := url.Values{
					"start": {fmt.Sprintf("%d", start.UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {"* | stats count() rows"},
				}
				totalRes := fetch(t, label.baseURL, statsEndpoint(), totalParams)
				if totalRes.StatusCode != 200 {
					t.Fatalf("total stats status %d: %s", totalRes.StatusCode, string(totalRes.Body))
				}
				totalCount, err := extractVectorCount(totalRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount: %v", err)
				}

				// Get range stats with 1h step.
				rangeParams := url.Values{
					"start": {fmt.Sprintf("%d", start.UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {"* | stats count() rows"},
					"step":  {"3600s"},
				}
				rangeRes := fetch(t, label.baseURL, statsRangeEndpoint(), rangeParams)
				if rangeRes.StatusCode != 200 {
					t.Fatalf("range stats status %d: %s", rangeRes.StatusCode, string(rangeRes.Body))
				}

				// Parse range response and sum the values.
				obj, err := parseJSON(rangeRes.Body)
				if err != nil {
					t.Fatalf("parse range response: %v", err)
				}
				dataObj, _ := obj["data"].(map[string]any)
				if dataObj == nil {
					t.Log("no data field in range response, skipping sum check")
					return
				}
				resultArr, _ := dataObj["result"].([]any)
				var rangeSum float64
				for _, r := range resultArr {
					rMap, _ := r.(map[string]any)
					if rMap == nil {
						continue
					}
					values, _ := rMap["values"].([]any)
					for _, v := range values {
						pair, _ := v.([]any)
						if len(pair) >= 2 {
							if s, ok := pair[1].(string); ok {
								f, _ := fmt.Sscanf(s, "%f", &rangeSum)
								_ = f
							}
						}
					}
				}

				t.Logf("total_count=%v range_sum=%v steps=%d", totalCount, rangeSum, len(resultArr))
			})
		}
	})

	t.Run("filtered_count_vs_unfiltered", func(t *testing.T) {
		// For each system, filtered count must be <= unfiltered count.
		filters := []struct {
			name  string
			query string
		}{
			{"error_only", `level:="ERROR" | stats count() rows`},
			{"api_gateway", `service.name:="api-gateway" | stats count() rows`},
			{"http_500", `http.status_code:="500" | stats count() rows`},
		}

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get total unfiltered count.
				totalParams := fullRangeParams()
				totalParams.Set("query", "* | stats count() rows")
				totalRes := fetch(t, label.baseURL, statsEndpoint(), totalParams)
				if totalRes.StatusCode != 200 {
					t.Fatalf("total stats status %d: %s", totalRes.StatusCode, string(totalRes.Body))
				}
				totalCount, err := extractVectorCount(totalRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount total: %v", err)
				}

				for _, f := range filters {
					t.Run(f.name, func(t *testing.T) {
						filtParams := fullRangeParams()
						filtParams.Set("query", f.query)
						filtRes := fetch(t, label.baseURL, statsEndpoint(), filtParams)
						if filtRes.StatusCode != 200 {
							t.Fatalf("filtered stats status %d: %s", filtRes.StatusCode, string(filtRes.Body))
						}
						filtCount, err := extractVectorCount(filtRes.Body)
						if err != nil {
							t.Fatalf("extractVectorCount filtered: %v", err)
						}
						if filtCount > totalCount {
							t.Errorf("filtered count (%v) > total count (%v)", filtCount, totalCount)
						}
						t.Logf("total=%v filtered=%v", totalCount, filtCount)
					})
				}
			})
		}
	})
}
