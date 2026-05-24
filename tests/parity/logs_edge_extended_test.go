//go:build parity

package parity

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParity_EdgeCasesExtended(t *testing.T) {
	t.Run("backward_time_range", func(t *testing.T) {
		now := time.Now()
		params := url.Values{
			"start": {fmt.Sprintf("%d", now.UnixNano())},
			"end":   {fmt.Sprintf("%d", now.Add(-24*time.Hour).UnixNano())},
			"query": {"* | stats count() rows"},
		}
		ref := fetch(t, vlBaseURL, statsEndpoint(), params)
		sut := fetch(t, lhBaseURL, statsEndpoint(), params)
		t.Logf("backward_time_range: ref_status=%d sut_status=%d", ref.StatusCode, sut.StatusCode)
		if ref.StatusCode != sut.StatusCode {
			t.Errorf("status mismatch: ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
			return
		}
		if ref.StatusCode == 200 && sut.StatusCode == 200 {
			refCount, refErr := extractVectorCount(ref.Body)
			sutCount, sutErr := extractVectorCount(sut.Body)
			if refErr == nil && sutErr == nil && refCount != sutCount {
				t.Errorf("count mismatch: ref=%v sut=%v", refCount, sutCount)
			}
			t.Logf("backward_time_range: ref_count=%v sut_count=%v", refCount, sutCount)
		}
	})

	t.Run("extra_filters_param", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "* | stats count() rows")
		params.Set("extra_filters", `level:="ERROR"`)
		withExtraVL := fetch(t, vlBaseURL, statsEndpoint(), params)
		withExtraLH := fetch(t, lhBaseURL, statsEndpoint(), params)

		directParams := fullRangeParams()
		directParams.Set("query", `level:="ERROR" | stats count() rows`)
		directVL := fetch(t, vlBaseURL, statsEndpoint(), directParams)
		directLH := fetch(t, lhBaseURL, statsEndpoint(), directParams)

		if withExtraVL.StatusCode == 200 && directVL.StatusCode == 200 {
			extraCount, err1 := extractVectorCount(withExtraVL.Body)
			directCount, err2 := extractVectorCount(directVL.Body)
			if err1 == nil && err2 == nil && extraCount != directCount {
				t.Errorf("VL extra_filters count=%v != direct count=%v", extraCount, directCount)
			}
			t.Logf("VL: extra_filters=%v direct=%v", extraCount, directCount)
		}
		if withExtraLH.StatusCode == 200 && directLH.StatusCode == 200 {
			extraCount, err1 := extractVectorCount(withExtraLH.Body)
			directCount, err2 := extractVectorCount(directLH.Body)
			if err1 == nil && err2 == nil && extraCount != directCount {
				t.Errorf("LH extra_filters count=%v != direct count=%v", extraCount, directCount)
			}
			t.Logf("LH: extra_filters=%v direct=%v", extraCount, directCount)
		}
	})

	t.Run("repeated_identical_queries", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "* | stats count() rows")

		var results []string
		for i := 0; i < 5; i++ {
			r := fetch(t, lhBaseURL, statsEndpoint(), params)
			if r.StatusCode != 200 {
				t.Fatalf("iteration %d: status=%d", i, r.StatusCode)
			}
			results = append(results, strings.TrimSpace(string(r.Body)))
		}
		for i := 1; i < len(results); i++ {
			if results[i] != results[0] {
				t.Errorf("iteration %d differs from iteration 0:\n  [0]: %s\n  [%d]: %s", i, results[0], i, results[i])
			}
		}
		t.Logf("repeated_identical_queries: 5 queries, all identical=%v", results[0] == results[len(results)-1])
	})

	t.Run("pipe_no_matching_data", func(t *testing.T) {
		pc := ParityCase{
			Name:     "pipe_no_matching_data",
			Endpoint: queryEndpoint(),
			Params: map[string]string{
				"query": `nonexistent_xyz:="impossible" | fields _time, _msg | sort by(_time) | limit 10`,
				"limit": "10",
			},
			Compare: StatusEqual,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})

		// Also verify both return empty rows
		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		if len(refRows) != 0 {
			t.Errorf("VL returned %d rows, expected 0", len(refRows))
		}
		if len(sutRows) != 0 {
			t.Errorf("LH returned %d rows, expected 0", len(sutRows))
		}
		t.Logf("pipe_no_matching_data: ref_rows=%d sut_rows=%d", len(refRows), len(sutRows))
	})

	t.Run("nested_parens", func(t *testing.T) {
		pc := ParityCase{
			Name:     "nested_parens",
			Endpoint: statsEndpoint(),
			Params: map[string]string{
				"query": `((level:="ERROR") OR ((level:="WARN") AND (service.name:="api-gateway"))) | stats count() rows`,
			},
			Compare: CountEqual,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("query_with_spaces", func(t *testing.T) {
		pc := ParityCase{
			Name:     "query_with_spaces",
			Endpoint: statsEndpoint(),
			Params: map[string]string{
				"query": `*  |  stats  count()  rows`,
			},
			Compare: CountEqual,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("concurrent_mixed", func(t *testing.T) {
		params := fullRangeParams()
		queries := []struct {
			name     string
			endpoint string
			extra    map[string]string
		}{
			{"wildcard_count", statsEndpoint(), map[string]string{"query": "* | stats count() rows"}},
			{"filtered_count", statsEndpoint(), map[string]string{"query": `level:="ERROR" | stats count() rows`}},
			{"field_names", "/select/logsql/field_names", map[string]string{"query": "*"}},
			{"field_values", "/select/logsql/field_values", map[string]string{"query": "*", "field": "level"}},
			{"hits", hitsEndpoint(), map[string]string{"query": "*", "step": "3600s"}},
		}

		type result struct {
			name   string
			status int
		}
		var mu sync.Mutex
		var wg sync.WaitGroup
		results := make([]result, len(queries))
		for i, q := range queries {
			wg.Add(1)
			go func(idx int, name, endpoint string, extra map[string]string) {
				defer wg.Done()
				p := url.Values{}
				for k, v := range params {
					p[k] = v
				}
				for k, v := range extra {
					p.Set(k, v)
				}
				r := fetch(t, lhBaseURL, endpoint, p)
				mu.Lock()
				results[idx] = result{name: name, status: r.StatusCode}
				mu.Unlock()
			}(i, q.name, q.endpoint, q.extra)
		}
		wg.Wait()

		for _, r := range results {
			if r.status != 200 {
				t.Errorf("concurrent_mixed %s: status=%d, expected 200", r.name, r.status)
			}
		}
		t.Logf("concurrent_mixed: all %d queries completed", len(queries))
	})

	t.Run("very_large_offset", func(t *testing.T) {
		pc := ParityCase{
			Name:     "very_large_offset",
			Endpoint: queryEndpoint(),
			Params: map[string]string{
				"query": "* | sort by(_time) | offset 1000000 | limit 10",
				"limit": "10",
			},
			Compare: StatusEqual,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})

		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		t.Logf("very_large_offset: ref_rows=%d sut_rows=%d", len(refRows), len(sutRows))
		if len(refRows) != 0 || len(sutRows) != 0 {
			t.Logf("note: one or both returned rows beyond offset 1M (may be valid if dataset is large)")
		}
	})

	t.Run("limit_equals_offset", func(t *testing.T) {
		pc := ParityCase{
			Name:     "limit_equals_offset",
			Endpoint: queryEndpoint(),
			Params: map[string]string{
				"query": "* | sort by(_time) desc | offset 5 | limit 5",
				"limit": "10",
			},
			Compare: RowsMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("empty_field_projection", func(t *testing.T) {
		pc := ParityCase{
			Name:     "empty_field_projection",
			Endpoint: queryEndpoint(),
			Params: map[string]string{
				"query": "* | fields _time, nonexistent_field | sort by(_time) desc",
				"limit": "5",
			},
			Compare: RowsMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("multiple_sort_keys", func(t *testing.T) {
		pc := ParityCase{
			Name:     "multiple_sort_keys",
			Endpoint: queryEndpoint(),
			Params: map[string]string{
				"query": "* | sort by(level, _time) | limit 10",
				"limit": "10",
			},
			Compare: RowsMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_small_step", func(t *testing.T) {
		pc := ParityCase{
			Name:     "hits_small_step",
			Endpoint: hitsEndpoint(),
			Params: map[string]string{
				"query": "*",
				"step":  "60s",
			},
			Compare: BucketMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_filtered", func(t *testing.T) {
		pc := ParityCase{
			Name:     "hits_filtered",
			Endpoint: hitsEndpoint(),
			Params: map[string]string{
				"query": `level:="ERROR"`,
				"step":  "3600s",
			},
			Compare: BucketMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("stats_range_small_step", func(t *testing.T) {
		pc := ParityCase{
			Name:     "stats_range_small_step",
			Endpoint: "/select/logsql/stats_query_range",
			Params: map[string]string{
				"query": "* | stats count() rows",
				"step":  "60s",
			},
			Compare: StructureMatch,
		}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("query_count_consistency", func(t *testing.T) {
		params := fullRangeParams()

		// Fetch stats count on both
		statsParams := url.Values{}
		for k, v := range params {
			statsParams[k] = v
		}
		statsParams.Set("query", "* | stats count() rows")
		vlStats := fetch(t, vlBaseURL, statsEndpoint(), statsParams)
		lhStats := fetch(t, lhBaseURL, statsEndpoint(), statsParams)

		// Fetch query rows with large limit on both
		queryParams := url.Values{}
		for k, v := range params {
			queryParams[k] = v
		}
		queryParams.Set("query", "*")
		queryParams.Set("limit", "100000")
		vlQuery := fetch(t, vlBaseURL, queryEndpoint(), queryParams)
		lhQuery := fetch(t, lhBaseURL, queryEndpoint(), queryParams)

		// Compare VL stats vs VL query row count
		if vlStats.StatusCode == 200 && vlQuery.StatusCode == 200 {
			statsCount, err := extractVectorCount(vlStats.Body)
			if err == nil {
				queryRows := parseNDJSON(vlQuery.Body)
				t.Logf("VL: stats_count=%v query_rows=%d", statsCount, len(queryRows))
				// Query with limit may return fewer; only flag if query returned more
				if float64(len(queryRows)) > statsCount {
					t.Errorf("VL query rows (%d) > stats count (%v)", len(queryRows), statsCount)
				}
			}
		}

		// Compare LH stats vs LH query row count
		if lhStats.StatusCode == 200 && lhQuery.StatusCode == 200 {
			statsCount, err := extractVectorCount(lhStats.Body)
			if err == nil {
				queryRows := parseNDJSON(lhQuery.Body)
				t.Logf("LH: stats_count=%v query_rows=%d", statsCount, len(queryRows))
				if float64(len(queryRows)) > statsCount {
					t.Errorf("LH query rows (%d) > stats count (%v)", len(queryRows), statsCount)
				}
			}
		}

		// Cross-system parity: VL stats vs LH stats
		if vlStats.StatusCode == 200 && lhStats.StatusCode == 200 {
			vlCount, err1 := extractVectorCount(vlStats.Body)
			lhCount, err2 := extractVectorCount(lhStats.Body)
			if err1 == nil && err2 == nil && vlCount != lhCount {
				t.Errorf("stats count parity: VL=%v LH=%v", vlCount, lhCount)
			}
		}
	})
}
