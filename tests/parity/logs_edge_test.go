//go:build parity

package parity

import (
	"net/url"
	"strings"
	"sync"
	"testing"
)

func TestParity_EdgeCases(t *testing.T) {
	t.Run("empty_filter", func(t *testing.T) {
		pc := ParityCase{Name: "empty_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `nonexistent_service:="xxx" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("tail_streaming", func(t *testing.T) {
		ref := fetchShort(t, vlBaseURL, "/select/logsql/tail", url.Values{"query": {"*"}})
		sut := fetchShort(t, lhBaseURL, "/select/logsql/tail", url.Values{"query": {"*"}})
		t.Logf("tail: ref_status=%d sut_status=%d", ref.StatusCode, sut.StatusCode)
		if ref.StatusCode == 0 {
			t.Log("VL timed out (streaming tail) — expected")
		}
		if sut.StatusCode == 501 || sut.StatusCode == 0 {
			t.Log("LH returned 501 or timeout — expected (tail not implemented)")
		} else if sut.StatusCode != ref.StatusCode {
			t.Errorf("unexpected SUT status %d", sut.StatusCode)
		}
	})

	t.Run("invalid_query", func(t *testing.T) {
		pc := ParityCase{Name: "invalid_query", Endpoint: queryEndpoint(), Params: map[string]string{"query": ")))invalid"}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("missing_query", func(t *testing.T) {
		pc := ParityCase{Name: "missing_query", Endpoint: queryEndpoint(), Params: map[string]string{}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("special_chars", func(t *testing.T) {
		pc := ParityCase{Name: "special_chars", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="hello \"world\"" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("unicode_msg", func(t *testing.T) {
		pc := ParityCase{Name: "unicode_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="日本語" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("empty_string_filter", func(t *testing.T) {
		pc := ParityCase{Name: "empty_string_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("very_long_query", func(t *testing.T) {
		longFilter := `_msg:="` + strings.Repeat("a", 1000) + `" | stats count() rows`
		pc := ParityCase{Name: "very_long_query", Endpoint: statsEndpoint(), Params: map[string]string{"query": longFilter}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("concurrent_queries", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "* | stats count() rows")

		var wg sync.WaitGroup
		results := make([]fetchResult, 10)
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = fetch(t, lhBaseURL, statsEndpoint(), params)
			}(i)
		}
		wg.Wait()

		refResult := fetch(t, vlBaseURL, statsEndpoint(), params)
		refCount, err := extractVectorCount(refResult.Body)
		if err != nil {
			t.Fatalf("ref count: %v", err)
		}

		for i, r := range results {
			if r.StatusCode != 200 {
				t.Errorf("concurrent[%d] status=%d", i, r.StatusCode)
				continue
			}
			sutCount, err := extractVectorCount(r.Body)
			if err != nil {
				t.Errorf("concurrent[%d] parse: %v", i, err)
				continue
			}
			if sutCount != refCount {
				t.Errorf("concurrent[%d] count=%v expected=%v", i, sutCount, refCount)
			}
		}
		t.Logf("concurrent: 10 queries all returned %v", refCount)
	})

	t.Run("stats_no_pipe", func(t *testing.T) {
		pc := ParityCase{Name: "stats_no_pipe", Endpoint: statsEndpoint(), Params: map[string]string{"query": "*"}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})
}
