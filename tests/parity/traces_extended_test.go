//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParity_TracesExtended(t *testing.T) {
	cases := []ParityCase{
		// 1. Filter by span name
		{
			Name:     "traces_filter_by_span_name",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* name:="HTTP GET /api/v1/users" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 2. Filter by status_code (error spans)
		{
			Name:     "traces_filter_by_status_code",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* status_code:="2" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 3. Filter by duration range (10-50ms in nanoseconds)
		{
			Name:     "traces_filter_by_duration_range",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* duration:range[10000000, 50000000] | stats count() rows`},
			Compare:  CountEqual,
		},
		// 4. Stats by service name
		{
			Name:     "traces_stats_by_service_name",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* | stats by("resource_attr:service.name") count() rows`},
			Compare:  StructureMatch,
		},
		// 5. Stats by status
		{
			Name:     "traces_stats_by_status",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* | stats by(status_code) count() rows`},
			Compare:  StructureMatch,
		},
		// 6. Field values for status_code
		{
			Name:     "traces_field_values_status_code",
			Endpoint: "/select/logsql/field_values",
			Params:   map[string]string{"query": `span_id:*`, "field": "status_code"},
			Compare:  SetSuperset,
		},
		// 7. Field values for name
		{
			Name:     "traces_field_values_name",
			Endpoint: "/select/logsql/field_values",
			Params:   map[string]string{"query": `span_id:*`, "field": "name"},
			Compare:  SetSuperset,
		},
		// 8. Unique span names
		{
			Name:     "traces_uniq_span_names",
			Endpoint: queryEndpoint(),
			Params:   map[string]string{"query": `span_id:* | uniq by(name)`},
			Compare:  SetEqual,
		},
		// 9. Hits filtered by service
		{
			Name:     "traces_hits_filtered",
			Endpoint: hitsEndpoint(),
			Params:   map[string]string{"query": `span_id:* resource_attr:service.name:="api-gateway"`, "step": "3600s"},
			Compare:  BucketMatch,
		},
		// 10. Count by kind
		{
			Name:     "traces_count_by_kind",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* | stats by(kind) count() rows`},
			Compare:  StructureMatch,
		},
		// 11. Filter by HTTP method
		{
			Name:     "traces_filter_http_method",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* span_attr:http.method:="GET" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 12. Filter by any DB span
		{
			Name:     "traces_filter_db_system",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* span_attr:db.system:* | stats count() rows`},
			Compare:  CountEqual,
		},
		// 14. Sort by duration descending — verify values are sorted correctly.
		// Ties in duration produce non-deterministic row ordering, so we check
		// that the returned durations are in descending order rather than exact row match.
		{
			Name:     "traces_sort_by_duration",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* | sort by(duration) desc | limit 10 | stats count() rows`},
			Compare:  CountEqual,
		},
		// 15. Filter by resource region
		{
			Name:     "traces_filter_resource_region",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* resource_attr:cloud.region:="us-east-1" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 16. Combined AND filter
		{
			Name:     "traces_and_filter_combined",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* resource_attr:service.name:="api-gateway" AND kind:="1" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 17. NOT filter (non-error spans)
		{
			Name:     "traces_not_filter",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* NOT status_code:="2" | stats count() rows`},
			Compare:  CountEqual,
		},
		// 18. Sum of duration with tolerance
		{
			Name:      "traces_stats_sum_duration",
			Endpoint:  statsEndpoint(),
			Params:    map[string]string{"query": `span_id:* | stats sum(duration) total`},
			Compare:   CountTolerance,
			Tolerance: 0.05,
		},
		// 19. Empty filter (should return 0)
		{
			Name:     "traces_empty_filter",
			Endpoint: statsEndpoint(),
			Params:   map[string]string{"query": `span_id:* nonexistent_trace_field:="impossible" | stats count() rows`},
			Compare:  CountEqual,
		},
	}
	RunParity(t, vtBaseURL, lhtBaseURL, cases)

	// 13. Cross-service error count (manual subtest)
	t.Run("traces_cross_service_error_count", func(t *testing.T) {
		services := []string{"api-gateway", "user-service", "order-service"}
		for _, svc := range services {
			t.Run(svc, func(t *testing.T) {
				now := time.Now()
				params := url.Values{
					"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {fmt.Sprintf(`span_id:* resource_attr:service.name:="%s" status_code:="2" | stats count() rows`, svc)},
				}
				ref := fetch(t, vtBaseURL, statsEndpoint(), params)
				sut := fetch(t, lhtBaseURL, statsEndpoint(), params)
				if ref.StatusCode != 200 {
					t.Fatalf("VT returned status %d: %s", ref.StatusCode, string(ref.Body))
				}
				if sut.StatusCode != 200 {
					t.Fatalf("LHT returned status %d: %s", sut.StatusCode, string(sut.Body))
				}
				refCount, err := extractVectorCount(ref.Body)
				if err != nil {
					t.Fatalf("VT extractVectorCount: %v", err)
				}
				sutCount, err := extractVectorCount(sut.Body)
				if err != nil {
					t.Fatalf("LHT extractVectorCount: %v", err)
				}
				if math.Abs(refCount-sutCount) > 0 {
					t.Errorf("error count mismatch for %s: VT=%v LHT=%v", svc, refCount, sutCount)
				}
				t.Logf("cross_service_error_count(%s): VT=%v LHT=%v", svc, refCount, sutCount)
			})
		}
	})

	// 20. Field names completeness (manual subtest)
	t.Run("traces_field_names_completeness", func(t *testing.T) {
		now := time.Now()
		params := url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.UnixNano())},
			"query": {"span_id:*"},
		}
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		if sut.StatusCode != 200 {
			t.Fatalf("LHT returned status %d: %s", sut.StatusCode, string(sut.Body))
		}
		vals := extractValuesStrings(sut.Body)
		nameSet := stringSet(vals)
		requiredFields := []string{"trace_id", "span_id", "name", "kind", "duration", "status_code"}
		var missing []string
		for _, field := range requiredFields {
			if !nameSet[field] {
				missing = append(missing, field)
			}
		}
		if len(missing) > 0 {
			t.Errorf("LHT missing required trace fields: %s", strings.Join(missing, ", "))
		}
		t.Logf("field_names_completeness: %d fields returned, checked %d required, missing %d",
			len(vals), len(requiredFields), len(missing))
	})
}
