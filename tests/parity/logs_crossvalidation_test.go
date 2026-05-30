//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"
)

func TestParity_CrossValidation(t *testing.T) {
	t.Run("sum_per_service_equals_total", func(t *testing.T) {
		services := []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get total count
				params := fullRangeParams()
				params.Set("query", "* | stats count() rows")
				totalRes := fetch(t, label.baseURL, statsEndpoint(), params)
				if totalRes.StatusCode != 200 {
					t.Fatalf("total query returned status %d: %s", totalRes.StatusCode, string(totalRes.Body))
				}
				totalCount, err := extractVectorCount(totalRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for total: %v", err)
				}

				// Sum per-service counts
				var sumPerService float64
				for _, svc := range services {
					p := fullRangeParams()
					p.Set("query", fmt.Sprintf(`service.name:="%s" | stats count() rows`, svc))
					res := fetch(t, label.baseURL, statsEndpoint(), p)
					if res.StatusCode != 200 {
						t.Fatalf("service %s query returned status %d: %s", svc, res.StatusCode, string(res.Body))
					}
					cnt, err := extractVectorCount(res.Body)
					if err != nil {
						t.Fatalf("extractVectorCount for service %s: %v", svc, err)
					}
					sumPerService += cnt
				}

				if totalCount != sumPerService {
					t.Errorf("sum of per-service counts (%v) != total count (%v)", sumPerService, totalCount)
				}
				t.Logf("total=%v sum_per_service=%v", totalCount, sumPerService)
			})
		}
	})

	t.Run("sum_per_level_equals_total", func(t *testing.T) {
		levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}

		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get total count
				params := fullRangeParams()
				params.Set("query", "* | stats count() rows")
				totalRes := fetch(t, label.baseURL, statsEndpoint(), params)
				if totalRes.StatusCode != 200 {
					t.Fatalf("total query returned status %d: %s", totalRes.StatusCode, string(totalRes.Body))
				}
				totalCount, err := extractVectorCount(totalRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for total: %v", err)
				}

				// Sum per-level counts
				var sumPerLevel float64
				for _, lvl := range levels {
					p := fullRangeParams()
					p.Set("query", fmt.Sprintf(`level:="%s" | stats count() rows`, lvl))
					res := fetch(t, label.baseURL, statsEndpoint(), p)
					if res.StatusCode != 200 {
						t.Fatalf("level %s query returned status %d: %s", lvl, res.StatusCode, string(res.Body))
					}
					cnt, err := extractVectorCount(res.Body)
					if err != nil {
						t.Fatalf("extractVectorCount for level %s: %v", lvl, err)
					}
					sumPerLevel += cnt
				}

				if totalCount != sumPerLevel {
					t.Errorf("sum of per-level counts (%v) != total count (%v)", sumPerLevel, totalCount)
				}
				t.Logf("total=%v sum_per_level=%v", totalCount, sumPerLevel)
			})
		}
	})

	t.Run("not_filter_complement", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Total count
				params := fullRangeParams()
				params.Set("query", "* | stats count() rows")
				totalRes := fetch(t, label.baseURL, statsEndpoint(), params)
				if totalRes.StatusCode != 200 {
					t.Fatalf("total query returned status %d: %s", totalRes.StatusCode, string(totalRes.Body))
				}
				totalCount, err := extractVectorCount(totalRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for total: %v", err)
				}

				// Error count
				errParams := fullRangeParams()
				errParams.Set("query", `level:="ERROR" | stats count() rows`)
				errRes := fetch(t, label.baseURL, statsEndpoint(), errParams)
				if errRes.StatusCode != 200 {
					t.Fatalf("error query returned status %d: %s", errRes.StatusCode, string(errRes.Body))
				}
				errorCount, err := extractVectorCount(errRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for error: %v", err)
				}

				// NOT error count
				notParams := fullRangeParams()
				notParams.Set("query", `NOT level:="ERROR" | stats count() rows`)
				notRes := fetch(t, label.baseURL, statsEndpoint(), notParams)
				if notRes.StatusCode != 200 {
					t.Fatalf("NOT error query returned status %d: %s", notRes.StatusCode, string(notRes.Body))
				}
				notErrorCount, err := extractVectorCount(notRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for NOT error: %v", err)
				}

				if errorCount+notErrorCount != totalCount {
					t.Errorf("error_count (%v) + not_error_count (%v) = %v != total_count (%v)",
						errorCount, notErrorCount, errorCount+notErrorCount, totalCount)
				}
				t.Logf("total=%v error=%v not_error=%v", totalCount, errorCount, notErrorCount)
			})
		}
	})

	t.Run("field_values_subset_of_field_names", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get field_names
				fnParams := fullRangeParams()
				fnParams.Set("query", "*")
				fnRes := fetch(t, label.baseURL, "/select/logsql/field_names", fnParams)
				if fnRes.StatusCode != 200 {
					t.Fatalf("field_names returned status %d: %s", fnRes.StatusCode, string(fnRes.Body))
				}
				fieldNames := extractValuesStrings(fnRes.Body)
				fieldNameSet := stringSet(fieldNames)

				// Get field_values for level
				fvParams := fullRangeParams()
				fvParams.Set("query", "*")
				fvParams.Set("field", "level")
				fvRes := fetch(t, label.baseURL, "/select/logsql/field_values", fvParams)
				if fvRes.StatusCode != 200 {
					t.Fatalf("field_values returned status %d: %s", fvRes.StatusCode, string(fvRes.Body))
				}
				levelValues := extractValuesStrings(fvRes.Body)

				// If level has values, "level" must appear in field_names
				if len(levelValues) > 0 && !fieldNameSet["level"] {
					t.Errorf("field 'level' has %d values but is not in field_names (fields: %v)", len(levelValues), fieldNames)
				}
				t.Logf("field_names=%d level_values=%d level_in_names=%v", len(fieldNames), len(levelValues), fieldNameSet["level"])
			})
		}
	})

	t.Run("hits_consistency_filtered", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Get hits with level:="ERROR" and step=3600s
				hitsParams := fullRangeParams()
				hitsParams.Set("query", `level:="ERROR"`)
				hitsParams.Set("step", "3600s")
				hitsRes := fetch(t, label.baseURL, hitsEndpoint(), hitsParams)
				if hitsRes.StatusCode != 200 {
					t.Fatalf("hits query returned status %d: %s", hitsRes.StatusCode, string(hitsRes.Body))
				}
				_, hitsCounts := extractHitsBuckets(hitsRes.Body)
				var hitsSum float64
				for _, c := range hitsCounts {
					hitsSum += c
				}

				// Get stats count for same filter
				statsParams := fullRangeParams()
				statsParams.Set("query", `level:="ERROR" | stats count() rows`)
				statsRes := fetch(t, label.baseURL, statsEndpoint(), statsParams)
				if statsRes.StatusCode != 200 {
					t.Fatalf("stats query returned status %d: %s", statsRes.StatusCode, string(statsRes.Body))
				}
				statsCount, err := extractVectorCount(statsRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for stats: %v", err)
				}

				// Sum of hits buckets should approximate stats count within 2%
				if statsCount > 0 {
					diff := math.Abs(hitsSum-statsCount) / statsCount
					if diff > 0.02 {
						t.Errorf("hits sum (%v) differs from stats count (%v) by %.2f%% (threshold 2%%)", hitsSum, statsCount, diff*100)
					}
				}
				t.Logf("hits_sum=%v stats_count=%v", hitsSum, statsCount)
			})
		}
	})

	t.Run("count_same_across_time_formats", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				now := time.Now()
				start := now.Add(-24 * time.Hour)

				// Query with nanosecond epoch
				nanoParams := url.Values{
					"start": {fmt.Sprintf("%d", start.UnixNano())},
					"end":   {fmt.Sprintf("%d", now.UnixNano())},
					"query": {"* | stats count() rows"},
				}
				nanoRes := fetch(t, label.baseURL, statsEndpoint(), nanoParams)
				if nanoRes.StatusCode != 200 {
					t.Fatalf("nano query returned status %d: %s", nanoRes.StatusCode, string(nanoRes.Body))
				}
				nanoCount, err := extractVectorCount(nanoRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for nano: %v", err)
				}

				// Query with second epoch
				secParams := url.Values{
					"start": {fmt.Sprintf("%d", start.Unix())},
					"end":   {fmt.Sprintf("%d", now.Unix())},
					"query": {"* | stats count() rows"},
				}
				secRes := fetch(t, label.baseURL, statsEndpoint(), secParams)
				if secRes.StatusCode != 200 {
					t.Fatalf("sec query returned status %d: %s", secRes.StatusCode, string(secRes.Body))
				}
				secCount, err := extractVectorCount(secRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for sec: %v", err)
				}

				// Second-precision timestamps can include/exclude boundary logs
				// due to truncation, allowing ±1 difference.
				diff := nanoCount - secCount
				if diff < 0 {
					diff = -diff
				}
				if diff > 1 {
					t.Errorf("nanosecond count (%v) != second count (%v), diff=%v", nanoCount, secCount, diff)
				}
				t.Logf("nano_count=%v sec_count=%v", nanoCount, secCount)
			})
		}
	})

	t.Run("filtered_rows_subset", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Query all rows with limit
				allParams := fullRangeParams()
				allParams.Set("query", "* | sort by(_time) desc | limit 100")
				allRes := fetch(t, label.baseURL, queryEndpoint(), allParams)
				if allRes.StatusCode != 200 {
					t.Fatalf("all query returned status %d: %s", allRes.StatusCode, string(allRes.Body))
				}
				allRows := parseNDJSON(allRes.Body)

				// Query filtered rows with limit
				filtParams := fullRangeParams()
				filtParams.Set("query", `level:="ERROR" | sort by(_time) desc | limit 100`)
				filtRes := fetch(t, label.baseURL, queryEndpoint(), filtParams)
				if filtRes.StatusCode != 200 {
					t.Fatalf("filtered query returned status %d: %s", filtRes.StatusCode, string(filtRes.Body))
				}
				filtRows := parseNDJSON(filtRes.Body)

				if len(filtRows) > len(allRows) {
					t.Errorf("filtered rows (%d) > total rows (%d)", len(filtRows), len(allRows))
				}
				t.Logf("all_rows=%d filtered_rows=%d", len(allRows), len(filtRows))
			})
		}
	})

	t.Run("and_filter_subset_of_parts", func(t *testing.T) {
		for _, label := range []struct {
			name    string
			baseURL string
		}{
			{"VL", vlBaseURL},
			{"LH", lhBaseURL},
		} {
			t.Run(label.name, func(t *testing.T) {
				// Count for service filter
				svcParams := fullRangeParams()
				svcParams.Set("query", `service.name:="api-gateway" | stats count() rows`)
				svcRes := fetch(t, label.baseURL, statsEndpoint(), svcParams)
				if svcRes.StatusCode != 200 {
					t.Fatalf("service query returned status %d: %s", svcRes.StatusCode, string(svcRes.Body))
				}
				svcCount, err := extractVectorCount(svcRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for service: %v", err)
				}

				// Count for level filter
				lvlParams := fullRangeParams()
				lvlParams.Set("query", `level:="ERROR" | stats count() rows`)
				lvlRes := fetch(t, label.baseURL, statsEndpoint(), lvlParams)
				if lvlRes.StatusCode != 200 {
					t.Fatalf("level query returned status %d: %s", lvlRes.StatusCode, string(lvlRes.Body))
				}
				lvlCount, err := extractVectorCount(lvlRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for level: %v", err)
				}

				// Count for AND filter
				andParams := fullRangeParams()
				andParams.Set("query", `service.name:="api-gateway" AND level:="ERROR" | stats count() rows`)
				andRes := fetch(t, label.baseURL, statsEndpoint(), andParams)
				if andRes.StatusCode != 200 {
					t.Fatalf("AND query returned status %d: %s", andRes.StatusCode, string(andRes.Body))
				}
				andCount, err := extractVectorCount(andRes.Body)
				if err != nil {
					t.Fatalf("extractVectorCount for AND: %v", err)
				}

				minCount := math.Min(svcCount, lvlCount)
				if andCount > minCount {
					t.Errorf("AND count (%v) > min(service=%v, level=%v) = %v", andCount, svcCount, lvlCount, minCount)
				}
				t.Logf("service=%v level=%v and=%v min=%v", svcCount, lvlCount, andCount, minCount)
			})
		}
	})
}
