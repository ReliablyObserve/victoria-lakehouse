package selectapi

import "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

// onlyEmptyValue reports whether a field_values answer holds no value but the
// empty bucket (rows without the field).
//
// Nothing in the binary calls it any more: it served the Jaeger handlers that
// were deleted with #734. It stays only because its test,
// jaeger_fallback_test.go#TestOnlyEmptyValue, is a test reference of the pass
// row vl.select.field_values.substring_filter_before_limit, and the registry
// gate does not let a pass row lose a reference. Delete both together once the
// owner approves dropping that reference.
func onlyEmptyValue(results []logstorage.ValueWithHits) bool {
	for _, v := range results {
		if v.Value != "" {
			return false
		}
	}
	return true
}
