package parquets3

import (
	"testing"
	"time"
)

// TestCheckFooterThriftBounds_AcceptsRealTraceFooters: the traces writer's
// footer (wide TraceRow schema, a bloom per row group, token-bloom and
// trace-id footer key-values, many row groups) must pass the thrift size
// guard unchanged and still parse. The guard charges nested lists against one
// footer-wide budget, so many row groups x many columns is the case that
// would expose an over-eager bound.
func TestCheckFooterThriftBounds_AcceptsRealTraceFooters(t *testing.T) {
	for _, tc := range []struct{ rows, rg int }{{600, 600}, {20000, 200}, {20000, 50}} {
		res, err := writeTracesParquet(traceRows(tc.rows, time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)), tc.rg, 3)
		if err != nil {
			t.Fatal(err)
		}
		obj := res.Data
		n := footerTotal(obj)
		if err := checkFooterThriftBounds(obj[len(obj)-n : len(obj)-8]); err != nil {
			t.Fatalf("rows=%d rg=%d: valid traces footer (%d bytes) rejected: %v", tc.rows, tc.rg, n, err)
		}
		cf, _, err := ParseFooterFromBytes("trace.parquet", obj[len(obj)-n:], int64(len(obj)))
		if err != nil {
			t.Fatalf("rows=%d rg=%d: valid traces footer no longer parses: %v", tc.rows, tc.rg, err)
		}
		if got, want := len(cf.File.RowGroups()), (tc.rows+tc.rg-1)/tc.rg; got != want {
			t.Fatalf("rows=%d rg=%d: %d row groups, want %d", tc.rows, tc.rg, got, want)
		}
		t.Logf("rows=%d rg=%d: footer %d bytes, %d row groups", tc.rows, tc.rg, n, len(cf.File.RowGroups()))
	}
}
