package parquets3

import (
	"context"
	"testing"
	"time"
)

// The end of a query's _time range is inclusive, as in VictoriaLogs: a row exactly at the end
// matches. A Parquet row group whose first row is that row was pruned (rgMin < endNs), so the
// row was served from the insert buffer but lost once flushed. The earliest row of an object is
// the first row of its only row group, which makes it the case.
func TestRunQuery_RowAtTheRangeEndIsRead(t *testing.T) {
	e := newRestartEnv(t)
	first := rwHour.Add(time.Second)
	e.ingest("info", first, first.Add(time.Second), first.Add(2*time.Second))
	check := func(when string) {
		t.Helper()
		if n := e.run(context.Background(), "*", first.Add(-time.Hour), first); n != 1 {
			t.Errorf("%s: %d rows in a range that ends at the first row, want 1", when, n)
		}
		if n := e.run(context.Background(), "*", first, first); n != 1 {
			t.Errorf("%s: %d rows in the range [first, first], want 1", when, n)
		}
		if n := e.run(context.Background(), "*", first.Add(-time.Hour), first.Add(-time.Nanosecond)); n != 0 {
			t.Errorf("%s: %d rows in a range that ends just before the first row, want 0", when, n)
		}
	}
	check("buffer")
	e.flush()
	e.reap()
	check("parquet")
}
