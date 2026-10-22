package parquets3

import (
	"fmt"
	"testing"
	"time"
)

// The acknowledged-row tests decide what the buffer holds by reading it back
// with a query (bufferMsgs), so that read has to return every row the segments
// hold. Upstream's query engine hands each block to the callback in memory it
// reuses for the next block of the same worker (WriteDataBlockFunc must not keep
// the values after it returns). A read that kept the values as map keys found
// the rows of every block but the last one of a worker under keys whose bytes
// had changed since: the lookup of such a row missed, and the test recorded a
// stored row as refused (#730, #625, #580).
//
// Two streams in one request on one day are two blocks of one part, read one
// after the other by one worker, so the reuse happens on every read.
func TestAckedRowsBufferRead_FindsEveryRowOfEveryBlock(t *testing.T) {
	e := newSegEnv(t)
	ts := time.Date(2023, 7, 29, 1, 13, 0, 901111261, time.UTC)
	var rows []adapterRow
	want := map[string]bool{}
	for i := 0; i < 7; i++ {
		for _, app := range []string{"api", "web"} {
			msg := fmt.Sprintf("%s-%d", app, i)
			rows = append(rows, adapterRow{ts: ts.Add(time.Duration(i) * time.Millisecond).UnixNano(), fields: ackFields(app, msg)})
			want[msg] = true
		}
	}
	e.sendViaAdapter(segTenantA, rows)
	for read := 0; read < 20; read++ {
		got := e.bufferMsgs()
		var missing []string
		for m := range want {
			if got[m] != 1 {
				missing = append(missing, fmt.Sprintf("%s×%d", m, got[m]))
			}
		}
		if len(missing) > 0 || len(got) != len(want) {
			t.Fatalf("read %d: the buffer read returned %d distinct rows, want the %d stored; not found once: %v", read, len(got), len(want), missing)
		}
	}
}
