package parquets3

import (
	"context"
	"math"
	"path/filepath"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// The parts of the acknowledged-row tests (acked_rows_*_test.go) that differ
// between the two binaries: how a row is shaped, which field is its stream, and
// which insert path it goes through. The tests themselves are the same text in
// both modules.

const (
	// ackStreamField is the field that makes the stream of a test row.
	ackStreamField = "app"
	// ackMsgField is the field that holds the unique id of a test row.
	ackMsgField = "_msg"
)

// adapterRow is one row of an insert request.
type adapterRow struct {
	ts     int64
	fields []logstorage.Field
}

// ackFields returns the fields of one test row of stream <app> with the unique
// id msg.
func ackFields(app, msg string) []logstorage.Field {
	return []logstorage.Field{{Name: ackStreamField, Value: app}, {Name: ackMsgField, Value: msg}}
}

// sendViaAdapter hands rows (field sets with their _time) of tenant to the
// buffer the way an insert request does: through the insert adapter of the binary
// and upstream's log-message processor, in one batch.
func (e *segEnv) sendViaAdapter(tn logstorage.TenantID, rows []adapterRow) {
	e.t.Helper()
	vlstorage.SetInsertStorage(e.segs, filepath.Join(e.dir, "buffer"))
	e.t.Cleanup(func() { insertutil.SetLogRowsStorage(nil) })
	cp := &insertutil.CommonParams{TenantID: tn, TimeFields: []string{"_time"}, StreamFields: []string{ackStreamField}}
	lmp := cp.NewLogMessageProcessor("test", false)
	for _, r := range rows {
		lmp.AddRow(r.ts, r.fields, -1)
	}
	lmp.MustClose()
}

// bufferMsgs runs a query for every row over the whole of int64 time against the
// segments the buffer serves (the active one and the sealed ones not yet
// removed), with the upstream engine, and returns the id of each row found. It
// is what a read from the buffer sees.
func (e *segEnv) bufferMsgs() map[string]int {
	e.t.Helper()
	e.segs.DebugFlush() // upstream makes added rows searchable within about a second on its own
	q, err := logstorage.ParseQueryAtTimestamp("*", math.MaxInt64)
	if err != nil {
		e.t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), math.MinInt64, math.MaxInt64)
	out := map[string]int{}
	var mu sync.Mutex
	for _, g := range append(e.segs.Pending(), e.segs.Active()) {
		tenants, err := g.GetTenantIDs(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			e.t.Fatalf("list tenants of segment %d: %v", g.Seq(), err)
		}
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, tenants, q, false, nil)
		err = g.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			for _, c := range db.GetColumns(false) {
				if c.Name != ackMsgField {
					continue
				}
				mu.Lock()
				for _, v := range c.Values {
					out[v]++
				}
				mu.Unlock()
			}
		})
		if err != nil {
			e.t.Fatalf("query segment %d: %v", g.Seq(), err)
		}
	}
	return out
}
