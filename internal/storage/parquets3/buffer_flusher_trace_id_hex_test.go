package parquets3

import (
	"bytes"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ingestTraceIDs adds one log row per trace id (an empty id is a row without
// one), through the durable insert buffer.
func (e *segEnv) ingestTraceIDs(tenant logstorage.TenantID, ts time.Time, ids ...string) {
	e.t.Helper()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i, id := range ids {
		fields := []logstorage.Field{
			{Name: "service.name", Value: "api"},
			{Name: "_msg", Value: "row-" + id},
		}
		if id != "" {
			fields = append(fields, logstorage.Field{Name: "trace_id", Value: id})
		}
		lr.MustAdd(tenant, ts.Add(time.Duration(i)*time.Millisecond).UnixNano(), fields, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// TestSegments_DrainAttestsTraceIDHex (logs): an object the segment drain writes
// carries lh.trace_id_hex=1 exactly when every trace_id in it is lowercase hex
// (rows without one included), and its manifest entry records the same bit. A
// shipper's UUID makes the object unattested.
func TestSegments_DrainAttestsTraceIDHex(t *testing.T) {
	e := newSegEnv(t)
	e.ingestTraceIDs(segTenantA, hourAgo.Add(10*time.Minute), "0af7651916cd43dd8448eb211c80319c", "")
	e.ingestTraceIDs(segTenantB, hourAgo.Add(10*time.Minute), "0af7651916cd43dd8448eb211c80319c", "4bf92f35-77b3-4da6-a3ce-929d0e0bf736")
	e.seal()
	e.drainAll(e.flusher(1000))
	keys := e.storedDataKeys()
	if len(keys) != 2 {
		t.Fatalf("stored %d objects, want 2 (one per tenant): %v", len(keys), keys)
	}
	attested := 0
	for _, k := range keys {
		e.u.mu.Lock()
		data := e.u.data[k]
		e.u.mu.Unlock()
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
		rows := make([]schema.LogRow, r.NumRows())
		n, _ := r.Read(rows)
		_ = r.Close()
		want := schema.LogRowsTraceIDHex(rows[:n])
		kv, ok := pf.Lookup(schema.TraceIDHexMetaKey)
		if !ok || (kv == "1") != want || (kv != "0" && kv != "1") {
			t.Errorf("%s: footer %q (present=%v), want attested=%v", k, kv, ok, want)
		}
		fi, ok := e.m.GetFileByKey(k)
		if !ok || fi.TraceIDHex != want {
			t.Errorf("%s: manifest TraceIDHex=%v (found=%v), want %v", k, fi.TraceIDHex, ok, want)
		}
		if want {
			attested++
		}
	}
	if attested != 1 {
		t.Errorf("%d attested objects, want 1", attested)
	}
}
