package parquets3

import (
	"bytes"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// ingestIDs adds one span per trace id, through the durable insert buffer.
func (e *segEnv) ingestIDs(tenant logstorage.TenantID, ts time.Time, ids ...string) {
	e.t.Helper()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i, id := range ids {
		lr.MustAdd(tenant, ts.Add(time.Duration(i)*time.Millisecond).UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "api"},
			{Name: "trace_id", Value: id},
			{Name: "span_id", Value: "row-" + id},
			{Name: "name", Value: "op"},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// TestSegments_DrainAttestsTraceIDHex: an object the segment drain writes
// carries lh.trace_id_hex=1 exactly when every span id in it is lowercase hex,
// and its manifest entry records the same bit. A segment holding one non-hex
// id (as VictoriaTraces stores OTLP/HTTP JSON and native ids) is unattested.
func TestSegments_DrainAttestsTraceIDHex(t *testing.T) {
	e := newSegEnv(t)
	e.ingestIDs(segTenantA, hourAgo.Add(10*time.Minute), "0af7651916cd43dd8448eb211c80319c", "4bf92f3577b34da6a3ce929d0e0bf736")
	e.ingestIDs(segTenantB, hourAgo.Add(10*time.Minute), "0af7651916cd43dd8448eb211c80319c", "abc-def-ghi")
	e.seal()
	e.drainAll(e.flusher(1000))
	keys := e.storedDataKeys()
	if len(keys) != 2 {
		t.Fatalf("stored %d objects, want 2 (one per tenant): %v", len(keys), keys)
	}
	for _, k := range keys {
		e.u.mu.Lock()
		data := e.u.data[k]
		e.u.mu.Unlock()
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		r := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
		rows := make([]schema.TraceRow, r.NumRows())
		n, _ := r.Read(rows)
		_ = r.Close()
		want := schema.TraceRowsTraceIDHex(rows[:n])
		kv, ok := pf.Lookup(schema.TraceIDHexMetaKey)
		if !ok || (kv == "1") != want || (kv != "0" && kv != "1") {
			t.Errorf("%s: footer %q (present=%v), want attested=%v", k, kv, ok, want)
		}
		fi, ok := e.m.GetFileByKey(k)
		if !ok || fi.TraceIDHex != want {
			t.Errorf("%s: manifest TraceIDHex=%v (found=%v), want %v", k, fi.TraceIDHex, ok, want)
		}
	}
	var attested int
	for _, k := range keys {
		if fi, _ := e.m.GetFileByKey(k); fi.TraceIDHex {
			attested++
		}
	}
	if attested != 1 {
		t.Errorf("%d attested objects, want 1 (tenant A all hex, tenant B one non-hex id)", attested)
	}
}
