package delete

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

// A delete never rewrites an object, so a traces object keeps DELTA_BINARY_PACKED
// on the optional start_time_unix_nano column, byte for byte, however many of its
// spans a tombstone hides: the pod that takes the delete issues no write outside
// the tombstone keys, and the object in the bucket is the one the writer made.
// (Compaction's own output keeps the encoding: internal/compaction.)
func TestTombstone_LeavesADeltaEncodedObjectUntouched(t *testing.T) {
	rows := make([]schema.TraceRow, 400)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		svc := "keep"
		if i%4 == 0 {
			svc = "drop"
		}
		rows[i] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts), TraceID: fmt.Sprintf("%032x", i), SpanID: fmt.Sprintf("%016x", i), ServiceName: svc}
	}
	var buf bytes.Buffer
	w := schema.NewTraceWriter(&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	object := append([]byte(nil), buf.Bytes()...)
	if !testutil.HasEncoding(testutil.ColumnEncodings(t, object), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatal("fixture: the production trace writer must delta-encode start_time_unix_nano")
	}

	b := deletetest.NewBucket()
	const key = "traces/dt=2026-05-02/hour=10/batch-01.parquet"
	b.Put(key, object)
	s, pod := newSyncedPod(t, b, "pod")
	pod.ResetCounts()
	mustCreate(t, s, Tombstone{ID: "t", Query: `service.name:="drop"`, StartNs: 0, EndNs: 1 << 62, Mode: "permanent", Tenants: []TenantRef{{}}})

	after, _ := b.Body(key)
	if !bytes.Equal(after, object) {
		t.Fatal("the object changed: a delete must only hide rows, never rewrite the object")
	}
	for _, op := range b.Log() {
		if op.Key == key && (op.Kind == "PUT" || op.Kind == "DELETE") {
			t.Fatalf("%s did %s on the object", op.Pod, op.Kind)
		}
	}
	if !testutil.HasEncoding(testutil.ColumnEncodings(t, after), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("start_time_unix_nano encodings after the delete: %v", testutil.ColumnEncodings(t, after)["start_time_unix_nano"])
	}
}
