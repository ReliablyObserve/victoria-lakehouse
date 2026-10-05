package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	adapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestTraceMessageSurvivesNativeBufferAndParquet(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{base.Format(time.RFC3339Nano)}},
		{Name: "_msg", Values: []string{"actual native message"}},
		{Name: "trace_id", Values: []string{"01234567890123456789012345678901"}},
		{Name: "span_id", Values: []string{"0123456789012345"}},
		{Name: "span_attr:_msg", Values: []string{"customer attribute"}},
	})
	rows := adapter.DataBlockToTraceRows(&db, logstorage.TenantID{})
	if len(rows) != 1 {
		t.Fatal("missing native row")
	}
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	uploadTraceRows(t, bw, rows)
	run := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	got := run("*")
	if len(got) != 1 || got[0]["_msg"] != "actual native message" || got[0]["span_attr:_msg"] != "customer attribute" {
		t.Fatalf("persisted native/customer messages=%v", got)
	}
	q := mustParseQueryWithTime(t, "*", base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]uint64{}
	for _, f := range fields {
		seen[f.Value] = f.Hits
	}
	if seen["_msg"] != 1 || seen["span_attr:_msg"] != 1 {
		t.Fatalf("message field names=%v", seen)
	}
	// A caller deleting the native message must still keep the customer field.
	got = run("* | delete _msg")
	if len(got) != 1 || got[0]["_msg"] != "" || got[0]["span_attr:_msg"] != "customer attribute" {
		t.Fatalf("message provenance after delete=%v", got)
	}
}
