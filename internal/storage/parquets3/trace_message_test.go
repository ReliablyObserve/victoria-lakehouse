package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTraceMessageSurvivesParquet(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.AddTraceRows([]schema.TraceRow{{TimestampUnixNano: base.UnixNano(), TraceID: "trace", SpanID: "span", Body: "persisted native message"}})
	bw.triggerFlush()
	got := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())("*")
	if len(got) != 1 || got[0]["_msg"] != "persisted native message" {
		t.Fatalf("trace message=%v", got)
	}
}
