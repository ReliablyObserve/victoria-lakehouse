package compaction

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/parquet-go/parquet-go"
)

func TestTraceMessageCompactionAndRestartPreserveProvenance(t *testing.T) {
	// A legacy object has no dedicated message column. Its ambiguous map key
	// must remain an attribute; compaction cannot recover a native message.
	type legacy struct {
		Timestamp  int64             `parquet:"timestamp_unix_nano"`
		TraceID    string            `parquet:"trace_id"`
		Attributes map[string]string `parquet:"span.attributes"`
	}
	var old bytes.Buffer
	w := parquet.NewGenericWriter[legacy](&old)
	if _, err := w.Write([]legacy{{100, "old", map[string]string{"_msg": "ambiguous historical value"}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fresh := makeTestTraceParquet(t, []schema.TraceRow{{TimestampUnixNano: 200, TraceID: "new", Body: "actual native message", SpanAttributes: map[string]string{"_msg": "customer attribute"}}})
	c := NewCompactor(CompactorConfig{Mode: config.ModeTraces})
	rows, err := c.mergeTraceFiles([][]byte{fresh, old.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	data, err := writeCompactedTraces(rows, 1, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "compacted.parquet")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	// Reopen persisted bytes with a fresh reader, then compact a second time.
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = c.mergeTraceFiles([][]byte{data})
	if err != nil {
		t.Fatal(err)
	}
	data, err = writeCompactedTraces(rows, 2, 3, false)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = readTraceRows(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].TraceID != "old" || rows[0].Body != "" || rows[0].SpanAttributes["_msg"] != "ambiguous historical value" || rows[1].Body != "actual native message" || rows[1].SpanAttributes["_msg"] != "customer attribute" {
		t.Fatalf("message provenance after restart and repeated compaction: %+v", rows)
	}
}
