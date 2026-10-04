package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTraceMessageColumnarNativeAndCustomerBody(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	for i := 0; i < 2; i++ {
		bw.AddTraceRows([]schema.TraceRow{{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), Body: fmt.Sprintf("native%d", i), SpanAttributes: map[string]string{"body": fmt.Sprintf("span%d", i)}, ResourceAttributes: map[string]string{"body": fmt.Sprintf("resource%d", i)}, ScopeAttributes: map[string]string{"body": fmt.Sprintf("scope%d", i)}}})
	}
	bw.triggerFlush()
	run := coldSelectRunner(t, s, base.Add(-time.Minute).UnixNano(), base.Add(time.Minute).UnixNano())
	got := run("* | fields _msg, span_attr:body, resource_attr:body, scope_attr:body")
	if len(got) != 2 {
		t.Fatal(got)
	}
	seen := map[string]bool{}
	for _, row := range got {
		native := row["_msg"]
		seen[native] = true
		i := "0"
		if native == "native1" {
			i = "1"
		} else if native != "native0" {
			t.Errorf("unexpected native %v", row)
		}
		for _, family := range []string{"span", "resource", "scope"} {
			if row[family+"_attr:body"] != family+i {
				t.Errorf("body map suppressed: %v", row)
			}
		}
	}
	if !seen["native0"] || !seen["native1"] {
		t.Fatal(got)
	}
}
