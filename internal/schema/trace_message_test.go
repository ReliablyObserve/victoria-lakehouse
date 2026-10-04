package schema

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestTraceMessageLegacyAndDurableReadback(t *testing.T) {
	type legacy struct {
		Time    int64  `parquet:"timestamp_unix_nano"`
		TraceID string `parquet:"trace_id"`
	}
	var old bytes.Buffer
	w := parquet.NewGenericWriter[legacy](&old)
	if _, err := w.Write([]legacy{{123, "old"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := parquet.NewGenericReader[TraceRow](bytes.NewReader(old.Bytes()))
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	rows := make([]TraceRow, 1)
	if n, err := r.Read(rows); n != 1 {
		t.Fatalf("legacy read=%d %v", n, err)
	}
	if rows[0].Body != "" || rows[0].TraceID != "old" {
		t.Fatalf("legacy read fabricated or lost values: %+v", rows)
	}
	for _, msg := range []string{"", "-", "actual message", "message with\x00null and unicode π"} {
		var data bytes.Buffer
		w := parquet.NewGenericWriter[TraceRow](&data)
		row := TraceRow{TimestampUnixNano: 123, TraceID: "new", Body: msg, SpanAttributes: map[string]string{"_msg": "customer attribute"}}
		if _, err := w.Write([]TraceRow{row}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		r := parquet.NewGenericReader[TraceRow](bytes.NewReader(data.Bytes()))
		out := make([]TraceRow, 1)
		n, err := r.Read(out)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("read=%d %v", n, err)
		}
		if out[0].Body != msg || out[0].SpanAttributes["_msg"] != "customer attribute" {
			t.Fatalf("message provenance lost: %+v", out)
		}
		without := row
		without.Body = ""
		if got := EstimateRawBytesTraces([]TraceRow{row}) - EstimateRawBytesTraces([]TraceRow{without}); got != int64(len(msg)) {
			t.Fatalf("raw-byte delta=%d want=%d", got, len(msg))
		}
	}
}

func TestTraceMessageAttributeNamespacesAreDistinct(t *testing.T) {
	keys := []string{"_msg", "body", "span_attr:_msg", "resource_attr:_msg", "scope_attr:_msg", "span_attr:span_attr:_msg", "scope_attr:resource_attr:body", "customer:body"}
	seen := map[string]bool{}
	for _, prefix := range []string{"span_attr:", "resource_attr:", "scope_attr:"} {
		for _, key := range keys {
			got := TraceMessageAttributeName(prefix, key)
			if got != prefix+key || seen[got] || got == "_msg" || got == "body" {
				t.Fatalf("customer namespace collision for %s/%s: %s", prefix, key, got)
			}
			seen[got] = true
		}
		for _, key := range []string{"ordinary", "bodywork", "_msg_suffix", "customer:bodywork"} {
			if got := TraceMessageAttributeName(prefix, key); got != key {
				t.Fatalf("unrelated attribute %q renamed to %q", key, got)
			}
		}
	}
}
