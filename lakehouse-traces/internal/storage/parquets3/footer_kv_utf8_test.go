package parquets3

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/footerkv"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

func footerKVTestLogRows(n int) []schema.LogRow {
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: int64(1_000_000 + i),
			Body:              fmt.Sprintf("request %d served by handler-%d status ok", i, i%7),
			ServiceName:       "svc",
			SeverityText:      "info",
		}
	}
	return rows
}

func footerKVTestTraceRows(n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: int64(1_000_000 + i),
			TraceID:           fmt.Sprintf("%032x", i%9),
			SpanID:            fmt.Sprintf("%016x", i),
			SpanName:          "op",
			ServiceName:       "svc",
			Body:              fmt.Sprintf("span %d did work in handler-%d", i, i%5),
			StartTimeUnixNano: schema.Int64Ptr(int64(1_000_000 + i)),
			DurationNs:        schema.Int64Ptr(500),
		}
	}
	return rows
}

// Every footer key-value entry of a flushed object must be valid UTF-8, so
// Arrow-rs readers (Polars, DataFusion) open it (issue #340). The property is
// asserted over all entries, so a future footer KV holding raw bytes fails here.
func TestFlushedLogsFooterKVAllUTF8(t *testing.T) {
	res, err := writeLogsParquet(footerKVTestLogRows(250), 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	kv := testutil.RequireFooterKVUTF8(t, res.Data)
	for rg := 0; rg < 3; rg++ {
		key := fmt.Sprintf("_bloom_body_rg_%d", rg)
		v, ok := kv[key]
		if !ok {
			t.Fatalf("missing %s; entries=%d", key, len(kv))
		}
		raw, ok := footerkv.Decode(v)
		if !ok {
			t.Fatalf("%s is not base64: %.40q", key, v)
		}
		var b TokenBloom
		if err := b.UnmarshalBinary(raw); err != nil || !b.Test("handler") {
			t.Fatalf("%s does not decode to a bloom holding the body tokens: %v", key, err)
		}
	}
	// A logs object carries no trace index.
	if _, ok := kv["_trace_idx"]; ok {
		t.Fatal("logs object must not carry _trace_idx")
	}
}

func TestFlushedTracesFooterKVAllUTF8(t *testing.T) {
	rows := footerKVTestTraceRows(250)
	res, err := writeTracesParquet(rows, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	kv := testutil.RequireFooterKVUTF8(t, res.Data)
	entries, ok := traceIndexFromMetadata(kv)
	if !ok || len(entries) != 9 {
		t.Fatalf("_trace_idx did not decode from the flushed footer: ok=%v entries=%d (want 9 traces)", ok, len(entries))
	}
	if _, ok := kv["_bloom_body_rg_0"]; !ok {
		t.Fatal("traces object lost its body token bloom")
	}
	for k, v := range kv {
		if strings.HasPrefix(k, "_bloom_body_rg_") {
			if _, ok := footerkv.Decode(v); !ok {
				t.Fatalf("%s is not base64", k)
			}
		}
	}
}

// The base64 footer value is read back by the production pruning path: a bloom
// built for a row group still prunes, and keeps a row group that has the token.
func TestTokenBloomFooterValueRoundTripPrunes(t *testing.T) {
	key, value := buildTokenBloomMetadata([]string{"alpha beta gamma", "delta"}, 4)
	if key != "_bloom_body_rg_4" {
		t.Fatalf("key = %q", key)
	}
	meta := map[string]string{key: value}
	if tokenBloomSkip(meta, 4, []string{"gamma"}) {
		t.Fatal("bloom pruned a row group that holds the token")
	}
	if !tokenBloomSkip(meta, 4, []string{"zzzabsent"}) {
		t.Fatal("bloom no longer prunes an absent token after the base64 round trip")
	}
}

// No dual-format reader: a value that is not valid base64 (the former raw
// encoding, truncation, junk) reads as an absent entry, never an error, never a
// panic, and never prunes a row group.
func TestFooterKVNotBase64IsAbsent(t *testing.T) {
	key, value := buildTokenBloomMetadata([]string{"alpha beta"}, 0)
	raw, _ := footerkv.Decode(value)
	for name, v := range map[string]string{
		"raw bloom bytes":   string(raw),
		"truncated base64":  value[:len(value)-5],
		"junk":              "not base64 at all !!",
		"empty":             "",
		"valid b64 garbage": footerkv.Encode([]byte{1, 2, 3}),
		"degenerate bloom":  footerkv.Encode(make([]byte, 6)),
	} {
		if tokenBloomSkip(map[string]string{key: v}, 0, []string{"zzzabsent"}) {
			t.Errorf("bloom %s: pruned a row group on an unusable value", name)
		}
	}

	idx := marshalTraceIndex([]TraceIndexEntry{{TraceID: "t1", Partition: 1, StartNs: 1, EndNs: 2}})
	for name, v := range map[string]string{
		"raw index bytes": string(idx),
		"truncated":       footerkv.Encode(idx)[:20],
		"junk":            "\x01\x02\xff",
		"empty":           "",
		"short payload":   footerkv.Encode(idx[:3]),
	} {
		if e, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: v}); ok {
			t.Errorf("trace index %s: decoded %v, want absent", name, e)
		}
	}
	if _, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: footerkv.Encode(idx)}); !ok {
		t.Fatal("control: base64 trace index must decode")
	}
}

func FuzzFooterKVReaders(f *testing.F) {
	_, bloom := buildTokenBloomMetadata([]string{"alpha beta gamma"}, 0)
	f.Add(bloom)
	f.Add(footerkv.Encode(marshalTraceIndex([]TraceIndexEntry{{TraceID: "t1", Partition: 1, StartNs: 1, EndNs: 2}})))
	f.Add("")
	f.Add("\x01\x05\x00\x00\x00")
	f.Add("AAAAAAAA")
	f.Fuzz(func(t *testing.T, v string) {
		meta := map[string]string{"_bloom_body_rg_0": v, traceIndexMetadataKey: v}
		_ = tokenBloomSkip(meta, 0, []string{"alpha"}) // must not panic
		_, _ = traceIndexFromMetadata(meta)            // must not panic
	})
}
