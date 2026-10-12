package parquets3

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// presenceSizeSpans are rows of `streams` services, each drawing its attribute
// names from its own pool of namesPerStream names (4 attributes per row).
func presenceSizeSpans(rng *rand.Rand, n, streams, namesPerStream int) []schema.TraceRow {
	base := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	rows := make([]schema.TraceRow, n)
	var tid string
	for i := range rows {
		if i%2 == 0 {
			tid = fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
		}
		s := rng.Intn(streams)
		attrs := make(map[string]string, 8)
		for len(attrs) < 8 {
			attrs[fmt.Sprintf("attr.s%d.k%d", s%97, rng.Intn(namesPerStream))] = "v"
		}
		ts := base.Add(time.Duration(i) * 100 * time.Millisecond).UnixNano()
		rows[i] = schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts - 1_000_000), TraceID: tid, SpanID: fmt.Sprintf("%016x", rng.Uint64()),
			SpanName: fmt.Sprintf("op-%d", s%40), ServiceName: fmt.Sprintf("svc-%d", s), DurationNs: schema.Int64Ptr(int64(1+rng.Intn(900)) * 1_000_000), SpanKind: schema.Int32Ptr(2),
			Stream: fmt.Sprintf(`{resource_attr:service.name="svc-%d"}`, s), StreamID: presenceStreamID(s),
			SpanAttributes: attrs,
		}
	}
	return rows
}

// TestFieldPresence_FooterSize reports what the facet adds to an object's footer
// at 10^2, 10^3 and 10^4 streams, through the production writer. Skipped unless
// FP_SIZE_OUT is set; the numbers go to docs/perf/field-presence-footer.md.
func TestFieldPresence_FooterSize(t *testing.T) {
	out := os.Getenv("FP_SIZE_OUT")
	if out == "" {
		t.Skip("set FP_SIZE_OUT to measure the footer growth")
	}
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	rowsPerObject := 24000
	if v, err := strconv.Atoi(os.Getenv("FP_SIZE_ROWS")); err == nil && v > 0 {
		rowsPerObject = v
	}
	for _, streams := range []int{100, 1000, 10000} {
		rows := presenceSizeSpans(rand.New(rand.NewSource(int64(streams))), rowsPerObject, streams, 300)
		t0 := time.Now()
		b := presenceOfTraceRows(rows, nil)
		res := b.Finish()
		took := time.Since(t0)
		obj, err := writeTracesParquet(rows, 10000, 3)
		if err != nil {
			t.Fatal(err)
		}
		fl := int(uint32(obj.Data[len(obj.Data)-8]) | uint32(obj.Data[len(obj.Data)-7])<<8 | uint32(obj.Data[len(obj.Data)-6])<<16 | uint32(obj.Data[len(obj.Data)-5])<<24)
		_ = enc.Encode(map[string]any{"signal": "traces", "streams": streams, "rows": len(rows), "stream_days": res.Streams, "object_bytes": len(obj.Data),
			"footer_bytes": fl, "facet_kv_bytes": len(res.Value), "over_cap": res.OverCap, "extract_and_encode_ms": float64(took.Microseconds()) / 1000,
			"bytes_per_stream": float64(len(res.Value)) / float64(res.Streams)})
	}
	_ = fieldpresence.KVKey
}

// presenceStreamID is a 48-digit id (8-byte tenant, 16-byte hash) of stream s,
// the shape of a VictoriaLogs _stream_id: the hash part does not compress.
func presenceStreamID(s int) string {
	mix := func(x uint64) uint64 {
		x += 0x9E3779B97F4A7C15
		x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
		x = (x ^ (x >> 27)) * 0x94D049BB133111EB
		return x ^ (x >> 31)
	}
	a := mix(uint64(s))
	return fmt.Sprintf("%016x%016x%016x", uint64(0x7ace), a, mix(a))
}
