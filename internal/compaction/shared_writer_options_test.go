package compaction

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The compaction writers add nothing to the shared writer and take nothing away:
// their output is the shared writer's output for the level, row-group size,
// attestation and slot binding they were given. A caller that drifts (another
// level, another row-group size, a dropped resolver, a flipped attestation) is
// caught here, per signal.
func TestWriteCompactedIsTheSharedWriterWithTheGivenOptions(t *testing.T) {
	saved := activeSlotResolver
	t.Cleanup(func() { activeSlotResolver = saved })
	resolver := schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier", Bloom: true}})
	logs := make([]schema.LogRow, 450)
	spans := make([]schema.TraceRow, 450)
	for i := range logs {
		logs[i] = schema.LogRow{TimestampUnixNano: 1_000 + int64(i), Body: fmt.Sprintf("request %d served", i), ServiceName: "svc", TraceID: fmt.Sprintf("%032x", i)}
		schema.SetLogSlot(&logs[i], "ded_s01", fmt.Sprintf("tier-%d", i%3))
		spans[i] = schema.TraceRow{TimestampUnixNano: 1_000 + int64(i), TraceID: fmt.Sprintf("%032x", i/3), SpanID: fmt.Sprintf("%016x", i), SpanName: "op", Body: fmt.Sprintf("event %d", i)}
	}
	for _, slots := range []*schema.SlotResolver{nil, resolver} {
		activeSlotResolver = slots
		for _, level := range []int{1, 7, 11} {
			for _, hex := range []bool{false, true} {
				o := parquetwriter.Options{RowGroupSize: 100, CompressionLevel: level, Slots: slots, TraceIDHex: hex}
				name := fmt.Sprintf("slots=%v/level=%d/hex=%v", slots != nil, level, hex)

				got, err := writeCompactedLogs(logs, o.RowGroupSize, level, hex)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				want, err := parquetwriter.WriteLogs(logs, o)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if !bytes.Equal(got, want.Data) {
					t.Errorf("logs %s: the compacted object is not the shared writer's output for the same options", name)
				}

				got, err = writeCompactedTraces(spans, o.RowGroupSize, level, hex)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				wantT, err := parquetwriter.WriteTraces(spans, o)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if !bytes.Equal(got, wantT.Data) {
					t.Errorf("traces %s: the compacted object is not the shared writer's output for the same options", name)
				}
			}
		}
	}
}
