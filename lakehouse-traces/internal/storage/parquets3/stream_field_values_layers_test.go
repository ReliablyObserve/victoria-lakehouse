package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Every layer the API answers from gives the same stream_field_values: cold
// objects and a peer's buffer through the bridge. The span name is a column on
// every span, but only the streams that carry it as a tag list it.
func TestStreamFieldValues_ColdBufferAndBridgeAgree(t *testing.T) {
	mk := func(at time.Duration, name, stream string) schema.TraceRow {
		r := fvcSpan(at, name)
		r.Stream = stream
		return r
	}
	rows := []schema.TraceRow{
		mk(1*time.Minute, "ZZ", `{resource_attr:service.name="a",name="ZZ"}`),
		mk(2*time.Minute, "ZZ", `{resource_attr:service.name="a",name="ZZ"}`),
		mk(3*time.Minute, "PUT", `{resource_attr:service.name="b"}`),
		mk(4*time.Minute, "", `{resource_attr:service.name="b"}`),
		mk(5*time.Minute, "AA", `{resource_attr:service.name="b",name="AA"}`),
	}
	cold := fvcStorageNonce(t, "", rows)
	bridged := fvcStorageNonce(t, "")
	bridged.bufferBridge = fvcPeer(t, rows)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	for _, tc := range []struct {
		field string
		limit uint64
		want  []logstorage.ValueWithHits
	}{
		{"resource_attr:service.name", 0, []logstorage.ValueWithHits{{Value: "b", Hits: 3}, {Value: "a", Hits: 2}}},
		{"name", 0, []logstorage.ValueWithHits{{Value: "ZZ", Hits: 2}, {Value: "AA", Hits: 1}}},
		{"resource_attr:service.name", 1, []logstorage.ValueWithHits{{Value: "b", Hits: 0}}},
		{"name", 1, []logstorage.ValueWithHits{{Value: "ZZ", Hits: 0}}},
		{"trace_id", 0, nil},
	} {
		for name, s := range map[string]*Storage{"cold": cold, "bridge": bridged} {
			got, err := s.GetStreamFieldValues(context.Background(), nil, q, tc.field, tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s: stream_field_values(%s, limit %d) = %v, want %v", name, tc.field, tc.limit, got, tc.want)
			}
		}
	}
}
