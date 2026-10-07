package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// streamFieldValuesRows: streams {svc=a} x3 rows (two with a namespace tag in
// the stream), {svc=b} x1 with no namespace tag, and a stream-less row.
func streamFieldValuesRows() []schema.LogRow {
	mk := func(at time.Duration, stream string) schema.LogRow {
		return schema.LogRow{TimestampUnixNano: fvcBase.Add(at).UnixNano(), Body: "x", SeverityText: "INFO",
			ServiceName: "svc", K8sNamespaceName: "colcol", Stream: stream}
	}
	return []schema.LogRow{
		mk(1*time.Minute, `{service.name="a",k8s.namespace.name="n1"}`),
		mk(2*time.Minute, `{service.name="a",k8s.namespace.name="n1"}`),
		mk(3*time.Minute, `{service.name="a"}`),
		mk(4*time.Minute, `{service.name="b"}`),
		mk(5*time.Minute, `{}`),
	}
}

// Every layer the API answers from gives the same stream_field_values: cold
// objects, a peer's buffer through the bridge, and both together. The field
// k8s.namespace.name is also a column on every row (value "colcol"), which hot
// never lists because it is not what the streams carry.
func TestStreamFieldValues_ColdBufferAndBridgeAgree(t *testing.T) {
	rows := streamFieldValuesRows()
	cold := fvcStorageNonce(t, "", rows)
	bridged := fvcStorageNonce(t, "")
	bridged.bufferBridge = fvcPeer(t, rows)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	for _, tc := range []struct {
		field string
		limit uint64
		want  []valueHits
	}{
		{"service.name", 0, []valueHits{{"a", 3}, {"b", 1}}},
		{"k8s.namespace.name", 0, []valueHits{{"n1", 2}}},
		{"service.name", 1, []valueHits{{"a", 0}}},
		{"level", 0, nil},
		{"nosuch", 0, nil},
	} {
		for name, s := range map[string]*Storage{"cold": cold, "bridge": bridged} {
			got, err := s.GetStreamFieldValues(context.Background(), nil, q, tc.field, tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			var vh []valueHits
			for _, v := range got {
				vh = append(vh, valueHits{v.Value, v.Hits})
			}
			if !reflect.DeepEqual(vh, tc.want) {
				t.Errorf("%s: stream_field_values(%s, limit %d) = %v, want %v", name, tc.field, tc.limit, vh, tc.want)
			}
		}
	}
}
