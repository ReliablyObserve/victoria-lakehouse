package main

import (
	"reflect"
	"testing"
)

func TestBuildManifestCountsWhatWasGenerated(t *testing.T) {
	logs := []logRow{
		{TimestampUnixNano: 30, ServiceName: "api-gateway", SeverityText: "ERROR", TraceID: "t1", LogAttrs: map[string]string{"format": "nginx", "x": "1"}},
		{TimestampUnixNano: 10, ServiceName: "api-gateway", SeverityText: "INFO", TraceID: "t1", LogAttrs: map[string]string{"format": "logfmt"}},
		{TimestampUnixNano: 20, ServiceName: "user-service", SeverityText: "ERROR", TraceID: "t2"},
	}
	spans := []traceRow{
		{TimestampUnixNano: 5, ServiceName: "api-gateway", StatusCode: 2, TraceID: "a", SpanAttrs: map[string]string{"rpc.system": "grpc"}},
		{TimestampUnixNano: 7, ServiceName: "api-gateway", TraceID: "a", SpanAttrs: map[string]string{"rpc.system": "other"}},
	}
	m := buildManifest("n", "1", "2", "", 7, logs, spans)
	if m.Logs.Count != 3 || m.Logs.Errors != 2 || m.Logs.FieldFilter != 1 || m.Logs.MapFilter != 1 {
		t.Fatalf("logs: %+v", m.Logs)
	}
	if !reflect.DeepEqual(m.Logs.ByService, map[string]int{"api-gateway": 2, "user-service": 1}) {
		t.Fatalf("by service: %v", m.Logs.ByService)
	}
	if !reflect.DeepEqual(m.Logs.MapKeys, map[string]int{"format": 2, "x": 1}) {
		t.Fatalf("map keys: %v", m.Logs.MapKeys)
	}
	if m.Logs.TsMin != 10 || m.Logs.TsMax != 30 || !reflect.DeepEqual(m.Logs.Timestamps, []int64{10, 20, 30}) {
		t.Fatalf("timestamps: %+v", m.Logs.Timestamps)
	}
	if !reflect.DeepEqual(m.Logs.TraceCounts, map[string]int{"t1": 2, "t2": 1}) {
		t.Fatalf("logs rows per trace id: %v", m.Logs.TraceCounts)
	}
	if m.Traces.Count != 2 || m.Traces.Errors != 1 || m.Traces.MapFilter != 1 || m.Traces.TraceCounts["a"] != 2 {
		t.Fatalf("traces: %+v", m.Traces)
	}
}

func TestSeedMakesIDsReproducible(t *testing.T) {
	saved := idRng
	defer func() { idRng = saved }()
	gen := func() []string {
		idRng = newSeededRand(42)
		return []string{randomHex(32), randomHex(16), randomHex(8)}
	}
	a, b := gen(), gen()
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed, different ids: %v vs %v", a, b)
	}
	idRng = newSeededRand(43)
	if randomHex(32) == a[0] {
		t.Fatal("different seeds produced the same id")
	}
}
