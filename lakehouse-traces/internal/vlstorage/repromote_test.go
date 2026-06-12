package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRepromoteTraceRow: a v1 trace row (promoted attrs still in the resource/span
// maps) must lift them into their dedicated columns and drop them from the maps;
// non-promoted keys stay in their map.
func TestRepromoteTraceRow(t *testing.T) {
	SetSlotResolver(nil)
	r := schema.TraceRow{
		ResourceAttributes: map[string]string{
			"k8s.pod.name": "pod-1", // Tier-1 resource
			"service.name": "api",   // Tier-1 resource
			"custom.res":   "keep-r",
		},
		SpanAttributes: map[string]string{
			"http.method": "GET", // Tier-1 span
			"db.system":   "pg",  // Tier-1 span
			"custom.span": "keep-s",
		},
	}
	RepromoteTraceRow(&r)

	if r.K8sPodName != "pod-1" {
		t.Errorf("k8s.pod.name not promoted: %q", r.K8sPodName)
	}
	if r.ServiceName != "api" {
		t.Errorf("service.name not promoted: %q", r.ServiceName)
	}
	if r.HTTPMethod != "GET" {
		t.Errorf("http.method not promoted: %q", r.HTTPMethod)
	}
	if r.DBSystem != "pg" {
		t.Errorf("db.system not promoted: %q", r.DBSystem)
	}
	if _, ok := r.ResourceAttributes["k8s.pod.name"]; ok {
		t.Error("k8s.pod.name should have left the resource map")
	}
	if _, ok := r.SpanAttributes["http.method"]; ok {
		t.Error("http.method should have left the span map")
	}
	if r.ResourceAttributes["custom.res"] != "keep-r" {
		t.Errorf("non-promoted resource attr must stay, got %v", r.ResourceAttributes)
	}
	if r.SpanAttributes["custom.span"] != "keep-s" {
		t.Errorf("non-promoted span attr must stay, got %v", r.SpanAttributes)
	}
}

// TestRepromoteTraceRow_Tier2Slots: operator custom attrs (with/without bloom) on
// both resource and span maps route to slots and leave their map.
func TestRepromoteTraceRow_Tier2Slots(t *testing.T) {
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{
		{Name: "thread.id", Bloom: true},
		{Name: "custom.code", Bloom: false},
	}))
	defer SetSlotResolver(nil)

	r := schema.TraceRow{
		ResourceAttributes: map[string]string{"thread.id": "42", "keep.r": "x"},
		SpanAttributes:     map[string]string{"custom.code": "fn", "keep.s": "y"},
	}
	RepromoteTraceRow(&r)

	if _, ok := r.ResourceAttributes["thread.id"]; ok {
		t.Error("slotted thread.id should have left the resource map")
	}
	if _, ok := r.SpanAttributes["custom.code"]; ok {
		t.Error("slotted custom.code should have left the span map")
	}
	if r.ResourceAttributes["keep.r"] != "x" || r.SpanAttributes["keep.s"] != "y" {
		t.Errorf("non-slotted attrs must stay: res=%v span=%v", r.ResourceAttributes, r.SpanAttributes)
	}
}

// TestRepromoteTraceRow_Edges locks boundaries: nil maps, empty key preserved,
// idempotency.
func TestRepromoteTraceRow_Edges(t *testing.T) {
	SetSlotResolver(nil)
	RepromoteTraceRow(&schema.TraceRow{}) // nil maps → no panic

	r := schema.TraceRow{ResourceAttributes: map[string]string{"": "x", "service.name": "api"}}
	RepromoteTraceRow(&r)
	if r.ResourceAttributes[""] != "x" {
		t.Error("empty key must be preserved in the resource map")
	}
	if r.ServiceName != "api" {
		t.Error("service.name beside an empty key should still promote")
	}

	// Idempotent: a second pass changes nothing.
	RepromoteTraceRow(&r)
	if r.ServiceName != "api" || r.ResourceAttributes[""] != "x" {
		t.Error("second pass must be a no-op")
	}
}

// FuzzRepromoteTraceRow locks the invariants over arbitrary resource/span maps: no
// panic; each output map's keys ⊆ that map's input keys (never invents a key);
// idempotent.
func FuzzRepromoteTraceRow(f *testing.F) {
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "thread.id", Bloom: true}}))
	f.Add("k8s.pod.name", "v1", "http.method", "GET", "custom.x", "y")
	f.Add("", "a", "service.name", "b", "db.system", "pg")
	f.Fuzz(func(t *testing.T, rk, rv, sk, sv, xk, xv string) {
		res := map[string]string{rk: rv, xk: xv}
		span := map[string]string{sk: sv}
		resKeys, spanKeys := keysOf(res), keysOf(span)

		r := schema.TraceRow{ResourceAttributes: cloneStrMap(res), SpanAttributes: cloneStrMap(span)}
		RepromoteTraceRow(&r) // must not panic

		for k := range r.ResourceAttributes {
			if !resKeys[k] {
				t.Fatalf("resource re-promote invented key %q", k)
			}
		}
		for k := range r.SpanAttributes {
			if !spanKeys[k] {
				t.Fatalf("span re-promote invented key %q", k)
			}
		}

		rs, ss := cloneStrMap(r.ResourceAttributes), cloneStrMap(r.SpanAttributes)
		RepromoteTraceRow(&r)
		if !strMapEq(rs, r.ResourceAttributes) || !strMapEq(ss, r.SpanAttributes) {
			t.Fatal("not idempotent across a second pass")
		}
	})
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func keysOf(m map[string]string) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

func strMapEq(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
