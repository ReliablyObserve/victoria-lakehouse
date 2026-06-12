package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRepromoteLogRow is the compaction-healing guard: a v1 row (promoted attrs
// still in the map) must lift them into their dedicated columns and drop them from
// the map; non-promoted keys stay; a v2 row is unchanged (idempotent).
func TestRepromoteLogRow(t *testing.T) {
	SetSlotResolver(nil)
	r := schema.LogRow{
		LogAttributes: map[string]string{
			"k8s.pod.name":      "pod-1",
			"service.name":      "api",
			"container.id":      "ctr-1",
			"custom.unpromoted": "keep-me",
		},
	}
	RepromoteLogRow(&r)

	if r.K8sPodName != "pod-1" {
		t.Errorf("k8s.pod.name not promoted to column: %q", r.K8sPodName)
	}
	if r.ServiceName != "api" {
		t.Errorf("service.name not promoted to column: %q", r.ServiceName)
	}
	if r.ContainerID != "ctr-1" {
		t.Errorf("container.id not promoted to column: %q", r.ContainerID)
	}
	if _, ok := r.LogAttributes["k8s.pod.name"]; ok {
		t.Error("k8s.pod.name should have left the map after promotion")
	}
	if r.LogAttributes["custom.unpromoted"] != "keep-me" {
		t.Errorf("non-promoted key must stay in the map, got %v", r.LogAttributes)
	}

	// Idempotent: a v2 row (column already set, map free of promoted keys) is unchanged.
	r2 := schema.LogRow{ServiceName: "api", LogAttributes: map[string]string{"custom.unpromoted": "x"}}
	RepromoteLogRow(&r2)
	if r2.ServiceName != "api" || r2.LogAttributes["custom.unpromoted"] != "x" {
		t.Errorf("v2 row should be unchanged, got ServiceName=%q attrs=%v", r2.ServiceName, r2.LogAttributes)
	}
}

// TestRepromoteLogRow_Tier2Slots covers operator-configured custom attributes
// (Tier-2 slots), with AND without bloom — the "added with bloom / without bloom"
// case. Both must leave the map (routed to a slot); the bloom flag is the writer's
// concern (TestRepromote_SlotBloomDistinction), but the routing must work for both.
func TestRepromoteLogRow_Tier2Slots(t *testing.T) {
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{
		{Name: "thread.id", Bloom: true},      // high-card custom attr, bloomed
		{Name: "code.function", Bloom: false}, // low-card custom attr, no bloom
	}))
	defer SetSlotResolver(nil)

	r := schema.LogRow{LogAttributes: map[string]string{
		"thread.id":     "42",
		"code.function": "doThing",
		"k8s.pod.name":  "pod-1", // Tier-1
		"custom.keep":   "x",     // not promoted, not slotted
	}}
	RepromoteLogRow(&r)

	for _, k := range []string{"thread.id", "code.function", "k8s.pod.name"} {
		if _, ok := r.LogAttributes[k]; ok {
			t.Errorf("%q should have left the map (promoted/slotted)", k)
		}
	}
	if r.LogAttributes["custom.keep"] != "x" {
		t.Errorf("non-promoted/non-slotted key must stay, got %v", r.LogAttributes)
	}
	if r.K8sPodName != "pod-1" {
		t.Errorf("Tier-1 k8s.pod.name not in its column: %q", r.K8sPodName)
	}
}

// TestRepromoteLogRow_Edges locks the boundary behaviors.
func TestRepromoteLogRow_Edges(t *testing.T) {
	SetSlotResolver(nil)

	// nil / empty map → no-op, no allocation.
	r := schema.LogRow{}
	RepromoteLogRow(&r)
	if r.LogAttributes != nil {
		t.Errorf("nil map must stay nil, got %v", r.LogAttributes)
	}

	// Empty key (VL's _msg form) must be PRESERVED in the map, not routed to Body.
	r2 := schema.LogRow{LogAttributes: map[string]string{"": "do-not-route", "service.name": "api"}}
	RepromoteLogRow(&r2)
	if r2.LogAttributes[""] != "do-not-route" {
		t.Error("empty key must be preserved in the map")
	}
	if r2.Body != "" {
		t.Errorf("empty key must NOT be routed to Body, got Body=%q", r2.Body)
	}
	if r2.ServiceName != "api" {
		t.Error("service.name beside an empty key should still promote")
	}

	// All-promoted map → map drained to empty.
	r3 := schema.LogRow{LogAttributes: map[string]string{"service.name": "api", "k8s.pod.name": "p", "host.name": "h"}}
	RepromoteLogRow(&r3)
	if len(r3.LogAttributes) != 0 {
		t.Errorf("all-promoted map should drain to empty, got %v", r3.LogAttributes)
	}

	// Empty value on a promoted key still promotes (and leaves the map).
	r4 := schema.LogRow{LogAttributes: map[string]string{"service.name": ""}}
	RepromoteLogRow(&r4)
	if _, ok := r4.LogAttributes["service.name"]; ok {
		t.Error("empty-valued promoted key should still leave the map")
	}
}

// FuzzRepromoteLogRow asserts the core invariants under arbitrary attribute maps:
// (1) no panic; (2) the output map only ever SHRINKS — every output key was an
// input key (re-promote removes promoted keys, never invents one); (3) idempotent
// — a second pass changes nothing.
func FuzzRepromoteLogRow(f *testing.F) {
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "thread.id", Bloom: true}}))
	f.Add("k8s.pod.name", "v1", "custom.x", "v2", "thread.id", "7")
	f.Add("", "a", "service.name", "b", "z", "c")
	f.Add("service.name", "", "service.name", "", "", "")
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3 string) {
		in := map[string]string{}
		for _, kv := range [][2]string{{k1, v1}, {k2, v2}, {k3, v3}} {
			in[kv[0]] = kv[1]
		}
		inKeys := make(map[string]bool, len(in))
		for k := range in {
			inKeys[k] = true
		}

		r := schema.LogRow{LogAttributes: cloneStrMap(in)}
		RepromoteLogRow(&r) // must not panic

		for k := range r.LogAttributes {
			if !inKeys[k] {
				t.Fatalf("re-promote invented a map key not present in input: %q", k)
			}
		}

		snap := cloneStrMap(r.LogAttributes)
		RepromoteLogRow(&r)
		if len(snap) != len(r.LogAttributes) {
			t.Fatalf("not idempotent: map size %d → %d", len(snap), len(r.LogAttributes))
		}
		for k, v := range snap {
			if r.LogAttributes[k] != v {
				t.Fatalf("not idempotent at key %q: %q != %q", k, v, r.LogAttributes[k])
			}
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
