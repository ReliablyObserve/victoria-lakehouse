package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRepromoteLogRow is the compaction-healing guard: a v1 row (promoted attrs
// still in the map) must lift them into their dedicated columns and drop them from
// the map; non-promoted keys stay; a v2 row is unchanged (idempotent).
func TestRepromoteLogRow(t *testing.T) {
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

	// Empty / nil map → no panic, no allocation.
	RepromoteLogRow(&schema.LogRow{})
}
