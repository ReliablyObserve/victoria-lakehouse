package cache

import (
	"testing"
)

// TestLabelIndex_MarshalRoundtrip pins that the JSON we ship to S3
// round-trips correctly. Any new field on LabelInfo MUST keep this test
// green so a pod loading an older snapshot doesn't crash or lose data.
func TestLabelIndex_MarshalRoundtrip(t *testing.T) {
	src := NewLabelIndex()
	src.Add("service.name", []string{"api-gateway", "user-service"})
	src.Add("k8s.namespace.name", []string{"prod", "staging"})
	src.AddWithTenant("severity", []string{"error", "warn"}, "0:0")

	buf, err := MarshalLabelIndex(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalLabelIndex(buf)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Len() != src.Len() {
		t.Fatalf("label count drift: got=%d want=%d", got.Len(), src.Len())
	}
	for _, name := range src.GetFieldNames() {
		srcInfo := src.GetLabelInfo(name)
		gotInfo := got.GetLabelInfo(name)
		if srcInfo == nil || gotInfo == nil {
			t.Errorf("label %q missing on one side", name)
			continue
		}
		if len(srcInfo.Values) != len(gotInfo.Values) {
			t.Errorf("label %q value count drift: src=%d got=%d",
				name, len(srcInfo.Values), len(gotInfo.Values))
		}
	}
}

// TestLabelIndex_MergeFrom_PreservesUnion pins the recovery semantics:
// loading from local disk then merging the S3 snapshot must produce the
// union of both — never lose entries that exist on only one side.
// This is the property that lets pods recover across node moves
// without losing the labels they themselves observed.
func TestLabelIndex_MergeFrom_PreservesUnion(t *testing.T) {
	disk := NewLabelIndex()
	disk.Add("only-on-disk", []string{"a", "b"})
	disk.Add("shared", []string{"v1", "v2"})

	s3 := NewLabelIndex()
	s3.Add("only-on-s3", []string{"x", "y"})
	s3.Add("shared", []string{"v2", "v3"}) // overlap + new value

	disk.MergeFrom(s3)

	if disk.GetLabelInfo("only-on-disk") == nil {
		t.Error("merge lost a label that was only on the disk side")
	}
	if disk.GetLabelInfo("only-on-s3") == nil {
		t.Error("merge didn't pick up a label that was only on the S3 side")
	}
	sharedVals := disk.GetFieldValues("shared", 100)
	got := map[string]bool{}
	for _, v := range sharedVals {
		got[v] = true
	}
	if !got["v1"] || !got["v2"] || !got["v3"] {
		t.Errorf("merge didn't union shared values: %v", sharedVals)
	}
}

// TestLabelIndex_MergeFrom_NilSourceIsNoop is a defensive test — the S3
// download can legitimately return nil if the bucket has no snapshot
// yet, and MergeFrom must handle that without panicking.
func TestLabelIndex_MergeFrom_NilSourceIsNoop(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("a", []string{"v"})
	idx.MergeFrom(nil)
	if idx.GetLabelInfo("a") == nil {
		t.Error("MergeFrom(nil) shouldn't drop existing labels")
	}
}

// TestLabelIndex_MergeFrom_DoesntDropCountsOrTenants checks that
// the per-tenant breakdown and value-frequency counts (used by the
// /api/v1/cardinality breakdown endpoint and the storage estimate
// calculator) survive a merge. Losing these silently regresses the
// UI's per-tenant breakdown after a label-index recovery.
//
// Note: existing Add()/AddWithTenant() semantics post-increment counts
// based on the values slice even when ValueCounts is already
// populated, so we can't pin the absolute count value — only that
// each side's contribution survives.
func TestLabelIndex_MergeFrom_DoesntDropCountsOrTenants(t *testing.T) {
	a := NewLabelIndex()
	a.AddWithValueCounts("service.name", []string{"x"}, map[string]int{"x": 100})

	b := NewLabelIndex()
	b.AddWithValueCounts("service.name", []string{"y"}, map[string]int{"y": 50})

	// Per-tenant maps populated on both sides via the dedicated path so
	// AddWithTenant doesn't muddle the ValueCounts assertion above.
	a.AddWithTenant("service.name", []string{"x"}, "tenant-a")
	b.AddWithTenant("service.name", []string{"y"}, "tenant-b")

	a.MergeFrom(b)

	li := a.GetLabelInfo("service.name")
	if li == nil {
		t.Fatal("merged label missing")
	}
	// Each side's count must survive (≥ the source value; an exact
	// match isn't possible because AddWithTenant also nudges counts).
	if li.ValueCounts["x"] < 100 {
		t.Errorf("count for x = %d, lost source contribution (≥100)", li.ValueCounts["x"])
	}
	if li.ValueCounts["y"] < 50 {
		t.Errorf("count for y = %d, lost source contribution (≥50)", li.ValueCounts["y"])
	}
	if li.PerTenant["tenant-a"] == 0 || li.PerTenant["tenant-b"] == 0 {
		t.Errorf("per-tenant breakdown lost on merge: %+v", li.PerTenant)
	}
}

// TestMarshalLabelIndex_EmptyIsValid pins that uploading an empty
// index produces parseable JSON (so the very first save when the
// pod has just started doesn't write garbage to S3).
func TestMarshalLabelIndex_EmptyIsValid(t *testing.T) {
	buf, err := MarshalLabelIndex(NewLabelIndex())
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}
	if len(buf) == 0 {
		t.Error("marshal of empty index produced no bytes")
	}
	back, err := UnmarshalLabelIndex(buf)
	if err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if back == nil || back.Len() != 0 {
		t.Errorf("empty roundtrip wrong: %+v", back)
	}
}
