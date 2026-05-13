package cache

import (
	"encoding/json"
	"testing"
)

func TestGuard_LabelInfoPerTenantJSONTag(t *testing.T) {
	li := NewLabelIndex()
	li.AddWithTenant("service.name", []string{"api"}, "t:p")

	info := li.GetLabelInfo("service.name")
	if info == nil {
		t.Fatal("GetLabelInfo returned nil")
	}

	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	json.Unmarshal(data, &m)

	if _, ok := m["per_tenant"]; !ok {
		t.Error("LabelInfo JSON missing per_tenant field — tag changed")
	}
}

func TestGuard_LabelInfoPerTenantRoundTrip(t *testing.T) {
	li := NewLabelIndex()
	li.AddWithTenant("svc", []string{"a", "b", "c"}, "tenant1")
	li.AddWithTenant("svc", []string{"x", "y"}, "tenant2")

	info := li.GetLabelInfo("svc")
	if info.PerTenant["tenant1"] != 3 {
		t.Errorf("tenant1 cardinality=%d want 3", info.PerTenant["tenant1"])
	}
	if info.PerTenant["tenant2"] != 2 {
		t.Errorf("tenant2 cardinality=%d want 2", info.PerTenant["tenant2"])
	}
}

func TestGuard_AddWithTenantDoesNotBreakRegularAdd(t *testing.T) {
	li := NewLabelIndex()
	li.Add("field1", []string{"v1", "v2"})
	li.AddWithTenant("field2", []string{"v3"}, "t:p")

	info1 := li.GetLabelInfo("field1")
	if info1 == nil {
		t.Fatal("regular Add field lost after AddWithTenant")
	}

	info2 := li.GetLabelInfo("field2")
	if info2 == nil {
		t.Fatal("AddWithTenant field not found")
	}
}

func TestGuard_GetAllLabelInfoReturnsAll(t *testing.T) {
	li := NewLabelIndex()
	li.Add("f1", []string{"a"})
	li.Add("f2", []string{"b"})
	li.AddWithTenant("f3", []string{"c"}, "t")

	all := li.GetAllLabelInfo()
	if len(all) != 3 {
		t.Errorf("GetAllLabelInfo returned %d want 3", len(all))
	}
}
