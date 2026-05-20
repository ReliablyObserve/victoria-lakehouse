package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestBuildPushDownFilter_TracesExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`service.name:="api"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil push down filter for exact match")
	}
	if len(pdf.Checks) == 0 {
		t.Fatal("expected at least one check")
	}
	found := false
	for _, c := range pdf.Checks {
		if c.Column == "service.name" && c.Value == "api" && c.Op == PushDownExact {
			found = true
		}
	}
	if !found {
		t.Errorf("expected exact match check for service.name=api, got %+v", pdf.Checks)
	}
}

func TestBuildPushDownFilter_TracesEmpty(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`*`, reg)
	if pdf != nil {
		t.Error("expected nil push down filter for wildcard query")
	}
}

func TestBuildPushDownFilter_TracesNilRegistry(t *testing.T) {
	pdf := buildPushDownFilter(`service.name:="api"`, nil)
	if pdf != nil {
		t.Error("expected nil push down filter for nil registry")
	}
}

func TestBuildPushDownFilter_TracesTraceID(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	pdf := buildPushDownFilter(`trace_id:="abc123"`, reg)
	if pdf == nil {
		t.Fatal("expected non-nil push down filter")
	}
	found := false
	for _, c := range pdf.Checks {
		if c.Column == "trace_id" && c.Value == "abc123" {
			found = true
		}
	}
	if !found {
		t.Error("expected check for trace_id")
	}
}

func TestCheckMatchesStats_Exact(t *testing.T) {
	if !checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "b"}, "a", "c") {
		t.Error("'b' should be in range [a,c]")
	}
	if checkMatchesStats(PushDownCheck{Op: PushDownExact, Value: "d"}, "a", "c") {
		t.Error("'d' should NOT be in range [a,c]")
	}
}
