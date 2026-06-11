package schema

import "testing"

func TestBloomCoverage_Logs(t *testing.T) {
	reg := NewRegistry(LogsProfile)
	expectedBloom := []string{
		"trace_id",
		"service.name",
		"span_id",
		"host.name",
		"k8s.pod.name",
		"k8s.node.name",
		"container.id",
		"service.instance.id",
	}
	for _, name := range expectedBloom {
		m := reg.ResolveFromParquet(name)
		if m == nil {
			m = reg.ResolveToParquet(name)
		}
		if m == nil {
			t.Errorf("field %q not found in logs registry", name)
			continue
		}
		if !m.HasBloom {
			t.Errorf("logs field %q: HasBloom = false, want true", name)
		}
	}
}

func TestBloomCoverage_Traces(t *testing.T) {
	reg := NewRegistry(TracesProfile)
	expectedBloom := []string{
		"trace_id",
		"service.name",
		"span.name",
	}
	for _, name := range expectedBloom {
		m := reg.ResolveFromParquet(name)
		if m == nil {
			m = reg.ResolveToParquet(name)
		}
		if m == nil {
			t.Errorf("field %q not found in traces registry", name)
			continue
		}
		if !m.HasBloom {
			t.Errorf("traces field %q: HasBloom = false, want true", name)
		}
	}
}

func TestBloomCoverage_LowCardinalityExcluded(t *testing.T) {
	logsReg := NewRegistry(LogsProfile)
	noBloom := []string{"severity_text", "_stream", "_stream_id"}
	for _, name := range noBloom {
		m := logsReg.ResolveFromParquet(name)
		if m != nil && m.HasBloom {
			t.Errorf("logs field %q should NOT have bloom (low cardinality)", name)
		}
	}

	tracesReg := NewRegistry(TracesProfile)
	tracesNoBloom := []string{"span.kind", "status.code", "_stream", "_stream_id"}
	for _, name := range tracesNoBloom {
		m := tracesReg.ResolveFromParquet(name)
		if m != nil && m.HasBloom {
			t.Errorf("traces field %q should NOT have bloom (low cardinality)", name)
		}
	}
}
