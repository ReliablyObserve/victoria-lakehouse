package metrics

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	vmmetrics "github.com/VictoriaMetrics/metrics"
	"gopkg.in/yaml.v3"
)

// The ring gauges carry the ring as a label and are what the alert reads.
func TestRingSelfGauges_ExportedPerRing(t *testing.T) {
	RingSelfResolved.Set("unit-test", 1)
	RingSelfInPeers.Set("unit-test", 0)
	var buf bytes.Buffer
	vmmetrics.WritePrometheus(&buf, false)
	for _, want := range []string{
		`lakehouse_ring_self_resolved{ring="unit-test"} 1`,
		`lakehouse_ring_self_in_peers{ring="unit-test"} 0`,
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("exposition lacks %q", want)
		}
	}
	if strings.Contains(buf.String(), "lakehouse_compaction_ownership_self_in_peers") {
		t.Error("the old unlabeled gauge is still exported")
	}
}

// The alert on a pod that does not find itself in the compaction ring exists, is a
// warning, waits at least five minutes, ignores pods that are not ready and reads
// the resolved gauge.
func TestAlerts_RingSelfUnresolved(t *testing.T) {
	b, err := os.ReadFile("../../alerts/alerts-lakehouse.yml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Groups []struct {
			Rules []struct {
				Alert  string            `yaml:"alert"`
				Expr   string            `yaml:"expr"`
				For    string            `yaml:"for"`
				Labels map[string]string `yaml:"labels"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	for _, g := range doc.Groups {
		for _, r := range g.Rules {
			if r.Alert != "LakehouseRingSelfUnresolved" {
				continue
			}
			d, err := time.ParseDuration(r.For)
			if err != nil || d < 5*time.Minute {
				t.Errorf("for = %q, want >= 5m", r.For)
			}
			if r.Labels["severity"] != "warning" {
				t.Errorf("severity = %q, want warning", r.Labels["severity"])
			}
			if !strings.Contains(r.Expr, `lakehouse_ring_self_resolved{ring="compaction"}`) || !strings.Contains(r.Expr, "== 0") {
				t.Errorf("expr %q does not fire on lakehouse_ring_self_resolved{ring=\"compaction\"} == 0", r.Expr)
			}
			if strings.Contains(r.Expr, "peercache") {
				t.Errorf("expr %q alerts on the peer-cache ring, which a pod outside the peer service is correctly not in", r.Expr)
			}
			if !strings.Contains(r.Expr, "lakehouse_ready == 1") {
				t.Errorf("expr %q does not exclude pods that are not ready", r.Expr)
			}
			return
		}
	}
	t.Fatal("alert LakehouseRingSelfUnresolved is missing from alerts/alerts-lakehouse.yml")
}
