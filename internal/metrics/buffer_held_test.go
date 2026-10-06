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

// The held-segment gauges are exported at zero from process start (so the alert
// and dashboards see a series before the first restart ever holds anything).
func TestBufferHeldGauges_ExportedAtZero(t *testing.T) {
	var buf bytes.Buffer
	vmmetrics.WritePrometheus(&buf, false)
	for _, name := range []string{"lakehouse_buffer_held_segments", "lakehouse_buffer_oldest_held_age_seconds"} {
		if !strings.Contains(buf.String(), name+" ") {
			t.Errorf("%s is not exported at start", name)
		}
	}
	if BufferHeldSegments.Get() != 0 || BufferOldestHeldAge.Get() != 0 {
		t.Errorf("held gauges start at %d and %d, want 0", BufferHeldSegments.Get(), BufferOldestHeldAge.Get())
	}
}

// The alert on a hold that does not end exists, is a warning, waits at least
// ten minutes and reads the gauge the flusher sets.
func TestAlerts_RestoredSegmentsHeld(t *testing.T) {
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
			if r.Alert != "LakehouseBufferRestoredSegmentsHeld" {
				continue
			}
			d, err := time.ParseDuration(r.For)
			if err != nil || d < 10*time.Minute {
				t.Errorf("for = %q, want >= 10m", r.For)
			}
			if r.Labels["severity"] != "warning" {
				t.Errorf("severity = %q, want warning", r.Labels["severity"])
			}
			if !strings.Contains(r.Expr, "lakehouse_buffer_oldest_held_age_seconds") {
				t.Errorf("expr %q does not read lakehouse_buffer_oldest_held_age_seconds", r.Expr)
			}
			return
		}
	}
	t.Fatal("alert LakehouseBufferRestoredSegmentsHeld is missing from alerts/alerts-lakehouse.yml")
}
