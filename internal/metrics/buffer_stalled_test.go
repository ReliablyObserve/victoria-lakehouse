package metrics

import (
	"strings"
	"testing"
	"time"
)

// The "must stay 0" series of the drain's plan check has alerts: any increase of
// stage="invariant" (warning, no wait: the first event matters) and a stalled
// segment that does not clear (critical, after a wait longer than a back-off).
func TestAlerts_DrainPlanMismatchAndStalledSegment(t *testing.T) {
	rules := loadAlertRules(t)
	r, ok := rules["LakehouseBufferDrainPlanMismatch"]
	if !ok {
		t.Fatal("alert LakehouseBufferDrainPlanMismatch is missing from alerts/alerts-lakehouse.yml")
	}
	if !strings.Contains(r.Expr, `lakehouse_buffer_flush_errors_total{stage="invariant"}`) || !strings.Contains(r.Expr, "increase(") {
		t.Errorf("expr %q does not read increase() of lakehouse_buffer_flush_errors_total{stage=\"invariant\"}", r.Expr)
	}
	if r.For != "" && r.For != "0m" && r.For != "0s" {
		t.Errorf("for = %q: a first mismatch must alert without waiting", r.For)
	}
	if r.Labels["severity"] != "warning" {
		t.Errorf("severity = %q, want warning", r.Labels["severity"])
	}
	s, ok := rules["LakehouseBufferSegmentStalled"]
	if !ok {
		t.Fatal("alert LakehouseBufferSegmentStalled is missing from alerts/alerts-lakehouse.yml")
	}
	if !strings.Contains(s.Expr, "lakehouse_buffer_stalled_segments") {
		t.Errorf("expr %q does not read lakehouse_buffer_stalled_segments", s.Expr)
	}
	if d, err := time.ParseDuration(s.For); err != nil || d < 10*time.Minute {
		t.Errorf("for = %q, want >= 10m (a mismatch that clears on retry must not page)", s.For)
	}
	if s.Labels["severity"] != "critical" {
		t.Errorf("severity = %q, want critical", s.Labels["severity"])
	}
}
