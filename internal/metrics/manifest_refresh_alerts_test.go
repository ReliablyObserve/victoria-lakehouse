package metrics

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	vmmetrics "github.com/VictoriaMetrics/metrics"
	"gopkg.in/yaml.v3"
)

type alertRule struct {
	Alert  string            `yaml:"alert"`
	Expr   string            `yaml:"expr"`
	For    string            `yaml:"for"`
	Labels map[string]string `yaml:"labels"`
}

func loadAlertRules(t *testing.T) map[string]alertRule {
	t.Helper()
	b, err := os.ReadFile("../../alerts/alerts-lakehouse.yml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Groups []struct {
			Rules []alertRule `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]alertRule{}
	for _, g := range doc.Groups {
		for _, r := range g.Rules {
			if r.Alert != "" {
				out[r.Alert] = r
			}
		}
	}
	return out
}

// The manifest refresh series the alerts read are exported from process start
// (#404 round 4): LakehouseManifestStale read a series nothing exported, so it
// could never fire.
func TestManifestRefreshSeries_ExportedAtStart(t *testing.T) {
	var buf bytes.Buffer
	vmmetrics.WritePrometheus(&buf, false)
	for _, name := range []string{
		"lakehouse_manifest_last_refresh_timestamp_seconds",
		"lakehouse_manifest_last_complete_refresh_timestamp_seconds",
		"lakehouse_manifest_refresh_interval_seconds",
		`lakehouse_duplicate_file_keys_total{site="manifest_refresh"}`,
		`lakehouse_duplicate_file_keys_total{site="manifest_load"}`,
		`lakehouse_duplicate_file_keys_total{site="compaction_input"}`,
	} {
		if !strings.Contains(buf.String(), name+" ") {
			t.Errorf("%s is not exported at start", name)
		}
	}
}

// Every lakehouse_* series the manifest alerts read is defined in this package:
// an alert on a series no code exports never fires.
func TestManifestAlerts_ReadExportedSeries(t *testing.T) {
	rules := loadAlertRules(t)
	src, err := os.ReadFile("lakehouse.go")
	if err != nil {
		t.Fatal(err)
	}
	name := regexp.MustCompile(`lakehouse_[a-z0-9_]+`)
	for _, alert := range []string{"LakehouseManifestStale", "LakehouseManifestNoCompleteRefresh", "LakehouseDuplicateFileKeys"} {
		r, ok := rules[alert]
		if !ok {
			t.Errorf("alert %s is missing from alerts/alerts-lakehouse.yml", alert)
			continue
		}
		if r.Labels["severity"] != "warning" {
			t.Errorf("%s: severity = %q, want warning", alert, r.Labels["severity"])
		}
		for _, series := range name.FindAllString(r.Expr, -1) {
			if !strings.Contains(string(src), `"`+series+`"`) {
				t.Errorf("%s reads %s, which internal/metrics does not define", alert, series)
			}
		}
	}
	nc := rules["LakehouseManifestNoCompleteRefresh"]
	if d, err := time.ParseDuration(nc.For); err != nil || d < 15*time.Minute {
		t.Errorf("LakehouseManifestNoCompleteRefresh for = %q, want >= 15m", nc.For)
	}
	for _, want := range []string{"lakehouse_manifest_last_complete_refresh_timestamp_seconds", "lakehouse_manifest_refresh_interval_seconds"} {
		if !strings.Contains(nc.Expr, want) {
			t.Errorf("LakehouseManifestNoCompleteRefresh does not read %s: %q", want, nc.Expr)
		}
	}
	// Warm-up (#404 round 5): the gauge is 0 until the first complete listing,
	// so a bare time() - gauge fired 15 minutes into every start whose first
	// LIST was slower than that. The steady-state term is guarded by > 0 and the
	// warm-up term measures from process start with the same budget.
	compact := strings.Join(strings.Fields(nc.Expr), " ")
	for _, want := range []string{
		"lakehouse_manifest_last_complete_refresh_timestamp_seconds > 0 and time() - lakehouse_manifest_last_complete_refresh_timestamp_seconds > clamp_min(4 * lakehouse_manifest_refresh_interval_seconds, 900)",
		"lakehouse_manifest_last_complete_refresh_timestamp_seconds == 0 and time() - process_start_time_seconds > clamp_min(4 * lakehouse_manifest_refresh_interval_seconds, 900)",
	} {
		if !strings.Contains(compact, want) {
			t.Errorf("LakehouseManifestNoCompleteRefresh lacks the term %q: %q", want, compact)
		}
	}
}
