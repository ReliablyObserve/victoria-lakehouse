package metrics

import (
	"bytes"
	"os"
	"strings"
	"testing"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

// The forward fence's counter is exported at 0 for every signal and op from
// process start, an alert reads it, and the dashboard shows it (#409 review).
func TestSkippedUnknownColumns_ExportedAndObservable(t *testing.T) {
	var buf bytes.Buffer
	vmmetrics.WritePrometheus(&buf, false)
	for _, s := range []string{"logs", "traces"} {
		for _, o := range []string{"compact", "delete_rewrite"} {
			series := `lakehouse_compaction_skipped_unknown_columns_total{signal="` + s + `",op="` + o + `"} `
			if !strings.Contains(buf.String(), series) {
				t.Errorf("%s is not exported at start", series)
			}
		}
	}

	r, ok := loadAlertRules(t)["LakehouseCompactionSkippedUnknownColumns"]
	if !ok {
		t.Fatal("alert LakehouseCompactionSkippedUnknownColumns is missing from alerts/alerts-lakehouse.yml")
	}
	if !strings.Contains(r.Expr, "increase(lakehouse_compaction_skipped_unknown_columns_total[1h]) > 0") || r.Labels["severity"] != "warning" {
		t.Errorf("alert = %+v", r)
	}

	dash, err := os.ReadFile("../../dashboards/victoria-lakehouse.json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(dash), "lakehouse_compaction_skipped_unknown_columns_total") {
		t.Error("the dashboard has no panel for lakehouse_compaction_skipped_unknown_columns_total")
	}

	// The count is per object: one more Inc is one more object.
	c := SkippedUnknownColumns("logs", "compact")
	before := c.Get()
	c.Inc()
	if c.Get() != before+1 {
		t.Error("counter does not count")
	}
}
