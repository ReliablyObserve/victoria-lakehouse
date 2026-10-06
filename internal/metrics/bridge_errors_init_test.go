package metrics

import (
	"bytes"
	"strings"
	"testing"

	vmmetrics "github.com/VictoriaMetrics/metrics"
)

// Every bridge error reason is exported at zero from process start.
func TestBufferBridgeErrors_ReasonsExportedAtZero(t *testing.T) {
	var buf bytes.Buffer
	vmmetrics.WritePrometheus(&buf, false)
	for _, r := range BufferBridgeErrorReasons {
		want := `lakehouse_buffer_bridge_errors_total{reason="` + r + `"} `
		if !strings.Contains(buf.String(), want) {
			t.Errorf("series for reason %q not exported at start", r)
		}
	}
}
