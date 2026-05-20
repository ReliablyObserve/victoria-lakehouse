//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

// goldenPath returns the path to the golden file for the given test name.
func goldenPath(name string) string {
	return filepath.Join("testdata", name+".golden.json")
}

// compareGoldenJSON indents the actual JSON, then compares it to the golden file.
// On first run (golden file absent) or when GOLDEN_UPDATE=1 is set, the golden
// file is written from the actual response and the test passes.
func compareGoldenJSON(t *testing.T, name string, actual []byte) {
	t.Helper()

	// Normalise: indent so diffs are readable and deterministic.
	var buf bytes.Buffer
	if err := json.Indent(&buf, actual, "", "  "); err != nil {
		// Not valid JSON — still write raw so the file is created.
		buf.Write(actual)
	}
	normalised := buf.Bytes()

	path := goldenPath(name)
	update := os.Getenv("GOLDEN_UPDATE") == "1"

	existing, err := os.ReadFile(path)
	if os.IsNotExist(err) || update {
		if writeErr := os.WriteFile(path, normalised, 0o644); writeErr != nil {
			t.Fatalf("golden: writing %s: %v", path, writeErr)
		}
		if update {
			t.Logf("golden: updated %s", path)
		} else {
			t.Logf("golden: created %s (first run)", path)
		}
		return
	}
	if err != nil {
		t.Fatalf("golden: reading %s: %v", path, err)
	}

	if !bytes.Equal(normalised, existing) {
		t.Errorf("golden mismatch for %s\n--- want (golden) ---\n%s\n--- got (actual) ---\n%s",
			name, string(existing), string(normalised))
		t.Logf("re-run with GOLDEN_UPDATE=1 to accept new output")
	}
}

// ---------------------------------------------------------------------------
// Logs golden tests
// ---------------------------------------------------------------------------

// TestGolden_Logs_FieldNames snapshots /select/logsql/field_names.
func TestGolden_Logs_FieldNames(t *testing.T) {
	params := wideTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	compareGoldenJSON(t, "logs_field_names", body)
}

// TestGolden_Logs_ManifestRange snapshots /manifest/range.
func TestGolden_Logs_ManifestRange(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/range", nil)
	compareGoldenJSON(t, "logs_manifest_range", body)
}

// TestGolden_Logs_LakehouseInfo snapshots /lakehouse/info.
func TestGolden_Logs_LakehouseInfo(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/lakehouse/info", nil)
	compareGoldenJSON(t, "logs_lakehouse_info", body)
}

// ---------------------------------------------------------------------------
// Traces golden tests
// ---------------------------------------------------------------------------

// TestGolden_Traces_JaegerServices snapshots /api/services.
func TestGolden_Traces_JaegerServices(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/api/services", nil)
	compareGoldenJSON(t, "traces_jaeger_services", body)
}

// TestGolden_Traces_JaegerDependencies snapshots /api/dependencies.
func TestGolden_Traces_JaegerDependencies(t *testing.T) {
	params := url.Values{
		"endTs":    {"9999999999999"},
		"lookback": {"604800000"},
	}

	resp := httpGetAllowStatus(t, tracesBaseURL, "/api/dependencies", params,
		http.StatusOK, http.StatusNotFound, http.StatusNotImplemented)
	defer func() { _ = resp.Body.Close() }()

	// Dependencies endpoint may not be implemented — skip gracefully.
	if resp.StatusCode != http.StatusOK {
		t.Skipf("Jaeger /api/dependencies returned %d — skipping golden capture", resp.StatusCode)
	}

	body := httpGetBody(t, tracesBaseURL, "/api/dependencies", params)
	compareGoldenJSON(t, "traces_jaeger_dependencies", body)
}

// TestGolden_Traces_ManifestRange snapshots traces /manifest/range.
func TestGolden_Traces_ManifestRange(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/manifest/range", nil)
	compareGoldenJSON(t, "traces_manifest_range", body)
}

// TestGolden_Traces_LakehouseInfo snapshots traces /lakehouse/info.
func TestGolden_Traces_LakehouseInfo(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/lakehouse/info", nil)
	compareGoldenJSON(t, "traces_lakehouse_info", body)
}
