//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

func TestVLSelect_QueryReturnsData(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	body := httpGetBody(t, vlselectURL, "/select/logsql/query", params)

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatal("expected log lines from vlselect, got empty response")
	}
	t.Logf("vlselect returned %d lines", len(lines))
}

func TestVLSelect_FieldNames(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	body := httpGetBody(t, vlselectURL, "/select/logsql/field_names", params)

	if len(body) == 0 {
		t.Fatal("expected field names from vlselect")
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "service.name") {
		t.Errorf("expected service.name in field names, got: %s", bodyStr)
	}
	t.Logf("vlselect field_names: %s", bodyStr[:min(len(bodyStr), 200)])
}

func TestVLSelect_ServiceFilter(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", `service.name:"api-gateway"`)
	params.Set("limit", "10")

	body := httpGetBody(t, vlselectURL, "/select/logsql/query", params)

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatal("expected filtered results for api-gateway")
	}
	for _, line := range lines {
		if !strings.Contains(line, "api-gateway") {
			t.Errorf("line does not contain api-gateway: %s", line[:min(len(line), 100)])
		}
	}
	t.Logf("vlselect service filter returned %d lines", len(lines))
}
