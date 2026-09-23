package main

import (
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// The delete API's operator view uses the global-read credential that widens a
// select; without one configured, no request is the operator.
func TestGlobalReadAuthorizer(t *testing.T) {
	cfg := &config.Config{}
	req := httptest.NewRequest("GET", "/delete/tracessql/tombstones", nil)
	req.Header.Set("X-Lakehouse-Global-Read", "letmein")
	if globalReadAuthorizer(cfg)(req) {
		t.Fatal("no credential configured: nothing may be authorized")
	}
	cfg.Tenant.GlobalReadHeader = "X-Lakehouse-Global-Read"
	cfg.Tenant.GlobalReadValue = "letmein"
	if !globalReadAuthorizer(cfg)(req) {
		t.Fatal("the configured credential must authorize")
	}
	req.Header.Set("X-Lakehouse-Global-Read", "wrong")
	if globalReadAuthorizer(cfg)(req) {
		t.Fatal("a wrong value must not authorize")
	}
}
