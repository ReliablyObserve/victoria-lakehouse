package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIntegration_FullRoundTrip(t *testing.T) {
	r := NewResolver(ResolverConfig{
		MetricsFormat: MetricsFormatName,
	})
	r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

	var capturedAccount, capturedProject string
	backend := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		capturedAccount = req.Header.Get("X-Scope-AccountID")
		capturedProject = req.Header.Get("X-Scope-ProjectID")
		w.WriteHeader(http.StatusOK)
	})

	handler := r.Middleware(backend)

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if capturedAccount != "42" || capturedProject != "3" {
		t.Errorf("middleware translated to account=%q project=%q, want 42/3", capturedAccount, capturedProject)
	}

	name := r.DisplayName(42, 3)
	if name != "prod-team-eu_staging" {
		t.Errorf("DisplayName = %q, want %q", name, "prod-team-eu_staging")
	}

	label := r.MetricLabel(42, 3)
	if label != "prod-team-eu_staging" {
		t.Errorf("MetricLabel = %q, want %q", label, "prod-team-eu_staging")
	}

	h := NewHandler(r, nil, "")
	body, _ := json.Marshal(AliasEntry{OrgID: "dev_default", AccountID: 1, ProjectID: 1})
	createReq := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	createRR := httptest.NewRecorder()
	h.handleAliases(createRR, createReq)

	if createRR.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createRR.Code)
	}

	tid, ok := r.Resolve("dev_default")
	if !ok || tid.AccountID != 1 || tid.ProjectID != 1 {
		t.Errorf("new alias resolve = %+v, %v", tid, ok)
	}

	listReq := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/aliases", nil)
	listRR := httptest.NewRecorder()
	h.handleAliases(listRR, listReq)

	var listResp AliasListResponse
	_ = json.Unmarshal(listRR.Body.Bytes(), &listResp)
	if len(listResp.Aliases) != 2 {
		t.Errorf("list returned %d aliases, want 2", len(listResp.Aliases))
	}

	delReq := httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases/dev_default", nil)
	delRR := httptest.NewRecorder()
	h.handleAliasDelete(delRR, delReq)

	if delRR.Code != http.StatusNoContent {
		t.Errorf("delete status = %d, want 204", delRR.Code)
	}

	_, ok = r.Resolve("dev_default")
	if ok {
		t.Error("deleted alias still resolves")
	}
}

func TestIntegration_OrgIDValidation_Comprehensive(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	validAliases := []string{
		"simple",
		"with-hyphens",
		"with_underscores",
		"with.dots",
		"with!bang",
		"with*star",
		"with'quote",
		"with(parens)",
		"MiXeD_CaSe",
		"123numeric",
		"prod-team-eu_staging",
		"acme-corp_us-east_production",
		"a",
	}

	for _, alias := range validAliases {
		if err := r.AddAlias(alias, TenantID{AccountID: 1, ProjectID: 1}); err != nil {
			t.Errorf("AddAlias(%q) failed: %v", alias, err)
		}
		r.RemoveAlias(alias)
	}

	invalidAliases := []string{
		"has/slash",
		"has|pipe",
		"has:colon",
		"has space",
		"has\ttab",
		"has@at",
		"has#hash",
		"has$dollar",
		"",
		".",
		"..",
	}

	for _, alias := range invalidAliases {
		if err := r.AddAlias(alias, TenantID{AccountID: 1, ProjectID: 1}); err == nil {
			t.Errorf("AddAlias(%q) should have failed", alias)
		}
	}
}
