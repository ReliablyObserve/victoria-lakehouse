package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler_ListAliases(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("prod_staging", TenantID{AccountID: 42, ProjectID: 3})
	h := NewHandler(r, nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/aliases", nil)
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var resp AliasListResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Aliases) != 1 {
		t.Errorf("got %d aliases, want 1", len(resp.Aliases))
	}
	if resp.Aliases[0].OrgID != "prod_staging" {
		t.Errorf("alias = %q, want %q", resp.Aliases[0].OrgID, "prod_staging")
	}
}

func TestHandler_CreateAlias(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	h := NewHandler(r, nil)

	body, _ := json.Marshal(AliasEntry{OrgID: "new_alias", AccountID: 10, ProjectID: 20})
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}

	tid, ok := r.Resolve("new_alias")
	if !ok {
		t.Fatal("expected alias to be resolvable")
	}
	if tid.AccountID != 10 || tid.ProjectID != 20 {
		t.Errorf("got %+v, want {10, 20}", tid)
	}
}

func TestHandler_CreateAlias_InvalidOrgID(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	h := NewHandler(r, nil)

	body, _ := json.Marshal(AliasEntry{OrgID: "has/slash", AccountID: 1, ProjectID: 1})
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestHandler_DeleteAlias(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("to_delete", TenantID{AccountID: 5, ProjectID: 6})
	h := NewHandler(r, nil)

	req := httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases/to_delete", nil)
	rr := httptest.NewRecorder()
	h.handleAliasDelete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}

	_, ok := r.Resolve("to_delete")
	if ok {
		t.Error("expected alias to be removed")
	}
}
