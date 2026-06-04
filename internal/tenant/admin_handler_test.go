package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestAdminHandler_Migrate_RequiresAuth(t *testing.T) {
	h := NewAdminHandler(nil, AdminAuthConfig{})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST", "/lakehouse/api/v1/admin/tenant/migrate", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 for missing auth", rr.Code)
	}
}

func TestAdminHandler_Migrate_HeaderAuthOpens(t *testing.T) {
	mf := &fakeManifest{files: map[string][]manifest.FileInfo{
		"p": {{Key: "1002/0/logs/a.parquet", Size: 5}},
	}}
	mig := NewMigrator(mf, &fakeS3{}, "default-bucket")
	h := NewAdminHandler(mig, AdminAuthConfig{
		HeaderName:  "X-Admin",
		HeaderValue: "secret-shared",
	})
	mux := http.NewServeMux()
	h.Register(mux)

	body := `{"tenant_key":"1002:0","target_bucket":"acme-bucket"}`
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/admin/tenant/migrate", strings.NewReader(body))
	req.Header.Set("X-Admin", "secret-shared")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var result MigrationResult
	_ = json.Unmarshal(rr.Body.Bytes(), &result)
	if result.FilesMoved != 1 {
		t.Errorf("files_moved = %d, want 1", result.FilesMoved)
	}
}

func TestAdminHandler_Migrate_BearerAuthOpens(t *testing.T) {
	mf := &fakeManifest{}
	mig := NewMigrator(mf, &fakeS3{}, "default-bucket")
	h := NewAdminHandler(mig, AdminAuthConfig{BearerToken: "super-secret"})
	mux := http.NewServeMux()
	h.Register(mux)

	body, _ := json.Marshal(map[string]any{
		"account_id": 1, "project_id": 1, "target_bucket": "any",
	})
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/admin/tenant/migrate", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer super-secret")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 with bearer; body=%s", rr.Code, rr.Body.String())
	}
}

func TestAdminHandler_Migrate_RejectsBadBody(t *testing.T) {
	h := NewAdminHandler(nil, AdminAuthConfig{HeaderName: "X-A", HeaderValue: "ok"})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST", "/lakehouse/api/v1/admin/tenant/migrate", strings.NewReader(`{not json}`))
	req.Header.Set("X-A", "ok")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 on bad JSON", rr.Code)
	}
}

func TestAdminHandler_Migrate_RejectsMissingTarget(t *testing.T) {
	h := NewAdminHandler(nil, AdminAuthConfig{HeaderName: "X-A", HeaderValue: "ok"})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST", "/lakehouse/api/v1/admin/tenant/migrate", strings.NewReader(`{"tenant_key":"1:1"}`))
	req.Header.Set("X-A", "ok")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 on missing target_bucket", rr.Code)
	}
}

func TestAdminHandler_Migrate_MethodGuard(t *testing.T) {
	h := NewAdminHandler(nil, AdminAuthConfig{HeaderName: "X-A", HeaderValue: "ok"})
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/tenant/migrate", nil)
	req.Header.Set("X-A", "ok")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rr.Code)
	}
}
