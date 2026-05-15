# Tenant Name Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bidirectional tenant name mapping that translates Loki/Tempo-compatible string aliases (`X-Scope-OrgID`) to VL/VT integer `{AccountID, ProjectID}` pairs at the HTTP boundary, and displays friendly names on all external surfaces.

**Architecture:** A `TenantResolver` component uses `sync.Map` for O(1) lock-free lookups. HTTP middleware translates inbound `X-Scope-OrgID` headers to integer headers before VL/VT handlers run. Outbound display paths (stats API, metrics, logs) call `DisplayName()` to show friendly names. Three data sources: static config, runtime CRUD API, optional auto-discovery. Runtime aliases persist to S3.

**Tech Stack:** Go 1.24, `sync.Map`, `VictoriaMetrics/metrics`, existing `s3reader.ClientPool` for persistence.

**Build/test command:** `GOWORK=off go test ./... -count=1` (from repo root). No AI co-authoring in commits.

---

## File Structure

| File | Responsibility |
|---|---|
| **Create:** `internal/tenant/validation.go` | `ValidateOrgID()` — Loki/Tempo charset enforcement |
| **Create:** `internal/tenant/validation_test.go` | Charset validation tests |
| **Create:** `internal/tenant/resolver.go` | `TenantResolver` — forward/reverse maps, `Resolve()`, `DisplayName()`, `MetricLabel()` |
| **Create:** `internal/tenant/resolver_test.go` | Resolver unit tests |
| **Create:** `internal/tenant/middleware.go` | HTTP middleware — `X-Scope-OrgID` → integer header translation |
| **Create:** `internal/tenant/middleware_test.go` | Middleware tests |
| **Create:** `internal/tenant/handler.go` | Aliases CRUD API handlers |
| **Create:** `internal/tenant/handler_test.go` | Handler tests |
| **Create:** `internal/tenant/persistence.go` | S3 read/write for `_meta/tenant-aliases.json` |
| **Create:** `internal/tenant/persistence_test.go` | Persistence tests |
| **Modify:** `internal/config/config.go:242-299` | Extend `TenantConfig` with alias fields, `{OrgID}` template support |
| **Modify:** `internal/stats/api.go:63-79,137-157,265-330,908-925` | Add `Name` field to response structs, alias URL routing |
| **Modify:** `internal/manifest/manifest.go:488-548` | Template-aware `TenantSummaries()` parsing |
| **Modify:** `internal/metrics/lakehouse.go:157-167` | No change to declarations (label format handled at call site) |
| **Modify:** `cmd/lakehouse-logs/main.go:74-85,352-369,433-519` | New flags, resolver wiring, middleware, alias handler registration |

---

### Task 1: OrgID Validation

**Files:**
- Create: `internal/tenant/validation.go`
- Create: `internal/tenant/validation_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/validation_test.go`:

```go
package tenant

import "testing"

func TestValidateOrgID(t *testing.T) {
	valid := []string{
		"prod-team-eu_staging",
		"prod-team-eu_prod",
		"dev_default",
		"acme-corp_us-east_production",
		"a",
		"abc123",
		"my-org!special_proj",
		"has.dots.in.it",
		"parens(ok)",
		"star*ok",
		"quote'ok",
	}
	for _, id := range valid {
		if err := ValidateOrgID(id); err != nil {
			t.Errorf("ValidateOrgID(%q) = %v, want nil", id, err)
		}
	}

	invalid := []struct {
		id   string
		desc string
	}{
		{"", "empty"},
		{".", "dot only"},
		{"..", "double dot"},
		{"has/slash", "slash"},
		{"has|pipe", "pipe"},
		{"has:colon", "colon"},
		{"has space", "space"},
		{"has@at", "at sign"},
		{string(make([]byte, 151)), "too long"},
	}
	for _, tc := range invalid {
		if err := ValidateOrgID(tc.id); err == nil {
			t.Errorf("ValidateOrgID(%q) [%s] = nil, want error", tc.id, tc.desc)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestValidateOrgID -v`
Expected: FAIL — package does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `internal/tenant/validation.go`:

```go
package tenant

import (
	"errors"
	"fmt"
)

const MaxOrgIDLength = 150

var validOrgIDChars [256]bool

func init() {
	for c := 'a'; c <= 'z'; c++ {
		validOrgIDChars[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		validOrgIDChars[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		validOrgIDChars[c] = true
	}
	for _, c := range "!-_.*'()" {
		validOrgIDChars[c] = true
	}
}

func ValidateOrgID(id string) error {
	if len(id) == 0 {
		return errors.New("tenant org ID is empty")
	}
	if len(id) > MaxOrgIDLength {
		return fmt.Errorf("tenant org ID too long: %d > %d", len(id), MaxOrgIDLength)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("tenant org ID %q is unsafe", id)
	}
	for i := 0; i < len(id); i++ {
		if !validOrgIDChars[id[i]] {
			return fmt.Errorf("tenant org ID %q contains invalid character %q at position %d", id, id[i], i)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestValidateOrgID -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/validation.go internal/tenant/validation_test.go
git commit -m "feat(tenant): add OrgID validation with Loki/Tempo charset"
```

---

### Task 2: TenantResolver Core

**Files:**
- Create: `internal/tenant/resolver.go`
- Create: `internal/tenant/resolver_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/resolver_test.go`:

```go
package tenant

import "testing"

func TestResolver_ResolveAndDisplayName(t *testing.T) {
	r := NewResolver(ResolverConfig{
		MetricsFormat: MetricsFormatID,
	})

	r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})
	r.AddAlias("dev_default", TenantID{AccountID: 1, ProjectID: 1})

	// Forward lookup
	tid, ok := r.Resolve("prod-team-eu_staging")
	if !ok {
		t.Fatal("expected to resolve prod-team-eu_staging")
	}
	if tid.AccountID != 42 || tid.ProjectID != 3 {
		t.Errorf("got %+v, want {42, 3}", tid)
	}

	// Unknown alias
	_, ok = r.Resolve("unknown")
	if ok {
		t.Error("expected unknown alias to return false")
	}

	// Reverse lookup
	name := r.DisplayName(42, 3)
	if name != "prod-team-eu_staging" {
		t.Errorf("DisplayName(42, 3) = %q, want %q", name, "prod-team-eu_staging")
	}

	// Fallback for unmapped tenant
	name = r.DisplayName(99, 99)
	if name != "99:99" {
		t.Errorf("DisplayName(99, 99) = %q, want %q", name, "99:99")
	}
}

func TestResolver_MetricLabel(t *testing.T) {
	tests := []struct {
		format MetricsFormat
		acc    uint32
		proj   uint32
		want   string
	}{
		{MetricsFormatID, 42, 3, "42:3"},
		{MetricsFormatName, 42, 3, "prod-team-eu_staging"},
		{MetricsFormatName, 99, 99, "99:99"}, // fallback
	}

	for _, tc := range tests {
		r := NewResolver(ResolverConfig{MetricsFormat: tc.format})
		r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

		got := r.MetricLabel(tc.acc, tc.proj)
		if got != tc.want {
			t.Errorf("MetricLabel(%d, %d) format=%v = %q, want %q", tc.acc, tc.proj, tc.format, got, tc.want)
		}
	}
}

func TestResolver_RemoveAlias(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("test_alias", TenantID{AccountID: 10, ProjectID: 20})

	_, ok := r.Resolve("test_alias")
	if !ok {
		t.Fatal("expected alias to exist")
	}

	r.RemoveAlias("test_alias")

	_, ok = r.Resolve("test_alias")
	if ok {
		t.Error("expected alias removed")
	}

	name := r.DisplayName(10, 20)
	if name != "10:20" {
		t.Errorf("after remove, DisplayName = %q, want %q", name, "10:20")
	}
}

func TestResolver_AllAliases(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("a_one", TenantID{AccountID: 1, ProjectID: 1})
	r.AddAlias("b_two", TenantID{AccountID: 2, ProjectID: 2})

	all := r.AllAliases()
	if len(all) != 2 {
		t.Errorf("AllAliases() len = %d, want 2", len(all))
	}
}

func TestResolver_AddAlias_Validation(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	if err := r.AddAlias("has/slash", TenantID{AccountID: 1, ProjectID: 1}); err == nil {
		t.Error("expected validation error for slash")
	}

	if err := r.AddAlias("valid_alias", TenantID{AccountID: 1, ProjectID: 1}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestResolver -v`
Expected: FAIL — `NewResolver` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/tenant/resolver.go`:

```go
package tenant

import (
	"fmt"
	"sync"
)

type TenantID struct {
	AccountID uint32
	ProjectID uint32
}

type MetricsFormat int

const (
	MetricsFormatID   MetricsFormat = iota // "42:3"
	MetricsFormatName                      // "prod-team-eu_staging"
	MetricsFormatBoth                      // both labels
)

func ParseMetricsFormat(s string) MetricsFormat {
	switch s {
	case "name":
		return MetricsFormatName
	case "both":
		return MetricsFormatBoth
	default:
		return MetricsFormatID
	}
}

type AliasEntry struct {
	OrgID     string   `json:"org_id"`
	AccountID uint32   `json:"account_id"`
	ProjectID uint32   `json:"project_id"`
	Source    string   `json:"source"` // "config", "api", "auto"
}

type ResolverConfig struct {
	MetricsFormat MetricsFormat
	AutoRegister  bool
	OrgIDHeader   string
}

type TenantResolver struct {
	forward sync.Map // string -> TenantID
	reverse sync.Map // "AccountID:ProjectID" -> string
	config  ResolverConfig
	mu      sync.Mutex // protects writes only
}

func NewResolver(cfg ResolverConfig) *TenantResolver {
	if cfg.OrgIDHeader == "" {
		cfg.OrgIDHeader = "X-Scope-OrgID"
	}
	return &TenantResolver{config: cfg}
}

func reverseKey(accountID, projectID uint32) string {
	return fmt.Sprintf("%d:%d", accountID, projectID)
}

func (r *TenantResolver) AddAlias(orgID string, tid TenantID) error {
	if err := ValidateOrgID(orgID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forward.Store(orgID, tid)
	r.reverse.Store(reverseKey(tid.AccountID, tid.ProjectID), orgID)
	return nil
}

func (r *TenantResolver) RemoveAlias(orgID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.forward.LoadAndDelete(orgID); ok {
		tid := v.(TenantID)
		r.reverse.Delete(reverseKey(tid.AccountID, tid.ProjectID))
	}
}

func (r *TenantResolver) Resolve(orgID string) (TenantID, bool) {
	v, ok := r.forward.Load(orgID)
	if !ok {
		return TenantID{}, false
	}
	return v.(TenantID), true
}

func (r *TenantResolver) DisplayName(accountID, projectID uint32) string {
	v, ok := r.reverse.Load(reverseKey(accountID, projectID))
	if !ok {
		return fmt.Sprintf("%d:%d", accountID, projectID)
	}
	return v.(string)
}

func (r *TenantResolver) MetricLabel(accountID, projectID uint32) string {
	switch r.config.MetricsFormat {
	case MetricsFormatName:
		return r.DisplayName(accountID, projectID)
	default:
		return fmt.Sprintf("%d:%d", accountID, projectID)
	}
}

func (r *TenantResolver) HasAliases() bool {
	has := false
	r.forward.Range(func(_, _ any) bool {
		has = true
		return false
	})
	return has
}

func (r *TenantResolver) AllAliases() []AliasEntry {
	var entries []AliasEntry
	r.forward.Range(func(k, v any) bool {
		orgID := k.(string)
		tid := v.(TenantID)
		entries = append(entries, AliasEntry{
			OrgID:     orgID,
			AccountID: tid.AccountID,
			ProjectID: tid.ProjectID,
		})
		return true
	})
	return entries
}

func (r *TenantResolver) Config() ResolverConfig {
	return r.config
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestResolver -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/resolver.go internal/tenant/resolver_test.go
git commit -m "feat(tenant): add TenantResolver with forward/reverse alias maps"
```

---

### Task 3: HTTP Middleware

**Files:**
- Create: `internal/tenant/middleware.go`
- Create: `internal/tenant/middleware_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/middleware_test.go`:

```go
package tenant

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_OrgIDResolved(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

	var gotAccount, gotProject string
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAccount = req.Header.Get("X-Scope-AccountID")
		gotProject = req.Header.Get("X-Scope-ProjectID")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "prod-team-eu_staging")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	if gotAccount != "42" {
		t.Errorf("account = %q, want %q", gotAccount, "42")
	}
	if gotProject != "3" {
		t.Errorf("project = %q, want %q", gotProject, "3")
	}
}

func TestMiddleware_OrgIDUnknown(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Error("handler should not be called for unknown org")
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "unknown-org")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestMiddleware_NoOrgIDPassthrough(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	called := false
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-AccountID", "42")
	req.Header.Set("X-Scope-ProjectID", "3")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if !called {
		t.Error("handler should be called when no X-Scope-OrgID")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestMiddleware_AutoRegister(t *testing.T) {
	r := NewResolver(ResolverConfig{AutoRegister: true})

	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("POST", "/insert/jsonline", nil)
	req.Header.Set("X-Scope-OrgID", "new-auto-tenant")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for auto-register", rr.Code)
	}

	_, ok := r.Resolve("new-auto-tenant")
	if !ok {
		t.Error("expected auto-registered tenant to be resolvable")
	}
}

func TestMiddleware_CustomHeader(t *testing.T) {
	r := NewResolver(ResolverConfig{OrgIDHeader: "X-Custom-Tenant"})
	r.AddAlias("custom_tenant", TenantID{AccountID: 5, ProjectID: 6})

	var gotAccount string
	handler := r.Middleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAccount = req.Header.Get("X-Scope-AccountID")
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Custom-Tenant", "custom_tenant")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if gotAccount != "5" {
		t.Errorf("account = %q, want %q", gotAccount, "5")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestMiddleware -v`
Expected: FAIL — `Middleware` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/tenant/middleware.go`:

```go
package tenant

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
)

var autoIDCounter uint32 = 1000

func (r *TenantResolver) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		orgID := req.Header.Get(r.config.OrgIDHeader)
		if orgID == "" {
			next.ServeHTTP(w, req)
			return
		}

		tid, ok := r.Resolve(orgID)
		if !ok {
			if r.config.AutoRegister {
				newID := atomic.AddUint32(&autoIDCounter, 1)
				tid = TenantID{AccountID: newID, ProjectID: 0}
				_ = r.AddAlias(orgID, tid)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error":  "unknown tenant",
					"org_id": orgID,
				})
				return
			}
		}

		req.Header.Set("X-Scope-AccountID", fmt.Sprintf("%d", tid.AccountID))
		req.Header.Set("X-Scope-ProjectID", fmt.Sprintf("%d", tid.ProjectID))
		req.Header.Del(r.config.OrgIDHeader)

		next.ServeHTTP(w, req)
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestMiddleware -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/middleware.go internal/tenant/middleware_test.go
git commit -m "feat(tenant): add HTTP middleware for X-Scope-OrgID translation"
```

---

### Task 4: Aliases CRUD API Handlers

**Files:**
- Create: `internal/tenant/handler.go`
- Create: `internal/tenant/handler_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/handler_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestHandler -v`
Expected: FAIL — `NewHandler` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/tenant/handler.go`:

```go
package tenant

import (
	"encoding/json"
	"net/http"
	"strings"
)

type AliasListResponse struct {
	Aliases []AliasEntry `json:"aliases"`
}

type Persister interface {
	SaveAliases(entries []AliasEntry) error
}

type Handler struct {
	resolver  *TenantResolver
	persister Persister
}

func NewHandler(r *TenantResolver, p Persister) *Handler {
	return &Handler{resolver: r, persister: p}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/lakehouse/api/v1/tenants/aliases", h.handleAliases)
	mux.HandleFunc("/lakehouse/api/v1/tenants/aliases/", h.handleAliasDelete)
}

func (h *Handler) handleAliases(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listAliases(w, r)
	case http.MethodPost:
		h.createAlias(w, r)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) listAliases(w http.ResponseWriter, _ *http.Request) {
	all := h.resolver.AllAliases()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(AliasListResponse{Aliases: all})
}

func (h *Handler) createAlias(w http.ResponseWriter, r *http.Request) {
	var entry AliasEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if err := h.resolver.AddAlias(entry.OrgID, TenantID{AccountID: entry.AccountID, ProjectID: entry.ProjectID}); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if h.persister != nil {
		_ = h.persister.SaveAliases(h.resolver.AllAliases())
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(entry)
}

func (h *Handler) handleAliasDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	orgID := strings.TrimPrefix(r.URL.Path, "/lakehouse/api/v1/tenants/aliases/")
	if orgID == "" {
		http.Error(w, `{"error":"missing org_id"}`, http.StatusBadRequest)
		return
	}

	h.resolver.RemoveAlias(orgID)

	if h.persister != nil {
		_ = h.persister.SaveAliases(h.resolver.AllAliases())
	}

	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestHandler -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/handler.go internal/tenant/handler_test.go
git commit -m "feat(tenant): add aliases CRUD API handlers"
```

---

### Task 5: S3 Persistence

**Files:**
- Create: `internal/tenant/persistence.go`
- Create: `internal/tenant/persistence_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/persistence_test.go`:

```go
package tenant

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

type mockPool struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockPool() *mockPool {
	return &mockPool{data: make(map[string][]byte)}
}

func (m *mockPool) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockPool) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	return d, nil
}

func TestS3Persister_SaveAndLoad(t *testing.T) {
	pool := newMockPool()
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	entries := []AliasEntry{
		{OrgID: "prod_staging", AccountID: 42, ProjectID: 3, Source: "api"},
		{OrgID: "dev_default", AccountID: 1, ProjectID: 1, Source: "api"},
	}

	if err := p.SaveAliases(entries); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadAliases()
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded) != 2 {
		t.Fatalf("loaded %d aliases, want 2", len(loaded))
	}

	// Verify JSON round-trip
	if loaded[0].OrgID != "prod_staging" || loaded[0].AccountID != 42 {
		t.Errorf("first alias = %+v, unexpected", loaded[0])
	}
}

func TestS3Persister_LoadEmpty(t *testing.T) {
	pool := newMockPool()
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	loaded, err := p.LoadAliases()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 aliases from empty store, got %d", len(loaded))
	}
}

func TestS3Persister_Roundtrip_JSON(t *testing.T) {
	entries := []AliasEntry{
		{OrgID: "acme-corp_us-east_production", AccountID: 100, ProjectID: 50, Source: "config"},
	}
	data, _ := json.Marshal(entries)

	var out []AliasEntry
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out[0].OrgID != "acme-corp_us-east_production" {
		t.Errorf("roundtrip failed: %+v", out[0])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestS3Persister -v`
Expected: FAIL — `NewS3Persister` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `internal/tenant/persistence.go`:

```go
package tenant

import (
	"context"
	"encoding/json"
)

type S3Pool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
}

type S3Persister struct {
	pool S3Pool
	key  string
}

func NewS3Persister(pool S3Pool, key string) *S3Persister {
	return &S3Persister{pool: pool, key: key}
}

func (p *S3Persister) SaveAliases(entries []AliasEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	return p.pool.Upload(context.Background(), p.key, data)
}

func (p *S3Persister) LoadAliases() ([]AliasEntry, error) {
	data, err := p.pool.Download(context.Background(), p.key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var entries []AliasEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestS3Persister -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/persistence.go internal/tenant/persistence_test.go
git commit -m "feat(tenant): add S3 persistence for runtime aliases"
```

---

### Task 6: Extend TenantConfig

**Files:**
- Modify: `internal/config/config.go:242-299`

- [ ] **Step 1: Write the failing test**

Create `internal/tenant/config_test.go` (tests config extension, lives in tenant package to avoid circular imports):

```go
package tenant

import "testing"

func TestPrefixTemplateSegments(t *testing.T) {
	tests := []struct {
		template string
		want     int
	}{
		{"{AccountID}/{ProjectID}/", 2},
		{"{OrgID}/", 1},
		{"{OrgID}/{ProjectID}/", 2},
		{"", 0},
		{"static-prefix/", 0},
	}
	for _, tc := range tests {
		got := CountTemplateSegments(tc.template)
		if got != tc.want {
			t.Errorf("CountTemplateSegments(%q) = %d, want %d", tc.template, got, tc.want)
		}
	}
}

func TestHasOrgIDTemplate(t *testing.T) {
	if !HasOrgIDTemplate("{OrgID}/") {
		t.Error("expected true for {OrgID}/")
	}
	if !HasOrgIDTemplate("{OrgID}/{ProjectID}/") {
		t.Error("expected true for {OrgID}/{ProjectID}/")
	}
	if HasOrgIDTemplate("{AccountID}/{ProjectID}/") {
		t.Error("expected false for integer template")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestPrefix -v`
Expected: FAIL — `CountTemplateSegments` not defined.

- [ ] **Step 3: Add template helpers to resolver.go and extend config**

Add to the bottom of `internal/tenant/resolver.go`:

```go
func CountTemplateSegments(template string) int {
	if template == "" {
		return 0
	}
	if !strings.Contains(template, "{") {
		return 0
	}
	clean := strings.TrimSuffix(template, "/")
	return len(strings.Split(clean, "/"))
}

func HasOrgIDTemplate(template string) bool {
	return strings.Contains(template, "{OrgID}")
}
```

Add `"strings"` to the imports of `resolver.go`.

Now modify `internal/config/config.go`. Add new fields to `TenantConfig` (after line 254, before `KnownTenants`):

```go
type TenantConfig struct {
	DefaultPrefix    string            `yaml:"default_prefix"`
	PrefixTemplate   string            `yaml:"prefix_template"`
	Isolation        string            `yaml:"isolation"`
	BucketTemplate   string            `yaml:"bucket_template"`
	DefaultAccount   string            `yaml:"default_account"`
	DefaultProject   string            `yaml:"default_project"`
	HeaderAccount    string            `yaml:"header_account"`
	HeaderProject    string            `yaml:"header_project"`
	GlobalReadHeader string            `yaml:"global_read_header"`
	GlobalReadValue  string            `yaml:"global_read_value"`
	GlobalReadToken  string            `yaml:"global_read_token"`
	KnownTenants     []KnownTenant     `yaml:"known_tenants"`
	OrgIDHeader      string            `yaml:"orgid_header"`
	MetricsFormat    string            `yaml:"metrics_format"`
	AutoRegister     bool              `yaml:"auto_register"`
	AliasSyncInterval time.Duration    `yaml:"alias_sync_interval"`
	Aliases          map[string]AliasTarget `yaml:"aliases"`
}

type AliasTarget struct {
	AccountID uint32 `yaml:"account_id"`
	ProjectID uint32 `yaml:"project_id"`
}
```

Update `DefaultConfig()` to set defaults for the new fields (find the tenant defaults section):

```go
// In the tenant section of DefaultConfig():
OrgIDHeader:       "X-Scope-OrgID",
MetricsFormat:     "id",
AutoRegister:      false,
AliasSyncInterval: 30 * time.Second,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestPrefix -v`
Expected: PASS

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./...`
Expected: compiles successfully.

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/config_test.go internal/tenant/resolver.go internal/config/config.go
git commit -m "feat(tenant): extend TenantConfig with alias fields and OrgID template helpers"
```

---

### Task 7: Stats API — Add Name Field and Alias URL Routing

**Files:**
- Modify: `internal/stats/api.go:63-79,137-157,265-330,908-925`

- [ ] **Step 1: Write the failing test**

Create `internal/stats/api_tenant_name_test.go`:

```go
package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

func TestTenantEntry_HasNameField(t *testing.T) {
	entry := TenantEntry{
		AccountID: "42",
		ProjectID: "3",
		Name:      "prod_staging",
	}
	data, _ := json.Marshal(entry)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	if m["name"] != "prod_staging" {
		t.Errorf("name field = %v, want %q", m["name"], "prod_staging")
	}
}

func TestTenantCostEntry_HasNameField(t *testing.T) {
	entry := TenantCostEntry{
		AccountID: "42",
		ProjectID: "3",
		Name:      "prod_staging",
	}
	data, _ := json.Marshal(entry)
	var m map[string]any
	_ = json.Unmarshal(data, &m)

	if m["name"] != "prod_staging" {
		t.Errorf("name field = %v, want %q", m["name"], "prod_staging")
	}
}

func TestTenantDetail_AliasRoute(t *testing.T) {
	resolver := tenant.NewResolver(tenant.ResolverConfig{})
	resolver.AddAlias("prod_staging", tenant.TenantID{AccountID: 42, ProjectID: 3})

	registry := NewTenantRegistry("test-node")
	registry.RecordWrite("42:3", 1000, 2000, 10, "STANDARD")

	api := NewAPI(APIConfig{
		Registry: registry,
		Resolver: resolver,
		Mode:     "logs",
		Bucket:   "test-bucket",
	})

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/prod_staging", nil)
	rr := httptest.NewRecorder()

	mux := http.NewServeMux()
	api.Register(mux)
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}

	var resp TenantDetailResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Name != "prod_staging" {
		t.Errorf("name = %q, want %q", resp.Name, "prod_staging")
	}
	if resp.AccountID != "42" {
		t.Errorf("account_id = %q, want %q", resp.AccountID, "42")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/stats/ -run TestTenantEntry_HasNameField -v`
Expected: FAIL — `TenantEntry` has no `Name` field.

- [ ] **Step 3: Modify stats API**

In `internal/stats/api.go`, add `Name` field to the three structs:

Line 63, add after `ProjectID`:
```go
type TenantEntry struct {
	AccountID        string           `json:"account_id"`
	ProjectID        string           `json:"project_id"`
	Name             string           `json:"name,omitempty"`
	// ... rest unchanged
}
```

Line 137, add after `ProjectID`:
```go
type TenantCostEntry struct {
	AccountID  string  `json:"account_id"`
	ProjectID  string  `json:"project_id"`
	Name       string  `json:"name,omitempty"`
	CostUSD    float64 `json:"cost_usd"`
	TotalBytes int64   `json:"total_bytes"`
}
```

Line 151, add after `ProjectID`:
```go
type TenantCompressionEntry struct {
	AccountID        string  `json:"account_id"`
	ProjectID        string  `json:"project_id"`
	Name             string  `json:"name,omitempty"`
	CompressionRatio float64 `json:"compression_ratio"`
	TotalBytes       int64   `json:"total_bytes"`
	RawBytes         int64   `json:"raw_bytes"`
}
```

Add `Resolver` field to `APIConfig` (line 17):
```go
type APIConfig struct {
	Registry        *TenantRegistry
	Manifest        *manifest.Manifest
	CostCalc        *CostCalculator
	ClassTracker    *StorageClassTracker
	LabelIndex      *cache.LabelIndex
	SchemaRegistry  *schema.Registry
	Resolver        *tenant.Resolver     // NEW
	Mode            string
	Bucket          string
	BloomColumns    []string
	BreakdownLabels []string
}
```

Add import for `"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"`.

In `tenantStatsToEntry` (line 908), add name resolution after building the entry:
```go
func tenantStatsToEntry(ts *TenantStats, cc *CostCalculator, resolver *tenant.TenantResolver) TenantEntry {
	// ... existing code builds entry ...
	// After entry is built, before return, add:
	// name resolution handled by caller via decorateName
	return entry
}
```

Actually, cleaner approach — add a helper that decorates entries:

```go
func (a *API) decorateName(entry *TenantEntry) {
	if a.cfg.Resolver == nil {
		return
	}
	accID, _ := strconv.ParseUint(entry.AccountID, 10, 32)
	projID, _ := strconv.ParseUint(entry.ProjectID, 10, 32)
	name := a.cfg.Resolver.DisplayName(uint32(accID), uint32(projID))
	if name != entry.AccountID+":"+entry.ProjectID {
		entry.Name = name
	}
}
```

Call `a.decorateName(&entry)` in `handleTenants` loop (line 222) and in `handleTenantDetail` (after building entry).

For alias URL routing in `handleTenantDetail` (line 279), modify the path parsing:

```go
func (a *API) handleTenantDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/lakehouse/api/v1/tenants/")
	
	var accountID, projectID string
	
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		// Classic format: /tenants/42/7
		accountID = parts[0]
		projectID = parts[1]
	} else if a.cfg.Resolver != nil && trimmed != "" && !strings.Contains(trimmed, "/") {
		// Alias format: /tenants/prod_staging
		tid, ok := a.cfg.Resolver.Resolve(trimmed)
		if !ok {
			http.Error(w, `{"error":"unknown tenant alias"}`, http.StatusNotFound)
			return
		}
		accountID = strconv.FormatUint(uint64(tid.AccountID), 10)
		projectID = strconv.FormatUint(uint64(tid.ProjectID), 10)
	} else {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	tenantKey := accountID + ":" + projectID
	// ... rest of handler unchanged
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/stats/ -run TestTenant -v`
Expected: PASS

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./...`
Expected: compiles.

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/stats/api.go internal/stats/api_tenant_name_test.go
git commit -m "feat(tenant): add name field to stats API responses and alias URL routing"
```

---

### Task 8: Template-Aware TenantSummaries

**Files:**
- Modify: `internal/manifest/manifest.go:488-548`

- [ ] **Step 1: Write the failing test**

Create `internal/manifest/tenant_template_test.go`:

```go
package manifest

import "testing"

func TestTenantSummaries_IntegerTemplate(t *testing.T) {
	m := New("test-bucket", "42/3/logs/")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	m.AddFile("dt=2026-05-15/hour=14", FileInfo{
		Key:  "42/3/logs/dt=2026-05-15/hour=14/abc.parquet",
		Size: 1000,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("got %d summaries, want 1", len(summaries))
	}
	if summaries[0].AccountID != "42" || summaries[0].ProjectID != "3" {
		t.Errorf("tenant = %s/%s, want 42/3", summaries[0].AccountID, summaries[0].ProjectID)
	}
}

func TestTenantSummaries_OrgIDTemplate(t *testing.T) {
	m := New("test-bucket", "prod-team-eu_staging/logs/")
	m.SetPrefixTemplate("{OrgID}/")

	m.AddFile("dt=2026-05-15/hour=14", FileInfo{
		Key:  "prod-team-eu_staging/logs/dt=2026-05-15/hour=14/abc.parquet",
		Size: 2000,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("got %d summaries, want 1", len(summaries))
	}
	if summaries[0].AccountID != "prod-team-eu_staging" {
		t.Errorf("AccountID = %q, want %q", summaries[0].AccountID, "prod-team-eu_staging")
	}
	if summaries[0].ProjectID != "" {
		t.Errorf("ProjectID = %q, want empty for OrgID template", summaries[0].ProjectID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/manifest/ -run TestTenantSummaries_ -v`
Expected: FAIL — `SetPrefixTemplate` not defined.

- [ ] **Step 3: Modify manifest.go**

Add `prefixTemplate` field and setter to the `Manifest` struct:

```go
// In the Manifest struct, add:
prefixTemplate string
```

```go
func (m *Manifest) SetPrefixTemplate(tmpl string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prefixTemplate = tmpl
}
```

Modify `TenantSummaries()` to be template-aware:

```go
func (m *Manifest) TenantSummaries() []TenantSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type key struct{ account, project string }
	type accum struct {
		files      int
		bytes      int64
		partitions map[string]struct{}
		minT, maxT time.Time
	}

	segments := 2 // default: {AccountID}/{ProjectID}
	hasOrgID := strings.Contains(m.prefixTemplate, "{OrgID}")
	if hasOrgID && !strings.Contains(m.prefixTemplate, "{ProjectID}") {
		segments = 1
	}

	byTenant := make(map[key]*accum)

	for partition, files := range m.files {
		for _, fi := range files {
			parts := strings.SplitN(fi.Key, "/", segments+2) // +signal+rest
			if len(parts) < segments+1 {
				continue
			}

			var k key
			if segments == 1 {
				k = key{account: parts[0], project: ""}
			} else {
				k = key{account: parts[0], project: parts[1]}
			}

			a, ok := byTenant[k]
			if !ok {
				a = &accum{partitions: make(map[string]struct{})}
				byTenant[k] = a
			}
			a.files++
			a.bytes += fi.Size
			a.partitions[partition] = struct{}{}

			if t, err := parsePartitionTime(partition); err == nil {
				if a.minT.IsZero() || t.Before(a.minT) {
					a.minT = t
				}
				end := t.Add(time.Hour)
				if a.maxT.IsZero() || end.After(a.maxT) {
					a.maxT = end
				}
			}
		}
	}

	result := make([]TenantSummary, 0, len(byTenant))
	for k, a := range byTenant {
		result = append(result, TenantSummary{
			AccountID:  k.account,
			ProjectID:  k.project,
			TotalFiles: a.files,
			TotalBytes: a.bytes,
			Partitions: len(a.partitions),
			MinTime:    a.minT,
			MaxTime:    a.maxT,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalBytes > result[j].TotalBytes
	})
	return result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/manifest/ -run TestTenantSummaries -v`
Expected: PASS

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/manifest/ -v`
Expected: All existing manifest tests still pass.

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/manifest/manifest.go internal/manifest/tenant_template_test.go
git commit -m "feat(tenant): template-aware TenantSummaries parsing for OrgID prefix"
```

---

### Task 9: Wire into Main Binary

**Files:**
- Modify: `cmd/lakehouse-logs/main.go:74-85,352-369,433-519`

- [ ] **Step 1: Add new CLI flags**

After line 84 (`tenantDefaultPrefix`), add:

```go
tenantOrgIDHeader     = flag.String("lakehouse.tenant.orgid-header", "", "HTTP header for string tenant ID (default: X-Scope-OrgID)")
tenantMetricsFormat   = flag.String("lakehouse.tenant.metrics-format", "", "Prometheus tenant label format: id, name, both (default: id)")
tenantAutoRegister    = flag.Bool("lakehouse.tenant.auto-register", false, "Auto-register unknown X-Scope-OrgID tenants")
```

- [ ] **Step 2: Add import and build resolver in `run()`**

Add import: `"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"`

After `applyFlags(cfg)` and config validation, before the `run()` function proceeds, build the resolver:

```go
// Build tenant resolver
resolverCfg := tenant.ResolverConfig{
	MetricsFormat: tenant.ParseMetricsFormat(cfg.Tenant.MetricsFormat),
	AutoRegister:  cfg.Tenant.AutoRegister,
	OrgIDHeader:   cfg.Tenant.OrgIDHeader,
}
resolver := tenant.NewResolver(resolverCfg)

// Load static aliases from config
for orgID, target := range cfg.Tenant.Aliases {
	if err := resolver.AddAlias(orgID, tenant.TenantID{
		AccountID: target.AccountID,
		ProjectID: target.ProjectID,
	}); err != nil {
		logger.Warnf("invalid tenant alias %q: %s", orgID, err)
	}
}

// Load runtime aliases from S3
var persister *tenant.S3Persister
```

After S3 pool is available (after `store` is created):

```go
persister = tenant.NewS3Persister(store.Pool(), cfg.AutoPrefix()+"_meta/tenant-aliases.json")
s3Aliases, err := persister.LoadAliases()
if err != nil {
	logger.Warnf("failed to load tenant aliases from S3: %s", err)
} else {
	for _, ae := range s3Aliases {
		if _, exists := resolver.Resolve(ae.OrgID); !exists {
			_ = resolver.AddAlias(ae.OrgID, tenant.TenantID{
				AccountID: ae.AccountID,
				ProjectID: ae.ProjectID,
			})
		}
	}
	if len(s3Aliases) > 0 {
		logger.Infof("loaded %d tenant aliases from S3", len(s3Aliases))
	}
}

if resolver.HasAliases() {
	logger.Infof("tenant resolver active; aliases=%d, metrics_format=%s, auto_register=%v",
		len(resolver.AllAliases()), cfg.Tenant.MetricsFormat, cfg.Tenant.AutoRegister)
}
```

- [ ] **Step 3: Wire middleware into mux**

Pass `resolver` to `newMux()` and wrap the mux with middleware. Modify the `newMux` signature:

```go
func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager, tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector, registry *stats.TenantRegistry, cardLimiter *stats.CardinalityLimiter, classTracker *stats.StorageClassTracker, costCalc *stats.CostCalculator, resolver *tenant.TenantResolver, persister *tenant.S3Persister) *http.ServeMux {
```

Inside `newMux`, register the alias handler:

```go
// Tenant alias handler
if resolver != nil {
	aliasHandler := tenant.NewHandler(resolver, persister)
	aliasHandler.Register(mux)
}
```

Pass `Resolver: resolver` to `stats.APIConfig`:

```go
statsAPI := stats.NewAPI(stats.APIConfig{
	Registry:        registry,
	Manifest:        m,
	CostCalc:        costCalc,
	ClassTracker:    classTracker,
	Resolver:        resolver,
	// ... other fields
})
```

In the `requestHandler`, wrap with middleware:

```go
var handler http.Handler = mux
if resolver != nil && resolver.HasAliases() {
	handler = resolver.Middleware(mux)
}

requestHandler := func(w http.ResponseWriter, r *http.Request) bool {
	handler.ServeHTTP(w, r)
	return true
}
```

- [ ] **Step 4: Update metrics loop to use resolver**

In the Prometheus metrics update loop (line 352-369), replace the tenant key formatting:

```go
go func() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, ts := range registry.All() {
				accID, _ := strconv.ParseUint(ts.AccountID, 10, 32)
				projID, _ := strconv.ParseUint(ts.ProjectID, 10, 32)
				key := resolver.MetricLabel(uint32(accID), uint32(projID))
				metrics.TenantFiles.Set(key, ts.TotalFiles)
				metrics.TenantBytes.Set(key, ts.TotalBytes)
				metrics.TenantRawBytes.Set(key, ts.RawBytes)
			}
		case <-stopCh:
			return
		}
	}
}()
```

Add import for `"strconv"` if not already present.

- [ ] **Step 5: Update applyFlags to pass new flag values to config**

In the `applyFlags` function, add:

```go
if *tenantOrgIDHeader != "" {
	cfg.Tenant.OrgIDHeader = *tenantOrgIDHeader
}
if *tenantMetricsFormat != "" {
	cfg.Tenant.MetricsFormat = *tenantMetricsFormat
}
if *tenantAutoRegister {
	cfg.Tenant.AutoRegister = true
}
```

- [ ] **Step 6: Verify build**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./cmd/lakehouse-logs/`
Expected: compiles.

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./... -count=1`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add cmd/lakehouse-logs/main.go
git commit -m "feat(tenant): wire resolver, middleware, and alias handlers into lakehouse-logs"
```

---

### Task 10: Helm Values Update

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml:296-323`

- [ ] **Step 1: Update the tenant section in values.yaml**

After the existing tenant fields (line 323), add the new alias fields:

```yaml
  tenant:
    # -- Static S3 prefix override.
    default_prefix: ""
    # -- S3 prefix template. Supports {AccountID}, {ProjectID}, and {OrgID} variables.
    # -- {AccountID}/{ProjectID}/  — VL/VT compatible, two integer segments (default).
    # -- {OrgID}/                  — standalone Lakehouse, single string segment.
    # -- {OrgID}/{ProjectID}/      — hybrid, string org with integer project.
    # -- {OrgID} templates require at least one alias or auto_register: true.
    # -- {OrgID} templates are not compatible with VL/VT upstream storage node mode.
    prefix_template: "{AccountID}/{ProjectID}/"
    # -- Tenant isolation mode: "prefix" (shared bucket, path separation) or "bucket" (separate S3 bucket per tenant).
    isolation: prefix
    # -- Bucket name template for bucket isolation mode (e.g., "obs-{AccountID}-{ProjectID}").
    bucket_template: ""
    # -- Default AccountID when no tenant header is present.
    default_account: "0"
    # -- Default ProjectID when no tenant header is present.
    default_project: "0"
    # -- HTTP header name for AccountID extraction (VL/VT native).
    header_account: X-Scope-AccountID
    # -- HTTP header name for ProjectID extraction (VL/VT native).
    header_project: X-Scope-ProjectID
    # -- HTTP header name for global (cross-tenant) read access.
    global_read_header: ""
    # -- Required header value for global read access.
    global_read_value: ""
    # -- Bearer token for global read access via Authorization header.
    global_read_token: ""
    # -- Known tenants for bucket-isolation mode, per-tenant lifecycle rules, and pricing.
    known_tenants: []

    # -- HTTP header name for Loki/Tempo-compatible string tenant ID.
    # -- When this header is present on a request, the alias map resolves it to integer IDs.
    # -- Allowed charset: a-z A-Z 0-9 ! - _ . * ' ( )  — max 150 bytes.
    orgid_header: X-Scope-OrgID
    # -- Prometheus tenant label format.
    # -- "id"   — tenant="42:3"                (default, zero change from current behavior).
    # -- "name" — tenant="prod-team-eu_staging" (friendly name, falls back to "42:3" if no alias).
    # -- "both" — tenant="42:3" + tenant_name="prod-team-eu_staging" (extra label, doubles cardinality).
    metrics_format: "id"
    # -- Auto-register unknown X-Scope-OrgID values. When true, first request from an unknown
    # -- org creates a mapping automatically (assigned sequential integer IDs).
    auto_register: false
    # -- Fleet sync interval for runtime aliases between peer nodes.
    alias_sync_interval: 30s
    # -- Static alias mappings. Map string names to VL/VT integer tenant IDs.
    # -- These are loaded at startup and cannot be overridden by runtime API.
    # -- Use underscore as compound separator: org_project.
    # -- Example:
    # --   aliases:
    # --     prod-team-eu_staging:
    # --       account_id: 42
    # --       project_id: 3
    # --     dev_default:
    # --       account_id: 1
    # --       project_id: 1
    aliases: {}
```

- [ ] **Step 2: Update values.schema.json**

Add the new tenant properties to the schema:

```json
"orgid_header": { "type": "string" },
"metrics_format": { "type": "string", "enum": ["id", "name", "both"] },
"auto_register": { "type": "boolean" },
"alias_sync_interval": { "type": "string" },
"aliases": { "type": "object" }
```

- [ ] **Step 3: Validate**

Run: `cd /private/tmp/victoria-lakehouse-fresh && /opt/homebrew/bin/helm lint charts/victoria-lakehouse/`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add charts/victoria-lakehouse/values.yaml charts/victoria-lakehouse/values.schema.json
git commit -m "feat(helm): add tenant alias configuration to values.yaml"
```

---

### Task 11: Integration Test

**Files:**
- Create: `internal/tenant/integration_test.go`

- [ ] **Step 1: Write integration test**

Create `internal/tenant/integration_test.go`:

```go
package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIntegration_FullRoundTrip(t *testing.T) {
	// 1. Create resolver with static alias
	r := NewResolver(ResolverConfig{
		MetricsFormat: MetricsFormatName,
	})
	r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

	// 2. Middleware translates X-Scope-OrgID to integer headers
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

	// 3. Reverse: DisplayName returns alias
	name := r.DisplayName(42, 3)
	if name != "prod-team-eu_staging" {
		t.Errorf("DisplayName = %q, want %q", name, "prod-team-eu_staging")
	}

	// 4. Metric label uses name format
	label := r.MetricLabel(42, 3)
	if label != "prod-team-eu_staging" {
		t.Errorf("MetricLabel = %q, want %q", label, "prod-team-eu_staging")
	}

	// 5. CRUD: add alias via handler
	h := NewHandler(r, nil)
	body, _ := json.Marshal(AliasEntry{OrgID: "dev_default", AccountID: 1, ProjectID: 1})
	createReq := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	createRR := httptest.NewRecorder()
	h.handleAliases(createRR, createReq)

	if createRR.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", createRR.Code)
	}

	// 6. Verify new alias resolves
	tid, ok := r.Resolve("dev_default")
	if !ok || tid.AccountID != 1 || tid.ProjectID != 1 {
		t.Errorf("new alias resolve = %+v, %v", tid, ok)
	}

	// 7. List aliases
	listReq := httptest.NewRequest("GET", "/lakehouse/api/v1/tenants/aliases", nil)
	listRR := httptest.NewRecorder()
	h.handleAliases(listRR, listReq)

	var listResp AliasListResponse
	_ = json.Unmarshal(listRR.Body.Bytes(), &listResp)
	if len(listResp.Aliases) != 2 {
		t.Errorf("list returned %d aliases, want 2", len(listResp.Aliases))
	}

	// 8. Delete alias
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

	// All valid Loki/Tempo characters
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
		r.RemoveAlias(alias) // cleanup for next iteration
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
```

- [ ] **Step 2: Run integration test**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/tenant/ -run TestIntegration -v`
Expected: PASS

- [ ] **Step 3: Run full test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./... -count=1`
Expected: All tests pass (no regressions).

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/tenant/integration_test.go
git commit -m "test(tenant): add integration tests for full alias round-trip"
```

---

## Verification Checklist

After all tasks are complete, verify:

1. `GOWORK=off go test ./internal/tenant/ -v` — all tenant package tests pass
2. `GOWORK=off go test ./internal/stats/ -v` — stats API tests pass with name field
3. `GOWORK=off go test ./internal/manifest/ -v` — template-aware parsing tests pass
4. `GOWORK=off go build ./cmd/lakehouse-logs/` — binary compiles
5. `GOWORK=off go test ./... -count=1` — full suite, no regressions
6. `helm lint charts/victoria-lakehouse/` — chart validates
7. Manual: start lakehouse-logs, send request with `X-Scope-OrgID: test_alias` configured in config, verify integer headers reach VL handler
