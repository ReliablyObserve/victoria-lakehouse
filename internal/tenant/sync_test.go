package tenant

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSyncHandler_MergesAliases(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	sh := NewSyncHandler(r, "")

	delta := AliasDelta{
		NodeID: "peer-1",
		Aliases: []AliasEntry{
			{OrgID: "prod_staging", AccountID: 42, ProjectID: 3},
			{OrgID: "dev_default", AccountID: 1, ProjectID: 1},
		},
	}
	body, _ := json.Marshal(delta)

	req := httptest.NewRequest("POST", "/internal/tenant/sync", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	tid, ok := r.Resolve("prod_staging")
	if !ok {
		t.Fatal("expected prod_staging to be resolvable after sync")
	}
	if tid.AccountID != 42 || tid.ProjectID != 3 {
		t.Errorf("got %+v, want {42, 3}", tid)
	}

	_, ok = r.Resolve("dev_default")
	if !ok {
		t.Fatal("expected dev_default to be resolvable after sync")
	}
}

func TestSyncHandler_NoOverwriteExisting(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("prod_staging", TenantID{AccountID: 42, ProjectID: 3})

	sh := NewSyncHandler(r, "")

	delta := AliasDelta{
		NodeID: "peer-1",
		Aliases: []AliasEntry{
			{OrgID: "prod_staging", AccountID: 99, ProjectID: 99},
		},
	}
	body, _ := json.Marshal(delta)

	req := httptest.NewRequest("POST", "/internal/tenant/sync", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	tid, _ := r.Resolve("prod_staging")
	if tid.AccountID != 42 {
		t.Errorf("existing alias was overwritten: got %+v", tid)
	}
}

func TestSyncHandler_AuthRequired(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	sh := NewSyncHandler(r, "secret-key")

	body := `{"node_id":"peer","aliases":[]}`
	req := httptest.NewRequest("POST", "/internal/tenant/sync", strings.NewReader(body))
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 without auth", rr.Code)
	}

	req2 := httptest.NewRequest("POST", "/internal/tenant/sync", strings.NewReader(body))
	req2.Header.Set("Authorization", "Bearer secret-key")
	rr2 := httptest.NewRecorder()
	sh.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 with auth", rr2.Code)
	}
}

func TestSyncPusher_DeltaDetection(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("prod_staging", TenantID{AccountID: 42, ProjectID: 3})

	var pushCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		pushCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")

	sp := NewSyncPusher(SyncPusherConfig{
		Resolver: r,
		GetPeers: func() []string { return []string{peer} },
		SelfAddr: "self:9428",
	})

	sp.Push(nil)
	if pushCount != 1 {
		t.Errorf("first push: count=%d, want 1", pushCount)
	}

	sp.Push(nil)
	if pushCount != 1 {
		t.Errorf("second push (no changes): count=%d, want 1", pushCount)
	}

	r.AddAlias("dev_default", TenantID{AccountID: 1, ProjectID: 1})
	sp.Push(nil)
	if pushCount != 2 {
		t.Errorf("third push (new alias): count=%d, want 2", pushCount)
	}
}
