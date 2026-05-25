package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestSyncHandler_WrongMethod exercises the method check in ServeHTTP.
func TestSyncHandler_WrongMethod(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	sh := NewSyncHandler(r, "")

	req := httptest.NewRequest("GET", "/internal/tenant/sync", nil)
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405 for GET", rr.Code)
	}
}

// TestSyncHandler_BadJSON exercises the JSON decode error path.
func TestSyncHandler_BadJSON(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	sh := NewSyncHandler(r, "")

	req := httptest.NewRequest("POST", "/internal/tenant/sync", strings.NewReader("not-json{"))
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for bad JSON", rr.Code)
	}
}

// TestSyncHandler_EmptyBody exercises the read body error path.
func TestSyncHandler_EmptyBody(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	sh := NewSyncHandler(r, "")

	// Valid JSON but empty aliases.
	req := httptest.NewRequest("POST", "/internal/tenant/sync",
		strings.NewReader(`{"node_id":"peer","aliases":[]}`))
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

// TestSyncPusher_Start exercises the Start method with a goroutine and
// cancellation via context.
func TestSyncPusher_Start(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

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
		Interval: 50 * time.Millisecond, // short interval for testing
	})

	ctx, cancel := context.WithCancel(context.Background())
	sp.Start(ctx)

	// Wait for at least one push to happen.
	time.Sleep(150 * time.Millisecond)
	cancel()

	// Give goroutine time to exit.
	time.Sleep(50 * time.Millisecond)

	if pushCount < 1 {
		t.Errorf("expected at least 1 push from Start, got %d", pushCount)
	}
}

// TestSyncPusher_Push_NilContext exercises the nil context fallback.
func TestSyncPusher_Push_NilContext(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

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

	sp.Push(nil) //nolint:staticcheck // intentionally testing nil context

	if pushCount != 1 {
		t.Errorf("pushCount = %d, want 1", pushCount)
	}
}

// TestSyncPusher_Push_AuthHeader exercises the auth header path.
func TestSyncPusher_Push_AuthHeader(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

	var gotAuth string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotAuth = req.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	peer := strings.TrimPrefix(ts.URL, "http://")

	sp := NewSyncPusher(SyncPusherConfig{
		Resolver: r,
		GetPeers: func() []string { return []string{peer} },
		AuthKey:  "my-secret",
		SelfAddr: "self:9428",
	})

	sp.Push(context.TODO())

	if gotAuth != "Bearer my-secret" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer my-secret")
	}
}

// TestSyncPusher_Push_SkipsSelf exercises the self-skip path.
func TestSyncPusher_Push_SkipsSelf(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

	var pushCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		pushCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	selfAddr := strings.TrimPrefix(ts.URL, "http://")

	sp := NewSyncPusher(SyncPusherConfig{
		Resolver: r,
		GetPeers: func() []string { return []string{selfAddr} },
		SelfAddr: selfAddr,
	})

	sp.Push(context.TODO())

	if pushCount != 0 {
		t.Errorf("pushCount = %d, want 0 (self should be skipped)", pushCount)
	}
}

// TestSyncPusher_Push_NoChanges exercises the no-changes path.
func TestSyncPusher_Push_NoChanges(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

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

	// First push sends changes.
	sp.Push(context.TODO())
	if pushCount != 1 {
		t.Errorf("first push: pushCount = %d, want 1", pushCount)
	}

	// Second push with no changes should not push.
	sp.Push(context.TODO())
	if pushCount != 1 {
		t.Errorf("second push (no changes): pushCount = %d, want 1", pushCount)
	}
}

// TestSyncPusher_Push_UnreachablePeer exercises the error path.
func TestSyncPusher_Push_UnreachablePeer(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

	sp := NewSyncPusher(SyncPusherConfig{
		Resolver: r,
		GetPeers: func() []string { return []string{"192.0.2.1:9999"} }, // RFC 5737 TEST-NET
		SelfAddr: "self:9428",
	})

	// Should not panic.
	sp.Push(context.TODO())
}

// TestHandler_HandleAliases_MethodNotAllowed exercises the default case.
func TestHandler_HandleAliases_MethodNotAllowed(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	h := NewHandler(r, nil, "")

	req := httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases", nil)
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405 for DELETE on aliases endpoint", rr.Code)
	}
}

// TestHandler_CreateAlias_WithPersister exercises the persister path.
func TestHandler_CreateAlias_WithPersister(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	pool := &mockCoveragePool{}
	p := NewS3Persister(pool, "test-key")
	h := NewHandler(r, p, "")

	body, _ := json.Marshal(AliasEntry{OrgID: "with_persist", AccountID: 10, ProjectID: 20})
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rr.Code, rr.Body.String())
	}

	// Verify persister was called.
	if pool.data == nil {
		t.Error("expected persister to be called")
	}
}

// TestHandler_DeleteAlias_WithPersister exercises the persister path on delete.
func TestHandler_DeleteAlias_WithPersister(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("to_delete", TenantID{AccountID: 5, ProjectID: 6})
	pool := &mockCoveragePool{}
	p := NewS3Persister(pool, "test-key")
	h := NewHandler(r, p, "")

	req := httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases/to_delete", nil)
	rr := httptest.NewRecorder()
	h.handleAliasDelete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rr.Code)
	}

	if pool.data == nil {
		t.Error("expected persister to be called on delete")
	}
}

// TestHandler_CreateAlias_PersisterError exercises the persister error path.
func TestHandler_CreateAlias_PersisterError(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	pool := &mockCoveragePool{uploadErr: errors.New("S3 error")}
	p := NewS3Persister(pool, "test-key")
	h := NewHandler(r, p, "")

	body, _ := json.Marshal(AliasEntry{OrgID: "persist_err", AccountID: 10, ProjectID: 20})
	req := httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.handleAliases(rr, req)

	// Should still return 201 (the error is logged, not returned to client).
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 despite persister error; body=%s", rr.Code, rr.Body.String())
	}
}

// TestSaveAliases_MarshalError exercises the marshal error path which is
// difficult to trigger with valid data, so we verify the happy path works.
func TestSaveAliases_EmptyList(t *testing.T) {
	pool := &mockCoveragePool{}
	p := NewS3Persister(pool, "test-key")

	err := p.SaveAliases([]AliasEntry{})
	if err != nil {
		t.Fatalf("SaveAliases empty: %v", err)
	}
	if pool.data == nil {
		t.Error("expected data to be uploaded even for empty list")
	}
}

type mockCoveragePool struct {
	data      []byte
	uploadErr error
}

func (m *mockCoveragePool) Upload(_ context.Context, _ string, data []byte) error {
	if m.uploadErr != nil {
		return m.uploadErr
	}
	m.data = append([]byte(nil), data...)
	return nil
}

func (m *mockCoveragePool) Download(_ context.Context, _ string) ([]byte, error) {
	return m.data, nil
}
