package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// recordingStore captures the tenant list and the global-read marker each
// handler passed down, so the tests can assert the scope a request was answered
// with instead of guessing from the response body.
type recordingStore struct {
	mockStore

	mu         sync.Mutex
	tenantIDs  [][]logstorage.TenantID
	globalRead []bool
}

func (r *recordingStore) record(ctx context.Context, ids []logstorage.TenantID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenantIDs = append(r.tenantIDs, ids)
	r.globalRead = append(r.globalRead, storage.IsGlobalRead(ctx))
}

func (r *recordingStore) calls() ([][]logstorage.TenantID, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tenantIDs, r.globalRead
}

func (r *recordingStore) GetFieldValues(ctx context.Context, ids []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	r.record(ctx, ids)
	return nil, nil
}

func (r *recordingStore) RunQuery(ctx context.Context, ids []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	r.record(ctx, ids)
	return nil
}

func tracesHandler(t *testing.T, store storage.Storage, mutate func(*config.Config)) *Handler {
	t.Helper()
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	if mutate != nil {
		mutate(cfg)
	}
	return NewHandler(store, cfg)
}

// The Jaeger handlers used to pass a nil tenant list, which the storage layer
// read as "no scope" and answered from every tenant's data. They must derive
// the tenant from the request exactly like every other select endpoint.
func TestJaeger_DerivesTenantFromRequest(t *testing.T) {
	paths := []struct {
		name    string
		path    string
		headers map[string]string
		want    logstorage.TenantID
	}{
		{"services, no headers", "/select/jaeger/api/services", nil, logstorage.TenantID{}},
		{"services, scoped", "/select/jaeger/api/services", map[string]string{"AccountID": "1001", "ProjectID": "0"}, logstorage.TenantID{AccountID: 1001}},
		{"operations, scoped", "/select/jaeger/api/services/svc/operations", map[string]string{"AccountID": "2002", "ProjectID": "7"}, logstorage.TenantID{AccountID: 2002, ProjectID: 7}},
		{"trace, scoped", "/select/jaeger/api/traces/abc", map[string]string{"AccountID": "1001", "ProjectID": "3"}, logstorage.TenantID{AccountID: 1001, ProjectID: 3}},
		{"search, scoped", "/select/jaeger/api/traces?service=svc", map[string]string{"AccountID": "5", "ProjectID": "0"}, logstorage.TenantID{AccountID: 5}},
	}

	for _, tc := range paths {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingStore{}
			h := tracesHandler(t, store, nil)
			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			mux.ServeHTTP(httptest.NewRecorder(), req)

			ids, _ := store.calls()
			if len(ids) == 0 {
				t.Fatal("handler never reached the storage layer")
			}
			for _, got := range ids {
				if len(got) != 1 {
					t.Fatalf("handler passed %d tenants, want exactly 1 (a nil list reads as unscoped)", len(got))
				}
				if got[0] != tc.want {
					t.Errorf("handler passed tenant %+v, want %+v", got[0], tc.want)
				}
			}
		})
	}
}

func TestJaeger_RejectsMalformedTenantHeader(t *testing.T) {
	store := &recordingStore{}
	h := tracesHandler(t, store, nil)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/select/jaeger/api/services", nil)
	req.Header.Set("AccountID", "not-a-number")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 — a malformed tenant header must be rejected, not silently downgraded to 0:0", rec.Code)
	}
	if ids, _ := store.calls(); len(ids) != 0 {
		t.Error("a request with a malformed tenant header must not reach the storage layer")
	}
}

// A query is answered across tenants only when the configured global-read
// credential validates. No credential configured, a wrong value, or a missing
// header all keep the request on its own tenant.
func TestGlobalRead_WidensOnlyWithTheCredential(t *testing.T) {
	withCredential := func(cfg *config.Config) {
		cfg.Tenant.GlobalReadHeader = "X-Lakehouse-Global-Read"
		cfg.Tenant.GlobalReadValue = "s3cr3t"
		cfg.Tenant.GlobalReadToken = "t0ken"
	}

	cases := []struct {
		name    string
		mutate  func(*config.Config)
		headers map[string]string
		want    bool
	}{
		{"not configured, header present anyway", nil, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, false},
		{"configured, no header", withCredential, nil, false},
		{"configured, wrong value", withCredential, map[string]string{"X-Lakehouse-Global-Read": "nope"}, false},
		{"configured, empty value", withCredential, map[string]string{"X-Lakehouse-Global-Read": ""}, false},
		{"configured, correct header", withCredential, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, true},
		{"configured, correct bearer", withCredential, map[string]string{"Authorization": "Bearer t0ken"}, true},
		{"configured, wrong bearer", withCredential, map[string]string{"Authorization": "Bearer nope"}, false},
		{"configured, bearer without prefix", withCredential, map[string]string{"Authorization": "t0ken"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingStore{}
			h := tracesHandler(t, store, tc.mutate)
			mux := http.NewServeMux()
			h.Register(mux)

			req := httptest.NewRequest(http.MethodGet, "/select/jaeger/api/services", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			mux.ServeHTTP(httptest.NewRecorder(), req)

			_, global := store.calls()
			if len(global) == 0 {
				t.Fatal("handler never reached the storage layer")
			}
			if global[0] != tc.want {
				t.Errorf("global read = %v, want %v", global[0], tc.want)
			}
		})
	}
}
