package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

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
			cfg := config.Default()
			cfg.Mode = config.ModeLogs
			if tc.mutate != nil {
				tc.mutate(cfg)
			}
			h := NewHandler(mockStore{}, cfg)

			called := false
			var global bool
			wrapped := h.wrapVL(func(ctx context.Context, _ http.ResponseWriter, _ *http.Request) {
				called = true
				global = storage.IsGlobalRead(ctx)
			})

			req := httptest.NewRequest(http.MethodGet, "/select/logsql/field_values", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			wrapped(httptest.NewRecorder(), req)

			if !called {
				t.Fatal("the wrapped handler never ran")
			}
			if global != tc.want {
				t.Errorf("global read = %v, want %v", global, tc.want)
			}
		})
	}
}
