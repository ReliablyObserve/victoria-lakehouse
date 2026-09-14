package selectapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// A select request is widened to every tenant only when the configured
// global-read credential validates; everything else keeps the request's own
// tenant. Twin of the logs module's selectapi tests.
func TestScopeContext_GlobalRead(t *testing.T) {
	configured := config.Default()
	configured.Mode = config.ModeTraces
	configured.Tenant.GlobalReadHeader = "X-Lakehouse-Global-Read"
	configured.Tenant.GlobalReadValue = "s3cr3t"
	configured.Tenant.GlobalReadToken = "t0ken"

	unconfigured := config.Default()
	unconfigured.Mode = config.ModeTraces

	cases := []struct {
		name    string
		cfg     *config.Config
		headers map[string]string
		want    bool
	}{
		{"configured, correct header", configured, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, true},
		{"configured, correct bearer", configured, map[string]string{"Authorization": "Bearer t0ken"}, true},
		{"configured, correct header with tenant headers", configured, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t", "AccountID": "5", "ProjectID": "0"}, true},
		{"configured, wrong header", configured, map[string]string{"X-Lakehouse-Global-Read": "nope"}, false},
		{"configured, wrong bearer", configured, map[string]string{"Authorization": "Bearer nope"}, false},
		{"configured, no credential", configured, nil, false},
		{"not configured, header present", unconfigured, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, false},
		{"not configured, empty bearer", unconfigured, map[string]string{"Authorization": "Bearer "}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(mockStore{}, tc.cfg)
			req := httptest.NewRequest(http.MethodGet, "/select/logsql/query", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			if got := storage.IsGlobalRead(h.scopeContext(req)); got != tc.want {
				t.Errorf("global read = %v, want %v", got, tc.want)
			}
		})
	}
}
