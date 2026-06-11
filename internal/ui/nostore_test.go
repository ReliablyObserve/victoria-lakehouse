package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNoCache_SetsNoStore locks that the embedded UI is served no-store, so a
// reload always runs the freshly-deployed JS (tab fixes etc. take effect without
// a hard refresh).
func TestNoCache_SetsNoStore(t *testing.T) {
	h := noCache(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/lakehouse/ui/", nil))
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control = %q, want to contain no-store", cc)
	}
}
