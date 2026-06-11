package stats

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWriteJSON_NoStore locks the cache fix: stats responses must carry no-store
// so the browser never renders cached-stale numbers (e.g. a pre-fix flat
// breakdown) after a reload.
func TestWriteJSON_NoStore(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, map[string]int{"x": 1})
	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control = %q, want to contain no-store", cc)
	}
}
