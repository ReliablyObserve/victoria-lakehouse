package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// The wrapper leaves every time argument as it received it: upstream reads an integer as seconds,
// milliseconds, microseconds or nanoseconds by its magnitude, to the unit's own precision. The
// wrapper used to divide an integer above 1e12 by 1000 before upstream saw it, which cut a
// nanosecond bound to microseconds and a millisecond bound to seconds.
func TestWrapVL_LeavesTheTimeArgumentsAlone(t *testing.T) {
	h := NewHandler(mockStore{}, testConfig(config.ModeTraces))
	var got url.Values
	wrapped := h.wrapVL(func(_ context.Context, _ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.Form
	})
	const q = "start=1790838001234567123&end=1790838002345678456&time=1790838001234&query=*"
	wrapped(httptest.NewRecorder(), httptest.NewRequest("GET", "/select/logsql/hits?"+q, nil))
	want, _ := url.ParseQuery(q)
	for k, v := range want {
		if got.Get(k) != v[0] {
			t.Errorf("%s = %q, want %q", k, got.Get(k), v[0])
		}
	}
}
