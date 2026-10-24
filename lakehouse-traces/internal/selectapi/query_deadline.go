package selectapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

// DefaultMaxQueryDuration is the default of -search.maxQueryDuration, the same
// 30s VictoriaTraces defaults to (app/vtselect/main.go). The flag itself is
// registered by the traces binary's main package (search_max_query_duration.go),
// which hands its value to NewHandler through WithMaxQueryDuration.
const DefaultMaxQueryDuration = 30 * time.Second

// WithMaxQueryDuration sets the longest a Jaeger or Tempo request may run
// (-search.maxQueryDuration). A request may lower it with its 'timeout' arg,
// never raise it. The value is used as given, as upstream uses its flag: 0 or a
// negative value makes every request fail at once with the deadline error
// (context.WithTimeout of a duration that has already passed).
func WithMaxQueryDuration(d time.Duration) HandlerOption {
	return func(h *Handler) { h.maxQueryDuration = d }
}

// upstream-copy: VictoriaTraces app/vtselect/main.go getMaxQueryDuration, with
// the -search.maxQueryDuration flag value passed in as max
// (query_deadline_drift_test.go fails when the vendored function stops matching).
//
// getMaxQueryDuration returns the maximum duration for query from r.
func getMaxQueryDuration(r *http.Request, max time.Duration) (time.Duration, error) {
	s := r.FormValue("timeout")
	if s == "" {
		s = "0s"
	}
	nsecs, ok := logstorage.TryParseDuration(s)
	if !ok {
		return 0, fmt.Errorf("cannot parse duration at 'timeout=%s' arg", s)
	}
	d := time.Duration(nsecs)
	if d <= 0 || d > max {
		d = max
	}
	return d, nil
}

// queryContext derives the context a Jaeger or Tempo request runs under, as
// VictoriaTraces' vtselect.RequestHandler does for every non-tail request: the
// request's context under context.WithTimeout(getMaxQueryDuration(r)). An
// unparsable 'timeout' is refused with upstream's answer (httpserver.Errorf,
// status 400) and ok is false. The handlers read the deadline from the context
// only; the answer for a deadline that passed is theirs, not this wrapper's.
func (h *Handler) queryContext(w http.ResponseWriter, r *http.Request) (ctx context.Context, cancel context.CancelFunc, ok bool) {
	d, err := getMaxQueryDuration(r, h.maxQueryDuration)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return nil, nil, false
	}
	ctx, cancel = context.WithTimeout(h.scopeContext(r), d)
	return ctx, cancel, true
}
