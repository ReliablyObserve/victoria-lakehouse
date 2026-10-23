package selectapi

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// query_deadline.go copies VictoriaTraces' getMaxQueryDuration and the way
// vtselect.RequestHandler applies it to the Jaeger and Tempo routes, because the
// traces binary cannot link vtselect (see internal_delete.go in the main
// package). Fail the moment the vendored source stops doing the same.

const vendoredVTSelect = "../../deps/VictoriaTraces/app/vtselect/main.go"

// normalise folds the one deliberate difference: the flag value is a parameter.
func normalise(fn string) string {
	fn = strings.ReplaceAll(fn, "*maxQueryDuration", "max")
	fn = strings.Replace(fn, "func getMaxQueryDuration(r *http.Request)", "func getMaxQueryDuration(r *http.Request, max time.Duration)", 1)
	// drop the doc comment and blank lines: only code is compared
	var out []string
	for _, l := range strings.Split(fn, "\n") {
		if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "//") {
			out = append(out, t)
		}
	}
	return strings.Join(out, "\n")
}

func TestGetMaxQueryDuration_MatchesVendoredVTSelect(t *testing.T) {
	getMaxQueryDurationFunc := regexp.MustCompile(`(?s)func getMaxQueryDuration\([^)]*\) \(time\.Duration, error\) \{.*?\n\}\n`)
	up, err := os.ReadFile(vendoredVTSelect)
	if err != nil {
		t.Fatalf("vendored VictoriaTraces source missing (run make deps-vt): %v", err)
	}
	mine, err := os.ReadFile("query_deadline.go")
	if err != nil {
		t.Fatal(err)
	}
	upFn := getMaxQueryDurationFunc.Find(up)
	myFn := getMaxQueryDurationFunc.Find(mine)
	if upFn == nil || myFn == nil {
		t.Fatalf("getMaxQueryDuration not found (vendored: %v, copy: %v)", upFn != nil, myFn != nil)
	}
	if a, b := normalise(string(upFn)), normalise(string(myFn)); a != b {
		t.Fatalf("getMaxQueryDuration drifted from the vendored vtselect/main.go\nvendored:\n%s\ncopy:\n%s", a, b)
	}
}

// vtselect.RequestHandler derives ONE context from the request's under the
// deadline and hands it to the Jaeger and the Tempo handler; a refused 'timeout'
// is httpserver.Errorf. The copy in handler.go (queryContext) must keep doing so.
func TestQueryContext_MatchesVendoredVTSelectRequestHandler(t *testing.T) {
	up, err := os.ReadFile(vendoredVTSelect)
	if err != nil {
		t.Fatalf("vendored VictoriaTraces source missing (run make deps-vt): %v", err)
	}
	src := string(up)
	for _, want := range []string{
		"d, err := getMaxQueryDuration(r)\n\tif err != nil {\n\t\thttpserver.Errorf(w, r, \"%s\", err)\n\t\treturn true\n\t}",
		"ctxWithTimeout, cancel := context.WithTimeout(ctx, d)\n\tdefer cancel()",
		"return jaeger.RequestHandler(ctxWithTimeout, w, r)",
		"return tempo.RequestHandler(ctxWithTimeout, w, r)",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("vendored vtselect.RequestHandler no longer contains\n%s\nre-check queryContext in query_deadline.go", want)
		}
	}
	// The tail route is the one exemption, as in upstream: no deadline on live tailing.
	if !strings.Contains(src, `if path == "/select/logsql/tail" {`) {
		t.Error("vendored vtselect.RequestHandler no longer special-cases /select/logsql/tail")
	}
	// The flag: name, default and help are held by TestSearchMaxQueryDuration_MatchesVendoredVTSelect
	// in the main package, where it is registered.
}
