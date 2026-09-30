package main

import (
	"flag"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/searchutil"
)

// VictoriaTraces v0.12.0 applies -search.allowPartialResponse (and the
// allow_partial_response argument) to the Tempo and Jaeger query APIs too;
// before, it only reached LogsQL. This binary links VictoriaLogs' LogsQL
// handlers, which register the same flag, so vtselect-flag-dedup.patch makes
// VictoriaTraces read that one flag at call time instead of registering its
// own (a second registration panics at init). It has no peer fan-out to
// change: Lakehouse's own fan-out (the buffer bridge, peer cache) does not
// take the flag, and Tempo/Jaeger reach storage through the Lakehouse adapter,
// not VictoriaTraces' netselect.
func TestAllowPartialResponse_FlagReachesTempoAndJaeger(t *testing.T) {
	f := flag.Lookup("search.allowPartialResponse")
	if f == nil {
		t.Fatal("-search.allowPartialResponse is not registered: the flag the VictoriaLogs LogsQL handlers register is what vtselect's searchutil reads, so it must exist")
	}
	old := f.Value.String()
	t.Cleanup(func() { _ = flag.Set("search.allowPartialResponse", old) })

	get := func(query string) (bool, error) {
		return searchutil.GetAllowPartialResponse(httptest.NewRequest("GET", "/select/tempo/api/search?"+query, nil))
	}

	if got, err := get(""); err != nil || got {
		t.Fatalf("default: got (%v, %v), want (false, nil)", got, err)
	}
	if err := flag.Set("search.allowPartialResponse", "true"); err != nil {
		t.Fatal(err)
	}
	if got, err := get(""); err != nil || !got {
		t.Errorf("flag set: got (%v, %v), want (true, nil): the command-line flag must reach the Tempo and Jaeger handlers at call time", got, err)
	}
	if got, err := get("allow_partial_response=false"); err != nil || got {
		t.Errorf("argument overrides the flag: got (%v, %v), want (false, nil)", got, err)
	}
	if err := flag.Set("search.allowPartialResponse", "false"); err != nil {
		t.Fatal(err)
	}
	if got, err := get("allow_partial_response=true"); err != nil || !got {
		t.Errorf("argument enables it: got (%v, %v), want (true, nil)", got, err)
	}
	if _, err := get("allow_partial_response=maybe"); err == nil {
		t.Error("a malformed allow_partial_response must be an error, as on VictoriaTraces")
	}
}
