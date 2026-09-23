package internaldelete

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(h http.HandlerFunc) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/internal/delete/run_task", nil))
	return rec
}

func upstreamStub(reached *bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusTeapot)
	}
}

func off() bool { return false }
func on() bool  { return true }

// While upstream's flag is off the request belongs to upstream, which answers
// its own "disabled" error — even when delete.enabled is also off, so the
// lakehouse never replaces upstream's answer with its own.
func TestHandler_FlagOffLeavesTheAnswerToUpstream(t *testing.T) {
	for _, deleteEnabled := range []bool{true, false} {
		reached := false
		rec := serve(Handler(off, deleteEnabled, upstreamStub(&reached)))
		if !reached || rec.Code != http.StatusTeapot {
			t.Fatalf("delete.enabled=%v: upstream reached=%v status=%d, want upstream to answer", deleteEnabled, reached, rec.Code)
		}
	}
}

func TestHandler_FlagOnButDeleteFeatureOffIsRefused(t *testing.T) {
	reached := false
	rec := serve(Handler(on, false, upstreamStub(&reached)))
	if reached {
		t.Fatal("upstream ran although delete.enabled is off")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got, want := rec.Body.String(), DeleteDisabledMessage+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHandler_FlagOnAndDeleteFeatureOnReachesUpstream(t *testing.T) {
	reached := false
	rec := serve(Handler(on, true, upstreamStub(&reached)))
	if !reached || rec.Code != http.StatusTeapot {
		t.Fatalf("upstream reached=%v status=%d, want upstream to answer", reached, rec.Code)
	}
}

func TestFlagEnabled_ReadsTheRegisteredFlag(t *testing.T) {
	if flag.Lookup(FlagName) == nil {
		if FlagEnabled() {
			t.Fatal("FlagEnabled() = true before the flag is registered")
		}
		flag.Bool(FlagName, false, "test registration")
	}
	if FlagEnabled() {
		t.Fatal("FlagEnabled() = true with the flag at its default")
	}
	if err := flag.Set(FlagName, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(FlagName, "false") })
	if !FlagEnabled() {
		t.Fatal("FlagEnabled() = false after -internaldelete.enable=true")
	}
}

// The public delete API (/delete/*) is gated the same way: upstream's
// -delete.enable first, then the lakehouse delete.enabled requirement.
func TestPublicHandler_GatesLikeHandler(t *testing.T) {
	for _, tc := range []struct {
		name          string
		flagOn        func() bool
		deleteEnabled bool
		wantUpstream  bool
	}{
		{"flag off, feature on", off, true, true},
		{"flag off, feature off", off, false, true},
		{"flag on, feature off", on, false, false},
		{"flag on, feature on", on, true, true},
	} {
		reached := false
		rec := serve(PublicHandler(tc.flagOn, tc.deleteEnabled, upstreamStub(&reached)))
		if reached != tc.wantUpstream {
			t.Fatalf("%s: upstream reached=%v, want %v", tc.name, reached, tc.wantUpstream)
		}
		if !tc.wantUpstream && rec.Body.String() != PublicDeleteDisabledMessage+"\n" {
			t.Fatalf("%s: body = %q, want the public delete.enabled refusal", tc.name, rec.Body.String())
		}
	}
}

func TestPublicFlagEnabled_ReadsTheRegisteredFlag(t *testing.T) {
	if PublicFlagEnabled() {
		t.Fatal("PublicFlagEnabled() = true before the flag is registered")
	}
	flag.Bool(PublicFlagName, false, "test registration")
	if PublicFlagEnabled() {
		t.Fatal("PublicFlagEnabled() = true with the flag at its default")
	}
	if err := flag.Set(PublicFlagName, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(PublicFlagName, "false") })
	if !PublicFlagEnabled() {
		t.Fatal("PublicFlagEnabled() = false after -delete.enable=true")
	}
}
