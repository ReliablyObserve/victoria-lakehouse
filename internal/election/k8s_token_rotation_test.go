// internal/election/k8s_token_rotation_test.go
//
// PR #98 Item 1 — SA token rotation mid-election.
//
// kubelet rotates projected ServiceAccount tokens periodically (default
// ~1 hour with token projection). The K8sElector must re-read the token
// from disk on each API call (or at least before each call when
// bearerTokenFile is set) so it doesn't hit 401 after the first rotation.
//
// We can't move the wall clock forward an hour in a unit test, so we
// simulate token rotation by writing a new token to the on-disk path and
// asserting the elector picks it up on the next request. The negative
// control is "Authorization header still carries the OLD token after we
// rewrote the file" — if you remove the file-re-read in
// bearerTokenForRequest, this test fails.
package election

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestK8sElector_TokenRotation_ReReadsServiceAccountToken locks the token
// re-read behaviour: when bearerTokenFile is set, every call to
// bearerTokenForRequest must re-read the file and use the freshest token.
//
// Fails-without: the `os.ReadFile(e.bearerTokenFile)` inside
// bearerTokenForRequest. Remove that and replace with `return e.bearer`,
// and this test fails because the second request still carries the old
// "TOKEN-1" Authorization header after we wrote "TOKEN-2" to disk.
func TestK8sElector_TokenRotation_ReReadsServiceAccountToken(t *testing.T) {
	// Write the initial token to a temp file.
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("TOKEN-1"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Mini server that records the last Authorization header it saw.
	var mu sync.Mutex
	var lastAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	e := &K8sElector{
		cfg:             K8sElectorConfig{LeaseName: "rotation-test", LeaseNamespace: "default"},
		apiBase:         srv.URL,
		client:          srv.Client(),
		bearerTokenFile: tokenPath,
		clock:           realClock{},
	}

	// First call: should send TOKEN-1.
	_, _, err := e.doRequest(t.Context(), http.MethodGet, e.leaseURL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got1 := lastAuth
	mu.Unlock()
	if !strings.HasSuffix(got1, "TOKEN-1") {
		t.Fatalf("first call Authorization = %q, want suffix TOKEN-1", got1)
	}

	// Simulate kubelet rotation: write a new token to the same file.
	if err := os.WriteFile(tokenPath, []byte("TOKEN-2"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Second call: must pick up the new token. This is the critical
	// assertion — without bearerTokenForRequest's file re-read, this
	// would still send TOKEN-1.
	_, _, err = e.doRequest(t.Context(), http.MethodGet, e.leaseURL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got2 := lastAuth
	mu.Unlock()
	if !strings.HasSuffix(got2, "TOKEN-2") {
		t.Fatalf("after rotation Authorization = %q, want suffix TOKEN-2", got2)
	}
}

// TestK8sElector_TokenRotation_NoFile_UsesInMemory locks the fallback when
// bearerTokenFile is empty: the elector uses the cached BearerToken value
// directly. This preserves the test-injection path and any non-projected
// SA scenarios (e.g., long-lived SA tokens mounted as a static secret).
//
// Fails-without: the `if e.bearerTokenFile == "" { return e.bearer }` early
// return at the top of bearerTokenForRequest.
func TestK8sElector_TokenRotation_NoFile_UsesInMemory(t *testing.T) {
	e := &K8sElector{
		bearer:          "STATIC-TOKEN",
		bearerTokenFile: "",
	}
	got := e.bearerTokenForRequest()
	if got != "STATIC-TOKEN" {
		t.Errorf("bearerTokenForRequest = %q, want STATIC-TOKEN", got)
	}
}

// TestK8sElector_TokenRotation_FileReadFailure_FallsBackToCache locks the
// safety-net behaviour: if the projected token file briefly disappears
// (kubelet rotation in flight, FS hiccup, etc.), the cached in-memory
// token is used to avoid tearing down leadership over a single bad read.
//
// Fails-without: the `if err != nil { return e.bearer }` and
// `if tok == "" { return e.bearer }` fallbacks in bearerTokenForRequest.
// Remove them and this test fails because the missing-file case would
// return "" and the elector would drop its Authorization header.
func TestK8sElector_TokenRotation_FileReadFailure_FallsBackToCache(t *testing.T) {
	dir := t.TempDir()
	missingPath := filepath.Join(dir, "no-such-token") // never created

	e := &K8sElector{
		bearer:          "CACHED-TOKEN",
		bearerTokenFile: missingPath,
	}
	got := e.bearerTokenForRequest()
	if got != "CACHED-TOKEN" {
		t.Errorf("bearerTokenForRequest on missing file = %q, want CACHED-TOKEN", got)
	}
}

// TestK8sElector_TokenRotation_FileTrimWhitespace locks the contract that
// trailing whitespace/newlines in a projected token file are trimmed. Some
// kubelet versions write the token with a trailing newline; sending
// `"Bearer eyJhbGc...\n"` to the apiserver yields a 401.
//
// Fails-without: the strings.TrimSpace call in bearerTokenForRequest.
func TestK8sElector_TokenRotation_FileTrimWhitespace(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("  eyJhbGciOi\n  "), 0o600); err != nil {
		t.Fatal(err)
	}
	e := &K8sElector{bearerTokenFile: tokenPath}
	got := e.bearerTokenForRequest()
	if got != "eyJhbGciOi" {
		t.Errorf("bearerTokenForRequest = %q, want trimmed eyJhbGciOi", got)
	}
}
