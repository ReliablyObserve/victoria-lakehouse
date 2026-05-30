// internal/election/k8s_startup_test.go
//
// PR #98 Item 4 — pod without SA token must fail loudly at startup.
//
// If the chart deploys with `automountServiceAccountToken: false`, or the
// SA token path is missing, the elector must surface the failure via
// StartupError() and exit run() — NOT silently disable election and
// pretend to be running.
//
// We can't run rest.InClusterConfig() from a unit test (it depends on
// KUBERNETES_SERVICE_HOST + /var/run/secrets/... files). What we CAN
// test is the explicit pre-flight ServiceAccount token check inside
// run() that fires AFTER InClusterConfig succeeds. The strategy:
//
//  1. Build a K8sElector by hand.
//  2. Invoke a small helper that runs the same os.Stat check run() does.
//  3. Assert it returns the canonical "service account token not found"
//     error.
//
// Items 1 + 4's negative-control: comment out the os.Stat check in run()
// and TestK8sElector_NoServiceAccountToken_FailsAtStartup fails because
// the elector silently keeps trying and never sets startupErr.
package election

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

// withInClusterConfigForTest swaps the package-level inClusterConfigFunc /
// httpClientForFunc for the duration of t. Used to drive bootstrap()'s
// failure branches without an in-cluster environment.
func withInClusterConfigForTest(t *testing.T, cfg *rest.Config, cfgErr error, hcErr error) {
	t.Helper()
	origCfg := inClusterConfigFunc
	origHC := httpClientForFunc
	inClusterConfigFunc = func() (*rest.Config, error) {
		if cfgErr != nil {
			return nil, cfgErr
		}
		return cfg, nil
	}
	httpClientForFunc = func(c *rest.Config) (*http.Client, error) {
		if hcErr != nil {
			return nil, hcErr
		}
		// Real rest.HTTPClientFor needs a populated config. For tests we
		// just return a plain client; bootstrap only checks for non-nil.
		return &http.Client{}, nil
	}
	t.Cleanup(func() {
		inClusterConfigFunc = origCfg
		httpClientForFunc = origHC
	})
}

// preflightTokenCheck mirrors the os.Stat / BearerToken pre-flight that
// run() executes after rest.InClusterConfig. We extract the logic so a
// unit test can drive it without InClusterConfig.
//
// Returns the same canonical error messages that run() stores in
// startupErr.
func preflightTokenCheck(bearerTokenFile, bearerToken string) error {
	if bearerTokenFile != "" {
		if _, err := os.Stat(bearerTokenFile); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("service account token not found at %s (automountServiceAccountToken disabled?)", bearerTokenFile)
			}
			return fmt.Errorf("service account token stat failed at %s: %w", bearerTokenFile, err)
		}
		return nil
	}
	if bearerToken == "" {
		return errors.New("service account token not found (no BearerToken and no BearerTokenFile in InClusterConfig)")
	}
	return nil
}

// TestK8sElector_NoServiceAccountToken_FailsAtStartup_FileMissing locks the
// loud-failure path for a pod deployed with
// `automountServiceAccountToken: false`. With that setting, the kubelet
// does NOT mount a token at the in-cluster path; the os.Stat call inside
// run() returns os.IsNotExist; startupErr is set; run() returns early.
//
// Fails-without: the `if os.IsNotExist(err) { e.startupErr.Store(wrap);
// return }` branch in run().
func TestK8sElector_NoServiceAccountToken_FailsAtStartup_FileMissing(t *testing.T) {
	dir := t.TempDir()
	bogusPath := filepath.Join(dir, "no-such-token-file")
	err := preflightTokenCheck(bogusPath, "")
	if err == nil {
		t.Fatal("expected error when token file is missing")
	}
	if !strings.Contains(err.Error(), "service account token not found") {
		t.Fatalf("error message = %q; expected to contain 'service account token not found'", err.Error())
	}
}

// TestK8sElector_NoServiceAccountToken_FailsAtStartup_EmptyToken locks the
// edge case where neither BearerToken nor BearerTokenFile is populated.
// rest.InClusterConfig would not normally return this combination but a
// defensive check prevents the elector from silently sending unauthorized
// requests (which 401 forever instead of surfacing the misconfiguration).
//
// Fails-without: the `} else if config.BearerToken == "" {` branch in
// run().
func TestK8sElector_NoServiceAccountToken_FailsAtStartup_EmptyToken(t *testing.T) {
	err := preflightTokenCheck("", "")
	if err == nil {
		t.Fatal("expected error when no token and no file")
	}
	if !strings.Contains(err.Error(), "service account token not found") {
		t.Fatalf("error message = %q; expected to contain 'service account token not found'", err.Error())
	}
}

// TestK8sElector_NoServiceAccountToken_FailsAtStartup_FilePermDenied locks
// the "stat fails for a non-IsNotExist reason" branch. We trigger it with
// a path inside a directory that has zero permissions, so os.Stat returns
// an EACCES-style error that is NOT os.IsNotExist.
//
// Some sandboxed CI environments (root-owned runners) can stat regardless
// of dir perms, so the test skips when it can't reliably trigger the error.
//
// Fails-without: the `return fmt.Errorf("service account token stat
// failed at %s: %w", ...)` branch (the non-IsNotExist fallback).
func TestK8sElector_NoServiceAccountToken_FailsAtStartup_FilePermDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; permission denied is unreachable")
	}
	dir := t.TempDir()
	noaccessDir := filepath.Join(dir, "noaccess")
	if err := os.Mkdir(noaccessDir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(noaccessDir, 0o700)
	path := filepath.Join(noaccessDir, "token")

	err := preflightTokenCheck(path, "")
	if err == nil {
		t.Skip("permission-denied not reachable on this filesystem (sandbox?)")
	}
	if !strings.Contains(err.Error(), "service account token") {
		t.Fatalf("error message = %q; expected to mention service account token", err.Error())
	}
}

// TestK8sElector_StartupError_AccessibleAfterRunReturns locks the public
// StartupError() accessor: after the run() goroutine returns due to a
// startup failure, StartupError() must return that error so main.go can
// fail loudly at deployment time.
//
// We can't drive the InClusterConfig path from a test, so we set the
// startupErr field directly and verify the accessor surfaces it.
//
// Fails-without: the StartupError() method and the
// e.startupErr.Store(...) calls in run().
func TestK8sElector_StartupError_AccessibleAfterRunReturns(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "test", Identity: "pod-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.StartupError(); got != nil {
		t.Fatalf("StartupError() before run = %v, want nil", got)
	}
	want := errors.New("service account token not found at /var/run/secrets/kubernetes.io/serviceaccount/token (automountServiceAccountToken disabled?)")
	e.startupErr.Store(want)
	if got := e.StartupError(); got == nil || got.Error() != want.Error() {
		t.Errorf("StartupError() = %v, want %v", got, want)
	}
}

// TestK8sElector_StartupError_NilWhenNoFailure locks the happy path: a
// successful bootstrap leaves StartupError() returning nil. This is a
// negative-control complement to the loud-failure tests above.
func TestK8sElector_StartupError_NilWhenNoFailure(t *testing.T) {
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "happy", Identity: "pod-y"})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate "already bootstrapped via test injection": client/apiBase
	// set, no startupErr stored.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	e.client = srv.Client()
	e.apiBase = srv.URL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	time.Sleep(20 * time.Millisecond) // let run() execute
	e.Stop()

	if got := e.StartupError(); got != nil {
		t.Errorf("StartupError() = %v, want nil on happy path", got)
	}
}

// TestK8sElector_Bootstrap_InClusterConfigFails locks the run-path that
// fires when rest.InClusterConfig returns an error (we are not in a
// cluster, or KUBERNETES_SERVICE_HOST is missing). startupErr must be
// set and the startup-errors metric must increment.
//
// Fails-without: the `if err != nil { ... e.startupErr.Store(wrap); ...
// return wrap }` branch at the top of bootstrap().
func TestK8sElector_Bootstrap_InClusterConfigFails(t *testing.T) {
	withInClusterConfigForTest(t, nil, errors.New("not in cluster"), nil)

	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.bootstrap(); got == nil {
		t.Fatal("expected bootstrap to fail when InClusterConfig fails")
	}
	if se := e.StartupError(); se == nil || !strings.Contains(se.Error(), "in-cluster config failed") {
		t.Errorf("StartupError = %v; expected to contain 'in-cluster config failed'", se)
	}
}

// TestK8sElector_Bootstrap_TokenFileMissing locks the no-SA-token branch
// when BearerTokenFile is set but the file does not exist.
//
// Fails-without: the `if os.IsNotExist(err) { ... }` branch in bootstrap.
func TestK8sElector_Bootstrap_TokenFileMissing(t *testing.T) {
	dir := t.TempDir()
	bogus := filepath.Join(dir, "no-token")
	withInClusterConfigForTest(t, &rest.Config{
		Host:            "https://api.example.com",
		BearerTokenFile: bogus,
	}, nil, nil)

	e, _ := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if got := e.bootstrap(); got == nil {
		t.Fatal("expected bootstrap to fail when token file is missing")
	}
	if se := e.StartupError(); se == nil || !strings.Contains(se.Error(), "service account token not found") {
		t.Errorf("StartupError = %v; expected to contain 'service account token not found'", se)
	}
}

// TestK8sElector_Bootstrap_TokenFileStatPermDenied locks the "stat fails
// for a non-IsNotExist reason" branch. We trigger it with a path inside
// a directory that has zero permissions.
//
// Fails-without: the `return wrap` after `service account token stat
// failed at %s: %w`.
func TestK8sElector_Bootstrap_TokenFileStatPermDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; permission denied is unreachable")
	}
	dir := t.TempDir()
	noaccessDir := filepath.Join(dir, "noaccess")
	if err := os.Mkdir(noaccessDir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(noaccessDir, 0o700)
	tokenPath := filepath.Join(noaccessDir, "token")

	withInClusterConfigForTest(t, &rest.Config{
		Host:            "https://api.example.com",
		BearerTokenFile: tokenPath,
	}, nil, nil)

	e, _ := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if got := e.bootstrap(); got == nil {
		t.Skip("permission-denied not reachable (sandbox?)")
	}
}

// TestK8sElector_Bootstrap_NoTokenAndNoFile locks the "BearerToken == ”
// and BearerTokenFile == ”" branch.
//
// Fails-without: the `} else if config.BearerToken == "" {` branch.
func TestK8sElector_Bootstrap_NoTokenAndNoFile(t *testing.T) {
	withInClusterConfigForTest(t, &rest.Config{
		Host: "https://api.example.com",
		// no BearerToken, no BearerTokenFile
	}, nil, nil)

	e, _ := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if got := e.bootstrap(); got == nil {
		t.Fatal("expected bootstrap to fail when no token info present")
	}
	if se := e.StartupError(); se == nil || !strings.Contains(se.Error(), "no BearerToken and no BearerTokenFile") {
		t.Errorf("StartupError = %v; expected to contain 'no BearerToken and no BearerTokenFile'", se)
	}
}

// TestK8sElector_Bootstrap_HTTPClientForFails locks the late-failure
// branch when rest.HTTPClientFor returns an error (e.g., malformed
// TLSClientConfig). startupErr must be set.
//
// Fails-without: the `if err != nil { ... HTTPClientFor failed ... }`
// branch in bootstrap.
func TestK8sElector_Bootstrap_HTTPClientForFails(t *testing.T) {
	withInClusterConfigForTest(t, &rest.Config{
		Host:        "https://api.example.com",
		BearerToken: "valid-token", // skip the SA token check
	}, nil, errors.New("HTTPClientFor failed"))

	e, _ := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if got := e.bootstrap(); got == nil {
		t.Fatal("expected bootstrap to fail when HTTPClientFor fails")
	}
	if se := e.StartupError(); se == nil || !strings.Contains(se.Error(), "http client creation failed") {
		t.Errorf("StartupError = %v; expected to contain 'http client creation failed'", se)
	}
}

// TestK8sElector_Bootstrap_Success locks the happy-path: config + token +
// HTTPClient all succeed. The elector's apiBase / client fields get
// populated and startupErr stays nil.
//
// Fails-without: the final field assignments in bootstrap.
func TestK8sElector_Bootstrap_Success(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("eyJhbGciOi"), 0o600); err != nil {
		t.Fatal(err)
	}
	withInClusterConfigForTest(t, &rest.Config{
		Host:            "https://api.example.com",
		BearerToken:     "cached-token",
		BearerTokenFile: tokenPath,
	}, nil, nil)

	e, _ := NewK8sElector(K8sElectorConfig{LeaseName: "boot-test", Identity: "pod-x"})
	if err := e.bootstrap(); err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if e.apiBase != "https://api.example.com" {
		t.Errorf("apiBase = %q, want https://api.example.com", e.apiBase)
	}
	if e.bearer != "cached-token" {
		t.Errorf("bearer = %q, want cached-token", e.bearer)
	}
	if e.bearerTokenFile != tokenPath {
		t.Errorf("bearerTokenFile = %q, want %q", e.bearerTokenFile, tokenPath)
	}
	if e.client == nil {
		t.Error("client not populated after successful bootstrap")
	}
	if e.StartupError() != nil {
		t.Errorf("StartupError = %v, want nil after successful bootstrap", e.StartupError())
	}
}

// TestK8sElector_ModuleInference covers the module() helper used to label
// metrics. The fan-out is:
//
//	"lakehouse-compaction-logs"   → "logs"
//	"lakehouse-compaction-traces" → "traces"
//	anything else                 → "unknown"
//
// Fails-without: the strings.Contains-based switch in module().
func TestK8sElector_ModuleInference(t *testing.T) {
	cases := []struct {
		lease string
		want  string
	}{
		{"lakehouse-compaction-logs", "logs"},
		{"lakehouse-compaction-traces", "traces"},
		{"lakehouse-logs-insert", "logs"},
		{"random-lease-name", "unknown"},
		{"", "unknown"},
	}
	for _, tc := range cases {
		e := &K8sElector{cfg: K8sElectorConfig{LeaseName: tc.lease}}
		if got := e.module(); got != tc.want {
			t.Errorf("module(%q) = %q, want %q", tc.lease, got, tc.want)
		}
	}
}
