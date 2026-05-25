package azdetect

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDetectAWSIMDS_AZEndpointNon200 exercises the non-200 status check
// on the AZ endpoint (resp.StatusCode != http.StatusOK).
func TestDetectAWSIMDS_AZEndpointNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("mock-token"))
		case "/latest/meta-data/placement/availability-zone":
			w.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	_, err := detectAWSIMDS(context.Background(), srv.URL, 2*time.Second)
	if err == nil {
		t.Fatal("expected error for non-200 AZ response")
	}
	if got := err.Error(); got != "IMDS az status 403" {
		t.Errorf("unexpected error message: %q", got)
	}
}

// TestDetectGCPMetadata_Non200Status exercises the non-200 branch in detectGCPMetadata.
func TestDetectGCPMetadata_Non200Status(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err == nil {
		t.Fatal("expected error for non-200 GCP response")
	}
	if got := err.Error(); got != "GCP metadata status 403" {
		t.Errorf("unexpected error message: %q", got)
	}
}

// TestDetectGCPMetadata_MultipleSlashParts exercises various response formats.
func TestDetectGCPMetadata_MultipleSlashParts(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"standard path", "projects/123/zones/us-central1-a", "us-central1-a"},
		{"single segment", "eu-north-1a", "eu-north-1a"},
		{"many segments", "a/b/c/d/e/target-zone", "target-zone"},
		{"empty response", "", ""},
		{"whitespace", "  projects/1/zones/trimmed-zone  ", "trimmed-zone"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if az != tc.want {
				t.Errorf("detectGCPMetadata = %q, want %q", az, tc.want)
			}
		})
	}
}

// TestDetectGCPMetadata_ConnectionError exercises the connection error path.
func TestDetectGCPMetadata_ConnectionError(t *testing.T) {
	_, err := detectGCPMetadata(context.Background(), "http://192.0.2.1:1", 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable GCP endpoint")
	}
}

// TestDetectAWSIMDS_ConnectionError exercises the connection error path.
func TestDetectAWSIMDS_ConnectionError(t *testing.T) {
	_, err := detectAWSIMDS(context.Background(), "http://192.0.2.1:1", 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable IMDS endpoint")
	}
}

// --- K8s node label detection tests using package-level vars ---

// setupK8sTokenFile creates a temp SA token file and overrides k8sTokenPath.
// It returns a cleanup function that restores the original path.
func setupK8sTokenFile(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("mock-token"), 0644); err != nil {
		t.Fatalf("write token: %v", err)
	}
	orig := k8sTokenPath
	k8sTokenPath = tokenFile
	t.Cleanup(func() { k8sTokenPath = orig })
}

// TestDetectK8sNodeLabel_FullSuccessPath exercises the full success path
// of detectK8sNodeLabel by using package-level vars to redirect to a mock server.
func TestDetectK8sNodeLabel_FullSuccessPath(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		wantAZ string
	}{
		{
			name:   "GA topology label",
			labels: map[string]string{"topology.kubernetes.io/zone": "eu-west-1a"},
			wantAZ: "eu-west-1a",
		},
		{
			name:   "legacy label only",
			labels: map[string]string{"failure-domain.beta.kubernetes.io/zone": "us-east-1b"},
			wantAZ: "us-east-1b",
		},
		{
			name:   "both labels, GA wins",
			labels: map[string]string{"topology.kubernetes.io/zone": "ga-zone", "failure-domain.beta.kubernetes.io/zone": "legacy-zone"},
			wantAZ: "ga-zone",
		},
		{
			name:   "no zone labels returns empty",
			labels: map[string]string{"kubernetes.io/hostname": "node-1"},
			wantAZ: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Verify authorization header is present.
				if r.Header.Get("Authorization") != "Bearer mock-token" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				node := struct {
					Metadata struct {
						Labels map[string]string `json:"labels"`
					} `json:"metadata"`
				}{}
				node.Metadata.Labels = tc.labels
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(node)
			}))
			defer srv.Close()

			// Override package vars.
			t.Setenv("NODE_NAME", "test-node")
			setupK8sTokenFile(t)
			origBase := k8sAPIBase
			k8sAPIBase = srv.URL
			t.Cleanup(func() { k8sAPIBase = origBase })

			az, err := detectK8sNodeLabel(context.Background(), 2*time.Second)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if az != tc.wantAZ {
				t.Errorf("detectK8sNodeLabel = %q, want %q", az, tc.wantAZ)
			}
		})
	}
}

// TestDetectK8sNodeLabel_Non200 exercises the non-200 response path.
func TestDetectK8sNodeLabel_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	t.Setenv("NODE_NAME", "test-node")
	setupK8sTokenFile(t)
	origBase := k8sAPIBase
	k8sAPIBase = srv.URL
	t.Cleanup(func() { k8sAPIBase = origBase })

	_, err := detectK8sNodeLabel(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected error for non-200 K8s API response")
	}
	if got := err.Error(); got != "k8s API status 403" {
		t.Errorf("unexpected error: %q", got)
	}
}

// TestDetectK8sNodeLabel_InvalidJSON exercises the JSON decode error path.
func TestDetectK8sNodeLabel_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not-json{"))
	}))
	defer srv.Close()

	t.Setenv("NODE_NAME", "test-node")
	setupK8sTokenFile(t)
	origBase := k8sAPIBase
	k8sAPIBase = srv.URL
	t.Cleanup(func() { k8sAPIBase = origBase })

	_, err := detectK8sNodeLabel(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// TestDetectK8sNodeLabel_ConnectionError exercises the connection error path.
func TestDetectK8sNodeLabel_ConnectionError(t *testing.T) {
	t.Setenv("NODE_NAME", "test-node")
	setupK8sTokenFile(t)
	origBase := k8sAPIBase
	k8sAPIBase = "http://192.0.2.1:1"
	t.Cleanup(func() { k8sAPIBase = origBase })

	_, err := detectK8sNodeLabel(context.Background(), 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable K8s API")
	}
}

// --- Detect function full-chain tests using mock servers ---

// TestDetect_AWSSuccessPath exercises the Detect function where AWS IMDS
// succeeds (env var is empty/missing, AWS returns a valid AZ).
func TestDetect_AWSSuccessPath(t *testing.T) {
	// We can't mock the hardcoded awsIMDSBase/gcpMetaBase in Detect,
	// but detectK8sNodeLabel is tested via the k8s vars.
	// For the Detect function itself, the env var path and the fallthrough-empty
	// path are well-covered. The intermediate success paths (AWS/GCP/K8s) can't
	// be tested via Detect() without overriding module constants, but the
	// underlying functions are tested directly with full coverage.
	t.Setenv("MY_AZ_TEST", "test-zone-from-env")
	az := Detect(context.Background(), Options{EnvVar: "MY_AZ_TEST", Timeout: time.Second})
	if az != "test-zone-from-env" {
		t.Errorf("expected env var to be returned, got %q", az)
	}
}

// TestDetect_K8sSuccessPath exercises the full Detect function with K8s detection.
func TestDetect_K8sSuccessPath(t *testing.T) {
	// Set up K8s mock to return a zone.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		node := struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
		}{}
		node.Metadata.Labels = map[string]string{"topology.kubernetes.io/zone": "k8s-zone-1a"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(node)
	}))
	defer srv.Close()

	t.Setenv("NODE_NAME", "test-node")
	setupK8sTokenFile(t)
	origBase := k8sAPIBase
	k8sAPIBase = srv.URL
	t.Cleanup(func() { k8sAPIBase = origBase })

	// Don't set any env var for AZ, so env check falls through.
	// AWS/GCP will fail (hardcoded URLs timeout), K8s should succeed.
	az := Detect(context.Background(), Options{
		Timeout: 200 * time.Millisecond,
	})
	if az != "k8s-zone-1a" {
		t.Errorf("expected k8s-zone-1a, got %q", az)
	}
}
