package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDetect_DefaultTimeout(t *testing.T) {
	t.Setenv("NONEXISTENT", "")
	az := Detect(context.Background(), Options{EnvVar: "NONEXISTENT"})
	if az != "" {
		t.Errorf("expected empty, got %q", az)
	}
}

func TestDetect_EmptyEnvVarName(t *testing.T) {
	az := Detect(context.Background(), Options{
		EnvVar:  "",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty with no env var name, got %q", az)
	}
}

func TestDetect_EnvVarWithWhitespaceValue(t *testing.T) {
	t.Setenv("WS_AZ", "  us-east-1a  ")

	az := Detect(context.Background(), Options{EnvVar: "WS_AZ"})
	if az != "  us-east-1a  " {
		t.Errorf("env var value should be returned as-is, got %q", az)
	}
}

func TestDetect_ContextCancelled(t *testing.T) {
	t.Setenv("NONEXISTENT", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	az := Detect(ctx, Options{
		EnvVar:  "NONEXISTENT",
		Timeout: time.Second,
	})
	if az != "" {
		t.Errorf("expected empty with cancelled context, got %q", az)
	}
}

func TestDetectAWSIMDS_TokenEndpointDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", 404)
	}))
	defer srv.Close()

	// Token request returns 404 but we still try the AZ endpoint
	_, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err == nil {
		t.Error("expected error when AZ endpoint not available")
	}
}

func TestDetectAWSIMDS_WhitespaceInResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("token"))
		case "/latest/meta-data/placement/availability-zone":
			_, _ = w.Write([]byte("  us-east-1a\n"))
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "us-east-1a" {
		t.Errorf("expected trimmed 'us-east-1a', got %q", az)
	}
}

func TestDetectAWSIMDS_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("token"))
		case "/latest/meta-data/placement/availability-zone":
			_, _ = w.Write([]byte(""))
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "" {
		t.Errorf("expected empty, got %q", az)
	}
}

func TestDetectAWSIMDS_LargeResponse(t *testing.T) {
	large := strings.Repeat("a", 10000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("token"))
		case "/latest/meta-data/placement/availability-zone":
			_, _ = w.Write([]byte(large))
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != large {
		t.Errorf("expected large string returned as-is")
	}
}

func TestDetectGCPMetadata_MissingHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing", 400)
			return
		}
		w.Write([]byte("projects/1/zones/zone-a"))
	}))
	defer srv.Close()

	// Should fail because our code sets the header correctly
	// Let's verify the function does set it
	az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "zone-a" {
		t.Errorf("expected zone-a, got %q", az)
	}
}

func TestDetectGCPMetadata_SinglePathSegment(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("just-a-zone"))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "just-a-zone" {
		t.Errorf("expected 'just-a-zone', got %q", az)
	}
}

func TestDetectGCPMetadata_TrailingSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("projects/1/zones/zone-b/"))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Split on "/" with trailing slash: last element is ""
	if az != "" {
		t.Errorf("trailing slash should give empty last segment, got %q", az)
	}
}

func TestDetectGCPMetadata_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(""))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "" {
		t.Errorf("expected empty, got %q", az)
	}
}
