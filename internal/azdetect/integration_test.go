package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIntegration_FullChain_EnvWins(t *testing.T) {
	t.Setenv("MY_AZ", "override-zone")

	az := Detect(context.Background(), Options{EnvVar: "MY_AZ", Timeout: time.Second})
	if az != "override-zone" {
		t.Errorf("env var should win, got %q", az)
	}
}

func TestIntegration_AllFail_ReturnsEmpty(t *testing.T) {
	t.Setenv("NONEXISTENT", "")

	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty when all methods fail, got %q", az)
	}
}

func TestIntegration_DefaultTimeoutUsed(t *testing.T) {
	t.Setenv("NONEXISTENT", "")

	az := Detect(context.Background(), Options{
		EnvVar: "NONEXISTENT",
	})
	if az != "" {
		t.Errorf("expected empty with default timeout, got %q", az)
	}
}

func TestIntegration_AWSIMDS_WhenEnvEmpty(t *testing.T) {
	t.Setenv("NONEXISTENT", "")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("test-token"))
		case "/latest/meta-data/placement/availability-zone":
			if r.Header.Get("X-aws-ec2-metadata-token") != "test-token" {
				http.Error(w, "bad token", http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte("us-east-1d"))
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "us-east-1d" {
		t.Errorf("expected us-east-1d, got %q", az)
	}
}

func TestIntegration_AWSIMDS_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			_, _ = w.Write([]byte("token"))
		case "/latest/meta-data/placement/availability-zone":
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer srv.Close()

	_, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)
	if err == nil {
		t.Error("expected error on non-200 status")
	}
}

func TestIntegration_GCPMetadata_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err == nil {
		t.Error("expected error on non-200 status")
	}
}

func TestIntegration_GCPMetadata_SimpleZonePath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing header", 400)
			return
		}
		_, _ = w.Write([]byte("us-central1-f"))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "us-central1-f" {
		t.Errorf("expected us-central1-f, got %q", az)
	}
}
