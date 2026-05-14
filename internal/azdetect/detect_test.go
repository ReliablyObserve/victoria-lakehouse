package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestDetect_EnvVar(t *testing.T) {
	os.Setenv("TEST_AZ_VAR", "us-east-1a")
	defer os.Unsetenv("TEST_AZ_VAR")

	az := Detect(context.Background(), Options{EnvVar: "TEST_AZ_VAR"})
	if az != "us-east-1a" {
		t.Errorf("expected us-east-1a, got %q", az)
	}
}

func TestDetect_EnvVarEmpty_FallsThrough(t *testing.T) {
	os.Unsetenv("NONEXISTENT_AZ")

	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT_AZ",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty, got %q", az)
	}
}

func TestDetectAWSIMDS(t *testing.T) {
	tokenCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			if r.Method != http.MethodPut {
				http.Error(w, "method", 405)
				return
			}
			tokenCalled = true
			w.Write([]byte("mock-token"))
		case "/latest/meta-data/placement/availability-zone":
			if r.Header.Get("X-aws-ec2-metadata-token") != "mock-token" {
				http.Error(w, "unauthorized", 401)
				return
			}
			w.Write([]byte("us-west-2b"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "us-west-2b" {
		t.Errorf("expected us-west-2b, got %q", az)
	}
	if !tokenCalled {
		t.Error("IMDSv2 token endpoint was not called")
	}
}

func TestDetectGCPMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing header", 400)
			return
		}
		w.Write([]byte("projects/123/zones/europe-west1-b"))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "europe-west1-b" {
		t.Errorf("expected europe-west1-b, got %q", az)
	}
}

func TestDetectAWSIMDS_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := detectAWSIMDS(ctx, srv.URL, 100*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestDetect_FullChain_EnvWins(t *testing.T) {
	os.Setenv("MY_AZ", "override-zone")
	defer os.Unsetenv("MY_AZ")

	az := Detect(context.Background(), Options{EnvVar: "MY_AZ", Timeout: time.Second})
	if az != "override-zone" {
		t.Errorf("env var should win, got %q", az)
	}
}

func TestDetect_AllFail_ReturnsEmpty(t *testing.T) {
	os.Unsetenv("NONEXISTENT")

	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty when all methods fail, got %q", az)
	}
}
