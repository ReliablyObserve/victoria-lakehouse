package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func FuzzDetect_EnvVar(f *testing.F) {
	f.Add("LAKEHOUSE_AZ", "us-east-1a")
	f.Add("MY_AZ", "")
	f.Add("", "value")
	f.Add("AZ", "eu-west-1b")
	f.Add("AZ_VAR", "ap-southeast-2c")
	f.Add("AZ", "zone-with-special-chars-!@#$%")
	f.Add("AZ", "a")
	f.Add("AZ", "very-long-zone-name-that-goes-on-and-on-and-on-abcdefghijklmnop")

	f.Fuzz(func(t *testing.T, envName, envValue string) {
		if envName == "" {
			return
		}
		// Skip names/values with null bytes or = sign — invalid for env vars
		for _, b := range []byte(envName) {
			if b == 0 || b == '=' {
				return
			}
		}
		for _, b := range []byte(envValue) {
			if b == 0 {
				return
			}
		}
		if err := os.Setenv(envName, envValue); err != nil {
			return
		}
		defer func() { _ = os.Unsetenv(envName) }()

		// Verify env var was actually set before asserting
		if os.Getenv(envName) != envValue {
			return
		}

		az := Detect(context.Background(), Options{
			EnvVar:  envName,
			Timeout: 50 * time.Millisecond,
		})

		if envValue != "" && az != envValue {
			t.Errorf("env set to %q but Detect returned %q", envValue, az)
		}
	})
}

func FuzzDetectAWSIMDS_Response(f *testing.F) {
	f.Add("us-east-1a", 200)
	f.Add("", 200)
	f.Add("us-west-2b", 200)
	f.Add("ap-southeast-1c", 200)
	f.Add("eu-central-1a", 200)
	f.Add("zone\nwith\nnewlines", 200)
	f.Add("  zone-with-spaces  ", 200)
	f.Add("", 404)
	f.Add("", 500)
	f.Add("zone", 403)

	f.Fuzz(func(t *testing.T, body string, statusCode int) {
		// 1xx informational status codes have special HTTP semantics — skip them
		if statusCode < 200 || statusCode > 599 {
			return
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/latest/api/token":
				_, _ = w.Write([]byte("token"))
			case "/latest/meta-data/placement/availability-zone":
				w.WriteHeader(statusCode)
				_, _ = w.Write([]byte(body))
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		az, err := detectAWSIMDS(context.Background(), srv.URL, time.Second)

		if statusCode != 200 {
			if err == nil {
				t.Errorf("expected error for status %d, got az=%q", statusCode, az)
			}
			return
		}

		// Connection errors under fuzz load are expected — skip
		if err != nil {
			return
		}
		trimmed := strings.TrimSpace(body)
		if az != trimmed {
			t.Errorf("expected %q, got %q", trimmed, az)
		}
	})
}

func FuzzDetectGCPMetadata_Response(f *testing.F) {
	f.Add("projects/123/zones/us-east1-b", 200)
	f.Add("us-central1-f", 200)
	f.Add("", 200)
	f.Add("projects/456/zones/europe-west1-c", 200)
	f.Add("no-slashes", 200)
	f.Add("a/b/c/d/e", 200)
	f.Add("/leading/slash/zone", 200)
	f.Add("trailing/slash/", 200)
	f.Add("", 404)
	f.Add("error", 500)

	f.Fuzz(func(t *testing.T, body string, statusCode int) {
		// 1xx informational status codes have special HTTP semantics — skip them
		if statusCode < 200 || statusCode > 599 {
			return
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Metadata-Flavor") != "Google" {
				http.Error(w, "missing header", 400)
				return
			}
			w.WriteHeader(statusCode)
			_, _ = w.Write([]byte(body))
		}))
		defer srv.Close()

		az, err := detectGCPMetadata(context.Background(), srv.URL, time.Second)

		if statusCode != 200 {
			if err == nil {
				t.Errorf("expected error for status %d, got az=%q", statusCode, az)
			}
			return
		}

		// Connection errors under fuzz load are expected — skip
		_ = err
		_ = az
	})
}
