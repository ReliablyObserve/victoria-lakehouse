package config

import (
	"strings"
	"testing"
)

// TestValidate_S3EndpointSSRF verifies that Validate rejects S3 endpoint URLs
// that could lead to SSRF (Server-Side Request Forgery) attacks. The current
// Validate() at config.go:736 has no S3 endpoint URL validation.
func TestValidate_S3EndpointSSRF(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		// Valid endpoints that should be accepted
		{"valid_s3_endpoint", "https://s3.amazonaws.com", false},
		{"valid_minio", "http://minio:9000", false},
		{"valid_with_port", "https://s3.us-east-1.amazonaws.com:443", false},
		{"valid_r2", "https://account.r2.cloudflarestorage.com", false},
		{"valid_gcs", "https://storage.googleapis.com", false},
		{"empty_endpoint", "", false}, // empty = use AWS default

		// SSRF attack vectors that should be rejected
		{"ssrf_file_scheme", "file:///etc/passwd", true},
		{"ssrf_ftp_scheme", "ftp://evil.com/data", true},
		{"ssrf_javascript", "javascript:alert(1)", true},
		{"ssrf_data_uri", "data:text/html,<script>", true},
		{"ssrf_gopher", "gopher://evil.com:25/", true},

		// Cloud metadata endpoints (common SSRF targets)
		{"ssrf_aws_metadata", "http://169.254.169.254/latest/meta-data/", true},
		{"ssrf_aws_metadata_v2", "http://169.254.169.254/latest/api/token", true},
		{"ssrf_gcp_metadata", "http://metadata.google.internal/computeMetadata/", true},
		{"ssrf_azure_metadata", "http://169.254.169.254/metadata/instance", true},
		{"ssrf_link_local", "http://169.254.169.254/", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test-bucket"
			cfg.S3.Region = "us-east-1"
			cfg.S3.Endpoint = tt.endpoint

			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() should reject S3 endpoint %q as potential SSRF vector; "+
					"BUG: no endpoint URL validation in Validate()", tt.endpoint)
			}
			if !tt.wantErr && err != nil && strings.Contains(err.Error(), "endpoint") {
				t.Errorf("Validate() should accept S3 endpoint %q, got: %v", tt.endpoint, err)
			}
		})
	}
}

// TestValidate_S3EndpointScheme verifies that only http:// and https:// schemes
// are accepted for S3 endpoints.
func TestValidate_S3EndpointScheme(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{"http_scheme", "http://minio:9000", false},
		{"https_scheme", "https://s3.amazonaws.com", false},
		{"no_scheme", "s3.amazonaws.com", true}, // missing scheme should be rejected
		{"ftp_scheme", "ftp://storage.example.com", true},
		{"unix_scheme", "unix:///var/run/minio.sock", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test-bucket"
			cfg.S3.Endpoint = tt.endpoint

			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() should reject S3 endpoint %q; "+
					"BUG: no scheme validation", tt.endpoint)
			}
			if !tt.wantErr && err != nil && strings.Contains(err.Error(), "endpoint") {
				t.Errorf("Validate() should accept S3 endpoint %q, got: %v", tt.endpoint, err)
			}
		})
	}
}

// TestValidate_S3EndpointInternalNetworks verifies that internal/private
// network addresses are rejected when used as S3 endpoints.
func TestValidate_S3EndpointInternalNetworks(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		// These are legitimate internal endpoints (e.g., minio in k8s)
		// so the test documents current behavior — some deployments need these.
		{"localhost", "http://localhost:9000", false},
		{"loopback", "http://127.0.0.1:9000", false},
		{"k8s_service", "http://minio.default.svc:9000", false},

		// But link-local/metadata should always be blocked
		{"link_local_metadata", "http://169.254.169.254/", true},
		{"link_local_other", "http://169.254.0.1/", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test-bucket"
			cfg.S3.Endpoint = tt.endpoint

			err := cfg.Validate()

			if tt.wantErr && err == nil {
				t.Errorf("Validate() should reject S3 endpoint %q; "+
					"BUG: no network address validation", tt.endpoint)
			}
			if !tt.wantErr && err != nil && strings.Contains(err.Error(), "endpoint") {
				t.Errorf("Validate() should accept S3 endpoint %q, got: %v", tt.endpoint, err)
			}
		})
	}
}
