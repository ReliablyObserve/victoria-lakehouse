package discovery

import (
	"testing"
)

func TestSplitHostPort_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		wantHost string
		wantPort string
	}{
		{"host:port", "host:9428", "host", "9428"},
		{"host only", "host", "host", ""},
		{"ip:port", "10.0.0.1:9428", "10.0.0.1", "9428"},
		{"ipv6:port", "[::1]:9428", "::1", "9428"},
		{"fqdn:port", "vlstorage.monitoring.svc.cluster.local:9428", "vlstorage.monitoring.svc.cluster.local", "9428"},
		{"fqdn only", "vlstorage.monitoring.svc.cluster.local", "vlstorage.monitoring.svc.cluster.local", ""},
		{"empty", "", "", ""},
		{"port only", ":9428", "", "9428"},
		{"ipv6 no port", "[::1]", "[::1]", ""},
		{"colon only", ":", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port := splitHostPort(tt.addr)
			if host != tt.wantHost {
				t.Errorf("splitHostPort(%q) host = %q, want %q", tt.addr, host, tt.wantHost)
			}
			if port != tt.wantPort {
				t.Errorf("splitHostPort(%q) port = %q, want %q", tt.addr, port, tt.wantPort)
			}
		})
	}
}
