package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

func validCfg() *Config {
	c := Default()
	c.Mode = ModeLogs
	c.S3.Bucket = "b"
	return c
}

// Peer authentication is off by default and enforces nothing; enabled it needs
// the key, and EnforcedKey is the key only when enabled.
func TestPeerAuthEnabled_DefaultOffAndEnforcedKey(t *testing.T) {
	c := validCfg()
	if c.Peer.AuthEnabled || c.Peer.AuthKey != "" || c.Peer.EnforcedKey() != "" {
		t.Fatalf("default peer config = %+v, want everything off", c.Peer)
	}
	if c.Peer.MaxRequestBodyBytes != peerauth.DefaultMaxBodyBytes {
		t.Fatalf("default body cap = %d, want %d", c.Peer.MaxRequestBodyBytes, peerauth.DefaultMaxBodyBytes)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}

	// a key without the switch is only sent, not enforced
	c.Peer.AuthKey = "k"
	if err := c.Validate(); err != nil || c.Peer.EnforcedKey() != "" {
		t.Fatalf("key without peer.auth_enabled: err %v, enforced key %q, want none", err, c.Peer.EnforcedKey())
	}
	c.Peer.AuthEnabled = true
	if err := c.Validate(); err != nil || c.Peer.EnforcedKey() != "k" {
		t.Fatalf("enabled with a key: err %v, enforced key %q", err, c.Peer.EnforcedKey())
	}
}

func TestValidate_PeerAuthEnabledNeedsAKey(t *testing.T) {
	c := validCfg()
	c.Peer.AuthEnabled = true
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "peer.auth_enabled") || !strings.Contains(err.Error(), "peer.auth_key") {
		t.Fatalf("enabled without a key: err = %v, want one message naming both settings", err)
	}
}

func TestValidate_PeerBodyCapNotNegative(t *testing.T) {
	c := validCfg()
	c.Peer.MaxRequestBodyBytes = -1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "peer.max_request_body_bytes") {
		t.Fatalf("negative cap: err = %v", err)
	}
	c.Peer.MaxRequestBodyBytes = 0 // 0 restores the default at startup
	if err := c.Validate(); err != nil {
		t.Fatalf("zero cap: %v", err)
	}
}

// The settings load from the config file, and an overlay (a profile or a
// per-role file) can switch the enforcement on and change the cap but never
// switches it off.
func TestPeerAuthEnabled_YAMLAndMerge(t *testing.T) {
	var file struct {
		Peer PeerConfig `yaml:"peer"`
	}
	if err := yaml.Unmarshal([]byte("peer:\n  auth_key: k\n  auth_enabled: true\n  max_request_body_bytes: 1048576\n"), &file); err != nil {
		t.Fatal(err)
	}
	if !file.Peer.AuthEnabled || file.Peer.MaxRequestBodyBytes != 1048576 || file.Peer.EnforcedKey() != "k" {
		t.Fatalf("peer config from yaml = %+v", file.Peer)
	}

	base := Default()
	overlay := &Config{}
	overlay.Peer.AuthEnabled = true
	overlay.Peer.MaxRequestBodyBytes = 2048
	merged := MergeConfigs(base, overlay)
	if !merged.Peer.AuthEnabled || merged.Peer.MaxRequestBodyBytes != 2048 {
		t.Fatalf("overlay not applied: %+v", merged.Peer)
	}
	merged = MergeConfigs(merged, &Config{})
	if !merged.Peer.AuthEnabled || merged.Peer.MaxRequestBodyBytes != 2048 {
		t.Fatalf("an empty overlay changed the peer settings: %+v", merged.Peer)
	}
}
