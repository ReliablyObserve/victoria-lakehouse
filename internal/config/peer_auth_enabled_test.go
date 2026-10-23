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

func boolPtr(v bool) *bool { return &v }

// Peer authentication is tri-state. Unset (the default) enforces a configured
// key, as before peer.auth_enabled existed, and nothing when there is no key;
// false is send-only; true enforces and needs the key.
func TestPeerAuthEnabled_DefaultAndEnforcedKey(t *testing.T) {
	c := validCfg()
	if c.Peer.AuthEnabled != nil || c.Peer.AuthKey != "" || c.Peer.EnforcedKey() != "" {
		t.Fatalf("default peer config = %+v, want unset, no key, nothing enforced", c.Peer)
	}
	if c.Peer.MaxRequestBodyBytes != peerauth.DefaultMaxBodyBytes {
		t.Fatalf("default body cap = %d, want %d", c.Peer.MaxRequestBodyBytes, peerauth.DefaultMaxBodyBytes)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		enabled *bool
		key     string
		want    string
		invalid bool
	}{
		{"unset, no key: nothing to enforce", nil, "", "", false},
		{"unset, key: enforced (the behaviour before the setting existed)", nil, "k", "k", false},
		{"false, key: send-only", boolPtr(false), "k", "", false},
		{"false, no key: nothing", boolPtr(false), "", "", false},
		{"true, key: enforced", boolPtr(true), "k", "k", false},
		{"true, no key: refused at startup", boolPtr(true), "", "", true},
	}
	for _, tc := range cases {
		c := validCfg()
		c.Peer.AuthEnabled, c.Peer.AuthKey = tc.enabled, tc.key
		err := c.Validate()
		if (err != nil) != tc.invalid {
			t.Fatalf("%s: Validate err = %v, want invalid=%v", tc.name, err, tc.invalid)
		}
		if got := c.Peer.EnforcedKey(); got != tc.want {
			t.Fatalf("%s: EnforcedKey = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestValidate_PeerAuthEnabledNeedsAKey(t *testing.T) {
	c := validCfg()
	c.Peer.AuthEnabled = boolPtr(true)
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "peer.auth_enabled") || !strings.Contains(err.Error(), "peer.auth_key") {
		t.Fatalf("enabled without a key: err = %v, want one message naming both settings", err)
	}
	c.Peer.AuthEnabled = boolPtr(false)
	if err := c.Validate(); err != nil {
		t.Fatalf("explicit false without a key is allowed (nothing to enforce): %v", err)
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

// The settings load from the config file, and a non-nil overlay value (a
// profile or a per-role file) wins over the base, false as well as true, while
// an unset overlay leaves the base alone.
func TestPeerAuthEnabled_YAMLAndMerge(t *testing.T) {
	load := func(doc string) PeerConfig {
		var file struct {
			Peer PeerConfig `yaml:"peer"`
		}
		if err := yaml.Unmarshal([]byte(doc), &file); err != nil {
			t.Fatal(err)
		}
		return file.Peer
	}
	p := load("peer:\n  auth_key: k\n  auth_enabled: true\n  max_request_body_bytes: 1048576\n")
	if p.AuthEnabled == nil || !*p.AuthEnabled || p.MaxRequestBodyBytes != 1048576 || p.EnforcedKey() != "k" {
		t.Fatalf("peer config from yaml = %+v", p)
	}
	if p := load("peer:\n  auth_key: k\n  auth_enabled: false\n"); p.AuthEnabled == nil || *p.AuthEnabled || p.EnforcedKey() != "" {
		t.Fatalf("explicit false from yaml = %+v, want send-only", p)
	}
	if p := load("peer:\n  auth_key: k\n"); p.AuthEnabled != nil || p.EnforcedKey() != "k" {
		t.Fatalf("key only from yaml = %+v, want unset and enforced", p)
	}

	base := Default()
	overlay := &Config{}
	overlay.Peer.AuthEnabled = boolPtr(true)
	overlay.Peer.MaxRequestBodyBytes = 2048
	merged := MergeConfigs(base, overlay)
	if merged.Peer.AuthEnabled == nil || !*merged.Peer.AuthEnabled || merged.Peer.MaxRequestBodyBytes != 2048 {
		t.Fatalf("overlay not applied: %+v", merged.Peer)
	}
	merged = MergeConfigs(merged, &Config{})
	if merged.Peer.AuthEnabled == nil || !*merged.Peer.AuthEnabled || merged.Peer.MaxRequestBodyBytes != 2048 {
		t.Fatalf("an empty overlay changed the peer settings: %+v", merged.Peer)
	}
	off := &Config{}
	off.Peer.AuthEnabled = boolPtr(false)
	merged = MergeConfigs(merged, off)
	if merged.Peer.AuthEnabled == nil || *merged.Peer.AuthEnabled {
		t.Fatalf("an explicit false overlay did not win: %+v", merged.Peer)
	}
	// the merge copies the value, it does not alias the overlay's pointer
	*off.Peer.AuthEnabled = true
	if *merged.Peer.AuthEnabled {
		t.Fatal("merged config aliases the overlay's pointer")
	}
}
