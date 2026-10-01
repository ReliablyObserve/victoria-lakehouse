package config

import (
	"strings"
	"testing"
)

func autoRegisterCfg(mutate func(*Config)) *Config {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test-bucket"
	mutate(cfg)
	return cfg
}

func TestTenantAutoRegisterRangeDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Tenant.AutoRegisterMinID != 2147483648 || cfg.Tenant.AutoRegisterMaxID != 4294967294 {
		t.Fatalf("default range = [%d, %d], want [2147483648, 4294967294]", cfg.Tenant.AutoRegisterMinID, cfg.Tenant.AutoRegisterMaxID)
	}
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "b"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
}

func TestTenantAutoRegisterRangeValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"custom range", func(c *Config) { c.Tenant.AutoRegisterMinID, c.Tenant.AutoRegisterMaxID = 100000, 200000 }, ""},
		{"single id range", func(c *Config) { c.Tenant.AutoRegisterMinID, c.Tenant.AutoRegisterMaxID = 7, 7 }, ""},
		{"min above max", func(c *Config) { c.Tenant.AutoRegisterMinID, c.Tenant.AutoRegisterMaxID = 500, 400 }, "must not exceed"},
		{"max is the null account", func(c *Config) { c.Tenant.AutoRegisterMaxID = 4294967295 }, "must be below"},
		{"only min set", func(c *Config) { c.Tenant.AutoRegisterMaxID = 0 }, "must both be set"},
		{"only max set", func(c *Config) { c.Tenant.AutoRegisterMinID = 0 }, "must both be set"},
		{"alias at range start", func(c *Config) {
			c.Tenant.Aliases = map[string]AliasTarget{"acme": {AccountID: 2147483648}}
		}, "inside the reserved auto-register range"},
		{"alias at range end", func(c *Config) {
			c.Tenant.Aliases = map[string]AliasTarget{"acme": {AccountID: 4294967294}}
		}, "inside the reserved auto-register range"},
		{"alias on the null account", func(c *Config) {
			c.Tenant.Aliases = map[string]AliasTarget{"acme": {AccountID: 4294967295}}
		}, "reserved for unknown tenants"},
		{"alias below the range", func(c *Config) {
			c.Tenant.Aliases = map[string]AliasTarget{"acme": {AccountID: 1001}, "staging": {AccountID: 2147483647}}
		}, ""},
		{"alias inside a custom range", func(c *Config) {
			c.Tenant.AutoRegisterMinID, c.Tenant.AutoRegisterMaxID = 1000, 2000
			c.Tenant.Aliases = map[string]AliasTarget{"acme": {AccountID: 1001}}
		}, "inside the reserved auto-register range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := autoRegisterCfg(tc.mutate).Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("expected an error containing %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestTenantAutoRegisterRangeMerge(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Tenant.AutoRegisterMinID = 5000
	overlay.Tenant.AutoRegisterMaxID = 6000
	mergeConfig(base, overlay)
	if base.Tenant.AutoRegisterMinID != 5000 || base.Tenant.AutoRegisterMaxID != 6000 {
		t.Fatalf("merged range = [%d, %d]", base.Tenant.AutoRegisterMinID, base.Tenant.AutoRegisterMaxID)
	}
	mergeConfig(base, &Config{}) // an empty overlay keeps the range
	if base.Tenant.AutoRegisterMinID != 5000 || base.Tenant.AutoRegisterMaxID != 6000 {
		t.Fatalf("empty overlay changed the range: [%d, %d]", base.Tenant.AutoRegisterMinID, base.Tenant.AutoRegisterMaxID)
	}
}
