package config

import (
	"strings"
	"testing"
)

func TestTenantConfig_Defaults(t *testing.T) {
	cfg := Default()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"PrefixTemplate", cfg.Tenant.PrefixTemplate, "{AccountID}/{ProjectID}/"},
		{"Isolation", cfg.Tenant.Isolation, "prefix"},
		{"BucketTemplate", cfg.Tenant.BucketTemplate, ""},
		{"DefaultAccount", cfg.Tenant.DefaultAccount, "0"},
		{"DefaultProject", cfg.Tenant.DefaultProject, "0"},
		{"HeaderAccount", cfg.Tenant.HeaderAccount, "X-Scope-AccountID"},
		{"HeaderProject", cfg.Tenant.HeaderProject, "X-Scope-ProjectID"},
		{"GlobalReadHeader", cfg.Tenant.GlobalReadHeader, ""},
		{"GlobalReadValue", cfg.Tenant.GlobalReadValue, ""},
		{"DefaultPrefix", cfg.Tenant.DefaultPrefix, ""},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("Tenant.%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

func TestTenantConfig_SingleTenantMode(t *testing.T) {
	cfg := Default()
	cfg.Tenant.DefaultAccount = "0"
	cfg.Tenant.DefaultProject = "0"

	if cfg.Tenant.PrefixTemplate != "{AccountID}/{ProjectID}/" {
		t.Fatalf("unexpected prefix template: %s", cfg.Tenant.PrefixTemplate)
	}
	if cfg.Tenant.Isolation != "prefix" {
		t.Fatalf("unexpected isolation: %s", cfg.Tenant.Isolation)
	}
}

func TestTenantConfig_MultiTenantPrefix(t *testing.T) {
	cfg := Default()
	cfg.Tenant.PrefixTemplate = "{AccountID}/{ProjectID}/"
	cfg.Tenant.DefaultAccount = "100"
	cfg.Tenant.DefaultProject = "1"

	if cfg.Tenant.Isolation != "prefix" {
		t.Errorf("isolation = %q, want prefix", cfg.Tenant.Isolation)
	}
	if cfg.Tenant.DefaultAccount != "100" {
		t.Errorf("default account = %q, want 100", cfg.Tenant.DefaultAccount)
	}
}

func TestTenantConfig_BucketIsolation(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.Isolation = "bucket"
	overlay.Tenant.BucketTemplate = "obs-{AccountID}-{ProjectID}"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.Isolation != "bucket" {
		t.Errorf("isolation = %q, want bucket", result.Tenant.Isolation)
	}
	if result.Tenant.BucketTemplate != "obs-{AccountID}-{ProjectID}" {
		t.Errorf("bucket template = %q", result.Tenant.BucketTemplate)
	}
	if result.Tenant.PrefixTemplate != "{AccountID}/{ProjectID}/" {
		t.Errorf("prefix template should be preserved: %q", result.Tenant.PrefixTemplate)
	}
}

func TestTenantConfig_GlobalReadDisabledByDefault(t *testing.T) {
	cfg := Default()

	if cfg.Tenant.GlobalReadHeader != "" {
		t.Errorf("global read header should be empty by default, got %q", cfg.Tenant.GlobalReadHeader)
	}
	if cfg.Tenant.GlobalReadValue != "" {
		t.Errorf("global read value should be empty by default, got %q", cfg.Tenant.GlobalReadValue)
	}
}

func TestTenantConfig_GlobalReadEnabled(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.GlobalReadHeader = "X-Lakehouse-Global-Read"
	overlay.Tenant.GlobalReadValue = "super-secret"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.GlobalReadHeader != "X-Lakehouse-Global-Read" {
		t.Errorf("global read header = %q", result.Tenant.GlobalReadHeader)
	}
	if result.Tenant.GlobalReadValue != "super-secret" {
		t.Errorf("global read value = %q", result.Tenant.GlobalReadValue)
	}
}

func TestTenantConfig_MergePreservesDefaults(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Tenant.Isolation = "bucket"

	result := mergeConfig(base, overlay)

	if result.Tenant.DefaultAccount != "0" {
		t.Errorf("merge should preserve default account: %q", result.Tenant.DefaultAccount)
	}
	if result.Tenant.DefaultProject != "0" {
		t.Errorf("merge should preserve default project: %q", result.Tenant.DefaultProject)
	}
	if result.Tenant.HeaderAccount != "X-Scope-AccountID" {
		t.Errorf("merge should preserve header account: %q", result.Tenant.HeaderAccount)
	}
	if result.Tenant.HeaderProject != "X-Scope-ProjectID" {
		t.Errorf("merge should preserve header project: %q", result.Tenant.HeaderProject)
	}
	if result.Tenant.PrefixTemplate != "{AccountID}/{ProjectID}/" {
		t.Errorf("merge should preserve prefix template: %q", result.Tenant.PrefixTemplate)
	}
}

func TestTenantConfig_CustomHeaders(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.HeaderAccount = "X-Org-ID"
	overlay.Tenant.HeaderProject = "X-Team-ID"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.HeaderAccount != "X-Org-ID" {
		t.Errorf("header account = %q, want X-Org-ID", result.Tenant.HeaderAccount)
	}
	if result.Tenant.HeaderProject != "X-Team-ID" {
		t.Errorf("header project = %q, want X-Team-ID", result.Tenant.HeaderProject)
	}
}

func TestTenantConfig_CustomPrefixTemplate(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.PrefixTemplate = "tenants/{AccountID}/"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.PrefixTemplate != "tenants/{AccountID}/" {
		t.Errorf("prefix template = %q", result.Tenant.PrefixTemplate)
	}
}

func TestTenantConfig_MergeDoesNotOverrideWithEmpty(t *testing.T) {
	base := Default()
	base.Tenant.GlobalReadHeader = "X-Global"
	base.Tenant.GlobalReadValue = "secret"

	overlay := &Config{}

	result := mergeConfig(base, overlay)

	if result.Tenant.GlobalReadHeader != "X-Global" {
		t.Errorf("empty overlay should not clear global read header: %q", result.Tenant.GlobalReadHeader)
	}
	if result.Tenant.GlobalReadValue != "secret" {
		t.Errorf("empty overlay should not clear global read value: %q", result.Tenant.GlobalReadValue)
	}
}

func TestTenantConfig_GlobalReadBearerToken(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.GlobalReadToken = "eyJhbGciOiJIUzI1NiIs"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.GlobalReadToken != "eyJhbGciOiJIUzI1NiIs" {
		t.Errorf("global read token = %q", result.Tenant.GlobalReadToken)
	}
}

func TestTenantConfig_GlobalReadBothMethods(t *testing.T) {
	cfg := Default()
	overlay := &Config{}
	overlay.Tenant.GlobalReadHeader = "X-Admin"
	overlay.Tenant.GlobalReadValue = "secret"
	overlay.Tenant.GlobalReadToken = "bearer-token-123"

	result := mergeConfig(cfg, overlay)

	if result.Tenant.GlobalReadHeader != "X-Admin" {
		t.Errorf("header = %q", result.Tenant.GlobalReadHeader)
	}
	if result.Tenant.GlobalReadValue != "secret" {
		t.Errorf("value = %q", result.Tenant.GlobalReadValue)
	}
	if result.Tenant.GlobalReadToken != "bearer-token-123" {
		t.Errorf("token = %q", result.Tenant.GlobalReadToken)
	}
}

func TestTenantConfig_ResolvedPrefix(t *testing.T) {
	tests := []struct {
		name     string
		tenant   TenantConfig
		expected string
	}{
		{
			"default single tenant",
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "0", DefaultProject: "0"},
			"0/0/",
		},
		{
			"multi-tenant",
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "100", DefaultProject: "42"},
			"100/42/",
		},
		{
			"custom template",
			TenantConfig{PrefixTemplate: "tenants/{AccountID}/", DefaultAccount: "org1", DefaultProject: ""},
			"tenants/org1/",
		},
		{
			"default prefix overrides template",
			TenantConfig{DefaultPrefix: "custom/", PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "0", DefaultProject: "0"},
			"custom/",
		},
		{
			"empty template returns empty",
			TenantConfig{PrefixTemplate: "", DefaultAccount: "0", DefaultProject: "0"},
			"",
		},
		{
			"no account or project returns empty",
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "", DefaultProject: ""},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tenant.ResolvedPrefix(); got != tt.expected {
				t.Errorf("ResolvedPrefix() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTenantConfig_AutoPrefixWithTenant(t *testing.T) {
	tests := []struct {
		name     string
		mode     Mode
		tenant   TenantConfig
		s3prefix string
		expected string
	}{
		{
			"logs with default tenant",
			ModeLogs,
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "0", DefaultProject: "0"},
			"",
			"0/0/logs/",
		},
		{
			"traces with default tenant",
			ModeTraces,
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "0", DefaultProject: "0"},
			"",
			"0/0/traces/",
		},
		{
			"logs with multi-tenant",
			ModeLogs,
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "100", DefaultProject: "1"},
			"",
			"100/1/logs/",
		},
		{
			"s3 prefix overrides everything",
			ModeLogs,
			TenantConfig{PrefixTemplate: "{AccountID}/{ProjectID}/", DefaultAccount: "0", DefaultProject: "0"},
			"override/",
			"override/",
		},
		{
			"no tenant config falls back to signal only",
			ModeLogs,
			TenantConfig{},
			"",
			"logs/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Mode: tt.mode, Tenant: tt.tenant, S3: S3Config{Prefix: tt.s3prefix}}
			if got := cfg.AutoPrefix(); got != tt.expected {
				t.Errorf("AutoPrefix() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTenantConfig_AllFieldsMerge(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Tenant.DefaultPrefix = "custom/"
	overlay.Tenant.PrefixTemplate = "{OrgID}/"
	overlay.Tenant.Isolation = "bucket"
	overlay.Tenant.BucketTemplate = "obs-{OrgID}"
	overlay.Tenant.DefaultAccount = "999"
	overlay.Tenant.DefaultProject = "42"
	overlay.Tenant.HeaderAccount = "X-Org"
	overlay.Tenant.HeaderProject = "X-Proj"
	overlay.Tenant.GlobalReadHeader = "X-Admin"
	overlay.Tenant.GlobalReadValue = "admin-key"
	overlay.Tenant.GlobalReadToken = "bearer-token-xyz"

	result := mergeConfig(base, overlay)

	fields := []struct {
		name string
		got  string
		want string
	}{
		{"DefaultPrefix", result.Tenant.DefaultPrefix, "custom/"},
		{"PrefixTemplate", result.Tenant.PrefixTemplate, "{OrgID}/"},
		{"Isolation", result.Tenant.Isolation, "bucket"},
		{"BucketTemplate", result.Tenant.BucketTemplate, "obs-{OrgID}"},
		{"DefaultAccount", result.Tenant.DefaultAccount, "999"},
		{"DefaultProject", result.Tenant.DefaultProject, "42"},
		{"HeaderAccount", result.Tenant.HeaderAccount, "X-Org"},
		{"HeaderProject", result.Tenant.HeaderProject, "X-Proj"},
		{"GlobalReadHeader", result.Tenant.GlobalReadHeader, "X-Admin"},
		{"GlobalReadValue", result.Tenant.GlobalReadValue, "admin-key"},
		{"GlobalReadToken", result.Tenant.GlobalReadToken, "bearer-token-xyz"},
	}

	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("Tenant.%s = %q, want %q", f.name, f.got, f.want)
		}
	}
}

// TestTenantConfig_PrefixTemplateMustNameATenant: the writer only expands
// {AccountID}/{ProjectID}. A template without them (an {OrgID} one, say) writes
// every tenant under one static prefix; such objects carry no tenant segment,
// which makes them tenant 0:0's data — every other tenant's rows would land in
// 0:0's layout and be readable only by 0:0. Startup must refuse it.
func TestTenantConfig_PrefixTemplateMustNameATenant(t *testing.T) {
	cases := []struct {
		name     string
		template string
		wantErr  bool
	}{
		{"account and project", "{AccountID}/{ProjectID}/", false},
		{"nested under a prefix", "tenants/{AccountID}/{ProjectID}/", false},
		{"empty (single-tenant layout)", "", false},
		// One segment: the signal directory ("logs/") is parsed as the other.
		{"account only", "{AccountID}/", true},
		{"project only", "tenants/{ProjectID}/", true},
		// Placeholders the writer never expands land in the key literally.
		{"orgid only", "{OrgID}/", true},
		{"orgid and project", "{OrgID}/{ProjectID}/", true},
		{"static prefix", "tenants/", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test-bucket"
			cfg.Tenant.PrefixTemplate = tc.template
			err := cfg.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("template %q was accepted; it names no tenant", tc.template)
				}
				if !strings.Contains(err.Error(), "prefix-template") || !strings.Contains(err.Error(), "{AccountID}/{ProjectID}") {
					t.Errorf("error %q should name the flag and the placeholder an operator must add", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("template %q rejected: %v", tc.template, err)
			}
		})
	}
}
