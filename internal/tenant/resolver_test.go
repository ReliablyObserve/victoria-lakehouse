package tenant

import "testing"

func TestResolver_ResolveAndDisplayName(t *testing.T) {
	r := NewResolver(ResolverConfig{
		MetricsFormat: MetricsFormatID,
	})

	_ = r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})
	_ = r.AddAlias("dev_default", TenantID{AccountID: 1, ProjectID: 1})

	tid, ok := r.Resolve("prod-team-eu_staging")
	if !ok {
		t.Fatal("expected to resolve prod-team-eu_staging")
	}
	if tid.AccountID != 42 || tid.ProjectID != 3 {
		t.Errorf("got %+v, want {42, 3}", tid)
	}

	_, ok = r.Resolve("unknown")
	if ok {
		t.Error("expected unknown alias to return false")
	}

	name := r.DisplayName(42, 3)
	if name != "prod-team-eu_staging" {
		t.Errorf("DisplayName(42, 3) = %q, want %q", name, "prod-team-eu_staging")
	}

	name = r.DisplayName(99, 99)
	if name != "99:99" {
		t.Errorf("DisplayName(99, 99) = %q, want %q", name, "99:99")
	}
}

func TestResolver_MetricLabel(t *testing.T) {
	tests := []struct {
		format MetricsFormat
		acc    uint32
		proj   uint32
		want   string
	}{
		{MetricsFormatID, 42, 3, "42:3"},
		{MetricsFormatName, 42, 3, "prod-team-eu_staging"},
		{MetricsFormatName, 99, 99, "99:99"},
		{MetricsFormatBoth, 42, 3, "prod-team-eu_staging"},
		{MetricsFormatBoth, 99, 99, "99:99"},
	}

	for _, tc := range tests {
		r := NewResolver(ResolverConfig{MetricsFormat: tc.format})
		_ = r.AddAlias("prod-team-eu_staging", TenantID{AccountID: 42, ProjectID: 3})

		got := r.MetricLabel(tc.acc, tc.proj)
		if got != tc.want {
			t.Errorf("MetricLabel(%d, %d) format=%v = %q, want %q", tc.acc, tc.proj, tc.format, got, tc.want)
		}
	}
}

func TestResolver_RemoveAlias(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("test_alias", TenantID{AccountID: 10, ProjectID: 20})

	_, ok := r.Resolve("test_alias")
	if !ok {
		t.Fatal("expected alias to exist")
	}

	r.RemoveAlias("test_alias")

	_, ok = r.Resolve("test_alias")
	if ok {
		t.Error("expected alias removed")
	}

	name := r.DisplayName(10, 20)
	if name != "10:20" {
		t.Errorf("after remove, DisplayName = %q, want %q", name, "10:20")
	}
}

func TestResolver_AllAliases(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	r.AddAlias("a_one", TenantID{AccountID: 1, ProjectID: 1})
	r.AddAlias("b_two", TenantID{AccountID: 2, ProjectID: 2})

	all := r.AllAliases()
	if len(all) != 2 {
		t.Errorf("AllAliases() len = %d, want 2", len(all))
	}
}

func TestResolver_AddAlias_Validation(t *testing.T) {
	r := NewResolver(ResolverConfig{})

	if err := r.AddAlias("has/slash", TenantID{AccountID: 1, ProjectID: 1}); err == nil {
		t.Error("expected validation error for slash")
	}

	if err := r.AddAlias("valid_alias", TenantID{AccountID: 1, ProjectID: 1}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
