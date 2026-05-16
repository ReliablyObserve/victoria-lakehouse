package manifest

import "testing"

func TestTenantSummaries_IntegerTemplate(t *testing.T) {
	m := New("test-bucket", "42/3/logs/")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	m.AddFile("dt=2026-05-15/hour=14", FileInfo{
		Key:  "42/3/logs/dt=2026-05-15/hour=14/abc.parquet",
		Size: 1000,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("got %d summaries, want 1", len(summaries))
	}
	if summaries[0].AccountID != "42" || summaries[0].ProjectID != "3" {
		t.Errorf("tenant = %s/%s, want 42/3", summaries[0].AccountID, summaries[0].ProjectID)
	}
}

func TestTenantSummaries_OrgIDTemplate(t *testing.T) {
	m := New("test-bucket", "prod-team-eu_staging/logs/")
	m.SetPrefixTemplate("{OrgID}/")

	m.AddFile("dt=2026-05-15/hour=14", FileInfo{
		Key:  "prod-team-eu_staging/logs/dt=2026-05-15/hour=14/abc.parquet",
		Size: 2000,
	})

	summaries := m.TenantSummaries()
	if len(summaries) != 1 {
		t.Fatalf("got %d summaries, want 1", len(summaries))
	}
	if summaries[0].AccountID != "prod-team-eu_staging" {
		t.Errorf("AccountID = %q, want %q", summaries[0].AccountID, "prod-team-eu_staging")
	}
	if summaries[0].ProjectID != "" {
		t.Errorf("ProjectID = %q, want empty for OrgID template", summaries[0].ProjectID)
	}
}
