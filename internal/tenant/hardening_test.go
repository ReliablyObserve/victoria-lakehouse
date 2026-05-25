package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestMetricLabel_BothFormat_IncludesIDAndName(t *testing.T) {
	r := NewResolver(ResolverConfig{MetricsFormat: MetricsFormatBoth})
	_ = r.AddAlias("prod-eu", TenantID{AccountID: 42, ProjectID: 3})

	got := r.MetricLabel(42, 3)
	if got != "42:3/prod-eu" {
		t.Errorf("MetricLabel(42,3) with Both = %q, want %q", got, "42:3/prod-eu")
	}

	got = r.MetricLabel(99, 99)
	if got != "99:99" {
		t.Errorf("MetricLabel(99,99) no alias = %q, want %q", got, "99:99")
	}
}

func TestLoadAliases_SkipsInvalidOrgIDs(t *testing.T) {
	entries := []AliasEntry{
		{OrgID: "valid_alias", AccountID: 1, ProjectID: 1},
		{OrgID: "has/slash", AccountID: 2, ProjectID: 2},
		{OrgID: "", AccountID: 3, ProjectID: 3},
		{OrgID: "also-valid", AccountID: 4, ProjectID: 4},
	}
	data, _ := json.Marshal(entries)

	pool := &mockS3Pool{data: data}
	p := NewS3Persister(pool, "test-key")

	loaded, err := p.LoadAliases()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 valid entries, got %d", len(loaded))
	}
	if loaded[0].OrgID != "valid_alias" {
		t.Errorf("first entry = %q, want valid_alias", loaded[0].OrgID)
	}
	if loaded[1].OrgID != "also-valid" {
		t.Errorf("second entry = %q, want also-valid", loaded[1].OrgID)
	}
}

func TestConcurrentAddRemoveAliases(t *testing.T) {
	r := NewResolver(ResolverConfig{MetricsFormat: MetricsFormatID})

	var wg sync.WaitGroup
	for i := uint32(0); i < 100; i++ {
		wg.Add(2)
		i := i
		go func() {
			defer wg.Done()
			_ = r.AddAlias("alias_"+string(rune('a'+i%26)), TenantID{AccountID: i, ProjectID: i})
		}()
		go func() {
			defer wg.Done()
			r.RemoveAlias("alias_" + string(rune('a'+i%26)))
		}()
	}
	wg.Wait()

	_ = r.AllAliases()
	_ = r.HasAliases()
}

func TestConcurrentResolveAndMetricLabel(t *testing.T) {
	r := NewResolver(ResolverConfig{MetricsFormat: MetricsFormatBoth})
	for i := uint32(0); i < 50; i++ {
		_ = r.AddAlias("tenant_"+string(rune('a'+i%26)), TenantID{AccountID: i, ProjectID: i})
	}

	var wg sync.WaitGroup
	for i := uint32(0); i < 200; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			r.Resolve("tenant_" + string(rune('a'+i%26)))
			r.DisplayName(i%50, i%50)
			r.MetricLabel(i%50, i%50)
		}()
	}
	wg.Wait()
}

type mockS3Pool struct {
	data    []byte
	saveErr error
}

func (m *mockS3Pool) Upload(_ context.Context, _ string, data []byte) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.data = data
	return nil
}

func (m *mockS3Pool) Download(_ context.Context, _ string) ([]byte, error) {
	return m.data, nil
}

func TestPersisterSaveFailure_DoesNotCorruptResolver(t *testing.T) {
	pool := &mockS3Pool{saveErr: errors.New("S3 unavailable")}
	p := NewS3Persister(pool, "test-key")

	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("test_alias", TenantID{AccountID: 1, ProjectID: 1})

	err := p.SaveAliases(r.AllAliases())
	if err == nil {
		t.Error("expected save error")
	}

	tid, ok := r.Resolve("test_alias")
	if !ok {
		t.Fatal("alias should still exist in resolver despite save failure")
	}
	if tid.AccountID != 1 {
		t.Errorf("AccountID = %d, want 1", tid.AccountID)
	}
}
