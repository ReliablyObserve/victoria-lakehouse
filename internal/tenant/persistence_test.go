package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

type errPool struct {
	err error
}

func (e *errPool) Upload(_ context.Context, _ string, _ []byte) error {
	return e.err
}

func (e *errPool) Download(_ context.Context, _ string) ([]byte, error) {
	return nil, e.err
}

type mockPool struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMockPool() *mockPool {
	return &mockPool{data: make(map[string][]byte)}
}

func (m *mockPool) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockPool) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	return d, nil
}

func TestS3Persister_SaveAndLoad(t *testing.T) {
	pool := newMockPool()
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	entries := []AliasEntry{
		{OrgID: "prod_staging", AccountID: 42, ProjectID: 3, Source: "api"},
		{OrgID: "dev_default", AccountID: 1, ProjectID: 1, Source: "api"},
	}

	if err := p.SaveAliases(entries); err != nil {
		t.Fatal(err)
	}

	loaded, err := p.LoadAliases()
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded) != 2 {
		t.Fatalf("loaded %d aliases, want 2", len(loaded))
	}

	if loaded[0].OrgID != "prod_staging" || loaded[0].AccountID != 42 {
		t.Errorf("first alias = %+v, unexpected", loaded[0])
	}
}

func TestS3Persister_LoadEmpty(t *testing.T) {
	pool := newMockPool()
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	loaded, err := p.LoadAliases()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 aliases from empty store, got %d", len(loaded))
	}
}

func TestS3Persister_SaveAliases_UploadError(t *testing.T) {
	pool := &errPool{err: errors.New("s3 upload failed")}
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	err := p.SaveAliases([]AliasEntry{{OrgID: "test", AccountID: 1, ProjectID: 1}})
	if err == nil {
		t.Error("expected upload error, got nil")
	}
}

func TestS3Persister_LoadAliases_DownloadError(t *testing.T) {
	pool := &errPool{err: errors.New("s3 download failed")}
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	_, err := p.LoadAliases()
	if err == nil {
		t.Error("expected download error, got nil")
	}
}

func TestS3Persister_LoadAliases_InvalidJSON(t *testing.T) {
	pool := newMockPool()
	// Store invalid JSON at the expected key.
	_ = pool.Upload(context.Background(), "_meta/tenant-aliases.json", []byte("not-json["))
	p := NewS3Persister(pool, "_meta/tenant-aliases.json")

	_, err := p.LoadAliases()
	if err == nil {
		t.Error("expected error for invalid JSON in storage, got nil")
	}
}

func TestS3Persister_Roundtrip_JSON(t *testing.T) {
	entries := []AliasEntry{
		{OrgID: "acme-corp_us-east_production", AccountID: 100, ProjectID: 50, Source: "config"},
	}
	data, _ := json.Marshal(entries)

	var out []AliasEntry
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out[0].OrgID != "acme-corp_us-east_production" {
		t.Errorf("roundtrip failed: %+v", out[0])
	}
}
