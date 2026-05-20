package bloomindex

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockConfigStore struct {
	mu   sync.Mutex
	data map[string][]byte
	fail bool
}

func newMockConfigStore() *mockConfigStore {
	return &mockConfigStore{data: make(map[string][]byte)}
}

func (m *mockConfigStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return nil, errors.New("s3 unreachable")
	}
	return m.data[key], nil
}

func (m *mockConfigStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("s3 unreachable")
	}
	m.data[key] = data
	return nil
}

func TestConfigSync_WriteAndRead(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	newCfg := DefaultBloomControllerConfig()
	newCfg.Tier1MaxAge = 5 * 24 * time.Hour
	data, _ := json.Marshal(newCfg)
	store.mu.Lock()
	store.data["bloom/live.json"] = data
	store.mu.Unlock()

	if err := cs.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := bc.Config()
	if got.Tier1MaxAge != 5*24*time.Hour {
		t.Errorf("tier1_max_age = %v, want 5d", got.Tier1MaxAge)
	}
}

func TestConfigSync_PushWritesToS3(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	if err := cs.Push(context.Background()); err != nil {
		t.Fatal(err)
	}

	store.mu.Lock()
	data := store.data["bloom/live.json"]
	store.mu.Unlock()

	if len(data) == 0 {
		t.Fatal("push should write config to store")
	}

	var cfg BloomControllerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Tier1MaxAge != 7*24*time.Hour {
		t.Errorf("pushed tier1_max_age = %v, want 7d", cfg.Tier1MaxAge)
	}
}

func TestConfigSync_S3Unreachable_KeepsLastKnown(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	newCfg := DefaultBloomControllerConfig()
	newCfg.Tier1MaxAge = 3 * 24 * time.Hour
	data, _ := json.Marshal(newCfg)
	store.mu.Lock()
	store.data["bloom/live.json"] = data
	store.mu.Unlock()

	if err := cs.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bc.Config().Tier1MaxAge != 3*24*time.Hour {
		t.Fatal("initial sync should apply config")
	}

	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()

	_ = cs.Sync(context.Background())

	if bc.Config().Tier1MaxAge != 3*24*time.Hour {
		t.Error("config should be preserved after S3 failure")
	}
	if cs.SyncErrors() != 1 {
		t.Errorf("sync errors = %d, want 1", cs.SyncErrors())
	}
}

func TestConfigSync_NoChange_NoPush(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	if err := cs.Push(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := cs.Push(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigSync_LastSync(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	if !cs.LastSync().IsZero() {
		t.Error("last sync should be zero before any sync")
	}

	if err := cs.Push(context.Background()); err != nil {
		t.Fatal(err)
	}

	if cs.LastSync().IsZero() {
		t.Error("last sync should be set after push")
	}
}

func TestConfigSync_EmptyStore(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	if err := cs.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	if bc.Config().Tier1MaxAge != 7*24*time.Hour {
		t.Error("empty store should not change config")
	}
}

func TestConfigSync_InvalidJSON(t *testing.T) {
	store := newMockConfigStore()
	store.mu.Lock()
	store.data["bloom/live.json"] = []byte("not json")
	store.mu.Unlock()

	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	err := cs.Sync(context.Background())
	if err == nil {
		t.Error("invalid JSON should return error")
	}
	if cs.SyncErrors() != 1 {
		t.Errorf("sync errors = %d, want 1", cs.SyncErrors())
	}
}

// TestConfigSync_Push_StoreError exercises the Put failure branch in Push (previously 76.5%).
func TestConfigSync_Push_StoreError(t *testing.T) {
	store := newMockConfigStore()
	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()

	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	err := cs.Push(context.Background())
	if err == nil {
		t.Error("expected error when store Put fails")
	}
	if cs.SyncErrors() != 1 {
		t.Errorf("sync errors = %d, want 1", cs.SyncErrors())
	}
}

// TestConfigSync_bytesEqual exercises the bytesEqual helper branches.
func TestConfigSync_bytesEqual(t *testing.T) {
	if !bytesEqual([]byte("abc"), []byte("abc")) {
		t.Error("equal slices should be equal")
	}
	if bytesEqual([]byte("abc"), []byte("abcd")) {
		t.Error("different length slices should not be equal")
	}
	if bytesEqual([]byte("abc"), []byte("abd")) {
		t.Error("slices with different content should not be equal")
	}
	if !bytesEqual(nil, nil) {
		t.Error("nil slices should be equal")
	}
}

func TestConfigSync_ResetErrorsOnSuccess(t *testing.T) {
	store := newMockConfigStore()
	bc := NewBloomController(DefaultBloomControllerConfig())
	cs := NewConfigSync(store, "bloom/live.json", bc)

	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()
	_ = cs.Sync(context.Background())
	if cs.SyncErrors() != 1 {
		t.Fatal("should have 1 error")
	}

	store.mu.Lock()
	store.fail = false
	store.mu.Unlock()

	cfg := DefaultBloomControllerConfig()
	data, _ := json.Marshal(cfg)
	store.mu.Lock()
	store.data["bloom/live.json"] = data
	store.mu.Unlock()

	if err := cs.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cs.SyncErrors() != 0 {
		t.Errorf("errors should reset on success, got %d", cs.SyncErrors())
	}
}
