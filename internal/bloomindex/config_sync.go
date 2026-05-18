package bloomindex

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// ConfigStore abstracts S3 operations for config persistence.
type ConfigStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
}

// ConfigSync synchronizes bloom configuration with S3.
type ConfigSync struct {
	mu         sync.RWMutex
	store      ConfigStore
	key        string
	controller *BloomController
	lastConfig []byte
	lastSync   time.Time
	syncErrors int
}

// NewConfigSync creates a new config syncer.
func NewConfigSync(store ConfigStore, key string, controller *BloomController) *ConfigSync {
	return &ConfigSync{
		store:      store,
		key:        key,
		controller: controller,
	}
}

// Sync reads the latest config from S3 and applies it.
func (cs *ConfigSync) Sync(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	data, err := cs.store.Get(ctx, cs.key)
	if err != nil {
		cs.syncErrors++
		logger.Warnf("config sync failed: %v", err)
		return err
	}

	if data == nil || len(data) == 0 {
		return nil
	}

	if bytesEqual(data, cs.lastConfig) {
		cs.lastSync = time.Now()
		return nil
	}

	var cfg BloomControllerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		cs.syncErrors++
		logger.Warnf("config sync unmarshal failed: %v", err)
		return err
	}

	cs.controller.ApplyConfig(cfg)
	cs.lastConfig = data
	cs.lastSync = time.Now()
	cs.syncErrors = 0

	return nil
}

// Push writes the current config to S3 (leader-only).
func (cs *ConfigSync) Push(ctx context.Context) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cfg := cs.controller.Config()
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}

	if bytesEqual(data, cs.lastConfig) {
		return nil
	}

	if err := cs.store.Put(ctx, cs.key, data); err != nil {
		cs.syncErrors++
		return err
	}

	cs.lastConfig = data
	cs.lastSync = time.Now()
	cs.syncErrors = 0
	return nil
}

// LastSync returns the timestamp of the last successful sync.
func (cs *ConfigSync) LastSync() time.Time {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.lastSync
}

// SyncErrors returns the number of consecutive sync failures.
func (cs *ConfigSync) SyncErrors() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.syncErrors
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
