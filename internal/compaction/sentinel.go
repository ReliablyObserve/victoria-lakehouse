package compaction

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type SentinelStore interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type sentinelData struct {
	Holder    string    `json:"holder"`
	CreatedAt time.Time `json:"created_at"`
}

type Sentinel struct {
	store        SentinelStore
	staleTimeout time.Duration
}

func NewSentinel(store SentinelStore, staleTimeout time.Duration) *Sentinel {
	return &Sentinel{store: store, staleTimeout: staleTimeout}
}

func sentinelKey(prefix, partition string) string {
	return fmt.Sprintf("%s%s/_compacting", prefix, partition)
}

func (s *Sentinel) Acquire(ctx context.Context, prefix, partition, holder string) (bool, error) {
	key := sentinelKey(prefix, partition)
	locked, err := s.IsLocked(ctx, prefix, partition)
	if err != nil {
		return false, err
	}
	if locked {
		return false, nil
	}
	data, _ := json.Marshal(sentinelData{Holder: holder, CreatedAt: time.Now()})
	if err := s.store.Upload(ctx, key, data); err != nil {
		return false, fmt.Errorf("write sentinel: %w", err)
	}
	return true, nil
}

func (s *Sentinel) IsLocked(ctx context.Context, prefix, partition string) (bool, error) {
	key := sentinelKey(prefix, partition)
	data, err := s.store.Download(ctx, key)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil
	}
	var sd sentinelData
	if err := json.Unmarshal(data, &sd); err != nil {
		return false, nil
	}
	if time.Since(sd.CreatedAt) > s.staleTimeout {
		return false, nil
	}
	return true, nil
}

func (s *Sentinel) Release(ctx context.Context, prefix, partition string) error {
	return s.store.Delete(ctx, sentinelKey(prefix, partition))
}
