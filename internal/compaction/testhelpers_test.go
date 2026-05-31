// internal/compaction/testhelpers_test.go
//
// Shared test helpers for the compaction package. This file replaces the
// helper types that previously lived in sentinel_test.go (deleted in PR A
// alongside the rest of the election machinery).
//
// mockPool implements just enough of the S3 surface (Upload / Download /
// Delete) for compaction tests to drive scheduler / compactor / coverage
// hardening flows without standing up a real pool. It is intentionally
// minimal — anything beyond the three CRUD operations should be added in
// a dedicated mock alongside the test that needs it.

package compaction

import (
	"context"
	"sync"
)

type mockPool struct {
	mu       sync.Mutex
	uploaded map[string][]byte
}

func newMockPool() *mockPool {
	return &mockPool{uploaded: make(map[string][]byte)}
}

func (m *mockPool) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploaded[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockPool) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.uploaded[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), d...), nil
}

func (m *mockPool) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.uploaded, key)
	return nil
}
