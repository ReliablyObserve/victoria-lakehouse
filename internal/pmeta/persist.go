package pmeta

import (
	"bytes"
	"context"
	"errors"
	"sync"
)

// ErrNotFound is returned by an ObjectStore.GetObject when the key is absent.
// WarmPartitions treats it as "rebuild this partition from files".
var ErrNotFound = errors.New("pmeta: object not found")

// ObjectStore is the minimal S3 surface pmeta needs. The real implementation
// adapts the repo's S3 client (exported API only); tests use an in-memory map.
// One bundle object per partition → one GET to warm, one PUT to persist.
type ObjectStore interface {
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, data []byte) error
}

// SetPrefix sets the S3 key prefix under which partition bundles live.
func (s *Store) SetPrefix(prefix string) {
	s.mu.Lock()
	s.prefix = prefix
	s.mu.Unlock()
}

// bundleKey is the S3 object key for a partition's bundle.
func (s *Store) bundleKey(partition string) string {
	s.mu.RLock()
	p := s.prefix
	s.mu.RUnlock()
	return p + partition + "/_pmeta.bundle"
}

// PersistDirty writes every dirty partition's bundle to the object store (one
// PUT each) and clears its dirty flag on success. Returns the number persisted.
// On the first PUT error it stops and returns the error with the count so far
// (the un-persisted partitions stay dirty and are retried next cycle).
func (s *Store) PersistDirty(ctx context.Context, os ObjectStore) (int, error) {
	parts := s.DirtyPartitions()
	n := 0
	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		b := s.Bundle(p)
		// Snapshot the generation BEFORE encoding: contributions that arrive
		// while the encode/PUT is in flight bump gen past this snapshot, so the
		// bundle stays dirty and they persist next cycle (no lost update).
		g := b.snapshotGen()
		var buf bytes.Buffer
		if err := b.Encode(&buf); err != nil {
			return n, err
		}
		if err := os.PutObject(ctx, s.bundleKey(p), buf.Bytes()); err != nil {
			return n, err
		}
		b.persisted(g)
		n++
	}
	return n, nil
}

// WarmResult reports the outcome of a WarmPartitions sweep.
type WarmResult struct {
	Loaded        int                    // bundles loaded into the store
	NeedsRebuild  []string               // partitions whose bundle was missing or structurally corrupt → rebuild whole
	SkippedFacets map[string][]FacetKind // partition → per-facet failures (CRC/unknown/decode) → rebuild those facets
}

// WarmPartitions loads the given partitions' bundles from the object store with
// bounded concurrency (one GET each). A missing object or a structural decode
// error routes the partition to NeedsRebuild; per-facet failures (the bundle
// decoded but a facet's CRC failed / kind unknown) are recorded in SkippedFacets.
// Both are the self-heal signal: the caller replays those partitions' files
// through Store.Rebuild. WarmPartitions itself never errors on bad data — only
// ctx cancellation stops it early.
func (s *Store) WarmPartitions(ctx context.Context, os ObjectStore, partitions []string, concurrency int) WarmResult {
	if concurrency < 1 {
		concurrency = 1
	}
	reg := s.Registry()

	type out struct {
		partition string
		bundle    *Bundle
		skipped   []FacetKind
		rebuild   bool
	}
	jobs := make(chan string)
	results := make(chan out)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					results <- out{partition: p, rebuild: true}
					continue
				}
				data, err := os.GetObject(ctx, s.bundleKey(p))
				if err != nil {
					results <- out{partition: p, rebuild: true} // missing/transport → rebuild
					continue
				}
				b, res, derr := DecodeBundle(bytes.NewReader(data), reg)
				if derr != nil {
					results <- out{partition: p, rebuild: true} // structural → whole-partition rebuild
					continue
				}
				results <- out{partition: p, bundle: b, skipped: res.Skipped}
			}
		}()
	}
	go func() {
		for _, p := range partitions {
			jobs <- p
		}
		close(jobs)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	wr := WarmResult{SkippedFacets: map[string][]FacetKind{}}
	for r := range results {
		if r.rebuild {
			wr.NeedsRebuild = append(wr.NeedsRebuild, r.partition)
			continue
		}
		// PutWarm, not Put: with serve-while-warming a flush may already have
		// populated this partition's live bundle — absorb the decoded content
		// instead of clobbering those contributions (and their dirty state).
		s.PutWarm(r.bundle)
		wr.Loaded++
		if len(r.skipped) > 0 {
			wr.SkippedFacets[r.partition] = r.skipped
		}
	}
	return wr
}
