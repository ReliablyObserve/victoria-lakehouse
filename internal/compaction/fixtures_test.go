package compaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// markListed puts a fixture manifest in the state a running node reaches after
// its first bucket listing: until then nothing infers "the object is gone" from
// an absent key (see Manifest.Listed).
func markListed(t *testing.T, m *manifest.Manifest, keys []string) {
	t.Helper()
	objects := make([]manifest.ListedObject, 0, len(keys))
	for _, k := range keys {
		objects = append(objects, manifest.ListedObject{Key: k, Size: 1})
	}
	if !m.ApplyListing(objects, time.Now()) {
		t.Fatal("fixture: the listing was rejected")
	}
}

// gatedPool blocks the first Upload whose key matches a predicate until
// released, which lets a test freeze one actor between its read and its
// publish while the other actor runs to completion.
type gatedPool struct {
	*mockPool

	mu       sync.Mutex
	match    func(key string) bool
	reached  chan struct{}
	release  chan struct{}
	consumed bool

	failDelete  func(key string) bool
	afterMatch  func(key string) bool
	afterUpload func(key string)
}

// A source can disappear between the manifest lookup and GET when the other
// actor retires it. S3 reports an error in that case, not an empty successful
// object. Preserve that contract in the concurrent fixture.
func (g *gatedPool) Download(ctx context.Context, key string) ([]byte, error) {
	data, err := g.mockPool.Download(ctx, key)
	if err == nil && data == nil {
		return nil, fmt.Errorf("object %q not found", key)
	}
	return data, err
}

func TestDeleteRace_DownloadRetiredSourceReturnsError(t *testing.T) {
	p := &gatedPool{mockPool: newMockPool()}
	p.put("source.parquet", []byte("source"))
	if err := p.Delete(context.Background(), "source.parquet"); err != nil {
		t.Fatal(err)
	}
	if data, err := p.Download(context.Background(), "source.parquet"); err == nil || data != nil {
		t.Fatalf("missing source GET returned data=%q err=%v", data, err)
	}
}

func (g *gatedPool) Upload(ctx context.Context, key string, data []byte) error {
	g.mu.Lock()
	hit := g.match != nil && !g.consumed && g.match(key)
	var reached, release chan struct{}
	if hit {
		g.consumed = true
		reached, release = g.reached, g.release
	}
	g.mu.Unlock()
	if hit {
		close(reached)
		<-release
	}
	if err := g.mockPool.Upload(ctx, key, data); err != nil {
		return err
	}
	g.mu.Lock()
	after := g.afterUpload
	if after != nil && g.afterMatch != nil && g.afterMatch(key) {
		g.afterUpload = nil
	} else {
		after = nil
	}
	g.mu.Unlock()
	if after != nil {
		after(key)
	}
	return nil
}

// Delete fails for every key failDelete matches, until the predicate is
// cleared — a delete that keeps failing, not a single blip.
func (g *gatedPool) Delete(ctx context.Context, key string) error {
	g.mu.Lock()
	fail := g.failDelete != nil && g.failDelete(key)
	g.mu.Unlock()
	if fail {
		return errors.New("injected delete failure")
	}
	return g.mockPool.Delete(ctx, key)
}

func (g *gatedPool) setFailDelete(match func(string) bool) {
	g.mu.Lock()
	g.failDelete = match
	g.mu.Unlock()
}
