// internal/election/s3.go
package election

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// S3Store abstracts S3 operations used by S3Elector, allowing test mocks.
type S3Store interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// S3Lock is the JSON structure stored in the S3 lock file.
type S3Lock struct {
	Holder    string    `json:"holder"`
	Address   string    `json:"address"`
	Acquired  time.Time `json:"acquired"`
	Heartbeat time.Time `json:"heartbeat"`
}

// S3ElectorConfig holds all configuration for S3Elector.
type S3ElectorConfig struct {
	LockKey            string
	Identity           string
	Address            string
	HeartbeatInterval  time.Duration
	LockTTL            time.Duration
	HealthCheckTimeout time.Duration
	Logger             *slog.Logger
}

// S3Elector implements Leader using an S3 lock file with liveness detection.
type S3Elector struct {
	cfg      S3ElectorConfig
	store    S3Store
	isLeader atomic.Bool

	mu     sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewS3Elector constructs an S3Elector. Defaults are applied for zero-value durations.
func NewS3Elector(store S3Store, cfg S3ElectorConfig) *S3Elector {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.LockTTL == 0 {
		cfg.LockTTL = 30 * time.Second
	}
	if cfg.HealthCheckTimeout == 0 {
		cfg.HealthCheckTimeout = 3 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &S3Elector{
		cfg:    cfg,
		store:  store,
		stopCh: make(chan struct{}),
	}
}

// IsLeader reports whether this instance currently holds the lock.
func (e *S3Elector) IsLeader() bool {
	return e.isLeader.Load()
}

// Start begins the election loop. It returns immediately; the loop runs in a goroutine.
func (e *S3Elector) Start(ctx context.Context) {
	e.mu.Lock()
	// Reset stop channel if re-used after Stop.
	select {
	case <-e.stopCh:
		e.stopCh = make(chan struct{})
	default:
	}
	e.mu.Unlock()

	e.wg.Add(1)
	go e.run(ctx)
}

// Stop releases the lock and shuts down the election loop.
func (e *S3Elector) Stop() {
	e.mu.Lock()
	select {
	case <-e.stopCh:
		// already closed
	default:
		close(e.stopCh)
	}
	e.mu.Unlock()

	e.wg.Wait()
}

func (e *S3Elector) run(ctx context.Context) {
	defer e.wg.Done()

	// Attempt acquisition immediately, then on heartbeat ticker.
	e.tryAcquire(ctx)

	ticker := time.NewTicker(e.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			e.release(ctx)
			return
		case <-ctx.Done():
			e.release(ctx)
			return
		case <-ticker.C:
			if e.isLeader.Load() {
				e.heartbeat(ctx)
			} else {
				e.tryAcquire(ctx)
			}
		}
	}
}

// tryAcquire attempts to acquire the S3 lock.
func (e *S3Elector) tryAcquire(ctx context.Context) {
	data, err := e.store.Download(ctx, e.cfg.LockKey)
	if err != nil {
		// Lock does not exist — attempt to write it.
		e.writeLock(ctx)
		return
	}

	var lock S3Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		// Corrupted lock — overwrite.
		e.cfg.Logger.Warn("s3elector: corrupted lock, overwriting", "key", e.cfg.LockKey, "err", err)
		e.writeLock(ctx)
		return
	}

	// We already hold the lock — renew heartbeat.
	if lock.Holder == e.cfg.Identity {
		e.heartbeat(ctx)
		return
	}

	// Someone else holds the lock. Check liveness.
	if e.isHolderAlive(lock) {
		e.cfg.Logger.Debug("s3elector: another holder is alive, not taking over",
			"holder", lock.Holder, "address", lock.Address)
		return
	}

	// Holder is dead — take over.
	e.cfg.Logger.Info("s3elector: holder appears dead, taking over",
		"holder", lock.Holder, "address", lock.Address)
	e.writeLock(ctx)
}

// isHolderAlive returns true if the existing lock holder is still alive,
// either because the heartbeat is fresh or because its /health endpoint responds 200.
func (e *S3Elector) isHolderAlive(lock S3Lock) bool {
	// Check TTL first — if heartbeat is fresh enough, no need for HTTP call.
	if time.Since(lock.Heartbeat) < e.cfg.LockTTL {
		// Heartbeat is within TTL; do an HTTP health check to confirm.
		if lock.Address != "" {
			return e.httpHealthCheck(lock.Address)
		}
		return true
	}
	// Heartbeat expired — holder is considered dead regardless of HTTP.
	return false
}

// httpHealthCheck performs GET http://{address}/health and returns true on HTTP 200.
func (e *S3Elector) httpHealthCheck(address string) bool {
	client := &http.Client{Timeout: e.cfg.HealthCheckTimeout}
	url := fmt.Sprintf("http://%s/health", address)
	resp, err := client.Get(url) //nolint:noctx
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// writeLock writes our identity to the S3 lock file and marks us as leader.
func (e *S3Elector) writeLock(ctx context.Context) {
	now := time.Now().UTC()
	lock := S3Lock{
		Holder:    e.cfg.Identity,
		Address:   e.cfg.Address,
		Acquired:  now,
		Heartbeat: now,
	}
	data, err := json.Marshal(lock)
	if err != nil {
		e.cfg.Logger.Error("s3elector: failed to marshal lock", "err", err)
		return
	}
	if err := e.store.Upload(ctx, e.cfg.LockKey, data); err != nil {
		e.cfg.Logger.Error("s3elector: failed to write lock", "key", e.cfg.LockKey, "err", err)
		return
	}
	e.isLeader.Store(true)
	e.cfg.Logger.Info("s3elector: acquired leadership", "identity", e.cfg.Identity)
}

// heartbeat updates the Heartbeat timestamp in the lock file.
func (e *S3Elector) heartbeat(ctx context.Context) {
	data, err := e.store.Download(ctx, e.cfg.LockKey)
	if err != nil {
		// Lock disappeared — re-acquire.
		e.isLeader.Store(false)
		e.tryAcquire(ctx)
		return
	}

	var lock S3Lock
	if err := json.Unmarshal(data, &lock); err != nil || lock.Holder != e.cfg.Identity {
		// Lock was taken by someone else.
		e.isLeader.Store(false)
		e.cfg.Logger.Warn("s3elector: lost leadership during heartbeat")
		return
	}

	lock.Heartbeat = time.Now().UTC()
	updated, err := json.Marshal(lock)
	if err != nil {
		e.cfg.Logger.Error("s3elector: failed to marshal heartbeat", "err", err)
		return
	}
	if err := e.store.Upload(ctx, e.cfg.LockKey, updated); err != nil {
		e.cfg.Logger.Error("s3elector: failed to write heartbeat", "err", err)
	}
}

// release deletes the lock file if we hold it.
func (e *S3Elector) release(ctx context.Context) {
	if !e.isLeader.Load() {
		return
	}
	// Use a background context since the caller's context may already be cancelled.
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.Delete(releaseCtx, e.cfg.LockKey); err != nil {
		e.cfg.Logger.Error("s3elector: failed to release lock", "key", e.cfg.LockKey, "err", err)
	} else {
		e.cfg.Logger.Info("s3elector: released leadership", "identity", e.cfg.Identity)
	}
	e.isLeader.Store(false)
}
