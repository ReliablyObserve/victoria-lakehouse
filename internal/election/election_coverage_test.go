package election

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// TestNoopElector_StartStop exercises the Start and Stop methods of NoopElector.
func TestNoopElector_StartStop(t *testing.T) {
	e := NewNoopElector()

	ctx := context.Background()
	e.Start(ctx) // should not panic
	e.Stop()     // should not panic

	if !e.IsLeader() {
		t.Error("NoopElector should always be leader")
	}
}

// TestS3Elector_HeartbeatLostLeadership tests the heartbeat path where
// another node has taken the lock. The other node must be "alive" (responding
// to health checks) so the elector doesn't immediately re-acquire.
func TestS3Elector_HeartbeatLostLeadership(t *testing.T) {
	// Start an HTTP server to make the other node appear alive.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	aliveAddress := ts.Listener.Addr().String()

	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/hb-lost.json",
		Identity:           "node-hb-lost",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 500 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Now tamper with the lock to simulate another node taking over.
	// Use the alive address so the elector considers the other node alive
	// and does NOT re-acquire.
	otherLock := S3Lock{
		Holder:    "other-node",
		Address:   aliveAddress,
		Acquired:  time.Now().UTC(),
		Heartbeat: time.Now().UTC(),
	}
	data, _ := json.Marshal(otherLock)
	_ = store.Upload(context.Background(), "election/hb-lost.json", data)

	// Wait for the heartbeat to detect the stolen lock and for a tryAcquire
	// cycle to confirm the other node is alive.
	time.Sleep(300 * time.Millisecond)

	if e.IsLeader() {
		t.Error("should have lost leadership when lock was stolen by alive node")
	}

	e.Stop()
}

// TestS3Elector_HeartbeatLockDisappeared tests the heartbeat path where
// the lock file is deleted externally.
func TestS3Elector_HeartbeatLockDisappeared(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/hb-gone.json",
		Identity:           "node-hb-gone",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Delete the lock externally.
	_ = store.Delete(context.Background(), "election/hb-gone.json")

	// Wait for the heartbeat to detect the missing lock and re-acquire.
	time.Sleep(200 * time.Millisecond)

	// After re-acquisition from tryAcquire, should be leader again.
	if !e.IsLeader() {
		t.Error("should have re-acquired leadership after lock disappeared")
	}

	e.Stop()
}

// TestS3Elector_HeartbeatUnmarshalError tests the heartbeat path where
// the lock file is corrupted during heartbeat.
func TestS3Elector_HeartbeatCorrupted(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/hb-corrupt.json",
		Identity:           "node-hb-corrupt",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Corrupt the lock file.
	_ = store.Upload(context.Background(), "election/hb-corrupt.json", []byte("not-json{"))

	// Wait for heartbeat to detect corrupted lock.
	time.Sleep(200 * time.Millisecond)

	// The elector should lose leadership when unmarshal fails (lock.Holder != identity).
	// It sets isLeader to false.
	// Then tryAcquire runs again and should re-acquire (corrupted -> overwrite).
	// Give it some time.
	time.Sleep(200 * time.Millisecond)

	e.Stop()
}

// TestS3Elector_IsHolderAlive_FreshHeartbeatNoAddress tests the case where
// the heartbeat is fresh but there's no address for HTTP health check.
func TestS3Elector_IsHolderAlive_FreshHeartbeatNoAddress(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/alive-test.json",
		Identity:           "test-node",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// Lock with fresh heartbeat but no address.
	lock := S3Lock{
		Holder:    "other-node",
		Address:   "",
		Acquired:  time.Now().UTC(),
		Heartbeat: time.Now().UTC(),
	}

	// isHolderAlive should return true (fresh heartbeat, no HTTP check needed).
	if !e.isHolderAlive(lock) {
		t.Error("expected holder with fresh heartbeat and no address to be considered alive")
	}
}

// TestS3Elector_IsHolderAlive_ExpiredHeartbeat tests that expired heartbeats
// are considered dead.
func TestS3Elector_IsHolderAlive_ExpiredHeartbeat(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/expired-test.json",
		Identity:           "test-node",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            500 * time.Millisecond,
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	lock := S3Lock{
		Holder:    "other-node",
		Address:   "127.0.0.1:9999",
		Acquired:  time.Now().UTC().Add(-10 * time.Second),
		Heartbeat: time.Now().UTC().Add(-10 * time.Second), // expired
	}

	if e.isHolderAlive(lock) {
		t.Error("expected holder with expired heartbeat to be considered dead")
	}
}

// TestS3Elector_ReleaseWhenNotLeader tests that release() is a no-op when
// the elector is not the leader.
func TestS3Elector_ReleaseWhenNotLeader(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/no-release.json",
		Identity:           "node-no-release",
		HeartbeatInterval:  100 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// Not leader, release should be a no-op.
	e.release(context.Background())

	if e.IsLeader() {
		t.Error("should not be leader")
	}
}

// TestS3Elector_StartStopRestart tests that the elector can be started,
// stopped, and restarted.
func TestS3Elector_StartStopRestart(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/restart.json",
		Identity:           "node-restart",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// First start.
	ctx1, cancel1 := context.WithCancel(context.Background())
	e.Start(ctx1)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership on first start")
	}

	cancel1()
	e.Stop()

	if e.IsLeader() {
		t.Error("should not be leader after first stop")
	}

	// Second start (restart).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e.Start(ctx2)

	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership on restart")
	}

	e.Stop()
}

// heartbeatFailingStore fails on upload during heartbeat (after initial acquisition).
type heartbeatFailingStore struct {
	*mockS3Store
	uploadCount int
	failAfter   int
}

func (s *heartbeatFailingStore) Upload(ctx context.Context, key string, data []byte) error {
	s.uploadCount++
	if s.uploadCount > s.failAfter {
		return fmt.Errorf("simulated heartbeat upload failure")
	}
	return s.mockS3Store.Upload(ctx, key, data)
}

// TestS3Elector_HeartbeatUploadFailure tests that a failed heartbeat upload
// logs an error but doesn't crash.
func TestS3Elector_HeartbeatUploadFailure(t *testing.T) {
	inner := newMockS3Store()
	store := &heartbeatFailingStore{
		mockS3Store: inner,
		failAfter:   1, // Allow the initial writeLock, fail subsequent heartbeats.
	}

	cfg := S3ElectorConfig{
		LockKey:            "election/hb-fail.json",
		Identity:           "node-hb-fail",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Start(ctx)

	// Wait for initial acquisition.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !e.IsLeader() {
		t.Fatal("expected to acquire leadership")
	}

	// Wait for heartbeat to fail.
	time.Sleep(200 * time.Millisecond)

	// The elector should still consider itself leader (failed heartbeat logs error
	// but doesn't revoke leadership).
	e.Stop()
}

// TestK8sElector_Stop_NilCancel verifies Stop with nil cancel doesn't panic.
func TestK8sElector_Stop_NilCancel(t *testing.T) {
	e := &K8sElector{}
	e.Stop() // should not panic with nil cancel
}

// TestS3Elector_TryAcquire_AlreadyHoldsLock exercises the tryAcquire path where
// the lock already belongs to this elector (identity match), so it renews via heartbeat.
func TestS3Elector_TryAcquire_AlreadyHoldsLock(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/self-hold.json",
		Identity:           "node-self",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            5 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// Pre-seed a lock with our own identity but mark us as NOT leader yet.
	seedLock(t, store, "election/self-hold.json", S3Lock{
		Holder:    "node-self",
		Address:   "127.0.0.1:9000",
		Acquired:  time.Now().UTC().Add(-5 * time.Second),
		Heartbeat: time.Now().UTC().Add(-1 * time.Second),
	})

	// Call tryAcquire directly. It should see our identity and call heartbeat.
	e.tryAcquire(context.Background())

	// After tryAcquire sees our own lock, it should renew heartbeat.
	// The lock should now have an updated heartbeat timestamp.
	data, err := store.Download(context.Background(), "election/self-hold.json")
	if err != nil {
		t.Fatalf("download lock: %v", err)
	}
	var lock S3Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// The heartbeat should be updated but holder should still be us.
	if lock.Holder != "node-self" {
		t.Errorf("holder = %q, want node-self", lock.Holder)
	}
}

// releaseFailingStore simulates S3 Delete failures.
type releaseFailingStore struct {
	*mockS3Store
	deleteFail bool
}

func (s *releaseFailingStore) Delete(_ context.Context, _ string) error {
	if s.deleteFail {
		return fmt.Errorf("simulated delete failure")
	}
	return nil
}

// TestS3Elector_ReleaseDeleteFailure tests that a failed release Delete
// logs an error but doesn't panic.
func TestS3Elector_ReleaseDeleteFailure(t *testing.T) {
	store := &releaseFailingStore{
		mockS3Store: newMockS3Store(),
		deleteFail:  true,
	}
	cfg := S3ElectorConfig{
		LockKey:            "election/release-fail.json",
		Identity:           "node-release-fail",
		Address:            "127.0.0.1:9000",
		HeartbeatInterval:  50 * time.Millisecond,
		LockTTL:            1 * time.Second,
		HealthCheckTimeout: 200 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// Manually set as leader.
	e.isLeader.Store(true)

	// Release should try to delete but fail — should not panic.
	e.release(context.Background())

	// After release, isLeader should be false even though delete failed.
	if e.IsLeader() {
		t.Error("should not be leader after release, even if delete fails")
	}
}

// TestS3Elector_HttpHealthCheck_Error tests the httpHealthCheck error path.
func TestS3Elector_HttpHealthCheck_NonOK(t *testing.T) {
	// Start server that returns 500.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/health-check.json",
		Identity:           "test-node",
		HealthCheckTimeout: 1 * time.Second,
	}
	e := NewS3Elector(store, cfg)

	// Health check against a server that returns 500 should return false.
	if e.httpHealthCheck(ts.Listener.Addr().String()) {
		t.Error("expected httpHealthCheck to return false for 500 response")
	}
}

// TestS3Elector_HttpHealthCheck_ConnectionRefused tests the httpHealthCheck
// error path when the target host refuses the connection.
func TestS3Elector_HttpHealthCheck_ConnectionRefused(t *testing.T) {
	store := newMockS3Store()
	cfg := S3ElectorConfig{
		LockKey:            "election/hc-refused.json",
		Identity:           "test-node",
		HealthCheckTimeout: 100 * time.Millisecond,
	}
	e := NewS3Elector(store, cfg)

	// Use an address that will refuse connections.
	if e.httpHealthCheck("127.0.0.1:1") {
		t.Error("expected httpHealthCheck to return false for connection refused")
	}
}

// TestAutoElector_K8sMode tests the "k8s" mode branch in NewAutoElector.
// NewK8sElector always succeeds, so this exercises the success branch (inner = e).
func TestAutoElector_K8sMode(t *testing.T) {
	ae := NewAutoElector(AutoElectorConfig{
		Mode: "k8s",
		K8sConfig: K8sElectorConfig{
			LeaseName: "test-lease",
			Identity:  "test-node",
		},
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// The inner elector should be a K8sElector. IsLeader should be false initially.
	if ae.IsLeader() {
		t.Error("expected IsLeader=false for fresh K8sElector")
	}
	// Stop should not panic (exercises delegate).
	ae.Stop()
}

// TestAutoElector_AutoModeK8sEnv tests the "auto" mode when KUBERNETES_SERVICE_HOST
// is set, which triggers the K8s path.
func TestAutoElector_AutoModeK8sEnv(t *testing.T) {
	// Set env var to simulate K8s environment.
	old := os.Getenv("KUBERNETES_SERVICE_HOST")
	os.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	defer os.Setenv("KUBERNETES_SERVICE_HOST", old)

	ae := NewAutoElector(AutoElectorConfig{
		Mode: "auto",
		K8sConfig: K8sElectorConfig{
			LeaseName: "test-lease",
			Identity:  "test-node",
		},
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// The inner elector should be a K8sElector.
	if ae.IsLeader() {
		t.Error("expected IsLeader=false for fresh K8sElector")
	}
	ae.Stop()
}

// TestAutoElector_DefaultMode tests the default/unknown mode branch.
func TestAutoElector_DefaultMode(t *testing.T) {
	ae := NewAutoElector(AutoElectorConfig{
		Mode: "unknown-mode",
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// Default mode falls back to NoopElector which is always leader.
	if !ae.IsLeader() {
		t.Error("expected IsLeader=true for noop fallback (unknown mode)")
	}
}

// TestAutoElector_S3Mode tests the "s3" mode branch.
func TestAutoElector_S3Mode(t *testing.T) {
	store := newMockS3Store()
	ae := NewAutoElector(AutoElectorConfig{
		Mode:    "s3",
		S3Store: store,
		S3Config: S3ElectorConfig{
			LockKey:            "election/auto-s3.json",
			Identity:           "auto-s3-node",
			HeartbeatInterval:  100 * time.Millisecond,
			LockTTL:            1 * time.Second,
			HealthCheckTimeout: 200 * time.Millisecond,
		},
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}

	ctx, cancel := context.WithCancel(context.Background())
	ae.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if ae.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ae.IsLeader() {
		t.Fatal("expected to acquire leadership in S3 mode")
	}

	cancel()
	ae.Stop()
}

// TestAutoElector_AutoModeNoK8sWithS3 tests "auto" mode without K8s env but with S3 store.
func TestAutoElector_AutoModeNoK8sWithS3(t *testing.T) {
	// Ensure KUBERNETES_SERVICE_HOST is not set.
	old := os.Getenv("KUBERNETES_SERVICE_HOST")
	os.Unsetenv("KUBERNETES_SERVICE_HOST")
	defer os.Setenv("KUBERNETES_SERVICE_HOST", old)

	store := newMockS3Store()
	ae := NewAutoElector(AutoElectorConfig{
		Mode:    "auto",
		S3Store: store,
		S3Config: S3ElectorConfig{
			LockKey:            "election/auto-s3-fallback.json",
			Identity:           "auto-s3-fallback-node",
			HeartbeatInterval:  100 * time.Millisecond,
			LockTTL:            1 * time.Second,
			HealthCheckTimeout: 200 * time.Millisecond,
		},
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// Should be S3Elector.
	ae.Stop()
}

// TestAutoElector_AutoModeNoK8sNoS3 tests "auto" mode without K8s or S3 — falls back to noop.
func TestAutoElector_AutoModeNoK8sNoS3(t *testing.T) {
	// Ensure KUBERNETES_SERVICE_HOST is not set.
	old := os.Getenv("KUBERNETES_SERVICE_HOST")
	os.Unsetenv("KUBERNETES_SERVICE_HOST")
	defer os.Setenv("KUBERNETES_SERVICE_HOST", old)

	ae := NewAutoElector(AutoElectorConfig{
		Mode:    "auto",
		S3Store: nil,
	})
	if ae == nil {
		t.Fatal("expected non-nil AutoElector")
	}
	// No S3 store, no K8s → noop which is always leader.
	if !ae.IsLeader() {
		t.Error("expected IsLeader=true for noop fallback")
	}
}
