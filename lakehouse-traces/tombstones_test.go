package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
)

// Twin of cmd/lakehouse-logs/tombstones_test.go — keep the two identical.

func attachTestView(t *testing.T, cfg *config.Config, pod *deletetest.Pod) *delete.TombstoneStore {
	t.Helper()
	ts := delete.NewTombstoneStore()
	err := attachTombstones(cfg, nil, ts, &tombstoneRetention{}, func(o *delete.Options) {
		o.Backend = pod
		o.Now = pod.Now
		o.Interval = 40 * time.Millisecond
	})
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// The staleness gate follows delete.enabled: on, a view that cannot be refreshed
// refuses reads; off, nothing is ever refused, and the tombstones in the bucket
// are still enforced.
func TestAttachTombstones_GateFollowsDeleteEnabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		b := deletetest.NewBucket()
		pod := b.Pod("p")
		cfg := &config.Config{}
		cfg.Role = config.RoleSelect
		cfg.Delete.Enabled = enabled
		ts := attachTestView(t, cfg, pod)
		if err := ts.SyncNow(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		if err := delete.RunTask(context.Background(), ts, false, "1", time.Now().UnixNano(), []delete.TenantRef{{AccountID: 7}}, "error", "hide"); err != nil {
			t.Fatalf("enabled=%v: create: %v", enabled, err)
		}
		pod.FailNext("list", 1_000_000)
		time.Sleep(250 * time.Millisecond)
		_ = ts.SyncNow(context.Background(), false)

		err := ts.Gate(context.Background(), nil)
		if enabled && !errors.Is(err, delete.ErrViewStale) {
			t.Errorf("delete.enabled=true: gate on a stale view = %v, want ErrViewStale", err)
		}
		if !enabled && err != nil {
			t.Errorf("delete.enabled=false: gate = %v, want it open", err)
		}
		if ts.Count() != 1 {
			t.Errorf("enabled=%v: %d tombstones in force, want the delete still enforced", enabled, ts.Count())
		}
	}
}

// A select pod is ready only after its first read of the deletes (its reads are
// refused until then); other roles and a deployment with deletes off are not held.
func TestTombstonesReady_OnlyASelectPodWithDeletesOnWaits(t *testing.T) {
	cases := []struct {
		role    config.Role
		enabled bool
		held    bool
	}{
		{config.RoleSelect, true, true},
		{config.RoleSelect, false, false},
		{config.RoleInsert, true, false},
		{config.RoleAll, true, false},
	}
	for _, c := range cases {
		b := deletetest.NewBucket()
		cfg := &config.Config{}
		cfg.Role = c.role
		cfg.Delete.Enabled = c.enabled
		ts := attachTestView(t, cfg, b.Pod("p"))
		if got := tombstonesReady(cfg, ts); got == c.held {
			t.Errorf("role=%v delete.enabled=%v before the first sync: ready=%v, want %v", c.role, c.enabled, got, !c.held)
		}
		if err := ts.SyncNow(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		if !tombstonesReady(cfg, ts) {
			t.Errorf("role=%v delete.enabled=%v after the first sync: not ready", c.role, c.enabled)
		}
	}
}

// Only the pods that run maintenance (roles all and insert) delete the keys of
// expired tombstones; a select pod learns the expiry and forgets the tombstone but
// never issues a DELETE, so the shared bucket is written by the writers only.
func TestAttachTombstones_OnlyMaintenancePodsDeleteExpiredKeys(t *testing.T) {
	policy, err := retention.NewPolicy(retention.Config{Enabled: true, Default: "30d"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		role    config.Role
		deletes bool
	}{{config.RoleSelect, false}, {config.RoleInsert, true}, {config.RoleAll, true}} {
		b := deletetest.NewBucket()
		pod := b.Pod("p")
		cfg := &config.Config{}
		cfg.Role = c.role
		cfg.Delete.Enabled = true
		ts := delete.NewTombstoneStore()
		ret := &tombstoneRetention{}
		ret.set(policy)
		err := attachTombstones(cfg, nil, ts, ret, func(o *delete.Options) {
			o.Backend = pod
			o.Now = pod.Now
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.SyncNow(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		if _, err := ts.Create(context.Background(), delete.Tombstone{ID: "old", Query: "error", EndNs: b.Now().UnixNano(), Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 7}}}); err != nil {
			t.Fatal(err)
		}
		b.Advance(policy.MaxTTL() + 3*24*time.Hour) // past retention plus the margin
		if err := ts.SyncNow(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		if got := pod.Counts().Deletes; (got > 0) != c.deletes {
			t.Errorf("role %v: %d DELETEs after the tombstone expired, want deletes=%v", c.role, got, c.deletes)
		}
		if ts.Count() != 0 {
			t.Errorf("role %v: the expired tombstone is still in the view", c.role)
		}
	}
}
