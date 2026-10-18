package retention

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func mustPolicy(t *testing.T, cfg Config) *Policy {
	t.Helper()
	p, err := NewPolicy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPolicy_Basics(t *testing.T) {
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	file := func(age time.Duration, labels map[string][]string) manifest.FileInfo {
		return manifest.FileInfo{Key: "k", MaxTimeNs: now.Add(-age).UnixNano(), Labels: labels}
	}
	if (&Policy{}).Enabled() || (*Policy)(nil).Enabled() {
		t.Fatal("a zero or nil policy must be disabled")
	}
	if mustPolicy(t, Config{}).Enabled() {
		t.Fatal("retention off must give a disabled policy")
	}
	if mustPolicy(t, Config{}).Expired(file(1000*24*time.Hour, nil), now) {
		t.Fatal("a disabled policy expired a file")
	}

	p := mustPolicy(t, Config{Enabled: true, Default: "30d", Rules: []Rule{
		{Match: map[string]string{"account_id": "7"}, Keep: "365d"},
		{Match: map[string]string{"account_id": "8"}, Keep: "7d"},
	}})
	acc := func(id string) map[string][]string { return map[string][]string{"account_id": {id}} }
	for _, tc := range []struct {
		name string
		fi   manifest.FileInfo
		want bool
	}{
		{"young default", file(29*24*time.Hour, nil), false},
		{"old default", file(31*24*time.Hour, nil), true},
		{"rule keeps longer", file(100*24*time.Hour, acc("7")), false},
		{"rule keeps shorter", file(8*24*time.Hour, acc("8")), true},
		{"rule too old", file(400*24*time.Hour, acc("7")), true},
		{"unmatched label uses the default", file(31*24*time.Hour, acc("9")), true},
	} {
		if got := p.Expired(tc.fi, now); got != tc.want {
			t.Errorf("%s: Expired = %v, want %v", tc.name, got, tc.want)
		}
	}
	if _, err := NewPolicy(Config{Enabled: true, Default: "zzz"}); err == nil {
		t.Error("an invalid default must be refused")
	}
	if _, err := NewPolicy(Config{Enabled: true, Default: "1d", Rules: []Rule{{Keep: "1d"}}}); err == nil {
		t.Error("a rule without a match must be refused")
	}
}

// One predicate for eviction and the read cut: the manager deletes exactly the
// files the policy calls expired.
func TestPolicy_AgreesWithTheEvictionLoop(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	now := time.Now()
	cfg := Config{Enabled: true, Default: "10d", CheckInterval: "1h", Rules: []Rule{
		{Match: map[string]string{"account_id": "1"}, Keep: "3d"},
		{Match: map[string]string{"account_id": "2"}, Keep: "60d"},
	}}
	mgr, err := New(cfg, nil, nil, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	p := mustPolicy(t, cfg)
	for i := 0; i < 5000; i++ {
		fi := manifest.FileInfo{MaxTimeNs: now.Add(-time.Duration(rng.Int63n(int64(90 * 24 * time.Hour)))).UnixNano()}
		if k := rng.Intn(4); k > 0 {
			fi.Labels = map[string][]string{"account_id": {string(rune('0' + k))}}
		}
		want := mgr.fileAge(fi, now) > mgr.ResolveTTL(fi)
		if got := p.Expired(fi, now); got != want {
			t.Fatalf("policy and eviction disagree for %+v: policy %v, eviction %v", fi, got, want)
		}
	}
}

// The boundaries of the predicate: a file is kept at exactly its TTL, expired one
// nanosecond later, and judged by its newest row (MaxTimeNs), so a file that
// straddles the horizon (partial day) stays readable until its newest row is past
// it. A rule that keeps zero expires everything the way eviction does, and a very
// long retention expires nothing.
func TestPolicy_Boundaries(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	p := mustPolicy(t, Config{Enabled: true, Default: "7d"})
	ttl := 7 * 24 * time.Hour
	fileAt := func(maxAge time.Duration) manifest.FileInfo {
		return manifest.FileInfo{MaxTimeNs: now.Add(-maxAge).UnixNano(), MinTimeNs: now.Add(-maxAge - 24*time.Hour).UnixNano()}
	}
	if p.Expired(fileAt(ttl), now) {
		t.Error("a file whose newest row is exactly TTL old was expired")
	}
	if !p.Expired(fileAt(ttl+time.Nanosecond), now) {
		t.Error("a file whose newest row is TTL+1ns old was kept")
	}
	// Straddling the horizon: the oldest row is 8 days old, the newest 6.5 days.
	straddle := manifest.FileInfo{MinTimeNs: now.Add(-8 * 24 * time.Hour).UnixNano(), MaxTimeNs: now.Add(-156 * time.Hour).UnixNano()}
	if p.Expired(straddle, now) {
		t.Error("a file with rows inside the retention was cut for its oldest row")
	}

	zero := mustPolicy(t, Config{Enabled: true, Default: "7d", Rules: []Rule{{Match: map[string]string{"account_id": "5"}, Keep: "0s"}}})
	five := manifest.FileInfo{MaxTimeNs: now.Add(-time.Second).UnixNano(), Labels: map[string][]string{"account_id": {"5"}}}
	if !zero.Expired(five, now) {
		t.Error("a zero keep must expire everything it matches, as eviction does")
	}
	if zero.Expired(fileAt(time.Hour), now) {
		t.Error("a zero keep for one tenant expired another tenant's file")
	}

	forever := mustPolicy(t, Config{Enabled: true, Default: "36500d"})
	if forever.Expired(fileAt(3000*24*time.Hour), now) {
		t.Error("a 100-year retention expired a file")
	}
	// A rule shorter than the default must still win through the cheap exit.
	short := mustPolicy(t, Config{Enabled: true, Default: "30d", Rules: []Rule{{Match: map[string]string{"account_id": "1"}, Keep: "2d"}}})
	one := manifest.FileInfo{MaxTimeNs: now.Add(-3 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"account_id": {"1"}}}
	if !short.Expired(one, now) {
		t.Error("a tenant rule shorter than the default was ignored")
	}
}

// The cost of the cut per planned file: a file younger than the shortest TTL
// takes the cheap exit, an older one resolves its TTL against the rules.
func BenchmarkPolicy_Expired(b *testing.B) {
	p, err := NewPolicy(Config{Enabled: true, Default: "30d", Rules: []Rule{
		{Match: map[string]string{"account_id": "1"}, Keep: "7d"},
		{Match: map[string]string{"account_id": "2"}, Keep: "90d"},
	}})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	labels := map[string][]string{"account_id": {"2"}, "project_id": {"0"}}
	young := manifest.FileInfo{Labels: labels, MaxTimeNs: now.Add(-time.Hour).UnixNano()}
	old := manifest.FileInfo{Labels: labels, MaxTimeNs: now.Add(-60 * 24 * time.Hour).UnixNano()}
	b.Run("young_cheap_exit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = p.Expired(young, now)
		}
	})
	b.Run("old_resolves_rules", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = p.Expired(old, now)
		}
	})
}
