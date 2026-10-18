package retention

import (
	"context"
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

// For a TTL of a day or more the rule is upstream's: whole day partitions are
// dropped, a file is expired once the day of its newest row is before the day of
// now-TTL (VictoriaLogs storage.go getMinAllowedDay). The probes use a TTL that is
// neither the default nor the shortest, and sit one nanosecond either side of the
// day floor.
func TestPolicy_DayFloorMatchesUpstream(t *testing.T) {
	now := time.Date(2026, 10, 18, 15, 30, 0, 0, time.UTC)
	p := mustPolicy(t, Config{Enabled: true, Default: "7d", Rules: []Rule{
		{Match: map[string]string{"account_id": "1"}, Keep: "3d"},
		{Match: map[string]string{"account_id": "2"}, Keep: "30d"},
	}})
	labels := map[string][]string{"account_id": {"2"}}
	floor := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC) // the day of now-30d
	at := func(ts time.Time) manifest.FileInfo {
		return manifest.FileInfo{Labels: labels, MaxTimeNs: ts.UnixNano()}
	}
	for _, tc := range []struct {
		name string
		max  time.Time
		want bool
	}{
		{"one ns before the day floor", floor.Add(-time.Nanosecond), true},
		{"exactly the day floor", floor, false},
		{"one ns after the day floor", floor.Add(time.Nanosecond), false},
		// Older than 30 days by the clock, still inside the floor day: upstream keeps
		// the partition, so no early hiding (the exact rule would cut this file).
		{"exactly the TTL old", now.Add(-30 * 24 * time.Hour), false},
		{"15h past the TTL, same day", now.Add(-30*24*time.Hour - 15*time.Hour), false},
		{"a whole day before the floor", floor.Add(-24 * time.Hour), true},
		{"young", now.Add(-time.Hour), false},
	} {
		if got := p.Expired(at(tc.max), now); got != tc.want {
			t.Errorf("%s: Expired = %v, want %v", tc.name, got, tc.want)
		}
	}

	// A TTL of exactly one day is still a day-floor rule.
	one := mustPolicy(t, Config{Enabled: true, Default: "24h"})
	yesterday := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	if one.Expired(manifest.FileInfo{MaxTimeNs: yesterday.UnixNano()}, now) {
		t.Error("with a 1d TTL a file of the day of now-24h was expired")
	}
	if !one.Expired(manifest.FileInfo{MaxTimeNs: yesterday.Add(-time.Nanosecond).UnixNano()}, now) {
		t.Error("with a 1d TTL a file of the day before now-24h's day was kept")
	}
}

// The formula is upstream's, checked against its text on random inputs:
// day < (now - retention) / nsPerDay, with integer division.
func TestPolicy_DayFloorProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const day = int64(24 * time.Hour)
	for _, ttlDays := range []int64{1, 2, 7, 30, 90, 365} {
		p := mustPolicy(t, Config{Enabled: true, Default: (time.Duration(ttlDays) * 24 * time.Hour).String()})
		for i := 0; i < 5000; i++ {
			nowNs := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() + rng.Int63n(400*day)
			maxNs := nowNs - rng.Int63n(500*day)
			want := maxNs/day < (nowNs-ttlDays*day)/day
			if got := p.Expired(manifest.FileInfo{MaxTimeNs: maxNs}, time.Unix(0, nowNs)); got != want {
				t.Fatalf("ttl %dd now %d max %d: Expired = %v, upstream formula says %v", ttlDays, nowNs, maxNs, got, want)
			}
		}
	}
}

// Below a day (a Lakehouse extension: upstream cannot go below one day) the rule
// is exact: kept at exactly the TTL, expired one nanosecond later, for a TTL that
// is neither the default nor the shortest. A keep of zero expires everything it
// matches, and a very long retention expires nothing.
func TestPolicy_SubDayTTLIsExact(t *testing.T) {
	now := time.Date(2026, 10, 18, 15, 30, 0, 0, time.UTC)
	p := mustPolicy(t, Config{Enabled: true, Default: "6h", Rules: []Rule{
		{Match: map[string]string{"account_id": "1"}, Keep: "2h"},
		{Match: map[string]string{"account_id": "2"}, Keep: "12h"},
		{Match: map[string]string{"account_id": "5"}, Keep: "0s"},
	}})
	lab := func(a string) map[string][]string { return map[string][]string{"account_id": {a}} }
	old := func(a string, age time.Duration) manifest.FileInfo {
		return manifest.FileInfo{Labels: lab(a), MaxTimeNs: now.Add(-age).UnixNano()}
	}
	if p.Expired(old("2", 12*time.Hour), now) {
		t.Error("a file exactly 12h old was expired under a 12h TTL")
	}
	if !p.Expired(old("2", 12*time.Hour+time.Nanosecond), now) {
		t.Error("a file 12h+1ns old was kept under a 12h TTL")
	}
	if p.Expired(old("", 6*time.Hour), now) || !p.Expired(old("", 6*time.Hour+time.Nanosecond), now) {
		t.Error("the default 6h boundary is not exact")
	}
	if p.Expired(old("1", 2*time.Hour), now) || !p.Expired(old("1", 2*time.Hour+time.Nanosecond), now) {
		t.Error("the shortest 2h boundary is not exact")
	}
	if !p.Expired(old("5", time.Second), now) {
		t.Error("a zero keep must expire everything it matches")
	}
	if p.Expired(old("2", time.Hour), now) {
		t.Error("a zero keep for one tenant expired another tenant's file")
	}
	// A file that straddles the horizon stays until its newest row is past it.
	straddle := manifest.FileInfo{Labels: lab("2"), MinTimeNs: now.Add(-20 * time.Hour).UnixNano(), MaxTimeNs: now.Add(-11 * time.Hour).UnixNano()}
	if p.Expired(straddle, now) {
		t.Error("a file with rows inside the retention was cut for its oldest row")
	}
	forever := mustPolicy(t, Config{Enabled: true, Default: "36500d"})
	if forever.Expired(old("", 3000*24*time.Hour), now) {
		t.Error("a 100-year retention expired a file")
	}
}

// What the eviction deletes: RunOnce calls the same Policy.Expired. Expected
// values are written out (a 7d tenant is expired at 8 days, a 90d tenant is not),
// and a policy handed to NewWithPolicy is the very object the manager uses.
func TestRunOnce_DeletesWhatThePolicyExpires(t *testing.T) {
	now := time.Date(2026, 10, 18, 15, 30, 0, 0, time.UTC)
	cfg := Config{Enabled: true, Default: "30d", CheckInterval: "1h", Rules: []Rule{
		{Match: map[string]string{"account_id": "1"}, Keep: "7d"},
		{Match: map[string]string{"account_id": "2"}, Keep: "90d"},
		{Match: map[string]string{"account_id": "3"}, Keep: "12h"},
	}}
	pol := mustPolicy(t, cfg)
	file := func(key, acct string, age time.Duration) manifest.FileInfo {
		return manifest.FileInfo{Key: key, MaxTimeNs: now.Add(-age).UnixNano(), Labels: map[string][]string{"account_id": {acct}}}
	}
	mf := newMockManifest(map[string][]manifest.FileInfo{"p": {
		file("t7d-8d", "1", 8*24*time.Hour),       // 7d tenant, 8 days old: deleted
		file("t90d-8d", "2", 8*24*time.Hour),      // 90d tenant, 8 days old: kept
		file("default-31d", "0", 31*24*time.Hour), // default 30d: deleted
		file("default-29d", "0", 29*24*time.Hour), // kept
		file("t12h-12h", "3", 12*time.Hour),       // sub-day TTL, exactly the TTL: kept
		file("t12h-13h", "3", 13*time.Hour),       // sub-day TTL, past it: deleted
	}})
	deleter := &mockDeleter{}
	mgr, err := NewWithPolicy(pol, cfg.CheckInterval, mf, deleter, "b", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if mgr.Policy() != pol {
		t.Fatal("the manager does not use the policy it was given")
	}
	mgr.nowFunc = func() time.Time { return now }
	if _, err := mgr.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, k := range deleter.getDeleted() {
		got[k] = true
	}
	want := map[string]bool{"b/t7d-8d": true, "b/default-31d": true, "b/t12h-13h": true}
	if len(got) != len(want) {
		t.Fatalf("deleted %v, want %v", got, want)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("deleted %v, want %v", got, want)
		}
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
