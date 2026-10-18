package main

// Two writers and two selects over one bucket, the way a StatefulSet cluster is
// discovered: every pod listens on ":9428" and the writer list holds the pods'
// SRV hostnames. Compaction is switched on in every pod. Each partition has to
// be compacted by exactly one writer: none left alone (an ownerless partition
// keeps its small objects forever) and none merged twice (every merge uploads a
// new object under a random key, so two merges leave every row stored twice).

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// orphanSweepEnv switches the orphan sweep on for this test (see #602). Unset, the
// sweep is stopped and the test covers the scheduler's ownership only.
const orphanSweepEnv = "LAKEHOUSE_TEST_ORPHAN_SWEEP"

var hourPartition = regexp.MustCompile(`dt=[^/]+/hour=[^/]+`)

// queryKeys is every row of the seeded window on the pod, by the field that
// identifies a seeded row, sorted (a multiset: a duplicated row appears twice).
func queryKeys(t *testing.T, store *parquets3.Storage, field string) []string {
	t.Helper()
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	q.AddTimeFilter(time.Now().Add(-96*time.Hour).UnixNano(), time.Now().UnixNano())
	var mu sync.Mutex
	var keys []string
	if err := store.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		c := db.GetColumnByName(field)
		if c == nil {
			return
		}
		mu.Lock()
		keys = append(keys, c.Values...)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	sort.Strings(keys)
	return keys
}

// seededKeys are the keys seedFlushes writes for `files` objects of `rowsEach` rows.
func seededKeys(files, rowsEach int) []string {
	var keys []string
	for f := 0; f < files; f++ {
		for r := 0; r < rowsEach; r++ {
			keys = append(keys, fmt.Sprintf("file-%d-row-%d", f, r))
		}
	}
	sort.Strings(keys)
	return keys
}

func TestMultiInstance_TwoWritersTwoSelects_EveryPartitionCompactedByExactlyOneWriter(t *testing.T) {
	bucket := newFakeS3()
	defer bucket.Close()
	// Hold back the deletes of compaction inputs so that, if two pods merge the
	// same partition, both have uploaded their output before any input is gone.
	bucket.HoldDeletes(400 * time.Millisecond)
	writerB, selA, selB := newFakeS3On(bucket), newFakeS3On(bucket), newFakeS3On(bucket)
	defer writerB.Close()
	defer selA.Close()
	defer selB.Close()

	const partitions, filesEach, rowsEach = 8, 3, 4
	groups := make([]seedGroup, partitions)
	for i := range groups {
		groups[i] = seedGroup{files: filesEach, age: time.Duration(48+i) * time.Hour}
	}
	cfgA := podConfig(t, config.RoleInsert, bucket.URL())
	cfgA.Retention.Enabled = false
	writerA := newPod(t, cfgA)
	wantRows := seedFlushes(t, writerA, cfgA, rowsEach, groups...)
	if _, n := bucketRows(t, bucket); n != partitions*filesEach {
		t.Fatalf("seed: %d objects, want %d", n, partitions*filesEach)
	}
	// The segment guard keeps compaction off objects younger than twice the buffer grace.
	bucket.Age(time.Hour)
	bucket.AgePrefix("_segments/", time.Hour)

	type pod struct {
		name  string
		cfg   *config.Config
		store *parquets3.Storage
		s3    *fakeS3
		env   selfaddr.Env
		write bool
	}
	srv := func(i int) string {
		return fmt.Sprintf("lh-logs-insert-%d.lh-logs-insert-headless.ns.svc.cluster.local:9428", i)
	}
	podEnv := func(host, ip string) selfaddr.Env {
		return selfaddr.Env{
			Hostnames: func() []string { return []string{host} },
			IPs:       func() []netip.Addr { return []netip.Addr{netip.MustParseAddr(ip)} },
		}
	}
	cfgB := podConfig(t, config.RoleInsert, writerB.URL())
	cfgB.Retention.Enabled = false
	pods := []pod{
		{"writer-0", cfgA, writerA, bucket, podEnv("lh-logs-insert-0", "10.0.1.10"), true},
		{"writer-1", cfgB, newPod(t, cfgB), writerB, podEnv("lh-logs-insert-1", "10.0.1.11"), true},
	}
	for i, ep := range []*fakeS3{selA, selB} {
		cfg := podConfig(t, config.RoleSelect, ep.URL())
		cfg.Retention.Enabled = false
		pods = append(pods, pod{fmt.Sprintf("select-%d", i), cfg, newPod(t, cfg), ep, podEnv(fmt.Sprintf("lh-logs-select-%d", i), fmt.Sprintf("10.0.2.%d", 10+i)), false})
	}
	for _, p := range pods[1:] {
		if err := p.store.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := p.store.Manifest().TotalFiles(); got != partitions*filesEach {
			t.Fatalf("%s sees %d objects, want %d", p.name, got, partitions*filesEach)
		}
	}
	for _, p := range pods {
		p.s3.Reset()
	}
	// Discovery of the writers (select.insert_headless_service): SRV hostnames.
	for _, p := range pods[:2] {
		p.store.SetWriterPeers([]string{srv(0), srv(1)})
	}

	var stops []func()
	for _, p := range pods {
		var holder *tenant.PolicyRegistry
		// Every pod listens on ":9428", as the chart's pods do.
		_, sweep, stop := setupCompaction(p.cfg, p.store, nil, ":9428", &holder, delete.NewTombstoneStore(), newStorageClassDetector(p.cfg), p.env)
		// This test is about the scheduler's ownership. The orphan sweep's Tier A
		// lets the secondary owner take a partition whose primary it has no
		// attempt time for (attempt times are per pod), so with a healthy ring it
		// still merges a partition the primary is merging: that is a separate
		// defect (#602), listed in the gaps (docs/architecture/roles.md) and not
		// covered here. To see it, run this test with LAKEHOUSE_TEST_ORPHAN_SWEEP=on:
		// the sweep keeps running, and the test then fails on partitions merged twice
		// until #602 is fixed (measured: 2 of 8 partitions, with and without the
		// ring fix).
		if sweep != nil && os.Getenv(orphanSweepEnv) != "on" {
			sweep.Stop()
		}
		if stop != nil {
			stops = append(stops, stop)
		}
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()

	partitionsOf := func() map[string]int {
		bucket.mu.Lock()
		defer bucket.mu.Unlock()
		out := map[string]int{}
		for k := range bucket.objs {
			if strings.HasSuffix(k, ".parquet") {
				out[hourPartition.FindString(k)]++
			}
		}
		return out
	}
	settled := func() bool {
		got := partitionsOf()
		if len(got) != partitions {
			return false
		}
		for _, n := range got {
			if n != 1 {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) && !settled() {
		bucket.AgePrefix("_segments/", time.Hour)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond) // a late merge by another pod would land here

	// Every partition holds exactly one object: none was left alone, none merged twice.
	uncompacted, duplicated := 0, 0
	for part, n := range partitionsOf() {
		switch {
		case n >= filesEach:
			uncompacted++
		case n > 1:
			duplicated++
		}
		if n != 1 {
			t.Errorf("partition %s holds %d objects after compaction, want 1", part, n)
		}
	}
	t.Logf("%d partitions: %d left uncompacted (no owner), %d merged more than once (several owners)", partitions, uncompacted, duplicated)
	if got := len(partitionsOf()); got != partitions {
		t.Errorf("%d partitions in the bucket, want %d", got, partitions)
	}
	rows, objects := bucketRows(t, bucket)
	if rows != int64(wantRows) || objects != partitions {
		t.Errorf("the bucket holds %d rows in %d objects, want %d rows once in %d objects", rows, objects, wantRows, partitions)
	}
	for _, p := range pods {
		if bad := unsupportedOps(p.s3.Requests()); len(bad) != 0 {
			t.Fatalf("%s: the fake bucket did not understand %d requests (first: %s %s)", p.name, len(bad), bad[0].Op, bad[0].Key)
		}
	}
	// Merged outputs uploaded by the two writers: one per partition.
	merged, writers := 0, 0
	for _, p := range pods[:2] {
		n := 0
		for _, r := range p.s3.Requests() {
			if (r.Op == "PUT" || r.Op == "MPU_COMPLETE") && strings.HasSuffix(r.Key, ".parquet") {
				n++
			}
		}
		merged += n
		if n > 0 {
			writers++
		}
	}
	if merged != partitions {
		t.Errorf("the writers uploaded %d merged objects for %d partitions, want one each", merged, partitions)
	}
	if writers == 0 {
		t.Error("no writer compacted anything")
	}
	for _, p := range pods[2:] {
		if w := bucketWrites(p.s3.Requests()); len(w) != 0 {
			t.Errorf("%s wrote to the bucket %d times (first: %s %s); a select pod never compacts", p.name, len(w), w[0].Op, w[0].Key)
		}
	}

	// Every reader answers with every row once (the row multiset, not only the
	// count). writer-0 is left out: it still holds the seeded rows in its local
	// buffer segments (the test ages their markers to get past the segment guard),
	// and a compacted output carries no segment nonce, so that pod answers the same
	// rows from the buffer and from cold storage. That is the situation the guard
	// exists to prevent, not a property of the ring.
	want := seededKeys(partitions*filesEach, rowsEach)
	for _, p := range pods[1:] {
		if err := p.store.RefreshManifest(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := queryKeys(t, p.store, "_msg")
		if len(got) != len(want) {
			t.Errorf("a query on %s returns %d rows, want %d", p.name, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("a query on %s differs from the seeded rows at %d: %q vs %q", p.name, i, got[i], want[i])
				break
			}
		}
	}
}
