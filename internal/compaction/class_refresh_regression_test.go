package compaction

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestCompactionSafety_CachedTieredInputsNeverMerge(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, path := range []string{"scan", "force", "tierA"} {
			t.Run(path, func(t *testing.T) {
				w := newPlanWorld(t, mode)
				p := partitionAt(time.Now().Add(-3 * time.Hour))
				keys := w.add("1001/0", p, 0, 10, 2, nil)
				detector := delete.NewStorageClassDetector(nil)
				for _, k := range keys {
					detector.SetCache(k, delete.ClassStandardIA)
				}
				s := w.shippedScheduler()
				s.freeze = &LifecycleFreeze{Detector: detector}
				var n int
				var err error
				switch path {
				case "scan":
					n, err = s.Scan(context.Background())
				case "force":
					r, e := s.ForceCompactPartition(context.Background(), p, 0)
					if e == nil {
						n = len(r.OutputFiles)
					}
				case "tierA":
					w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
					n, err = tierAWorld(w, s.freeze, nil).RunTierA(context.Background())
				}
				if err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("cached STANDARD_IA objects rewritten: merges=%d, want 0", n)
				}
			})
		}
	})
}

func TestCompactionSafety_PlannerCachedClassSnapshot(t *testing.T) {
	w := newPlanWorld(t, config.ModeLogs)
	now := time.Now()
	p := partitionAt(now.Add(-3 * time.Hour))
	keys := w.add("1001/0", p, 0, 10, 2, nil)
	d := delete.NewStorageClassDetector(nil)
	for _, k := range keys {
		d.SetCache(k, delete.ClassStandardIA)
	}
	pt, _ := manifest.ParsePartitionTime(p)
	pl := newPlanner(shippedPolicy(), planFP, now, nil, &LifecycleFreeze{Detector: d}, nil)
	for _, k := range keys {
		d.SetCache(k, delete.ClassStandard)
	}
	if plans := pl.partition(p, w.m.FilesForPartition(p), pt); len(plans) != 0 {
		t.Fatalf("planner did not retain frozen snapshot: plans=%d", len(plans))
	}
}

func TestCompactionSafety_ListTransitionAfterPlanningNeverMerge(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, path := range []string{"scan", "force", "tierA"} {
			t.Run(path, func(t *testing.T) {
				w := newPlanWorld(t, mode)
				w.m = manifest.New("test-bucket", "")
				p := partitionAt(time.Now().Add(-3 * time.Hour))
				w.add("1001/0", p, 0, 10, 2, nil)
				targetKeys := w.add("1002/0", p, 0, 10, 2, nil)
				lister := &classLister{class: map[string]string{}}
				for _, f := range w.m.FilesForPartition(p) {
					lister.class[f.Key] = "STANDARD"
				}
				srv := httptest.NewServer(lister)
				defer srv.Close()
				cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion("us-east-1"), awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("t", "t", "")))
				if err != nil {
					t.Fatal(err)
				}
				endpoint := srv.URL
				client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true })
				s := w.shippedScheduler()
				s.onCompacted = func(added []manifest.FileInfo, _ []string, _ map[string]map[string][]string) {
					if len(added) == 0 || !strings.HasPrefix(added[0].Key, "1001/0/") {
						return
					}
					lister.mu.Lock()
					for _, k := range targetKeys {
						lister.class[k] = "STANDARD_IA"
					}
					lister.mu.Unlock()
					if err := w.m.RefreshFromS3(context.Background(), client); err != nil {
						t.Fatal(err)
					}
					for _, f := range w.m.FilesForPartition(p) {
						if strings.HasPrefix(f.Key, "1002/0/") && f.StorageClass != "STANDARD_IA" {
							t.Fatalf("refresh did not change target: %+v", f)
						}
					}
				}
				var n int
				switch path {
				case "scan":
					n, err = s.Scan(context.Background())
				case "force":
					r, e := s.ForceCompactPartition(context.Background(), p, 0)
					err = e
					if r != nil {
						n = len(r.OutputFiles)
					}
				case "tierA":
					w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute))
					sweep := tierAWorld(w, nil, nil)
					transitioned := false
					targetPrefix := ""
					sweep.cfg.Pool = &faultPool{mockPool: w.pool, uploadErr: func(key string) error {
						if transitioned {
							return nil
						}
						transitioned = true
						targetPrefix = "1001/0/"
						if strings.HasPrefix(key, targetPrefix) {
							targetPrefix = "1002/0/"
						}
						lister.mu.Lock()
						for k := range lister.class {
							if strings.HasPrefix(k, targetPrefix) {
								lister.class[k] = "STANDARD_IA"
							}
						}
						lister.mu.Unlock()
						if e := w.m.RefreshFromS3(context.Background(), client); e != nil {
							t.Fatal(e)
						}
						return nil
					}}
					_, err = sweep.RunTierA(context.Background())
					if !transitioned {
						t.Fatal("no Tier A merge attempted")
					}
					count := 0
					for _, f := range w.m.FilesForPartition(p) {
						if strings.HasPrefix(f.Key, targetPrefix) {
							count++
						}
					}
					if count != 10 {
						t.Fatalf("Tier A rewrote refreshed tiered target: %d files, want untouched 10", count)
					}
					n = 1
				}
				if err != nil {
					t.Fatal(err)
				}
				if n != 1 {
					t.Fatalf("inputs became STANDARD_IA before their merge: merges=%d, want only first tenant's 1", n)
				}
			})
		}
	})
}

func TestCompactionSafety_ForceMustReportFailedTenant(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		p := partitionAt(time.Now().Add(-3 * time.Hour))
		w.add("1001/0", p, 0, 10, 2, nil)
		w.add("1002/0", p, 0, 10, 2, nil)
		pool := &faultPool{mockPool: w.pool, downloadErr: ofTenant("1002/0/")}
		s := w.schedulerOn(pool)
		result, err := s.ForceCompactPartition(context.Background(), p, 0)
		if err == nil {
			t.Fatalf("force reported success despite injected tenant download failure: result=%+v", result)
		}
	})
}
