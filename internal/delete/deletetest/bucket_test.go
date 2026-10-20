package deletetest

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBucket_CreateIsConditionalAndOrdered(t *testing.T) {
	b := NewBucket()
	a, c := b.Pod("a"), b.Pod("c")
	ctx := context.Background()
	if ok, err := a.PutIfAbsent(ctx, "p/k1", []byte("one")); err != nil || !ok {
		t.Fatalf("first create: %v %v", ok, err)
	}
	if ok, err := c.PutIfAbsent(ctx, "p/k1", []byte("two")); err != nil || ok {
		t.Fatalf("second create of one key = %v %v, want refused", ok, err)
	}
	if body, _ := b.Body("p/k1"); string(body) != "one" {
		t.Fatalf("the loser overwrote the key: %q", body)
	}
	b.IgnoreIfNoneMatch(true)
	if ok, _ := c.PutIfAbsent(ctx, "p/k1", []byte("two")); !ok {
		t.Fatal("a store that ignores If-None-Match must overwrite and report success")
	}
	if body, _ := b.Body("p/k1"); string(body) != "two" {
		t.Fatalf("overwrite not applied: %q", body)
	}
	b.IgnoreIfNoneMatch(false)
}

func TestBucket_ListAfterPagesShortAndFailures(t *testing.T) {
	b := NewBucket()
	b.SetPageSize(2)
	for i := 0; i < 5; i++ {
		b.Put(fmt.Sprintf("p/%d", i), []byte("x"))
	}
	b.Put("q/other", []byte("x"))
	pod := b.Pod("r")
	ctx := context.Background()

	var got []string
	if err := pod.ListAfter(ctx, "p/", "p/1", func(k string) error { got = append(got, k); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "p/2" {
		t.Fatalf("listed %v, want p/2..p/4", got)
	}
	if c := pod.Counts(); c.ListPages != 2 {
		t.Fatalf("5 keys at 2 per page took %d pages... after p/1 there are 3 keys, want 2 pages: %+v", c.ListPages, c)
	}

	pod.ShortListOnce(1)
	got = nil
	_ = pod.ListAfter(ctx, "p/", "", func(k string) error { got = append(got, k); return nil })
	if len(got) != 1 {
		t.Fatalf("short listing returned %v, want one key", got)
	}
	got = nil
	_ = pod.ListAfter(ctx, "p/", "", func(k string) error { got = append(got, k); return nil })
	if len(got) != 5 {
		t.Fatalf("the short listing must apply once, then %v", got)
	}

	pod.FailNext("list", 1)
	if err := pod.ListAfter(ctx, "p/", "", func(string) error { return nil }); err == nil {
		t.Fatal("injected list failure not returned")
	}
	stop := fmt.Errorf("stop")
	if err := pod.ListAfter(ctx, "p/", "", func(string) error { return stop }); err != stop {
		t.Fatalf("callback error = %v", err)
	}
}

func TestPod_GetExistsDeleteCountAndFail(t *testing.T) {
	b := NewBucket()
	b.Put("p/k", []byte("v"))
	pod := b.Pod("r")
	ctx := context.Background()
	if d, ok, err := pod.Get(ctx, "p/k"); err != nil || !ok || string(d) != "v" {
		t.Fatalf("Get = %q %v %v", d, ok, err)
	}
	if _, ok, _ := pod.Get(ctx, "p/none"); ok {
		t.Fatal("Get of a missing key reports it exists")
	}
	if ok, err := pod.Exists(ctx, "p/k"); err != nil || !ok {
		t.Fatalf("Exists = %v %v", ok, err)
	}
	pod.FailNext("get", 1)
	if _, _, err := pod.Get(ctx, "p/k"); err == nil {
		t.Fatal("injected get failure not returned")
	}
	pod.FailNext("head", 1)
	if _, err := pod.Exists(ctx, "p/k"); err == nil {
		t.Fatal("injected head failure not returned")
	}
	pod.FailNext("put", 1)
	if _, err := pod.PutIfAbsent(ctx, "p/n", nil); err == nil {
		t.Fatal("injected put failure not returned")
	}
	pod.FailNext("delete", 1)
	if err := pod.Delete(ctx, "p/k"); err == nil {
		t.Fatal("injected delete failure not returned")
	}
	if err := pod.Delete(ctx, "p/k"); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Body("p/k"); ok {
		t.Fatal("Delete left the key")
	}
	if c := pod.Counts(); c.Gets != 3 || c.Heads != 2 || c.Deletes != 2 {
		t.Fatalf("counts = %+v", c)
	}
	pod.ResetCounts()
	if c := pod.Counts(); c != (Counts{}) {
		t.Fatalf("counts after reset = %+v", c)
	}
	b.Remove("p/none")
	if keys := b.Keys("p/"); len(keys) != 0 {
		t.Fatalf("keys = %v", keys)
	}
	if len(b.Log()) == 0 {
		t.Fatal("no operations were logged")
	}
}

func TestBucket_Clock(t *testing.T) {
	b := NewBucket()
	before := b.Now()
	b.Advance(time.Hour)
	if got := b.Now().Sub(before); got != time.Hour {
		t.Fatalf("clock advanced %v", got)
	}
	pod := b.Pod("x")
	if !pod.Now().Equal(b.Now()) {
		t.Fatal("a pod without skew does not read the bucket's clock")
	}
	pod.SetSkew(-time.Hour)
	if got := b.Now().Sub(pod.Now()); got != time.Hour {
		t.Fatalf("a pod skewed an hour behind reads %v behind", got)
	}
}
