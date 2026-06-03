package s3reader

import "testing"

func TestPoolRegistry_PoolForDefault_ReturnsSelf(t *testing.T) {
	def := &ClientPool{bucket: "default-bucket"}
	reg := NewPoolRegistry(def)
	if got := reg.PoolFor("default-bucket"); got != def {
		t.Errorf("default lookup must return the original pool")
	}
	if got := reg.PoolFor(""); got != def {
		t.Errorf("empty bucket must return default")
	}
}

func TestPoolRegistry_PoolFor_CachesPerBucket(t *testing.T) {
	def := &ClientPool{bucket: "default-bucket"}
	reg := NewPoolRegistry(def)

	a1 := reg.PoolFor("tenant-a")
	a2 := reg.PoolFor("tenant-a")
	if a1 != a2 {
		t.Error("second lookup must return cached pool")
	}
	if a1 == def {
		t.Error("non-default lookup must clone")
	}
	if a1.Bucket() != "tenant-a" {
		t.Errorf("cloned pool bucket = %q, want tenant-a", a1.Bucket())
	}

	b := reg.PoolFor("tenant-b")
	if b == a1 {
		t.Error("different buckets must produce different pools")
	}
}

func TestPoolRegistry_Buckets_ListsCached(t *testing.T) {
	def := &ClientPool{bucket: "default"}
	reg := NewPoolRegistry(def)
	_ = reg.PoolFor("a")
	_ = reg.PoolFor("b")
	got := reg.Buckets()
	if len(got) != 3 {
		t.Errorf("want 3 cached buckets (default+a+b), got %d: %v", len(got), got)
	}
}

func TestClientPool_BucketRouter_RoutesPerKey(t *testing.T) {
	p := &ClientPool{bucket: "default"}
	if got := p.resolveBucket("any-key"); got != "default" {
		t.Errorf("no router: resolveBucket = %q, want default", got)
	}
	p.SetBucketRouter(func(key string) string {
		if len(key) > 4 && key[:4] == "1002" {
			return "acme-bucket"
		}
		return ""
	})
	if got := p.resolveBucket("1002/0/logs/foo"); got != "acme-bucket" {
		t.Errorf("router hit: got %q, want acme-bucket", got)
	}
	if got := p.resolveBucket("0/0/logs/foo"); got != "default" {
		t.Errorf("router miss: got %q, want default fallback", got)
	}
}
