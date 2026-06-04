package tenant

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestPolicyRegistry_BucketEntries_SkipsEmptyAndDuplicates(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("acme-corp", TenantID{AccountID: 1002, ProjectID: 0})

	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"acme-corp": {S3: config.TenantS3Override{Bucket: "acme-private-bucket"}},
		"1:1":       {Retention: config.TenantRetentionOverride{Keep: "7d"}},
		"5:0":       {S3: config.TenantS3Override{Bucket: "isolated-team-5"}},
	}, r)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	entries := pr.BucketEntries()
	if len(entries) != 2 {
		t.Fatalf("got %d bucket entries, want 2 (retention-only entry must drop)", len(entries))
	}
	byKey := map[string]string{}
	for _, e := range entries {
		key := tenantKeyString(e.AccountID, e.ProjectID)
		byKey[key] = e.Bucket
	}
	if byKey["1002:0"] != "acme-private-bucket" {
		t.Errorf("acme alias bucket = %q, want acme-private-bucket", byKey["1002:0"])
	}
	if byKey["5:0"] != "isolated-team-5" {
		t.Errorf("5:0 bucket = %q, want isolated-team-5", byKey["5:0"])
	}
}

func TestPolicyRegistry_BucketEntries_NilReturnsNil(t *testing.T) {
	var pr *PolicyRegistry
	if got := pr.BucketEntries(); got != nil {
		t.Errorf("nil registry: got %v, want nil", got)
	}
}
