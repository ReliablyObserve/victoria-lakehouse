package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// #418: a refresh that skipped an account (its project LIST failed) was applied
// as a complete listing: the account's files left the manifest and the manifest
// counted as listed, which every "absent means gone" decision relies on.
func TestIssue418_PartialListingIsNotTreatedAsComplete(t *testing.T) {
	keys := map[string]int64{}
	for _, tenant := range []string{"1/0", "2/0", "3/0"} {
		keys[fmt.Sprintf("%s/logs/%s/f.parquet", tenant, refreshPartition)] = 100
	}
	b := newMockBucket(keys)
	failing := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing && r.URL.Query().Get("delimiter") != "" && r.URL.Query().Get("prefix") == "3/" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))
			return
		}
		b.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	client := coverageS3Client(t, srv.URL)

	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.SetSignalSuffix("logs/")
	failing = true
	err := m.RefreshFromS3(context.Background(), client)
	if !errors.Is(err, ErrRefreshPartial) {
		t.Fatalf("err=%v, want ErrRefreshPartial", err)
	}
	if m.Listed() {
		t.Fatal("a partial listing marked the manifest listed: absent keys would be read as deleted")
	}
}
