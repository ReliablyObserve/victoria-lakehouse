package main

import (
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// -search.maxQueryDuration is VictoriaTraces' flag, registered here because
// vtselect cannot be linked. Fail the moment the vendored registration changes.
func TestSearchMaxQueryDuration_MatchesVendoredVTSelect(t *testing.T) {
	src, err := os.ReadFile("deps/VictoriaTraces/app/vtselect/main.go")
	if err != nil {
		t.Fatalf("vendored VictoriaTraces source missing (run make deps-vt): %v", err)
	}
	joined := regexp.MustCompile(`"\s*\+\s*"`).ReplaceAllString(string(src), "")
	f := flag.Lookup("search.maxQueryDuration")
	if f == nil {
		t.Fatal("-search.maxQueryDuration is not registered in lakehouse-traces")
	}
	// upstream writes the default as time.Second*30
	if f.DefValue != "30s" {
		t.Errorf("default %q, upstream's is time.Second*30", f.DefValue)
	}
	want := `flag.Duration("search.maxQueryDuration", time.Second*30, "` + f.Usage + `")`
	if !strings.Contains(joined, want) {
		t.Errorf("VT's -search.maxQueryDuration registration changed; update search_max_query_duration.go\nwant: %s", want)
	}
}

// stallingStore blocks every read until its context ends.
type stallingStore struct{ storage.Storage }

func (stallingStore) RunQuery(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

func (stallingStore) GetFieldNames(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stallingStore) GetFieldValues(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stallingStore) GetStreamFieldNames(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (stallingStore) GetStreamFieldValues(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The pod's select handler (newSelectHandler, as newMux mounts it) takes the
// Jaeger and Tempo deadline from the flag.
func TestNewSelectHandler_JaegerAndTempoDeadlineIsTheFlag(t *testing.T) {
	old := *searchMaxQueryDuration
	*searchMaxQueryDuration = 200 * time.Millisecond
	t.Cleanup(func() { *searchMaxQueryDuration = old })

	st := stallingStore{}
	vtstorageadapter.Init(st)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	mux := http.NewServeMux()
	newSelectHandler(st, cfg).Register(mux)

	for _, path := range []string{"/select/jaeger/api/services", "/api/services", "/select/tempo/api/v2/search/tags"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rec := httptest.NewRecorder()
		began := time.Now()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil).WithContext(ctx))
		cancel()
		if took := time.Since(began); took > time.Second {
			t.Errorf("%s answered after %s with -search.maxQueryDuration=200ms (%d %q)", path, took, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "deadline exceeded") {
			t.Errorf("%s: %d %q does not carry the deadline error", path, rec.Code, rec.Body.String())
		}
	}
}
