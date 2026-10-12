package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	lhdelete "github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

// run() must install this binary's row-to-facet functions before any writer
// (flush, compaction, delete rewrite) runs; without them no object gets a facet.
func TestFieldPresence_RunInstallsTheComputers(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "fieldpresence.SetComputers(parquets3.FieldPresenceComputers())") {
		t.Fatal("main.go does not install parquets3.FieldPresenceComputers")
	}
}

// A file rewritten to remove deleted rows carries a facet recomputed from the
// rows that stay: the field only the deleted rows had is gone.
func TestNewDeleteRewriter_ReplacementCarriesARecomputedFieldPresence(t *testing.T) {
	fieldpresence.SetComputers(parquets3.FieldPresenceComputers())
	t.Cleanup(func() { fieldpresence.SetComputers(fieldpresence.Computers{}) })
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	pool := &memPool{objects: map[string][]byte{}}
	rw := newDeleteRewriter(pool, cfg, "logs")

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep", SeverityText: "info", ServiceName: "web", Stream: `{a="1"}`},
		{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error", ServiceName: "web", Stream: `{a="1"}`, LogAttributes: map[string]string{"only.in.deleted": "x"}},
	}
	var src bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&src)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	key := "logs/dt=2026-09-01/hour=00/src.parquet"
	pool.objects[key] = src.Bytes()
	res, err := rw.RewriteFile(context.Background(), key, []lhdelete.Tombstone{{ID: "t", Query: `severity_text:="error"`, StartNs: 0, EndNs: 9000}})
	if err != nil {
		t.Fatal(err)
	}
	data := pool.objects[res.NewKey]
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	facet, overCap, err := fieldpresence.FromFooter(f.Metadata().KeyValueMetadata)
	if err != nil || overCap || facet == nil {
		t.Fatalf("replacement has no facet: %v overCap=%v", err, overCap)
	}
	if len(facet.Entries) != 1 || facet.Entries[0].Rows != 1 {
		t.Fatalf("facet entries = %+v, want the one surviving row", facet.Entries)
	}
	for _, n := range facet.NamesOf(facet.Entries[0]) {
		if n == "only.in.deleted" {
			t.Fatal("the facet still lists a field only the deleted row had")
		}
	}
}
