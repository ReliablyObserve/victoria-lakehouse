package parquets3

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Test fixture: tests that need Parquet objects in the bucket stage rows and
// write them with the same group upload the buffer flusher uses (one object per
// partition and tenant). Nothing in production stages rows: the insert buffer
// (membuffer.Segments) holds them until the flusher writes them.

type stagedRows struct {
	logs   []schema.LogRow
	traces []schema.TraceRow
}

var (
	stagedMu sync.Mutex
	staged   = map[*BatchWriter]*stagedRows{}
)

func (w *BatchWriter) stagedFor() *stagedRows {
	s := staged[w]
	if s == nil {
		s = &stagedRows{}
		staged[w] = s
	}
	return s
}

// stageLogRows holds rows for the next flushStaged.
func (w *BatchWriter) stageLogRows(rows []schema.LogRow) {
	stagedMu.Lock()
	defer stagedMu.Unlock()
	s := w.stagedFor()
	s.logs = append(s.logs, rows...)
}

// stageTraceRows holds spans for the next flushStaged.
func (w *BatchWriter) stageTraceRows(rows []schema.TraceRow) {
	stagedMu.Lock()
	defer stagedMu.Unlock()
	s := w.stagedFor()
	s.traces = append(s.traces, rows...)
}

// flushStaged writes the staged rows as objects and clears them. A group whose
// upload fails is dropped; the first error is returned.
func (w *BatchWriter) flushStaged(ctx context.Context) error {
	stagedMu.Lock()
	s := w.stagedFor()
	logs, traces := s.logs, s.traces
	s.logs, s.traces = nil, nil
	stagedMu.Unlock()

	var firstErr error
	note := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("flush errors: %w", err)
		}
	}
	byPartition := map[string][]schema.LogRow{}
	for _, r := range logs {
		p := partitionFromNano(r.TimestampUnixNano)
		byPartition[p] = append(byPartition[p], r)
	}
	for _, p := range fixtureKeys(byPartition) {
		rows := byPartition[p]
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano })
		for _, g := range groupLogRowsByTenant(rows) {
			up := &logGroupUpload{partition: p, accountID: g.AccountID, projectID: g.ProjectID, rows: g.Rows}
			note(w.uploadLogGroup(ctx, up))
		}
	}
	tByPartition := map[string][]schema.TraceRow{}
	for _, r := range traces {
		p := partitionFromNano(r.TimestampUnixNano)
		tByPartition[p] = append(tByPartition[p], r)
	}
	for _, p := range fixtureKeys(tByPartition) {
		rows := tByPartition[p]
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano })
		for _, g := range groupTraceRowsByTenant(rows) {
			up := &traceGroupUpload{partition: p, accountID: g.AccountID, projectID: g.ProjectID, rows: g.Rows}
			note(w.uploadTraceGroup(ctx, up))
		}
	}
	w.persistCatalog(ctx)
	return firstErr
}

// flushStagedNow is flushStaged for tests that have no context at hand.
func (w *BatchWriter) flushStagedNow() { _ = w.flushStaged(context.Background()) }

func fixtureKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
