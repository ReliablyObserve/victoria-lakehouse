package s3reader

import (
	"sync/atomic"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Read-path phases for PhaseReaderAt / metrics.S3GetsByPhase.
// Phase constants are int32 so the current phase can live in an atomic —
// the async page readers (parquet ReadModeAsync) call ReadAt concurrently
// with the goroutine that flips the phase after OpenFile returns.
const (
	PhaseOpen int32 = iota // GETs issued while parquet.OpenFile parses magic+footer
	PhasePage              // GETs issued by page/column-chunk reads after the open
)

var phaseLabels = [...]string{"open", "page"}

// PhaseReaderAt wraps the RAW S3 reader (below the buffered/coalescing
// layers, so each ReadAt is exactly one S3 GET) and attributes every GET to
// the current read-path phase. The open phase's GET count is exposed via
// OpenGets so callers can observe the per-open GET histogram — the
// research-doc "serial 4-6 GET open" number, now measurable per open.
type PhaseReaderAt struct {
	inner    ReaderAtSizer
	phase    atomic.Int32
	openGets atomic.Int64
}

// NewPhaseReaderAt wraps inner, starting in PhaseOpen.
func NewPhaseReaderAt(inner ReaderAtSizer) *PhaseReaderAt {
	return &PhaseReaderAt{inner: inner}
}

// ReadAt forwards to the inner reader, counting the GET against the current phase.
func (r *PhaseReaderAt) ReadAt(p []byte, off int64) (int, error) {
	ph := r.phase.Load()
	metrics.S3GetsByPhase.Inc(phaseLabels[ph])
	if ph == PhaseOpen {
		r.openGets.Add(1)
	}
	return r.inner.ReadAt(p, off)
}

// SetPhase switches the phase attributed to subsequent GETs.
func (r *PhaseReaderAt) SetPhase(phase int32) {
	r.phase.Store(phase)
}

// OpenGets returns the number of GETs issued during the open phase.
func (r *PhaseReaderAt) OpenGets() int64 {
	return r.openGets.Load()
}

// Size returns the inner reader's size.
func (r *PhaseReaderAt) Size() int64 {
	return r.inner.Size()
}
