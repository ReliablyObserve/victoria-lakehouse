package buffer

// SegmentRows is one live insert-buffer segment's row count in a time window,
// and whether the segment is committed (every object of it is stored and in
// the manifest).
type SegmentRows struct {
	Nonce     string
	Committed bool
	Rows      int64
	// Dropped is how many of Rows the flush never writes to Parquet
	// (VictoriaTraces' trace_id_idx rows). A query counts them from the buffer
	// for as long as the segment is live; the objects never hold them.
	Dropped int64
}

// WindowReport is what a node's insert buffer holds in a time window, for the
// admin parity check.
//
//   - Rows is every tenant's rows with _time in the window across the live
//     segments (the co-located ones, or every insert peer's).
//   - Nonces names those segments.
//   - Segments has the per-segment split when the segments are co-located. It is
//     nil when the rows came from insert peers through the buffer bridge, which
//     reports rows and nonces only; the caller then has the aggregate only.
type WindowReport struct {
	Rows     int64
	Nonces   map[string]struct{}
	Segments []SegmentRows
}
