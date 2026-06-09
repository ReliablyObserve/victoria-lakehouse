// Package pmeta is the unified per-partition metadata layer for the cold tier.
//
// It collapses what used to be several independent sidecar formats, snapshots,
// parallel-GET loaders, dirty-trackers and eviction policies (the manifest
// _file_metadata.json, the _bloom.bin index, the _label_index.json, …) into ONE
// per-partition Bundle persisted as a single S3 object and read with a single
// GET. Each metadata kind plugs in as a Facet.
//
// Every facet is a cache of data that is re-derivable from S3 (the .parquet
// files + footers are the source of truth), so a corrupt or unknown facet is
// never fatal: it is skipped on load and rebuilt from the partition's files
// (self-heal). See docs/architecture/metadata-consolidation.md.
//
// This package adds NO new on-disk framing inside .parquet files — bundles are
// ordinary sidecar objects, preserving Pure-Parquet-on-S3 portability.
package pmeta

import "io"

// FacetKind is a stable wire tag for one section of a partition Bundle. Values
// are persisted, so they must never be reused for a different meaning.
type FacetKind uint8

const (
	FacetBloom        FacetKind = 1 // file-level bloom (was bloomindex._bloom.bin)
	FacetFileMeta     FacetKind = 2 // per-file Labels/ColumnStats/RowCount (was _file_metadata.json)
	FacetLabels       FacetKind = 3 // label -> values (was cache.LabelIndex / _label_index.json)
	FacetColumnStats  FacetKind = 4 // per-field min/max/null (was manifest column stats)
	FacetFieldCatalog FacetKind = 5 // dropdown value catalog: dict + bitmaps + HLL (new)
	FacetTraceIdx     FacetKind = 6 // VIRTUAL: backed by the Parquet footer KV, not bundle bytes
)

// FileContribution is the per-file delta handed to every facet at flush time so
// each can fold the new file's metadata into its partition without re-reading
// the column data. The same struct is replayed file-by-file to REBUILD a facet
// that was skipped on load — that is the self-heal path.
type FileContribution struct {
	Partition string
	FileKey   string
	RowCount  int64
	// Per-file metadata, consumed by FacetFileMeta (mirrors _file_metadata.json).
	MinTimeNs         int64
	MaxTimeNs         int64
	RawBytes          int64
	SchemaFingerprint string
	// Labels: low-cardinality field -> values present in this file (already
	// capped by the extractor). Consumed by FacetLabels / FacetFieldCatalog.
	Labels map[string][]string
	// HighCardValues: field -> raw values for high-cardinality fields, fed into
	// HLL sketches (FacetFieldCatalog). Never enumerated back to the user.
	HighCardValues map[string][]string
}

// Facet is the per-partition unit of metadata. One implementation per kind.
// Implementations must be safe for concurrent Merge (flush) and Encode (persist).
type Facet interface {
	Kind() FacetKind
	// Encode writes this facet's payload only — the Bundle frames it (len+CRC).
	Encode(w io.Writer) error
	// Decode reads a payload previously produced by Encode.
	Decode(r io.Reader) error
	// Merge folds a newly-flushed file's contribution into the facet. Called
	// both at flush time and when rebuilding a skipped facet from existing files.
	Merge(c FileContribution)
	// EstimateBytes drives the single shared eviction policy.
	EstimateBytes() int64
}

// FacetFactory builds an empty facet for a partition (registry pattern).
type FacetFactory func(partition string) Facet
