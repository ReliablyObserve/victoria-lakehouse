package pmeta

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// bundleMagic prefixes every serialized bundle. The trailing byte is the format
// version; bump it on an incompatible framing change (old readers then return a
// structural error → whole-partition rebuild, never a misparse).
// v3: header CRC added (covers magic..facetCount, incl. the partition string) and
// the catalog facet payload gained the high-card section — a v2 bundle now fails
// decode and routes to the (wired) rebuild-from-manifest self-heal.
var bundleMagic = [5]byte{'L', 'H', 'P', 'M', 0x03}

const (
	maxPartitionLen = 0xFFFF     // uint16 partition-key length
	maxFacets       = 0xFF       // uint8 facet count
	tocEntrySize    = 10         // kind(1) flags(1) len(4) crc(4)
	maxPayloadBytes = 256 << 20  // per-facet payload cap (DoS guard on corrupt len)
	maxBundleBytes  = 1024 << 20 // total payload cap
)

// Wire format (v2). A CRC-protected table of contents (TOC) precedes the
// payloads so that:
//   - a corrupt PAYLOAD is caught by its per-facet CRC and SKIPPED, and the
//     reader stays in sync (the next payload's length comes from the TOC, not
//     from the corrupt bytes) — corruption is isolated to one facet;
//   - a corrupt TOC is caught by the TOC CRC and fails the whole bundle, which
//     the caller treats as "rebuild this partition from S3".
//
//	magic[4] | version[1]
//	partLen[2] | partition[partLen]
//	facetCount[1]
//	tocCRC[4]                              crc32 over the TOC bytes
//	TOC: facetCount × { kind[1] flags[1] len[4] payloadCRC[4] }   (sorted by kind)
//	payloads: facetCount × payload[len]   (TOC order)

// Bundle holds all facets for one partition — the unit of GET/PUT/snapshot/dirty.
//
// Dirtiness is generation-based, not a boolean: every mutation bumps gen, and a
// successful persist records the generation it ENCODED (cleanGen). A contribution
// arriving between the persist's Encode snapshot and its completion keeps
// gen > cleanGen, so it is never silently dropped from the next persist cycle
// (the lost-update race a plain clear-flag scheme has).
type Bundle struct {
	Partition string

	mu       sync.RWMutex
	facets   map[FacetKind]Facet
	gen      atomic.Uint64 // bumped on every mutation
	cleanGen atomic.Uint64 // gen at the last successful persist (or decode)
}

// NewBundle returns an empty bundle for a partition.
func NewBundle(partition string) *Bundle {
	return &Bundle{Partition: partition, facets: make(map[FacetKind]Facet)}
}

// Set installs/replaces a facet and marks the bundle dirty.
func (b *Bundle) Set(f Facet) {
	b.mu.Lock()
	b.facets[f.Kind()] = f
	b.mu.Unlock()
	b.markDirty()
}

// Get returns the facet of a kind, if present.
func (b *Bundle) Get(k FacetKind) (Facet, bool) {
	b.mu.RLock()
	f, ok := b.facets[k]
	b.mu.RUnlock()
	return f, ok
}

// Dirty reports whether the bundle has changes not yet persisted.
func (b *Bundle) Dirty() bool { return b.gen.Load() != b.cleanGen.Load() }

// markDirty records a mutation.
func (b *Bundle) markDirty() { b.gen.Add(1) }

// snapshotGen returns the generation a persist is about to encode.
func (b *Bundle) snapshotGen() uint64 { return b.gen.Load() }

// persisted records that everything up to snapshot gen g is durable. Later
// mutations (gen > g) keep the bundle dirty.
func (b *Bundle) persisted(g uint64) { b.cleanGen.Store(g) }

// clearDirty marks the bundle fully clean (decode path: content == durable).
func (b *Bundle) clearDirty() { b.cleanGen.Store(b.gen.Load()) }

// EstimateBytes is the resident size across facets (drives eviction).
func (b *Bundle) EstimateBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var n int64
	for _, f := range b.facets {
		n += f.EstimateBytes()
	}
	return n
}

// Encode serializes the bundle deterministically (facets sorted by kind) so
// golden byte-identity tests hold.
func (b *Bundle) Encode(w io.Writer) error {
	if len(b.Partition) > maxPartitionLen {
		return fmt.Errorf("pmeta: partition key too long: %d", len(b.Partition))
	}
	// Snapshot + encode all facet payloads under a SINGLE read lock so a
	// concurrent OnFileFlush (write lock) cannot mutate a facet mid-Encode.
	type enc struct {
		kind    FacetKind
		payload []byte
		crc     uint32
	}
	b.mu.RLock()
	ks := make([]FacetKind, 0, len(b.facets))
	for k := range b.facets {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	if len(ks) > maxFacets {
		b.mu.RUnlock()
		return fmt.Errorf("pmeta: too many facets: %d", len(ks))
	}
	encs := make([]enc, 0, len(ks))
	var total int64
	for _, k := range ks {
		var buf bytes.Buffer
		if err := b.facets[k].Encode(&buf); err != nil {
			b.mu.RUnlock()
			return fmt.Errorf("pmeta: encode facet %d: %w", k, err)
		}
		p := buf.Bytes()
		if len(p) > maxPayloadBytes {
			b.mu.RUnlock()
			return fmt.Errorf("pmeta: facet %d payload %d exceeds cap", k, len(p))
		}
		total += int64(len(p))
		if total > maxBundleBytes {
			b.mu.RUnlock()
			return fmt.Errorf("pmeta: bundle payload exceeds cap")
		}
		encs = append(encs, enc{kind: k, payload: p, crc: crc32.ChecksumIEEE(p)})
	}
	b.mu.RUnlock()

	// Build the TOC bytes, then its CRC.
	toc := make([]byte, 0, len(encs)*tocEntrySize)
	for _, e := range encs {
		var ent [tocEntrySize]byte
		ent[0] = byte(e.kind)
		ent[1] = 0 // flags (reserved)
		binary.BigEndian.PutUint32(ent[2:6], uint32(len(e.payload)))
		binary.BigEndian.PutUint32(ent[6:10], e.crc)
		toc = append(toc, ent[:]...)
	}

	// Header bytes (magic..facetCount) are built first so the header CRC can
	// cover them: without it a flipped facetCount byte (e.g. →0) would decode as
	// a VALID empty bundle — silent total facet loss instead of a rebuild signal.
	hdr := make([]byte, 0, len(bundleMagic)+2+len(b.Partition)+1)
	hdr = append(hdr, bundleMagic[:]...)
	var u16 [2]byte
	binary.BigEndian.PutUint16(u16[:], uint16(len(b.Partition)))
	hdr = append(hdr, u16[:]...)
	hdr = append(hdr, b.Partition...)
	hdr = append(hdr, byte(len(encs)))

	bw := bufio.NewWriter(w)
	if _, err := bw.Write(hdr); err != nil {
		return err
	}
	var crcb [4]byte
	binary.BigEndian.PutUint32(crcb[:], crc32.ChecksumIEEE(hdr))
	if _, err := bw.Write(crcb[:]); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(crcb[:], crc32.ChecksumIEEE(toc))
	if _, err := bw.Write(crcb[:]); err != nil {
		return err
	}
	if _, err := bw.Write(toc); err != nil {
		return err
	}
	for _, e := range encs {
		if _, err := bw.Write(e.payload); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// DecodeResult reports facets that loaded but were individually unusable
// (per-payload CRC fail, unregistered kind, or facet Decode error). These are
// rebuilt from S3 — the self-heal path. A non-empty Skipped is NOT an error.
type DecodeResult struct {
	Skipped []FacetKind
}

// DecodeBundle reads a bundle. It returns a structural error (bad magic/version,
// truncation, TOC-CRC failure, or an over-cap size) when the framing itself is
// untrustworthy — the caller rebuilds the WHOLE partition. Otherwise it returns
// the bundle with any per-facet failures recorded in DecodeResult.Skipped for
// targeted rebuild. It never panics and never allocates beyond the size caps,
// regardless of input (fuzz-hardened).
func DecodeBundle(r io.Reader, reg map[FacetKind]FacetFactory) (*Bundle, DecodeResult, error) {
	var res DecodeResult
	br := bufio.NewReader(r)

	var magic [5]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, res, fmt.Errorf("pmeta: read magic: %w", err)
	}
	if magic != bundleMagic {
		return nil, res, fmt.Errorf("pmeta: bad magic/version %v (want %v)", magic, bundleMagic)
	}
	var u16 [2]byte
	if _, err := io.ReadFull(br, u16[:]); err != nil {
		return nil, res, fmt.Errorf("pmeta: read partLen: %w", err)
	}
	part := make([]byte, binary.BigEndian.Uint16(u16[:]))
	if _, err := io.ReadFull(br, part); err != nil {
		return nil, res, fmt.Errorf("pmeta: read partition: %w", err)
	}
	b := NewBundle(string(part))

	cnt, err := br.ReadByte()
	if err != nil {
		return nil, res, fmt.Errorf("pmeta: read facetCount: %w", err)
	}
	// Header CRC covers magic..facetCount (incl. the partition string), so a
	// corrupted facetCount/partition can never decode as a valid (e.g. empty)
	// bundle — it becomes a structural error → whole-partition rebuild.
	hdr := make([]byte, 0, len(magic)+2+len(part)+1)
	hdr = append(hdr, magic[:]...)
	hdr = append(hdr, u16[:]...)
	hdr = append(hdr, part...)
	hdr = append(hdr, cnt)
	var hdrCRCb [4]byte
	if _, err := io.ReadFull(br, hdrCRCb[:]); err != nil {
		return nil, res, fmt.Errorf("pmeta: read headerCRC: %w", err)
	}
	if crc32.ChecksumIEEE(hdr) != binary.BigEndian.Uint32(hdrCRCb[:]) {
		return nil, res, fmt.Errorf("pmeta: header CRC mismatch — rebuild partition")
	}
	if cnt == 0 {
		b.clearDirty()
		return b, res, nil
	}

	var tocCRCb [4]byte
	if _, err := io.ReadFull(br, tocCRCb[:]); err != nil {
		return nil, res, fmt.Errorf("pmeta: read tocCRC: %w", err)
	}
	toc := make([]byte, int(cnt)*tocEntrySize) // bounded: cnt ≤ 255
	if _, err := io.ReadFull(br, toc); err != nil {
		return nil, res, fmt.Errorf("pmeta: read TOC: %w", err)
	}
	if crc32.ChecksumIEEE(toc) != binary.BigEndian.Uint32(tocCRCb[:]) {
		return nil, res, fmt.Errorf("pmeta: TOC CRC mismatch — rebuild partition")
	}

	// TOC is trusted now. Validate sizes BEFORE allocating any payload.
	type entry struct {
		kind FacetKind
		ln   uint32
		crc  uint32
	}
	entries := make([]entry, cnt)
	var total int64
	for i := 0; i < int(cnt); i++ {
		off := i * tocEntrySize
		ln := binary.BigEndian.Uint32(toc[off+2 : off+6])
		if ln > maxPayloadBytes {
			return nil, res, fmt.Errorf("pmeta: facet[%d] len %d over cap", i, ln)
		}
		total += int64(ln)
		if total > maxBundleBytes {
			return nil, res, fmt.Errorf("pmeta: bundle payload over cap")
		}
		entries[i] = entry{
			kind: FacetKind(toc[off]),
			ln:   ln,
			crc:  binary.BigEndian.Uint32(toc[off+6 : off+10]),
		}
	}

	for i, e := range entries {
		payload := make([]byte, e.ln)
		if _, err := io.ReadFull(br, payload); err != nil {
			// Truncated payload stream: structural → whole-partition rebuild.
			return nil, res, fmt.Errorf("pmeta: facet[%d] kind=%d payload: %w", i, e.kind, err)
		}
		if crc32.ChecksumIEEE(payload) != e.crc {
			res.Skipped = append(res.Skipped, e.kind) // isolated corruption → rebuild this facet
			continue
		}
		factory, ok := reg[e.kind]
		if !ok {
			res.Skipped = append(res.Skipped, e.kind) // unknown kind → ignore/rebuild
			continue
		}
		f := factory(string(part))
		if err := f.Decode(bytes.NewReader(payload)); err != nil {
			res.Skipped = append(res.Skipped, e.kind) // facet decode error → rebuild
			continue
		}
		b.facets[e.kind] = f
	}
	b.clearDirty()
	return b, res, nil
}
