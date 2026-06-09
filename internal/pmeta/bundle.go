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

// bundleMagic prefixes every serialized bundle: 'L','H','P','M', version byte.
// Bump the trailing version on an incompatible framing change.
var bundleMagic = [5]byte{'L', 'H', 'P', 'M', 0x01}

const (
	maxPartitionLen = 0xFFFF
	maxFacets       = 0xFF
	facetHdrLen     = 10 // kind(1) flags(1) len(4) crc(4)
)

// Bundle holds all facets for one partition — the unit of GET/PUT/snapshot/dirty.
type Bundle struct {
	Partition string

	mu     sync.RWMutex
	facets map[FacetKind]Facet
	dirty  atomic.Bool
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
	b.dirty.Store(true)
}

// Get returns the facet of a kind, if present.
func (b *Bundle) Get(k FacetKind) (Facet, bool) {
	b.mu.RLock()
	f, ok := b.facets[k]
	b.mu.RUnlock()
	return f, ok
}

// Dirty reports whether the bundle has unpersisted changes.
func (b *Bundle) Dirty() bool { return b.dirty.Load() }

// clearDirty is called after a successful persist (or a clean load).
func (b *Bundle) clearDirty() { b.dirty.Store(false) }

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

// kinds returns the facet kinds in stable (sorted) order for deterministic encoding.
func (b *Bundle) kinds() []FacetKind {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ks := make([]FacetKind, 0, len(b.facets))
	for k := range b.facets {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}

// Encode serializes the bundle into one self-describing object:
//
//	magic[5] | partLen u16 | partition | facetCount u8
//	per facet: kind u8 | flags u8 | len u32 | crc32 u32 | payload[len]
//
// Encoding is deterministic (facets sorted by kind) so golden tests can assert
// byte-identity against the legacy sidecar payloads.
func (b *Bundle) Encode(w io.Writer) error {
	if len(b.Partition) > maxPartitionLen {
		return fmt.Errorf("pmeta: partition key too long: %d", len(b.Partition))
	}
	ks := b.kinds()
	if len(ks) > maxFacets {
		return fmt.Errorf("pmeta: too many facets: %d", len(ks))
	}

	bw := bufio.NewWriter(w)
	if _, err := bw.Write(bundleMagic[:]); err != nil {
		return err
	}
	var u16 [2]byte
	binary.BigEndian.PutUint16(u16[:], uint16(len(b.Partition)))
	if _, err := bw.Write(u16[:]); err != nil {
		return err
	}
	if _, err := bw.WriteString(b.Partition); err != nil {
		return err
	}
	if err := bw.WriteByte(byte(len(ks))); err != nil {
		return err
	}

	for _, k := range ks {
		b.mu.RLock()
		f := b.facets[k]
		b.mu.RUnlock()
		var payload bytes.Buffer
		if err := f.Encode(&payload); err != nil {
			return fmt.Errorf("pmeta: encode facet %d: %w", k, err)
		}
		p := payload.Bytes()
		if int64(len(p)) > int64(^uint32(0)) {
			return fmt.Errorf("pmeta: facet %d too large: %d", k, len(p))
		}
		var hdr [facetHdrLen]byte
		hdr[0] = byte(k)
		hdr[1] = 0 // flags (reserved)
		binary.BigEndian.PutUint32(hdr[2:6], uint32(len(p)))
		binary.BigEndian.PutUint32(hdr[6:10], crc32.ChecksumIEEE(p))
		if _, err := bw.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := bw.Write(p); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// DecodeResult reports facets that could not be loaded (unknown kind, CRC or
// decode failure). The caller marks these partitions+kinds for rebuild from S3
// — the self-heal path. A non-empty Skipped is NOT an error.
type DecodeResult struct {
	Skipped []FacetKind
}

// DecodeBundle reads a bundle, building facets via the registry. A facet whose
// CRC fails, whose kind is unregistered, or that fails to Decode is SKIPPED
// (recorded in DecodeResult.Skipped) rather than failing the whole bundle —
// corruption self-heals via rebuild. A structural error (bad magic, truncated
// header/length) DOES return an error, which the caller treats as
// "rebuild the whole partition".
func DecodeBundle(r io.Reader, reg map[FacetKind]FacetFactory) (*Bundle, DecodeResult, error) {
	var res DecodeResult
	br := bufio.NewReader(r)

	var magic [5]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, res, fmt.Errorf("pmeta: read magic: %w", err)
	}
	if magic != bundleMagic {
		return nil, res, fmt.Errorf("pmeta: bad magic %v (want %v)", magic, bundleMagic)
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
	for i := 0; i < int(cnt); i++ {
		var hdr [facetHdrLen]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return nil, res, fmt.Errorf("pmeta: facet[%d] header: %w", i, err)
		}
		kind := FacetKind(hdr[0])
		ln := binary.BigEndian.Uint32(hdr[2:6])
		wantCRC := binary.BigEndian.Uint32(hdr[6:10])
		payload := make([]byte, ln)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil, res, fmt.Errorf("pmeta: facet[%d] kind=%d payload: %w", i, kind, err)
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			res.Skipped = append(res.Skipped, kind) // corrupt -> rebuild
			continue
		}
		factory, ok := reg[kind]
		if !ok {
			res.Skipped = append(res.Skipped, kind) // unknown kind -> rebuild/ignore
			continue
		}
		f := factory(string(part))
		if err := f.Decode(bytes.NewReader(payload)); err != nil {
			res.Skipped = append(res.Skipped, kind) // decode error -> rebuild
			continue
		}
		b.facets[kind] = f
	}
	b.clearDirty()
	return b, res, nil
}
