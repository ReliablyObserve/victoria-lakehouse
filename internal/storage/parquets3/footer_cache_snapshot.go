package parquets3

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// footerCacheSnapshotMagic identifies a binary footer-cache-snapshot
// file. The version byte allows future format evolution without
// silently mis-reading older snapshots; bump it whenever the encoded
// per-entry layout changes.
var footerCacheSnapshotMagic = []byte{'L', 'H', 'F', 'C', 1}

// maxFooterCacheSnapshotBytes caps the on-disk snapshot size so a
// corrupt or maliciously-large file can't OOM the loader. At ~150
// bytes per entry (S3 key path + length prefix), this cap admits
// ~33M cached keys — far above any realistic single-pod cache size.
const maxFooterCacheSnapshotBytes = 5 * 1024 * 1024 * 1024 // 5 GiB

// SaveFooterCacheKeys writes the cache's current key list to path,
// most-recently-used first. The output is a small newline-delimited
// binary file (magic + version + count-prefixed UTF-8 strings); a
// million cache entries fit in well under 200 MiB, so writes are
// fast and bounded.
//
// Called from the lakehouse-{logs,traces} shutdown sequence after
// the manifest snapshot persists; the next process will hand the
// loaded key list to PrefetchFromCacheSnapshot for an async warm.
func SaveFooterCacheKeys(fc *FooterCache, path string) error {
	if fc == nil {
		return errors.New("footer cache is nil")
	}
	keys := fc.Keys()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create footer-cache snapshot: %w", err)
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(footerCacheSnapshotMagic); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write magic: %w", err)
	}
	var sizeBuf [8]byte
	binary.LittleEndian.PutUint64(sizeBuf[:], uint64(len(keys)))
	if _, err := w.Write(sizeBuf[:]); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write count: %w", err)
	}
	for _, k := range keys {
		if len(k) > 0x7FFFFFFF {
			continue // skip absurdly long key (shouldn't happen for S3 keys)
		}
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(k)))
		if _, err := w.Write(hdr[:]); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("write key length: %w", err)
		}
		if _, err := w.WriteString(k); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("write key: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// LoadFooterCacheKeys reads a snapshot written by SaveFooterCacheKeys
// and returns the key list. Returns (nil, nil) when the file doesn't
// exist (first boot / fresh deploy) so the caller can treat absence
// as "no prior cache" rather than an error.
//
// The returned slice preserves the most-recently-used-first ordering
// the snapshot encodes; callers should prefetch from the head.
func LoadFooterCacheKeys(path string) ([]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open footer-cache snapshot: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat footer-cache snapshot: %w", err)
	}
	if info.Size() > maxFooterCacheSnapshotBytes {
		return nil, fmt.Errorf("footer-cache snapshot exceeds limit: %d > %d", info.Size(), maxFooterCacheSnapshotBytes)
	}

	r := bufio.NewReader(f)
	magic := make([]byte, len(footerCacheSnapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if !bytesEqual(magic, footerCacheSnapshotMagic) {
		return nil, fmt.Errorf("footer-cache snapshot magic mismatch: got %v want %v", magic, footerCacheSnapshotMagic)
	}

	var countBuf [8]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}
	n := binary.LittleEndian.Uint64(countBuf[:])
	if n > uint64(maxFooterCacheSnapshotBytes/4) {
		// Refuse to allocate a result slice larger than the input
		// could legitimately specify; the per-entry minimum is the
		// 4-byte length prefix + at least 1 byte of key.
		return nil, fmt.Errorf("footer-cache snapshot count implausible: %d", n)
	}
	keys := make([]string, 0, n)
	var hdr [4]byte
	var sb strings.Builder
	for i := uint64(0); i < n; i++ {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read key length at i=%d: %w", i, err)
		}
		length := binary.LittleEndian.Uint32(hdr[:])
		if uint64(length) > uint64(maxFooterCacheSnapshotBytes) {
			return nil, fmt.Errorf("key length implausible at i=%d: %d", i, length)
		}
		sb.Reset()
		sb.Grow(int(length))
		if _, err := io.CopyN(stringBuilderWriter{&sb}, r, int64(length)); err != nil {
			return nil, fmt.Errorf("read key at i=%d: %w", i, err)
		}
		keys = append(keys, sb.String())
	}
	return keys, nil
}

// bytesEqual exists so we don't pull bytes.Equal into a file that
// otherwise wouldn't need to import "bytes". Tiny enough to inline.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// stringBuilderWriter adapts a strings.Builder to io.Writer so we
// can use io.CopyN with a length-bounded read. Faster than the
// alternative (make([]byte, n) + ReadFull) at scale because it
// reuses the builder's internal []byte.
type stringBuilderWriter struct {
	sb *strings.Builder
}

func (s stringBuilderWriter) Write(p []byte) (int, error) {
	return s.sb.Write(p)
}
