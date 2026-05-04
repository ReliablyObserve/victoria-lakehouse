package wal

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	modeLog   byte = 'L'
	modeTrace byte = 'T'
)

// WAL is an append-only write-ahead log that persists LogRow and TraceRow
// entries to disk using gob encoding. Each entry is prefixed with a 4-byte
// little-endian length and a 1-byte mode marker ('L' or 'T').
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	size int64
	max  int64
}

// Open opens or creates the WAL at path with the given maximum byte capacity.
func Open(path string, maxBytes int64) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{file: f, path: path, size: info.Size(), max: maxBytes}, nil
}

// AppendLog encodes a LogRow and appends it to the WAL.
func (w *WAL) AppendLog(row *schema.LogRow) error {
	return w.append(modeLog, row)
}

// AppendTrace encodes a TraceRow and appends it to the WAL.
func (w *WAL) AppendTrace(row *schema.TraceRow) error {
	return w.append(modeTrace, row)
}

func (w *WAL) append(mode byte, row any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size >= w.max {
		return fmt.Errorf("WAL full (%d >= %d bytes)", w.size, w.max)
	}

	var buf []byte
	enc := gob.NewEncoder(writerFunc(func(p []byte) (int, error) {
		buf = append(buf, p...)
		return len(p), nil
	}))
	if err := enc.Encode(row); err != nil {
		return fmt.Errorf("gob encode: %w", err)
	}

	header := make([]byte, 5)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(buf)))
	header[4] = mode

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	w.size += int64(5 + len(buf))
	return nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// Replay reads all entries from the start of the WAL and returns them as
// separate log and trace slices. On partial or corrupt entries (as from a
// crash mid-write), replay stops at the first unreadable record rather than
// skipping it, ensuring a safe recovery boundary.
func (w *WAL) Replay() ([]schema.LogRow, []schema.TraceRow, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	var logs []schema.LogRow
	var traces []schema.TraceRow

replay:
	for {
		var header [5]byte
		if _, err := io.ReadFull(w.file, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return logs, traces, err
		}
		length := binary.LittleEndian.Uint32(header[:4])
		mode := header[4]

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			// Partial entry — crash boundary; stop here.
			break
		}

		switch mode {
		case modeLog:
			var row schema.LogRow
			rd := readerFunc(data)
			if err := gob.NewDecoder(&rd).Decode(&row); err != nil {
				// Corrupt entry — stop replay at this boundary.
				break replay
			}
			logs = append(logs, row)
		case modeTrace:
			var row schema.TraceRow
			rd := readerFunc(data)
			if err := gob.NewDecoder(&rd).Decode(&row); err != nil {
				break replay
			}
			traces = append(traces, row)
		default:
			// Unknown mode — corrupt WAL; stop.
			break replay
		}
	}

	return logs, traces, nil
}

// readerFunc is a byte-slice reader implementing io.Reader for gob.NewDecoder.
type readerFunc []byte

func (r *readerFunc) Read(p []byte) (int, error) {
	if len(*r) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *r)
	*r = (*r)[n:]
	return n, nil
}

// Truncate atomically replaces the WAL file with an empty one, discarding all
// entries. Call this after a successful flush to S3.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}

	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	f.Close()

	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}

	w.file, err = os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	w.size = 0
	return nil
}

// Size returns the current byte size of the WAL file.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// IsFull reports whether the WAL has reached its maximum capacity.
func (w *WAL) IsFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size >= w.max
}

// Close flushes and closes the underlying WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
