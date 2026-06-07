package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	modeLog   = 'L'
	modeTrace = 'T'
)

type WAL struct {
	file     *os.File
	maxBytes int64
	size     int64
	closed   bool
}

func Open(path string, maxBytes int64) (*WAL, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		if closeErr := f.Close(); closeErr != nil {
			return nil, fmt.Errorf("stat WAL: %w; close WAL: %v", err, closeErr)
		}
		return nil, fmt.Errorf("stat WAL: %w", err)
	}
	return &WAL{file: f, maxBytes: maxBytes, size: info.Size()}, nil
}

func (w *WAL) Close() error {
	if w.closed {
		return fmt.Errorf("WAL already closed")
	}
	w.closed = true
	return w.file.Close()
}

func (w *WAL) Write(data []byte) error { return nil }
func (w *WAL) Reader() io.Reader       { return nil }

func (w *WAL) appendEntry(mode byte, v any) error {
	if w.closed {
		return fmt.Errorf("WAL closed")
	}
	if w.maxBytes > 0 && w.size >= w.maxBytes {
		return fmt.Errorf("WAL full")
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	payload := buf.Bytes()

	header := make([]byte, 5)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	header[4] = mode

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(payload); err != nil {
		return err
	}

	w.size += int64(5 + len(payload))
	return nil
}

func (w *WAL) AppendLog(row *schema.LogRow) error     { return w.appendEntry(modeLog, row) }
func (w *WAL) AppendTrace(row *schema.TraceRow) error { return w.appendEntry(modeTrace, row) }

func (w *WAL) Truncate() error {
	if w.closed {
		return fmt.Errorf("WAL closed")
	}
	name := w.file.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	w.file = f
	w.size = 0
	return nil
}

func (w *WAL) Size() int64 { return w.size }
func (w *WAL) IsFull() bool {
	return w.maxBytes > 0 && w.size >= w.maxBytes
}

func (w *WAL) Replay() ([]schema.LogRow, []schema.TraceRow, error) {
	if w.closed {
		return nil, nil, fmt.Errorf("WAL closed")
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	var logs []schema.LogRow
	var traces []schema.TraceRow
	header := make([]byte, 5)

	for {
		if _, err := io.ReadFull(w.file, header); err != nil {
			break
		}
		entryLen := binary.LittleEndian.Uint32(header[:4])
		mode := header[4]

		// Defensive cap. entryLen is attacker-controlled: a corrupt
		// WAL or a bit-flipped length prefix can declare ~4.3 GB.
		// make([]byte, entryLen) without a guard then asks the
		// allocator for multi-GB and either panics in runtime or
		// gets OOM-killed mid-replay — the FuzzReplayCorruptedRecord
		// fuzz target lit this up on CI (exit 143 / SIGTERM after
		// ~24 s). 64 MiB is well above any honest record (a single
		// LogRow is a few KB; the largest realistic trace span at
		// full attribute fan-out is ~100 KB), and bounds the
		// worst-case allocation safely. Treat oversize lengths as
		// "trailing garbage" — break out and return what was
		// successfully decoded before the cliff, matching the same
		// behaviour as a truncated final record.
		const maxEntryLen = 64 << 20
		if entryLen > maxEntryLen {
			break
		}

		data := make([]byte, entryLen)
		if _, err := io.ReadFull(w.file, data); err != nil {
			break
		}

		switch mode {
		case modeLog:
			var row schema.LogRow
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&row); err != nil {
				return logs, traces, nil
			}
			logs = append(logs, row)
		case modeTrace:
			var row schema.TraceRow
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&row); err != nil {
				return logs, traces, nil
			}
			traces = append(traces, row)
		default:
			return logs, traces, nil
		}
	}

	return logs, traces, nil
}
