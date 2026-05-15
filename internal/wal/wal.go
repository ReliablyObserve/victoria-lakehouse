package wal

import "io"

type WAL struct{}

func Open(path string, maxBytes int64) (*WAL, error) {
	return &WAL{}, nil
}

func (w *WAL) Close() error { return nil }
func (w *WAL) Write(data []byte) error { return nil }
func (w *WAL) Replay(fn func([]byte) error) error { return nil }
func (w *WAL) Reader() io.Reader { return nil }
