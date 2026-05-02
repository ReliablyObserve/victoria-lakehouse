package parquets3

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func testConfig() *config.Config {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	return cfg
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNew(t *testing.T) {
	s, err := New(testConfig(), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s == nil {
		t.Fatal("storage is nil")
	}
}

func TestStorage_ImplementsInterface(t *testing.T) {
	s, _ := New(testConfig(), testLogger())
	var _ storage.Storage = s
}

func TestRunQuery_ReturnsEmpty(t *testing.T) {
	s, _ := New(testConfig(), testLogger())
	called := false
	err := s.RunQuery(context.Background(), &storage.QueryContext{}, func(workerID uint, db *storage.DataBlock) {
		called = true
	})
	if err != nil {
		t.Errorf("RunQuery: %v", err)
	}
	if called {
		t.Error("stub should not call writeBlock")
	}
}

func TestGetFieldNames_ReturnsEmpty(t *testing.T) {
	s, _ := New(testConfig(), testLogger())
	result, err := s.GetFieldNames(context.Background(), &storage.QueryContext{})
	if err != nil {
		t.Errorf("GetFieldNames: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty, got %d results", len(result))
	}
}

func TestGetFieldValues_ReturnsEmpty(t *testing.T) {
	s, _ := New(testConfig(), testLogger())
	result, err := s.GetFieldValues(context.Background(), &storage.QueryContext{}, "test", 100)
	if err != nil {
		t.Errorf("GetFieldValues: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty, got %d results", len(result))
	}
}

func TestClose(t *testing.T) {
	s, _ := New(testConfig(), testLogger())
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
