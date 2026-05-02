package parquets3

import (
	"context"
	"log/slog"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Storage struct {
	cfg    *config.Config
	logger *slog.Logger
}

func New(cfg *config.Config, logger *slog.Logger) (*Storage, error) {
	return &Storage{
		cfg:    cfg,
		logger: logger.With("component", "parquets3"),
	}, nil
}

func (s *Storage) RunQuery(ctx context.Context, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	s.logger.Debug("RunQuery stub", "start", qctx.StartNs, "end", qctx.EndNs, "query", qctx.Query)
	return nil
}

func (s *Storage) GetFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string, limit int) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreamFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreamFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreams(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetTenantIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.TenantID, error) {
	return nil, nil
}

func (s *Storage) Close() error {
	return nil
}
