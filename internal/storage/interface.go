package storage

import "context"

type BlockColumn struct {
	Name   string
	Values []string
}

type DataBlock struct {
	RowsCount int
	Columns   []BlockColumn
}

type WriteDataBlockFunc func(workerID uint, db *DataBlock)

type TenantID struct {
	AccountID uint32
	ProjectID uint32
}

type ValueWithHits struct {
	Value string
	Hits  uint64
}

type QueryContext struct {
	TenantIDs        []TenantID
	StartNs          int64
	EndNs            int64
	Query            string
	RequestedColumns []string // if non-empty, read only these columns (internal names)
}

type Storage interface {
	RunQuery(ctx context.Context, qctx *QueryContext, writeBlock WriteDataBlockFunc) error
	GetFieldNames(ctx context.Context, qctx *QueryContext) ([]ValueWithHits, error)
	GetFieldValues(ctx context.Context, qctx *QueryContext, fieldName string, limit int) ([]ValueWithHits, error)
	GetStreamFieldNames(ctx context.Context, qctx *QueryContext) ([]ValueWithHits, error)
	GetStreamFieldValues(ctx context.Context, qctx *QueryContext, fieldName string) ([]ValueWithHits, error)
	GetStreams(ctx context.Context, qctx *QueryContext) ([]ValueWithHits, error)
	GetStreamIDs(ctx context.Context, qctx *QueryContext) ([]ValueWithHits, error)
	GetTenantIDs(ctx context.Context, qctx *QueryContext) ([]TenantID, error)
	Close() error
}
