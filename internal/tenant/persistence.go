package tenant

import (
	"context"
	"encoding/json"
)

type S3Pool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
}

type S3Persister struct {
	pool S3Pool
	key  string
}

func NewS3Persister(pool S3Pool, key string) *S3Persister {
	return &S3Persister{pool: pool, key: key}
}

func (p *S3Persister) SaveAliases(entries []AliasEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	return p.pool.Upload(context.Background(), p.key, data)
}

func (p *S3Persister) LoadAliases() ([]AliasEntry, error) {
	data, err := p.pool.Download(context.Background(), p.key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var entries []AliasEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	var valid []AliasEntry
	for _, e := range entries {
		if err := ValidateOrgID(e.OrgID); err != nil {
			continue
		}
		valid = append(valid, e)
	}
	return valid, nil
}
