package parquets3

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// uploadTraceRows writes rows the way the buffer drain writes trace groups:
// one object per (tenant, partition), uploaded and added to the manifest.
func uploadTraceRows(t *testing.T, bw *BatchWriter, rows []schema.TraceRow) {
	t.Helper()
	type groupKey struct {
		account, project uint32
		partition        string
	}
	groups := map[groupKey][]schema.TraceRow{}
	var order []groupKey
	for _, r := range rows {
		k := groupKey{r.AccountID, r.ProjectID, partitionFromNano(r.TimestampUnixNano)}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}
	for _, k := range order {
		up := &traceGroupUpload{partition: k.partition, accountID: k.account, projectID: k.project, rows: groups[k]}
		if err := bw.uploadTraceGroup(context.Background(), up); err != nil {
			t.Fatalf("upload %s: %v", k.partition, err)
		}
	}
}
