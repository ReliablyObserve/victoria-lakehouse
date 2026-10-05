package ingestmatrix

import (
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestMixedNativeCarriesDistinctTenantsAndArbitraryTraceIDs(t *testing.T) {
	for _, sig := range []Signal{Logs, Traces} {
		params := []Params{{Marker: "native-id-with-slash/one", Base: time.Now(), Tenant: NumericTenant}, {Marker: "native-id-with-slash/two", Base: time.Now(), Tenant: AliasTenant}}
		req := MixedNativeRequest(sig, params)
		body := req.Body
		counts := map[logstorage.TenantID]int{}
		for len(body) > 0 {
			var row logstorage.InsertRow
			tail, err := row.UnmarshalInplace(body)
			if err != nil {
				t.Fatal(err)
			}
			body = tail
			counts[row.TenantID]++
			if sig == Traces {
				want := params[0].Marker
				if row.TenantID.AccountID == AliasTenant.Account {
					want = params[1].Marker
				}
				found := false
				for _, field := range row.Fields {
					if field.Name == "trace_id" && field.Value == want {
						found = true
					}
				}
				if !found {
					t.Fatal("arbitrary native trace ID changed")
				}
			}
		}
		if len(counts) != 2 || counts[logstorage.TenantID{AccountID: NumericTenant.Account, ProjectID: NumericTenant.Project}] != 3 || counts[logstorage.TenantID{AccountID: AliasTenant.Account, ProjectID: AliasTenant.Project}] != 3 {
			t.Fatalf("mixed tenants %v", counts)
		}
	}
}
