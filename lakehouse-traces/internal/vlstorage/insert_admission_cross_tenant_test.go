package vlstorage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// tenantGate refuses every stream of one tenant (account and project) and admits
// every stream of the others.
type tenantGate struct{ deny logstorage.TenantID }

func (g tenantGate) AllowStream(account, project uint32, _ string) bool {
	return account != g.deny.AccountID || project != g.deny.ProjectID
}

// One stream, one batch, rows of two tenants interleaved, one tenant at its limit:
// whichever tenant comes first, only the tenant with room keeps its rows. The
// tenants differ in the account in one case and only in the project in the other.
func TestVTInsertAdapter_CrossTenantAdmissionBothOrders(t *testing.T) {
	for _, c := range []struct {
		name        string
		full, roomy logstorage.TenantID
		fullFirst   bool
	}{
		{"accounts differ, full tenant first", logstorage.TenantID{AccountID: 2, ProjectID: 7}, logstorage.TenantID{AccountID: 1, ProjectID: 7}, true},
		{"accounts differ, full tenant second", logstorage.TenantID{AccountID: 2, ProjectID: 7}, logstorage.TenantID{AccountID: 1, ProjectID: 7}, false},
		{"projects differ, full tenant first", logstorage.TenantID{AccountID: 1, ProjectID: 8}, logstorage.TenantID{AccountID: 1, ProjectID: 7}, true},
		{"projects differ, full tenant second", logstorage.TenantID{AccountID: 1, ProjectID: 8}, logstorage.TenantID{AccountID: 1, ProjectID: 7}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			SetCardinalityGate(tenantGate{deny: c.full})
			t.Cleanup(func() { SetCardinalityGate(nil) })
			buf := &recordingBuffer{}
			a := &vtInsertAdapter{buf: buf, dir: "/d"}
			now := time.Now().UnixNano()
			lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
			defer logstorage.PutLogRows(lr)
			order := []struct {
				tn   logstorage.TenantID
				name string
			}{{c.roomy, "roomy"}, {c.full, "full"}}
			if c.fullFirst {
				order[0], order[1] = order[1], order[0]
			}
			for i := 0; i < 4; i++ {
				o := order[i%2]
				lr.MustAdd(o.tn, now+int64(i), []logstorage.Field{{Name: "resource_attr:service.name", Value: "api"}, {Name: "span_id", Value: fmt.Sprintf("%s-%d", o.name, i)}, {Name: "trace_id", Value: fmt.Sprintf("t-%s-%d", o.name, i)}}, 1)
			}
			a.MustAddRows(lr)
			got := strings.Join(buf.got(), ",")
			if strings.Contains(got, "full-") || strings.Count(got, "roomy-") != 2 {
				t.Errorf("the buffer got %q, want only the two rows of the tenant with room", got)
			}
		})
	}
}
