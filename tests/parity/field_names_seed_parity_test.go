//go:build parity

package parity

// The seeded corpus (datagen: many services, MAP-stored attributes, sparse
// fields such as exception.type, spans with events and links) answers
// field_names and stream_field_names exactly like hot VictoriaLogs and
// VictoriaTraces: the same names, the same hits, the same order, for every
// seeded tenant, with no filter and with filters (issues 280, 269, 461, 464).
// Traces are asked for spans (span_id:*): hot VictoriaTraces also lists the
// fields of its trace index rows under `*`, which Lakehouse does not store
// (issue 458).

import (
	"fmt"
	"net/url"
	"reflect"
	"testing"
)

func fieldNamesSeedPair(t *testing.T, hot, cold, endpoint, query string, tenant tenantSummary) {
	t.Helper()
	params := fullRangeParams()
	params.Set("query", query)
	params.Set("disable_latency_offset", "true")
	ref := tenantFetch(t, hot, endpoint, params, tenant.AccountID, tenant.ProjectID)
	sut := tenantFetch(t, cold, endpoint, params, tenant.AccountID, tenant.ProjectID)
	name := fmt.Sprintf("%s %s tenant %s:%s", endpoint, query, tenant.AccountID, tenant.ProjectID)
	compareFieldValues(t, name, ref, sut, false)
	if want, got := fieldNamesOrder(t, name+" reference", ref), fieldNamesOrder(t, name+" SUT", sut); !reflect.DeepEqual(want, got) {
		t.Errorf("%s: order differs:\n hot:  %v\n cold: %v", name, want, got)
	}
	reportLockCells(t, 1) // one cell per compared order (floor in lock_cells.txt)
}

func TestParity_FieldNames_SeedCorpus(t *testing.T) {
	endpoints := []string{"/select/logsql/field_names", "/select/logsql/stream_field_names"}
	t.Run("logs", func(t *testing.T) {
		for _, te := range requireSeededTenants(t) {
			for _, q := range []string{"*", "level:=ERROR", "service.name:=api-gateway", "exception.type:*"} {
				for _, ep := range endpoints {
					t.Run(fmt.Sprintf("%s/%s/%s", te.AccountID, ep[len("/select/logsql/"):], url.QueryEscape(q)), func(t *testing.T) {
						fieldNamesSeedPair(t, vlBaseURL, lhBaseURL, ep, q, te)
					})
				}
			}
		}
	})
	t.Run("traces", func(t *testing.T) {
		for _, te := range requireSeededTenants(t) {
			for _, q := range []string{"span_id:*", "span_id:* `resource_attr:service.name`:=\"api-gateway\"", "span_id:* name:*GET*"} {
				for _, ep := range endpoints {
					t.Run(fmt.Sprintf("%s/%s/%s", te.AccountID, ep[len("/select/logsql/"):], url.QueryEscape(q)), func(t *testing.T) {
						fieldNamesSeedPair(t, vtBaseURL, lhtBaseURL, ep, q, te)
					})
				}
			}
		}
	})
}
