//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestTenantS3Isolation_ParquetLandsUnderPerTenantPrefix verifies the
// in-path isolation contract: rows ingested with a given tenant identity
// must produce Parquet files at "{AccountID}/{ProjectID}/<mode>/..." in
// S3 — NOT at the default "0/0/<mode>/" prefix.
//
// Without this assertion the broader bidirectional-mapping work is
// stats-only and the physical layout silently degrades to single-prefix.
func TestTenantS3Isolation_ParquetLandsUnderPerTenantPrefix(t *testing.T) {
	stamp := time.Now().UnixNano()
	stringOrg := fmt.Sprintf("e2e-iso-string-%d", stamp)
	intAccount := "888"
	intProject := "2"

	ingestLog(t, logsBaseURL, withOrgID(stringOrg), "iso string log")
	ingestLog(t, logsBaseURL, withAccountProject(intAccount, intProject), "iso int log")
	ingestTrace(t, tracesBaseURL, withOrgID(stringOrg))
	ingestTrace(t, tracesBaseURL, withAccountProject(intAccount, intProject))

	// Wait long enough for the writer flush interval to roll over and
	// for the manifest to register the per-tenant files.
	deadline := time.Now().Add(150 * time.Second)
	var stringAccount uint32
	var seenPrefixes map[string]int
	for time.Now().Before(deadline) {
		var ok bool
		stringAccount, ok = lookupAliasAccount(t, logsBaseURL, stringOrg)
		if !ok {
			time.Sleep(3 * time.Second)
			continue
		}
		seenPrefixes = listS3PrefixSizes(t, "logs")
		stringKey := fmt.Sprintf("%d/0/logs/", stringAccount)
		intKey := fmt.Sprintf("%s/%s/logs/", intAccount, intProject)
		if seenPrefixes[stringKey] > 0 && seenPrefixes[intKey] > 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}

	if stringAccount == 0 {
		t.Fatalf("string tenant %q never resolved to an account ID", stringOrg)
	}

	stringPrefix := fmt.Sprintf("%d/0/logs/", stringAccount)
	intPrefix := fmt.Sprintf("%s/%s/logs/", intAccount, intProject)

	if seenPrefixes[stringPrefix] == 0 {
		t.Errorf("no Parquet files under per-tenant prefix %s (string-tenant data still landing at default)", stringPrefix)
	} else {
		t.Logf("string tenant %q (account=%d): %d S3 objects under %s",
			stringOrg, stringAccount, seenPrefixes[stringPrefix], stringPrefix)
	}
	if seenPrefixes[intPrefix] == 0 {
		t.Errorf("no Parquet files under per-tenant prefix %s (int-tenant data still landing at default)", intPrefix)
	} else {
		t.Logf("int tenant %s: %d S3 objects under %s",
			intPrefix, seenPrefixes[intPrefix], intPrefix)
	}

	// Mirror the assertion for traces.
	tracePrefixes := listS3PrefixSizes(t, "traces")
	stringTracePrefix := fmt.Sprintf("%d/0/traces/", stringAccount)
	intTracePrefix := fmt.Sprintf("%s/%s/traces/", intAccount, intProject)
	if tracePrefixes[stringTracePrefix] == 0 {
		t.Errorf("no trace Parquet files under per-tenant prefix %s", stringTracePrefix)
	} else {
		t.Logf("traces string tenant: %d S3 objects under %s",
			tracePrefixes[stringTracePrefix], stringTracePrefix)
	}
	if tracePrefixes[intTracePrefix] == 0 {
		t.Errorf("no trace Parquet files under per-tenant prefix %s", intTracePrefix)
	} else {
		t.Logf("traces int tenant: %d S3 objects under %s",
			tracePrefixes[intTracePrefix], intTracePrefix)
	}

	t.Logf("S3 prefix isolation verified — per-tenant prefixes hold data for both header forms")
}

// listS3PrefixSizes returns a map of {prefix → object count} for every
// "<account>/<project>/<mode>/" prefix that contains at least one
// Parquet object. Uses the e2e test's existing S3 client helpers.
func listS3PrefixSizes(t *testing.T, mode string) map[string]int {
	t.Helper()
	client := newS3Client(t)
	keys := listS3Objects(t, client, "")
	out := make(map[string]int)
	for _, k := range keys {
		if !strings.HasSuffix(k, ".parquet") {
			continue
		}
		parts := strings.SplitN(k, "/", 4)
		if len(parts) < 3 {
			continue
		}
		if parts[2] != mode {
			continue
		}
		prefix := parts[0] + "/" + parts[1] + "/" + parts[2] + "/"
		out[prefix]++
	}
	return out
}
