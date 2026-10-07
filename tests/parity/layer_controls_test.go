//go:build parity

package parity

// Controls and proofs for the data layers of Lakehouse a parity case compares
// in: the manual recompaction trigger (an existing Lakehouse endpoint), a
// restart of a Lakehouse service (compose_guard_test.go), and checks that the
// layer is the one the cell claims (buffer rows, object levels in S3).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// recompactHourPartition merges the objects of the hour partition holding at,
// through POST /lakehouse/compaction/recompact, and returns once Lakehouse
// reports it merged at least minInputs objects (two per tenant). A segment's objects are held back
// from compaction for twice the flush grace after it was committed, so the
// trigger answers 400 until then; it is retried.
func recompactHourPartition(t *testing.T, base string, at time.Time, minInputs int) {
	t.Helper()
	u := at.UTC()
	part := fmt.Sprintf("dt=%s/hour=%02d", u.Format("2006-01-02"), u.Hour())
	body, _ := json.Marshal(map[string]any{"partition": part})
	deadline := time.Now().Add(4 * time.Minute)
	var last string
	for {
		resp, err := httpClient.Post(base+"/lakehouse/compaction/recompact", "application/json", bytes.NewReader(body))
		if err == nil {
			var out struct {
				InputFiles  int `json:"input_files"`
				OutputFiles int `json:"output_files"`
			}
			raw := readAllOrEmpty(resp)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && json.Unmarshal(raw, &out) == nil && out.InputFiles >= minInputs {
				t.Logf("recompacted %s: %d objects into %d", part, out.InputFiles, out.OutputFiles)
				return
			}
			last = fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("recompact %s on %s never merged %d objects; last answer: %s", part, base, minInputs, last)
		}
		time.Sleep(5 * time.Second)
	}
}

// --- tenant forms ----------------------------------------------------------

// The parity stack gives each Lakehouse binary one static alias
// (-lakehouse.tenant.alias in docker-compose.yml). Hot VictoriaLogs and
// VictoriaTraces have no aliases, so a case that uses the alias form asks
// Lakehouse with the OrgID header and hot with the numeric IDs the alias maps
// to.
const (
	parityLogsOrgID   = "parity-logs-orgid"   // -> AccountID 7312, ProjectID 0 on lakehouse-logs
	parityTracesOrgID = "parity-traces-orgid" // -> parityTracesAccount, ProjectID 0 on lakehouse-traces

	// parityTracesAccount is the traces tenant of the alias form. It is written
	// by cases, not seeded: requireSeededTenants leaves it out.
	parityTracesAccount = "7302"
)

// tenantForm is one tenant a case writes and reads: its numeric IDs and, for
// the alias form, the string OrgID Lakehouse resolves to them.
type tenantForm struct {
	name    string // numeric | alias
	account string // AccountID (ProjectID is 0)
	orgID   string // empty for the numeric form
}

func numericTenant(account string) tenantForm { return tenantForm{"numeric", account, ""} }

func aliasTenant(account, orgID string) tenantForm { return tenantForm{"alias", account, orgID} }

// header returns the tenant headers of a request: the OrgID header for the
// alias form sent to Lakehouse (cold), the numeric AccountID/ProjectID headers
// otherwise (hot always).
func (f tenantForm) header(cold bool) http.Header {
	h := http.Header{}
	if cold && f.orgID != "" {
		h.Set("X-Scope-OrgID", f.orgID)
		return h
	}
	h.Set("AccountID", f.account)
	h.Set("ProjectID", "0")
	return h
}

// getWith issues a GET with the given headers.
func getWith(t *testing.T, base, path string, params url.Values, hdr http.Header) fetchResult {
	t.Helper()
	u := base + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = hdr.Clone()
	resp, err := httpClient.Do(req)
	if err != nil {
		return fetchResult{StatusCode: 0, Body: []byte(err.Error())}
	}
	defer func() { _ = resp.Body.Close() }()
	return fetchResult{StatusCode: resp.StatusCode, Body: readAllOrEmpty(resp)}
}

// post POSTs body with the given headers and requires a 2xx answer.
func post(t *testing.T, u, contentType string, body []byte, hdr http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = hdr.Clone()
	req.Header.Set("Content-Type", contentType)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", u, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("POST %s: status %d", u, resp.StatusCode)
	}
}

// pushSpanAs writes one span, ending at endAt, to a tier's OTLP endpoint with
// the given tenant headers.
func pushSpanAs(t *testing.T, base string, hdr http.Header, traceID, spanID, service string, endAt time.Time) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": []map[string]any{
			{"key": "service.name", "value": map[string]any{"stringValue": service}},
		}},
		"scopeSpans": []map[string]any{{
			"scope": map[string]any{"name": "layer-parity"},
			"spans": []map[string]any{{
				"traceId":           traceID,
				"spanId":            spanID,
				"name":              "layer-parity-probe",
				"kind":              2,
				"startTimeUnixNano": fmt.Sprintf("%d", endAt.Add(-time.Second).UnixNano()),
				"endTimeUnixNano":   fmt.Sprintf("%d", endAt.UnixNano()),
			}},
		}},
	}}})
	post(t, base+"/insert/opentelemetry/v1/traces", "application/json", body, hdr)
}

// --- layer proofs ----------------------------------------------------------

// bufferedNow returns how many rows of the tenant the insert buffer at base
// holds between from and to.
func bufferedNow(t *testing.T, base, mode, account string, from, to time.Time) int {
	t.Helper()
	sel := url.Values{"account_id": {account}, "project_id": {"0"}}
	n, err := bufferedRows(base, mode, sel, from, to)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// requireBuffered fails unless the insert buffer at base holds exactly want rows
// of the tenant between from and to.
func requireBuffered(t *testing.T, base, mode, account string, from, to time.Time, want int) {
	t.Helper()
	if n := bufferedNow(t, base, mode, account, from, to); n != want {
		t.Fatalf("layer proof: tenant %s holds %d rows in the insert buffer of %s, want %d", account, n, base, want)
	}
}

// waitBuffered returns once the insert buffer holds exactly want rows of the
// tenant: rows just written take a moment to show in /internal/buffer/query,
// and a wait for them to leave the buffer must not start before they are in.
func waitBuffered(t *testing.T, base, mode, account string, from, to time.Time, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for bufferedNow(t, base, mode, account, from, to) != want {
		if time.Now().After(deadline) {
			requireBuffered(t, base, mode, account, from, to, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func parityS3() *s3.Client {
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(envOrDefault("PARITY_S3_ENDPOINT", "http://s3:9000")),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
	})
}

// partitionObjects lists the objects the tenant (ProjectID 0) holds in the hour
// partition of at, as base names. Layout: <account>/0/<mode>/dt=D/hour=HH/.
func partitionObjects(t *testing.T, mode, account string, at time.Time) []string {
	t.Helper()
	var names []string
	for _, o := range partitionObjectInfo(t, mode, account, at) {
		names = append(names, o.name)
	}
	return names
}

type objectInfo struct {
	name     string
	modified time.Time
}

// partitionObjectInfo is partitionObjects with each object's S3 LastModified
// (second granularity).
func partitionObjectInfo(t *testing.T, mode, account string, at time.Time) []objectInfo {
	t.Helper()
	u := at.UTC()
	prefix := fmt.Sprintf("%s/0/%s/dt=%s/hour=%02d/", account, mode, u.Format("2006-01-02"), u.Hour())
	out, err := parityS3().ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(envOrDefault("PARITY_S3_BUCKET", "parity-bucket")), Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("list %s: %v", prefix, err)
	}
	var infos []objectInfo
	for _, o := range out.Contents {
		if k := aws.ToString(o.Key); strings.HasSuffix(k, ".parquet") {
			infos = append(infos, objectInfo{strings.TrimPrefix(k, prefix), aws.ToTime(o.LastModified)})
		}
	}
	return infos
}

// objectLevels counts a partition's objects by compaction level: names that
// start with compacted-L<n>- are level n, every other flushed object is L0.
func objectLevels(names []string) (l0, l1 int) {
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "compacted-L1-"):
			l1++
		case strings.HasPrefix(n, "compacted-L"):
			l1 += 1000 // a level above L1 is not expected here: fail the proofs
		default:
			l0++
		}
	}
	return l0, l1
}

// requireFlushedOnly is the parquet layer proof: the tenant holds only L0
// objects (at least one) in the partition and nothing in the insert buffer.
func requireFlushedOnly(t *testing.T, base, mode, account string, at, from, to time.Time) {
	t.Helper()
	requireBuffered(t, base, mode, account, from, to, 0)
	l0, l1 := objectLevels(partitionObjects(t, mode, account, at))
	if l0 < 1 || l1 != 0 {
		t.Fatalf("layer proof: tenant %s has %d L0 and %d compacted objects in the partition, want L0 only", account, l0, l1)
	}
}

// requireCompactedOnce is the compacted layer proof: no L0 object and exactly
// one L1 object left in the partition.
func requireCompactedOnce(t *testing.T, mode, account string, at time.Time) {
	t.Helper()
	l0, l1 := objectLevels(partitionObjects(t, mode, account, at))
	if l0 != 0 || l1 != 1 {
		t.Fatalf("layer proof: tenant %s has %d L0 and %d L1 objects in the partition, want 0 and 1", account, l0, l1)
	}
}

// pickQuietHour returns a time inside the newest hour, at least 97 minutes
// back and within hot's 48 h retention, in which none of the given tenants
// (account per mode) holds an object, so a case's partition holds only its own
// objects and a rerun on the same stack finds a clean partition. It fails when
// there is none.
func pickQuietHour(t *testing.T, accounts map[string][]string) time.Time {
	t.Helper()
	h := time.Now().Add(-97 * time.Minute).UTC().Truncate(time.Hour)
	for k := 0; k < 45; k++ {
		cand := h.Add(-time.Duration(k) * time.Hour)
		quiet := true
		for mode, accs := range accounts {
			for _, a := range accs {
				if len(partitionObjects(t, mode, a, cand)) > 0 {
					quiet = false
				}
			}
		}
		if quiet {
			return cand.Add(30 * time.Minute)
		}
	}
	t.Fatalf("no hour in the last 46 h is free of objects of tenants %v; wipe the stack volumes", accounts)
	return time.Time{}
}

// requireRecoveredSegmentFlushed is the restart layer proof: the tenant holds
// the compacted L1 object plus exactly one new L0 object, and that L0 was
// written after the container's new StartedAt (S3 LastModified has second
// granularity, so both are compared truncated to the second). A flush during
// the shutdown would have been written before the restart: this object comes
// from the segment the restarted pod recovered.
func requireRecoveredSegmentFlushed(t *testing.T, mode, account string, at, startedAt time.Time) {
	t.Helper()
	var l0 []objectInfo
	l1 := 0
	for _, o := range partitionObjectInfo(t, mode, account, at) {
		if strings.HasPrefix(o.name, "compacted-L1-") {
			l1++
		} else if !strings.HasPrefix(o.name, "compacted-L") {
			l0 = append(l0, o)
		}
	}
	if l1 != 1 || len(l0) != 1 {
		t.Fatalf("layer proof: tenant %s has %d L1 and %d L0 objects after the restart, want 1 and 1", account, l1, len(l0))
	}
	if l0[0].modified.Truncate(time.Second).Before(startedAt.Truncate(time.Second)) {
		t.Fatalf("layer proof: tenant %s's new L0 %s was written at %s, before the restart finished starting (%s): it was drained on shutdown, not flushed from the recovered segment",
			account, l0[0].name, l0[0].modified.UTC().Format(time.RFC3339), startedAt.UTC().Format(time.RFC3339Nano))
	}
}
