//go:build parity

package parity

// The suite compares hot VictoriaLogs/VictoriaTraces with Lakehouse's cold
// tier: Parquet on S3. Rows Lakehouse has acknowledged but not yet written live
// in its insert buffer, which answers with upstream's own engine and agrees with
// hot by construction. A drained buffer segment also stays readable for its
// grace period (2 x manifest.refresh_interval + 30 s, 40 s in the parity stack).
// So every comparison that means "the cold read path" must run only once the
// rows it reads have left the buffer. TestMain holds the suite until the seeded
// corpus has; a case that writes its own rows waits for them (waitLeftBuffer).

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if err := waitSeedInParquet(5 * time.Minute); err != nil {
		fmt.Fprintln(os.Stderr, "parity: "+err.Error())
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// bufferedRows returns how many rows Lakehouse's insert buffer at base holds
// for sel (account_id + project_id, or all_tenants=true) with a _time between
// from and to, read through the endpoint select pods read the buffer with
// (/internal/buffer/query). For traces it counts span rows only: the
// service-graph task writes rows without a span_id every minute.
func bufferedRows(base, mode string, sel url.Values, from, to time.Time) (int, error) {
	params := url.Values{
		"start":        {strconv.FormatInt(from.UnixNano(), 10)},
		"end":          {strconv.FormatInt(to.UnixNano(), 10)},
		"mode":         {mode},
		"tenant_scope": {"v1"},
	}
	for k, v := range sel {
		params[k] = v
	}
	resp, err := httpClient.Get(base + "/internal/buffer/query?" + params.Encode())
	if err != nil {
		return 0, err
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s/internal/buffer/query returned %d: %s", base, resp.StatusCode, body)
	}
	n := 0
	for _, line := range strings.Split(string(body), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if mode == "traces" {
			var row struct {
				SpanID string `json:"span_id"`
			}
			if json.Unmarshal([]byte(line), &row) != nil || row.SpanID == "" {
				continue
			}
		}
		n++
	}
	return n, nil
}

// manifestRows returns the rows the cold manifest at base records over every
// tenant: the rows that are in Parquet objects.
func manifestRows(base string) (int64, error) {
	resp, err := httpClient.Get(base + "/lakehouse/api/v1/tenants")
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var d struct {
		Tenants []tenantSummary `json:"tenants"`
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s/lakehouse/api/v1/tenants returned %d", base, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return 0, err
	}
	var total int64
	for _, te := range d.Tenants {
		total += te.TotalRows
	}
	return total, nil
}

// waitSeedInParquet returns once neither Lakehouse buffer holds a seeded row
// (any tenant, _time before now) and both cold manifests record rows, so the
// seeded corpus is read from Parquet only.
func waitSeedInParquet(budget time.Duration) error {
	cut := time.Now()
	all := url.Values{"all_tenants": {"true"}}
	deadline := time.Now().Add(budget)
	var last string
	for {
		logsBuf, err1 := bufferedRows(lhBaseURL, "logs", all, time.Unix(0, 0), cut)
		tracesBuf, err2 := bufferedRows(lhtBaseURL, "traces", all, time.Unix(0, 0), cut)
		logsPq, err3 := manifestRows(lhBaseURL)
		tracesPq, err4 := manifestRows(lhtBaseURL)
		last = fmt.Sprintf("seed rows in the insert buffers: logs=%d traces=%d; rows in Parquet: logs=%d traces=%d; errors: %v %v %v %v",
			logsBuf, tracesBuf, logsPq, tracesPq, err1, err2, err3, err4)
		if err1 == nil && err2 == nil && err3 == nil && err4 == nil &&
			logsBuf == 0 && tracesBuf == 0 && logsPq > 0 && tracesPq > 0 {
			fmt.Fprintln(os.Stderr, "parity: the seeded corpus is in Parquet: "+last)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the seeded corpus never left the insert buffers within %s: %s", budget, last)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitLeftBuffer returns once Lakehouse's insert buffer at base holds no row of
// account:0 between from and to; the rows are then read from Parquet only.
func waitLeftBuffer(t *testing.T, base, mode, account string, from, to time.Time) {
	t.Helper()
	sel := url.Values{"account_id": {account}, "project_id": {"0"}}
	deadline := time.Now().Add(150 * time.Second)
	for {
		n, err := bufferedRows(base, mode, sel, from, to)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d rows of tenant %s:0 still in the insert buffer of %s after 150s", n, account, base)
		}
		time.Sleep(2 * time.Second)
	}
}
