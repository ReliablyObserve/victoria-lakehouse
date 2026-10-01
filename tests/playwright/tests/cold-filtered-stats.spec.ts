import { test, expect, APIRequestContext, Page } from '@playwright/test';

// Filtered `stats count()` / `stats by (field) count()` on cold (flushed
// Parquet) data must show what the same query shows on hot VictoriaLogs (issue
// #273). The spec asks Grafana for the frames both datasources return, as the
// Explore panels do, and requires the Lakehouse frames to equal the reference
// frames: same series, same label sets, same counts, never empty.
//
// It needs a stack where the marker rows were written to Lakehouse AND to a hot
// VictoriaLogs and Lakehouse has flushed them to Parquet. Run with
//
//   COLD_FILTERED_STATS=1 GRAFANA_URL=http://127.0.0.1:3000 \
//   LH_DS_UID=lh-logs REF_DS_UID=hot-logs LH_LOKI_UID=lh-loki REF_LOKI_UID=hot-loki \
//   npx playwright test cold-filtered-stats
//
// The marker rows: 90 rows, `_msg` either `needle273` (every 9th row) or
// `MARKER273 request <i> from <service>`, fields repro_layer=cold,
// service.name in alpha|beta|gamma, level in info|error; stream fields
// repro_layer,service.name; written inside the last ten minutes.

const enabled = process.env.COLD_FILTERED_STATS === '1';
const LH = process.env.LH_DS_UID || 'lh-logs';
const REF = process.env.REF_DS_UID || 'hot-logs';
const LH_LOKI = process.env.LH_LOKI_UID || 'lh-loki';
const REF_LOKI = process.env.REF_LOKI_UID || 'hot-loki';
const VL_TYPE = 'victoriametrics-logs-datasource';

interface Frame {
  name: string;
  labels: string;
  rows: number;
  sums: number[];
}

// fingerprint reduces Grafana's /api/ds/query response to what must match
// between two datasources: per frame, its label set, row count, and the sum of
// every numeric (non-time) field. Timestamps are ignored on purpose; the two
// stacks do not share them.
async function frames(
  request: APIRequestContext,
  uid: string,
  type: string,
  query: Record<string, unknown>,
  from = 'now-1h',
): Promise<Frame[]> {
  const res = await request.post('/api/ds/query', {
    data: {
      queries: [{ refId: 'A', datasource: { type, uid }, ...query }],
      from,
      to: 'now',
    },
  });
  expect(res.status(), `${uid} ${JSON.stringify(query)}`).toBe(200);
  const body = await res.json();
  const result = body.results?.A;
  expect(result?.error, `${uid} returned an error for ${JSON.stringify(query)}`).toBeFalsy();
  const out: Frame[] = [];
  for (const f of result?.frames ?? []) {
    const fields = f.schema?.fields ?? [];
    const values = f.data?.values ?? [];
    const sums: number[] = [];
    let labels = '';
    fields.forEach((fl: any, i: number) => {
      if (fl.labels) labels += JSON.stringify(fl.labels);
      if (fl.type === 'number') {
        sums.push((values[i] ?? []).reduce((a: number, b: number) => a + (b ?? 0), 0));
      }
    });
    out.push({ name: f.schema?.name ?? '', labels, rows: values.length ? values[0].length : 0, sums });
  }
  return out.sort((a, b) => (a.labels + a.name).localeCompare(b.labels + b.name));
}

const total = (fs: Frame[]) => fs.reduce((n, f) => n + f.sums.reduce((a, b) => a + b, 0), 0);

// wantTotal is exact for queries bounded by `_time:30m` (the marker rows are
// written once, inside that window); minTotal is a floor for unbounded ones, so
// re-seeding a stack does not break the spec.
const cases: { name: string; expr: string; queryType: string; step?: string; wantTotal?: number; minTotal?: number }[] = [
  { name: 'default-field exact beside _time', expr: '_time:30m _msg:="needle273" | stats count() n', queryType: 'statsRange', step: '1m', wantTotal: 10 },
  { name: 'word + level beside _time', expr: '_time:30m MARKER273 level:=error | stats count() n', queryType: 'statsRange', step: '1m', wantTotal: 40 },
  { name: 'stats by an unregistered field', expr: 'MARKER273 | stats by (repro_layer) count() n', queryType: 'statsRange', step: '1m', minTotal: 80 },
  { name: 'stats by a field, filter beside _time', expr: '_time:30m _msg:="needle273" | stats by (service.name) count() n', queryType: 'statsRange', step: '1m', wantTotal: 10 },
  { name: 'instant stats by two fields', expr: 'MARKER273 | stats by (service.name, repro_layer) count() n', queryType: 'stats', minTotal: 80 },
];

test.describe('Cold filtered stats equal the hot reference (issue #273)', () => {
  test.skip(!enabled, 'set COLD_FILTERED_STATS=1 against a seeded stack');

  for (const c of cases) {
    test(`frames: ${c.name}`, async ({ request }) => {
      const q = { expr: c.expr, queryType: c.queryType, ...(c.step ? { step: c.step } : {}) };
      const lh = await frames(request, LH, VL_TYPE, q);
      const ref = await frames(request, REF, VL_TYPE, q);
      expect(ref.length, 'reference must return data (stack not seeded?)').toBeGreaterThan(0);
      expect(lh, `cold frames for ${c.expr}`).toEqual(ref);
      if (c.wantTotal !== undefined) {
        expect(total(lh)).toBe(c.wantTotal);
      }
      if (c.minTotal !== undefined) {
        expect(total(lh)).toBeGreaterThanOrEqual(c.minTotal);
      }
    });
  }

  test('frames: Loki datasource (loki-vl-proxy) filtered count by label', async ({ request }) => {
    const q = {
      expr: 'sum by (repro_layer) (count_over_time({service_name=~".+"} |= "MARKER273" [1m]))',
      queryType: 'range',
      step: '1m',
    };
    const lh = await frames(request, LH_LOKI, 'loki', q);
    const ref = await frames(request, REF_LOKI, 'loki', q);
    expect(ref.length).toBeGreaterThan(0);
    expect(lh).toEqual(ref);
  });

  for (const c of cases.slice(0, 3)) {
    test(`Explore panel shows data: ${c.name}`, async ({ page }: { page: Page }) => {
      const errors: string[] = [];
      page.on('pageerror', (e) => errors.push(String(e)));
      const panes = JSON.stringify({
        A: {
          datasource: LH,
          queries: [{ refId: 'A', datasource: { type: VL_TYPE, uid: LH }, expr: c.expr, queryType: c.queryType, ...(c.step ? { step: c.step } : {}) }],
          range: { from: 'now-1h', to: 'now' },
        },
      });
      await page.goto(`/explore?schemaVersion=1&orgId=1&panes=${encodeURIComponent(panes)}`, { waitUntil: 'domcontentloaded' });
      await page.waitForLoadState('networkidle');
      await expect(page.locator('body')).not.toContainText(/No data/i, { timeout: 20_000 });
      expect(errors, 'uncaught page errors').toEqual([]);
    });
  }
});
