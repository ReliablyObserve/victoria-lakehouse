import { test, expect, Page } from '@playwright/test';

// Rows buffered after a graceful restart, in the same UTC hour as the data the
// shutdown flushed, must be visible in Grafana exactly once (#272).
//
// Run against a stack seeded by scripts/bench/restart-ab/seed.py (cold rows,
// graceful Lakehouse restart, same-hour rows):
//   GRAFANA_URL=http://127.0.0.1:39303 DS_PREFIX=lh npx playwright test restart-buffer-visibility
// DS_PREFIX=hot checks the hot VictoriaLogs/VictoriaTraces reference with the
// same expectations.

const PREFIX = process.env.DS_PREFIX || 'lh';
const EXPECT_LOG_LINES = Number(process.env.EXPECT_LOG_LINES || 40);
const EXPECT_TRACES = Number(process.env.EXPECT_TRACES || 10);
const RANGE = process.env.RANGE || '1h';

function exploreUrl(ds: string, dsType: string, query: Record<string, unknown>) {
  const panes = JSON.stringify({
    A: { datasource: ds, queries: [{ refId: 'A', datasource: { type: dsType, uid: ds }, ...query }], range: { from: `now-${RANGE}`, to: 'now+5m' } },
  });
  return `/explore?schemaVersion=1&orgId=1&panes=${encodeURIComponent(panes)}`;
}

async function frames(page: Page, url: string) {
  const out: { refId: string; rows: number; error: string | null }[] = [];
  const consoleErrors: string[] = [];
  page.on('console', (m) => {
    // The anonymous Grafana answers one 401 for the sign-in probe on every page.
    if (m.type() === 'error' && !m.text().includes('401')) consoleErrors.push(m.text().slice(0, 200));
  });
  page.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)));
  page.on('response', async (r) => {
    if (!r.url().includes('/api/ds/query') || r.request().method() !== 'POST') return;
    const j = await r.json().catch(() => ({ results: {} }));
    for (const [refId, res] of Object.entries<any>(j.results || {})) {
      for (const f of res.frames || []) out.push({ refId, rows: f.data?.values?.[0]?.length || 0, error: res.error || null });
    }
  });
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(8_000);
  return { out, consoleErrors };
}

test.describe('post-restart same-hour visibility', () => {
  test('Explore logs shows cold and post-restart rows, once each', async ({ page }) => {
    const { out, consoleErrors } = await frames(
      page,
      exploreUrl(`${PREFIX}-logs`, 'victoriametrics-logs-datasource', { expr: '*', queryType: 'instant', maxLines: 1000 }),
    );
    const lines = out.filter((f) => f.refId === 'A');
    expect(lines.reduce((n, f) => n + f.rows, 0)).toBe(EXPECT_LOG_LINES);
    expect(out.every((f) => !f.error)).toBe(true);
    await expect(page.locator('body')).not.toContainText('No data');
    expect(consoleErrors).toEqual([]);
  });

  test('Jaeger search lists cold and post-restart traces', async ({ page }) => {
    const { out, consoleErrors } = await frames(
      page,
      exploreUrl(`${PREFIX}-jaeger`, 'jaeger', { queryType: 'search', service: 'repro', operation: '', limit: 50 }),
    );
    expect(out.reduce((n, f) => n + f.rows, 0)).toBe(EXPECT_TRACES);
    expect(consoleErrors).toEqual([]);
  });

  test('Tempo search lists cold and post-restart traces', async ({ page }) => {
    const { out, consoleErrors } = await frames(
      page,
      exploreUrl(`${PREFIX}-tempo`, 'tempo', { queryType: 'traceql', query: '{}', limit: 50, tableType: 'traces' }),
    );
    expect(out.reduce((n, f) => n + f.rows, 0)).toBe(EXPECT_TRACES);
    expect(consoleErrors).toEqual([]);
  });

  test('a post-restart trace opens in the Jaeger trace view', async ({ page }) => {
    const { out } = await frames(
      page,
      exploreUrl(`${PREFIX}-jaeger`, 'jaeger', { query: 'bb00aaaaaaaaaaaaaaaaaaaaaaaaaaaa' }),
    );
    expect(out.reduce((n, f) => n + f.rows, 0)).toBeGreaterThan(0);
  });
});
