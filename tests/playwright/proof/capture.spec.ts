// Visual and data proof capture for Lakehouse. Opens every page of spec.json on three sides (base = Lakehouse built
// from main, pr = Lakehouse built from the pull request, ref = hot VictoriaLogs / VictoriaTraces), saves a full-page
// screenshot and the backend traffic of the page load (Grafana /api/ds/query bodies and responses, datasource
// resource calls, the own API calls of VMUI, VTUI and the Jaeger UI) plus what the page shows (panel state).
//
//   VP_STATE=out/state.json VP_OUT=out/visual GRAFANA_URL=http://127.0.0.1:48300 npx playwright test
//   VP_PAGES=vl-builder-map-attr,jaeger-ui-trace VP_RANGES=cold   (optional filters)
//
// Ported from loki-vl-proxy/bench/visual/capture.spec.ts@429f15b9 (settle logic, backend capture, retry of a
// page that never settled); the page kinds, the sides and the panel state are Lakehouse's own.
// Windows are absolute (state.json), so all sides see the same window and the data is static.
import { test, Page } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

type PageSpec = {
  id: string; kind: string; query?: string; field?: string; family?: string; trace?: boolean;
  path?: string; service?: boolean; ranges?: string[]; clip?: { x: number; y: number; width: number; height: number };
};
const spec = JSON.parse(fs.readFileSync(process.env.VP_SPEC || path.join(__dirname, "spec.json"), "utf8"));
const state = JSON.parse(fs.readFileSync(process.env.VP_STATE || "state.json", "utf8"));
const OUT = process.env.VP_OUT || "out";
const only = (v?: string) => (v ? new Set(v.split(",")) : null);
const pagesOnly = only(process.env.VP_PAGES);
const rangesOnly = only(process.env.VP_RANGES);
const tierOnly = process.env.VP_TIER || "";
const sides: string[] = spec.sides;
const vars: Record<string, string> = spec.vars || {};
const expand = (s: string) => s.replace(/\{(\w+)\}/g, (_, k) => encodeURIComponent(vars[k] ?? ""));
const uid = (family: string, side: string) => (spec.uids[family] as string).replace("{side}", side);
const ms = (iso: string) => String(Date.parse(iso));
const win = (r: string) => ({ from: ms(state[r].start), to: ms(state[r].end) });

const BACKEND = /\/api\/ds\/query|\/api\/datasources\/uid\/[^/]+\/resources\/|\/api\/datasources\/proxy\//;
const OWN_API = /\/(select|api)\//;

function exploreUrl(ds: string, type: string, query: Record<string, unknown>, r: string): string {
  const { from, to } = win(r);
  const pane = { A: { datasource: ds, queries: [{ refId: "A", datasource: { type, uid: ds }, ...query }], range: { from, to } } };
  return `/explore?${new URLSearchParams({ schemaVersion: "1", panes: JSON.stringify(pane), orgId: "1" })}`;
}

function drilldownUrl(p: PageSpec, ds: string, r: string): string {
  const { from, to } = win(r);
  const q = new URLSearchParams({
    patterns: "[]", from, to, timezone: "browser", "var-lineFormat": "", "var-ds": ds,
    "var-filters": p.service ? `service_name|=|${vars.service}` : "", "var-fields": "", "var-levels": "",
    "var-metadata": "", "var-jsonFields": "", "var-all-fields": "", "var-patterns": "",
    "var-lineFilterV2": "", "var-lineFilters": "", "var-primary_label": "service_name|=~|.+",
    ...(p.service ? { displayedFields: "[]", urlColumns: "[]" } : {}),
  });
  return `/a/grafana-lokiexplore-app/explore${p.path ? "/" + expand(p.path) : ""}?${q}`;
}

// Where a page of the three sides lives: a Grafana path (relative to GRAFANA_URL) or an absolute URL of a UI.
function pageUrl(p: PageSpec, side: string, r: string): string {
  const port = (k: string) => state.ports[k];
  switch (p.kind) {
    case "vl-explore": case "vl-builder": case "vl-stream":
      return exploreUrl(uid("vl", side), "victoriametrics-logs-datasource",
        { expr: p.query || "*", queryType: "instant", editorMode: "code" }, r);
    case "loki-explore":
      return exploreUrl(uid("loki", side), "loki", { expr: p.query || "", queryType: "range", editorMode: "code", direction: "backward" }, r);
    case "drilldown":
      return drilldownUrl(p, uid("loki", side), r);
    case "jaeger-explore":
      return exploreUrl(uid("jaeger", side), "jaeger", p.trace ? { query: state.trace_id } : { queryType: "search", service: "api-gateway" }, r);
    case "vmui": {
      const fam = p.family === "traces" ? "traces" : "logs";
      const base = `http://127.0.0.1:${port(`${side}-${fam}`)}/select/vmui/`;
      const w = state[r];
      const q = new URLSearchParams({ "g0.expr": p.query || "*", "g0.range_input": "1h", "g0.end_input": w.end.replace("Z", ""), "g0.relative_time": "none", "g0.tab": "0" });
      return `${base}#/?${q}`;
    }
    case "jaeger-ui": {
      const base = `http://127.0.0.1:${port(`jaeger-${side}`)}`;
      if (p.trace) return `${base}/trace/${state.trace_id}`;
      const w = state[r];
      const q = new URLSearchParams({ service: "api-gateway", start: String(Date.parse(w.start) * 1000), end: String(Date.parse(w.end) * 1000), limit: "20", lookback: "custom" });
      return `${base}/search?${q}`;
    }
  }
  throw new Error(`unknown page kind ${p.kind}`);
}

// What the page shows once it settled, panel by panel (the DOM side of the panel state; scripts/proof/visual/states.py
// reads the data side from the captured responses and decides which one wins).
async function uiState(page: Page) {
  return page.evaluate(() => {
    const text = document.body.innerText || "";
    const banner = /(Plugin (unavailable|failed|not found)|Failed to load|Unable to load|Something went wrong|An unexpected error|Error loading|Query error|Bad Gateway|Internal Server Error|Cannot read propert|Network Error|Request failed)[^\n]{0,80}/g;
    const banners = [...new Set(text.match(banner) || [])].sort().slice(0, 8);
    const visible = (e: Element) => e.getClientRects().length > 0;
    const panelOf = (e: Element) => e.closest('[data-testid*="Panel"], section, [role="region"]') || e.parentElement;
    const titleOf = (e: Element | null) => (e?.querySelector('h1,h2,h3,h4,[role="heading"]')?.textContent || "").trim().slice(0, 60);
    const noDataPanels = [...document.querySelectorAll("*")]
      .filter((e) => e.children.length === 0 && (e.textContent || "").trim() === "No data" && visible(e))
      .map((e) => titleOf(panelOf(e)) || "(untitled)");
    return {
      noData: noDataPanels.length,
      noDataPanels: noDataPanels.slice(0, 8),
      banners,
      panelErrors: [...document.querySelectorAll('[data-testid="data-testid Panel status error"], [data-testid="data-testid Alert error"], [data-testid="data-testid Error boundary"], [data-testid="data-testid Query editor row"] [data-testid="icon-exclamation-triangle"]')]
        .filter(visible).map((e) => (e.textContent || "").trim().slice(0, 120)).slice(0, 8).length,
      jaegerErrors: [...document.querySelectorAll(".ant-alert-error, .ant-message-error, .ErrorMessage")].filter(visible).length,
    };
  }).catch(() => ({ noData: 0, noDataPanels: [], banners: ["page state unreadable"], panelErrors: 0, jaegerErrors: 0 }));
}

const QUIET = parseInt(process.env.VP_QUIET_MS || "3000");

async function settle(page: Page, pending: { n: number; last: number; seen: number }, max = parseInt(process.env.VP_SETTLE_MS || "45000")) {
  const t0 = Date.now();
  await page.waitForTimeout(3000);
  while (Date.now() - t0 < max) {
    if (pending.seen > 0 && pending.n === 0 && Date.now() - pending.last > QUIET) return true;
    await page.waitForTimeout(500);
  }
  return false;
}

// Logs Drilldown crashes a breakdown page ("Plugin failed to load") when a breakdown query is answered before the
// page's first time-series panel module has loaded; a fast answer loses that race on any datasource. The data queries
// of a Drilldown page wait for the module (at most MODULE_WAIT_MS each); only their timing changes, not the requests
// or the answers. Ported from loki-vl-proxy/bench/visual/capture.spec.ts@429f15b9.
const PANEL_MODULE = /\/public\/build\/timeseriesPanel\.[^/]*\.js/;
const MODULE_WAIT_MS = 3000;

async function queriesAfterPanelModule(page: Page) {
  let loaded: () => void = () => {};
  const ready = new Promise<void>((res) => { loaded = res; });
  const done = (rq: any) => { if (PANEL_MODULE.test(rq.url())) loaded(); };
  page.on("requestfinished", done);
  page.on("requestfailed", done);
  await page.route(/\/api\/ds\/query/, async (route) => {
    await Promise.race([ready, new Promise((res) => setTimeout(res, MODULE_WAIT_MS))]);
    await route.fallback();
  });
}

// The interaction that makes a page show the value lists the fix is about.
async function interact(page: Page, p: PageSpec) {
  const pause = (n = 2500) => page.waitForTimeout(n);
  if (p.kind === "vl-builder") {
    // Beta Builder -> "+" -> Exact -> field -> the value list of that field (field_names and field_values resource
    // calls). The controls are addressed by their position in the fixed 1500x1100 layout of a fresh Explore page.
    await page.mouse.click(1375, 173);
    await pause(1500);
    await page.mouse.click(557, 250);
    await pause(1500);
    await page.keyboard.press("Enter");
    await pause();
    await page.keyboard.type(p.field || "");
    await pause();
    await page.keyboard.press("Enter");
    await pause(3500);
  } else if (p.kind === "vl-stream") {
    await page.getByText("Stream filters").first().click({ timeout: 15_000 }).catch(() => {});
    await pause();
    const item = page.getByText(p.field || "", { exact: true }).last();  // the popup is portaled to the end of the body
    const box = await item.boundingBox().catch(() => null);
    if (box) { await page.mouse.move(box.x + 20, box.y + box.height / 2); await pause(500); await page.mouse.click(box.x + 20, box.y + box.height / 2); }
    await pause(3500);
  } else if (p.kind === "jaeger-ui" && p.trace) {
    // Expand the first span with details: its logs (events), references (links) and tags (scope) are what #430 is about.
    await page.locator(".span-row, [class*=SpanBarRow]").first().click({ timeout: 15_000 }).catch(() => {});
    await pause(1500);
    for (const t of ["Logs", "References", "Tags"]) await page.getByText(new RegExp(`^${t}`)).first().click({ timeout: 2000 }).catch(() => {});
    await pause(1500);
  } else if (p.kind === "jaeger-ui") {
    await pause(1500);
  }
}

for (const p of spec.pages as PageSpec[]) {
  if (pagesOnly && !pagesOnly.has(p.id)) continue;
  if (tierOnly && !(spec.tier[tierOnly] || []).includes(p.id)) continue;
  for (const r of p.ranges || spec.ranges) {
    if (rangesOnly && !rangesOnly.has(r)) continue;
    test(`${p.id} ${r}`, async ({ browser }) => {
      for (const side of sides) {
        for (let attempt = 0; attempt < 2; attempt++) {
          const ctx = await browser.newContext();
          const page = await ctx.newPage();
          const pending = { n: 0, last: Date.now(), seen: 0 };
          const records: any[] = [];
          const errors: string[] = [];
          if (p.kind === "drilldown") await queriesAfterPanelModule(page);
          const own = p.kind === "vmui" || p.kind === "jaeger-ui";
          const wanted = (u: string) => (own ? OWN_API.test(new URL(u).pathname) && !/\.(js|css|svg|png|woff2?)$/.test(u) : BACKEND.test(u));
          page.on("console", (m) => { if (m.type() === "error" && errors.length < 20) errors.push(m.text().slice(0, 1500)); });
          page.on("pageerror", (e) => { if (errors.length < 20) errors.push(`pageerror: ${String(e.stack || e).slice(0, 1500)}`); });
          page.on("request", (rq) => { if (wanted(rq.url())) { pending.n++; pending.seen++; pending.last = Date.now(); } });
          page.on("requestfailed", (rq) => { if (wanted(rq.url())) { pending.n--; pending.last = Date.now(); } });
          page.on("response", async (rs) => {
            const rq = rs.request();
            if (!wanted(rq.url())) return;
            let body: any = null;
            try { body = await rs.json(); } catch { try { body = await rs.text(); } catch { body = null; } }
            records.push({ url: rq.url().replace(/^https?:\/\/[^/]+/, ""), method: rq.method(), request: rq.postDataJSON?.() ?? null, status: rs.status(), response: body });
            pending.n--; pending.last = Date.now();
          });
          const t0 = Date.now();
          await page.goto(pageUrl(p, side, r), { waitUntil: "commit" });
          const dbg = (m: string) => { if (process.env.VP_DEBUG) console.log(`${p.id} ${r} ${side} ${m} +${Date.now() - t0}ms pending=${pending.n}`); };
          dbg("goto done");
          let ok = await settle(page, pending);
          dbg("settled");
          await interact(page, p);
          dbg("interacted");
          ok = (await settle(page, pending, 20_000)) || ok;
          const unavailable = records.some((x) => x.status === 500 && /plugin\.(unavailable|connectionUnavailable)/.test(JSON.stringify(x.response)));
          const empty = !ok && !records.some((x) => x.status === 200);
          const pluginLoad = (await uiState(page)).banners.some((b: string) => /^Plugin (failed|unavailable|not found)/.test(b));
          if ((unavailable || empty || pluginLoad) && attempt < 1) { await ctx.close(); await new Promise((res) => setTimeout(res, 5000)); continue; }
          const dir = path.join(OUT, "shots", p.id, r);
          const ddir = path.join(OUT, "data", p.id, r);
          fs.mkdirSync(dir, { recursive: true });
          fs.mkdirSync(ddir, { recursive: true });
          dbg("before screenshot");
          await page.screenshot({ path: path.join(dir, `${side}.png`), fullPage: false });
          dbg("screenshot");
          if (p.clip) await page.screenshot({ path: path.join(dir, `${side}-clip.png`), clip: p.clip });  // the region the page is about, legible in a montage
          const ui = await uiState(page);
          fs.writeFileSync(path.join(ddir, `${side}.json`), JSON.stringify({ settled: ok, settle_ms: pending.last - t0, attempts: attempt + 1, kind: p.kind, records, ui, errors }));
          await ctx.close();
          break;
        }
      }
    });
  }
}
