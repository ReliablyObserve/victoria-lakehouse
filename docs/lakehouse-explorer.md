# Lakehouse Explorer UI

The Lakehouse Explorer is a built-in web UI for monitoring storage, tenants, and field cardinality. It is served at `/lakehouse/ui/` and optionally injected as a tab into VL/VT's VMUI.

## Access

- **Standalone:** `http://lakehouse:9428/lakehouse/ui/`
- **VMUI tab:** When `ui.vmui_tab` is enabled (default), a "Lakehouse" tab appears in VL/VT's VMUI navigation

## Architecture

```mermaid
graph TD
    subgraph Lakehouse Binary
        VMUI[VL/VT VMUI Handler] -->|HTML response| INJ[VMUI Injection Middleware]
        INJ -->|inject script tag| BROWSER[Browser]
        UI[/lakehouse/ui/ Handler] --> BROWSER
        API[/lakehouse/api/v1/* Handlers] --> BROWSER
    end

    BROWSER -->|fetch JSON| API
    BROWSER -->|render| PREACT[Preact + uPlot]
```

The UI is a single HTML file using Preact (3KB), uPlot (35KB), and HTM (1KB) loaded from CDN. No build step required. All data comes from the JSON API endpoints.

### VMUI Tab Injection

The injection middleware wraps VL's existing VMUI handler:

1. Intercepts HTML responses from the upstream VMUI handler
2. Injects `<script src="/lakehouse/ui/vmui-tab.js"></script>` before `</body>`
3. The script uses `MutationObserver` to detect VMUI's nav rendering and adds a "Lakehouse" link
4. Clicking the link opens `/lakehouse/ui/` (either in-frame or new tab based on config)

This approach requires zero modifications to upstream VL/VT code. All VL/VT VMUI flags (`-vmui.*`) are honored.

## Tabs

### Tab 1: Storage Overview

Global storage health at a glance.

**Panels:**
- **Big numbers** — total files, total bytes (compressed + raw), compression ratio, monthly cost estimate, ingestion rate
- **Storage class donut chart** — bytes distribution across STANDARD, STANDARD_IA, GLACIER, DEEP_ARCHIVE with cost labels
- **Ingestion trend** — uPlot time series: rows/hour and bytes/hour over configurable range (24h/7d/30d)
- **Compression trend** — avg/p50/p99 compression ratios over time
- **File size histogram** — distribution of Parquet file sizes
- **Cost projection** — 30/90-day cost forecast based on current ingestion rate

### Tab 2: Tenants

Per-tenant storage breakdown with drill-down.

**Panels:**
- **Tenant table** — sortable by bytes, files, cost, rows, last activity. Columns: tenant ID, files, compressed/raw bytes, compression ratio, cost, last write, last query
- **Storage pie chart** — bytes distribution across top 10 tenants + "other"
- **Tenant drill-down** (click a row):
  - Partition heatmap — files per hour over date range
  - File size histogram — distribution for this tenant
  - Storage class breakdown — bar chart by class with cost
  - Top labels — field name, cardinality, bloom status

### Tab 3: Cardinality Explorer

Field-level storage intelligence.

**Panels:**
- **Field table** — sortable by cardinality, name. Columns: field name, unique values, type (string/int/float), bloom filter status, origin (promoted/MAP), high-cardinality warning flag
- **High-cardinality warnings** — highlighted fields exceeding threshold (default 10,000 unique values)
- **Top 20 fields bar chart** — horizontal bar chart of highest-cardinality fields
- **Per-tenant toggle** — optional filter to show cardinality for a specific tenant

## Auto-Refresh

Configurable via UI dropdown: Off, 10s, 30s, 60s. Default is controlled by `ui.refresh_default` (0 = off). The UI polls the JSON API at the selected interval.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `ui.enabled` | `true` | Serve `/lakehouse/ui/` |
| `ui.vmui_tab` | `true` | Inject tab into VMUI |
| `ui.refresh_default` | `0` | Auto-refresh seconds (0=off) |
| `ui.theme` | `auto` | Theme: `auto`, `dark`, `light` |

## Tech Stack

| Library | Size | Purpose |
|---------|------|---------|
| [Preact](https://preactjs.com/) | 3KB | Component rendering |
| [HTM](https://github.com/developit/htm) | 1KB | JSX-like tagged templates |
| [uPlot](https://github.com/leeoniya/uPlot) | 35KB | Time-series charts |

Total JS payload: ~39KB gzipped. No build step, no npm, no bundler.

## API Dependencies

The UI consumes these JSON endpoints (see [Tenant Stats](tenant-stats.md) for full reference):

| Endpoint | Used By |
|----------|---------|
| `GET /lakehouse/api/v1/stats/overview` | Storage Overview tab |
| `GET /lakehouse/api/v1/stats/ingestion` | Ingestion trend chart |
| `GET /lakehouse/api/v1/stats/cost` | Cost panel, donut chart |
| `GET /lakehouse/api/v1/stats/compression` | Compression trend chart |
| `GET /lakehouse/api/v1/tenants` | Tenants tab (table + pie) |
| `GET /lakehouse/api/v1/tenants/{a}/{p}` | Tenant drill-down |
| `GET /lakehouse/api/v1/cardinality/fields` | Cardinality Explorer tab |
