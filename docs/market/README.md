# Market comparison data

`docs/market-comparison.md` and the interactive matrix at `website/static/market/index.html` are generated from the files in this directory. Edit the data, never the generated files.

| Path | What it holds |
|---|---|
| `data/meta.yaml` | sections, dimensions (with counting rules), system groups and their order, the icon legend, the source labels, the review window (`stale_after_days`) |
| `data/systems/<system>.yaml` | one file per system: name, group, `reviewed` date, and one cell per dimension |
| `data/summary.yaml` | Lakehouse strengths, weaknesses, threats and the ranked fix list (with issue numbers) |
| `prose/*.md` | the hand-written sections: executive summary, interfaces, positions, method and sources |
| `snapshots/<date>.json` | frozen copies of every published review, used to show what changed |

A cell looks like this:

```yaml
in_otlp:
  icon: ✅                     # one of the legend icons in meta.yaml
  text: OTLP/HTTP logs+traces   # short; shown in the matrix
  note: longer explanation      # shown when the cell is opened, and as the footnote
  source: https://...           # required unless label is "unverified"
  label: docs                   # docs | vendor | measured | repo | unverified
  checked: 2026-10-02           # the date this cell was last checked against its source
```

## Updating

1. Edit the system files. Set `checked` on every cell you re-verified, and `reviewed` on the system.
2. Regenerate: `python3 scripts/market/build.py`. It validates the data first: unknown icons, labels or dimensions, missing sources and bad dates all fail.
3. When publishing a new review, freeze it: `python3 scripts/market/build.py --snapshot`. The page then offers "Show changes since <date>" for every older snapshot.
4. List the changes between two reviews: `python3 scripts/market/build.py --diff 2026-10-02 2027-01-15`.
5. List cells that are due for a re-check: `python3 scripts/market/build.py --stale`.

Lakehouse's own cells follow the code. A PR that changes a Lakehouse capability counted here (ingest protocols, read APIs, external readers, durability, tenancy, compaction) updates the Lakehouse system file in the same PR.

CI (`.github/workflows/market-data.yaml`) runs the tests and fails a PR whose generated files are out of date. A weekly run lists stale cells.
