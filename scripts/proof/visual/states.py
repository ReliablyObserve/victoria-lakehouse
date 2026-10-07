"""Panel state of a captured page, on every side, and the transitions judged against the reference.

The state is decided from two independent signals:
- the data side, which does not depend on the DOM: `results[refId].error`, frame notices of severity error,
  a non-200 status of a request, frames with no rows;
- the DOM side, which decides only when the data side is silent: a visible error banner or alert, a panel
  with "No data", the Jaeger UI error messages.
When the DOM shows no error but the data does, a warning is recorded and the data side wins.
Counting only "No data" panels would read a base that showed a query error as "a new empty panel" when the
fix made the same query answer nothing; the states below make that case `fixed`.
"""
from __future__ import annotations

from typing import Any

DATA, EMPTY, ERROR, UNSETTLED = "data", "empty", "error", "unsettled"
# Failed requests of known, tracked differences that must not decide the state of a page: the page renders without them.
KNOWN_ISSUES = {"/select/buildinfo": "#463"}
BACKEND_PREFIXES = ("/api/ds/query", "/api/datasources/")


def _frame_rows(frame: dict) -> int:
    values = (frame.get("data") or {}).get("values") or []
    return len(values[0]) if values else 0


def _result_errors(resp: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(resp, dict):
        return out
    for ref, res in (resp.get("results") or {}).items():
        if res.get("error"):
            out.append(f"{ref}: {str(res['error'])[:120]}")
        for fr in res.get("frames") or []:
            for n in ((fr.get("schema") or {}).get("meta") or {}).get("notices") or []:
                if n.get("severity") == "error":
                    out.append(f"{ref}: notice {str(n.get('text'))[:100]}")
    return out


def _body_rows(resp: Any) -> int | None:
    """Rows or items an answer carries; None when the shape has no rows to count."""
    if isinstance(resp, dict):
        if "results" in resp:
            return sum(_frame_rows(fr) for res in resp["results"].values() for fr in (res.get("frames") or []))
        for k in ("values", "data", "tagValues", "tags", "fields", "hits"):
            if isinstance(resp.get(k), list):
                return len(resp[k])
    if isinstance(resp, list):
        return len(resp)
    if isinstance(resp, str):  # an NDJSON answer (the VMUI/VTUI query call): one row per JSON line
        return sum(1 for line in resp.splitlines() if line.startswith("{"))
    return None


def data_state(records: list[dict]) -> dict:
    """{'state': data|empty|error|none, 'kind': ..., 'errors': [...]} from the page's backend responses."""
    errors: list[str] = []
    kind = ""
    rows = 0
    seen = 0
    known = []
    for r in records:
        path = r["url"].split("?")[0]
        issue = next((i for p, i in KNOWN_ISSUES.items() if path.endswith(p)), None)
        if issue and r["status"] != 200:
            known.append(f"{path}: HTTP {r['status']} ({issue})")
            continue
        seen += 1
        if r["status"] != 200:
            errors.append(f"{r['url'].split('?')[0][-60:]}: HTTP {r['status']}")
            kind = kind or "request"
            continue
        errs = _result_errors(r.get("response"))
        if errs:
            errors += errs
            kind = kind or ("query-row" if any("notice" not in e for e in errs) else "panel")
            continue
        n = _body_rows(r.get("response"))
        rows += n or 0
    if errors:
        return {"state": ERROR, "kind": kind, "errors": errors[:6], "rows": rows, "known": known}
    if not seen:
        return {"state": "none", "kind": "", "errors": [], "rows": 0, "known": known}
    return {"state": DATA if rows else EMPTY, "kind": "", "errors": [], "rows": rows, "known": known}


def dom_state(ui: dict | None) -> dict:
    ui = ui or {}
    if ui.get("banners"):
        return {"state": ERROR, "kind": "banner", "detail": ui["banners"][:3]}
    if ui.get("panelErrors") or ui.get("jaegerErrors"):
        return {"state": ERROR, "kind": "panel", "detail": []}
    if ui.get("noData"):
        return {"state": EMPTY, "kind": "", "detail": ui.get("noDataPanels", [])}
    return {"state": DATA, "kind": "", "detail": []}


def panel_state(capture: dict) -> dict:
    """One state for one side of one page; `warning` when the two signals disagree about an error."""
    if not capture.get("settled", True):
        return {"state": UNSETTLED, "kind": "", "warning": "", "errors": []}
    d, m = data_state(capture.get("records") or []), dom_state(capture.get("ui"))
    warning = ""
    if d["state"] == ERROR and m["state"] != ERROR:
        warning = "the data side shows an error the DOM does not"
        return {"state": ERROR, "kind": d["kind"], "warning": warning, "errors": d["errors"], "known": d["known"]}
    if d["state"] == "none":
        # no backend request was captured: nothing proves the page showed data, so it is never `data`
        if m["state"] == ERROR:
            return {"state": ERROR, "kind": m["kind"], "warning": "", "errors": m.get("detail", []), "known": d["known"]}
        return {"state": EMPTY, "kind": "no-requests", "warning": "no backend request was captured", "errors": [], "known": d["known"]}
    if m["state"] == ERROR:
        return {"state": ERROR, "kind": m["kind"], "warning": "", "errors": m.get("detail", []), "known": d["known"]}
    if d["state"] == DATA and m["state"] == EMPTY:
        warning = "the DOM shows 'No data' where responses carry rows"
    return {"state": d["state"], "kind": "", "warning": warning, "errors": [], "known": d["known"],
            "no_data_panels": m.get("detail", []) if m["state"] == EMPTY else []}


def transition(base: str, pr: str, ref: str | None) -> str | None:
    """The verdict a state change decides on its own, or None when the data metrics decide (the table of state changes below).

    `ref` is None when no reference was captured."""
    if UNSETTLED in (base, pr):
        return "unsettled" if pr == UNSETTLED else None
    if base == ERROR and pr == ERROR:
        return "same-as-reference" if ref == ERROR else "still-differs"
    if pr == ERROR and ref != ERROR:
        return "regression"
    if base == ERROR:
        return "fixed"
    if base == EMPTY and pr == DATA and ref == DATA:
        return "fixed"
    if base == DATA and pr == EMPTY and ref == DATA:
        return "regression"
    return None
