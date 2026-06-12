// vmui-tab.js — Injects a "Lakehouse" tab into the VMUI navigation bar.
// When clicked, replaces the VMUI content area with an inline Lakehouse dashboard
// that uses VMUI CSS variables for consistent styling.
(function () {
  "use strict";

  var TAB_ID = "lakehouse-tab";
  var TAB_TEXT = "Lakehouse";
  var ACTIVE_KEY = "lh_vmui_active"; // localStorage flag: Lakehouse tab was last active
  var SUBTAB_KEY = "lh_vmui_subtab"; // localStorage: which Lakehouse sub-view was active
  var CONTAINER_ID = "lakehouse-root";

  // ---- Helpers ----
  function fmtBytes(b) {
    if (b == null || isNaN(b) || b === 0) return "0 B";
    if (b < 1024) return b + " B";
    if (b < 1048576) return (b / 1024).toFixed(1) + " KB";
    if (b < 1073741824) return (b / 1048576).toFixed(1) + " MB";
    return (b / 1073741824).toFixed(2) + " GB";
  }
  function fmtNum(n) { if (n == null || isNaN(n)) return "\u2014"; return Number(n).toLocaleString(); }
  function fmtUSD(n) { if (n == null || isNaN(n)) return "\u2014"; return "$" + Number(n).toFixed(4); }
  function fmtTime(s) { if (!s) return "\u2014"; var d = new Date(s); return isNaN(d) ? s : d.toLocaleString(undefined, {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}); }
  function fmtRatio(r) { if (!r || r <= 0) return "\u2014"; return r.toFixed(1) + "x"; }

  function fetchJSON(url) {
    var ctrl = new AbortController();
    var timer = setTimeout(function () { ctrl.abort(); }, 10000);
    return fetch(url, { signal: ctrl.signal }).then(function (r) {
      clearTimeout(timer);
      if (!r.ok) throw new Error(r.status + " " + r.statusText);
      return r.json();
    }).catch(function (e) {
      clearTimeout(timer);
      if (e.name === "AbortError") throw new Error("Request timed out");
      throw e;
    });
  }

  function el(tag, attrs, children) {
    var e = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      if (k === "className") e.className = attrs[k];
      else if (k === "textContent") e.textContent = attrs[k];
      else if (k === "innerHTML") e.innerHTML = attrs[k];
      else if (k.indexOf("on") === 0) e.addEventListener(k.slice(2).toLowerCase(), attrs[k]);
      else e.setAttribute(k, attrs[k]);
    });
    if (children) {
      if (!Array.isArray(children)) children = [children];
      children.forEach(function (c) {
        if (typeof c === "string") e.appendChild(document.createTextNode(c));
        else if (c) e.appendChild(c);
      });
    }
    return e;
  }

  // ---- CSS (uses VMUI CSS vars) ----
  var STYLE = [
    "#" + CONTAINER_ID + " {",
    "  font-family: inherit;",
    "  background: var(--color-background-body, #fefeff);",
    "  color: var(--color-text, #110f0f);",
    "  padding: 0;",
    "  min-height: calc(100vh - 55px);",
    "}",
    ".lh-tabs {",
    "  display: flex; gap: 0; padding: 0 20px;",
    "  border-bottom: 2px solid var(--color-text-disabled, #ccc);",
    "  background: var(--color-background-block, #fff);",
    "}",
    ".lh-tab {",
    "  padding: 10px 20px; cursor: pointer; border: none;",
    "  background: transparent; font-size: 14px; font-weight: 500;",
    "  color: var(--color-text-secondary, #706f6f);",
    "  border-bottom: 2px solid transparent; margin-bottom: -2px;",
    "  transition: color .15s, border-color .15s;",
    "}",
    ".lh-tab:hover { color: var(--color-text, #110f0f); }",
    ".lh-tab.active {",
    "  color: var(--color-primary, #3f51b5);",
    "  border-bottom-color: var(--color-primary, #3f51b5);",
    "}",
    ".lh-content { padding: 20px; }",
    ".lh-cards {",
    "  display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));",
    "  gap: 12px; margin-bottom: 20px;",
    "}",
    ".lh-card {",
    "  background: var(--color-background-block, #fff);",
    "  border: 1px solid var(--color-text-disabled, #e0e0e0);",
    "  border-radius: 6px; padding: 16px; text-align: center;",
    "}",
    ".lh-card-label {",
    "  font-size: 11px; text-transform: uppercase; letter-spacing: .04em;",
    "  color: var(--color-text-secondary, #706f6f); margin-bottom: 4px;",
    "}",
    ".lh-card-value { font-size: 1.6rem; font-weight: 700; }",
    ".lh-table {",
    "  width: 100%; border-collapse: collapse; font-size: 13px;",
    "}",
    ".lh-table th, .lh-table td {",
    "  text-align: left; padding: 8px 12px;",
    "  border-bottom: 1px solid var(--color-text-disabled, #e0e0e0);",
    "}",
    ".lh-table th {",
    "  background: var(--color-background-block, #fff);",
    "  font-weight: 600; white-space: nowrap; cursor: pointer; user-select: none;",
    "}",
    ".lh-table th:hover { opacity: .8; }",
    ".lh-table tr:hover td { background: var(--color-hover-black, #0000000f); }",
    ".lh-badge {",
    "  display: inline-block; padding: 1px 7px; border-radius: 10px;",
    "  font-size: 11px; font-weight: 600;",
    "}",
    ".lh-badge-high { background: #fd080e22; color: var(--color-error, #fd080e); }",
    ".lh-badge-ok { background: #4caf5022; color: var(--color-success, #4caf50); }",
    ".lh-badge-promoted { background: var(--color-primary, #3f51b5)22; color: var(--color-primary, #3f51b5); }",
    ".lh-badge-map { background: var(--color-text-secondary, #706f6f)22; color: var(--color-text-secondary, #706f6f); }",
    ".lh-loading { text-align: center; padding: 40px; color: var(--color-text-secondary); }",
    ".lh-error { text-align: center; padding: 40px; color: var(--color-error, #fd080e); }",
    ".lh-section-title {",
    "  font-size: 14px; font-weight: 600; margin: 20px 0 10px;",
    "  color: var(--color-text, #110f0f);",
    "}",
    ".lh-bar-chart { display: flex; align-items: flex-end; gap: 3px; height: 120px; margin: 12px 0 4px; }",
    ".lh-bar {",
    "  flex: 1; min-width: 8px; border-radius: 3px 3px 0 0;",
    "  background: var(--color-primary, #3f51b5); cursor: pointer; position: relative;",
    "}",
    ".lh-bar:hover { opacity: .8; }",
    ".lh-bar-labels { display: flex; gap: 3px; font-size: 10px; color: var(--color-text-secondary); }",
    ".lh-bar-labels span { flex: 1; text-align: center; overflow: hidden; text-overflow: ellipsis; }",
    ".lh-empty { padding: 24px; text-align: center; color: var(--color-text-secondary); font-style: italic; }",
    ".lh-info-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 12px; font-size: 13px; }",
    ".lh-info-item { color: var(--color-text-secondary); }",
    ".lh-info-item strong { color: var(--color-text); }",
  ].join("\n");

  // ---- State ----
  var activeTab = "overview";
  // Restore the last-active sub-view so a reload returns to it (e.g. Cardinality
  // Explorer), not always Storage Overview.
  try {
    var _sv = localStorage.getItem(SUBTAB_KEY);
    if (["overview", "details", "tenants", "cardinality"].indexOf(_sv) >= 0) activeTab = _sv;
  } catch (x) { /* ignore */ }

  // ---- Render functions ----

  function renderOverview(container) {
    container.innerHTML = '<div class="lh-loading">Loading overview\u2026</div>';
    Promise.all([
      fetchJSON("/lakehouse/api/v1/stats/overview"),
      fetchJSON("/lakehouse/api/v1/stats/ingestion?period=day&range=7d"),
    ]).then(function (results) {
      var ov = results[0], ing = results[1];
      container.innerHTML = "";

      var cards = el("div", { className: "lh-cards" });
      var cardData = [
        ["Files", fmtNum(ov.total_files)],
        ["Compressed", fmtBytes(ov.total_bytes)],
        ["Raw Bytes", fmtBytes(ov.total_raw_bytes)],
        ["Total Rows", fmtNum(ov.total_rows)],
        ["Avg Row Size", fmtBytes(ov.avg_row_bytes)],
        ["Avg File Size", fmtBytes(ov.total_files > 0 ? Math.round(ov.total_bytes / ov.total_files) : 0)],
        ["Avg Rows/File", fmtNum(ov.total_files > 0 ? Math.round(ov.total_rows / ov.total_files) : 0)],
        ["Saved (raw\u2212comp)", fmtBytes(Math.max((ov.total_raw_bytes || 0) - (ov.total_bytes || 0), 0))],
        ["Partitions", fmtNum(ov.partition_count)],
        ["Tenants", fmtNum(ov.tenant_count || 0)],
        ["Storage Classes", fmtNum((ov.storage_by_class || []).length)],
        ["Fleet Nodes", fmtNum(ov.fleet_nodes || 0)],
        ["Bucket", ov.bucket || "\u2014"],
      ];
      if (ov.avg_compression_ratio > 0) cardData.push(["Compression", fmtRatio(ov.avg_compression_ratio)]);
      cardData.push(["Mode", ov.mode || "\u2014"]);
      cardData.forEach(function (d) {
        cards.appendChild(el("div", { className: "lh-card" }, [
          el("div", { className: "lh-card-label", textContent: d[0] }),
          el("div", { className: "lh-card-value", textContent: d[1] }),
        ]));
      });
      container.appendChild(cards);

      // Metadata footprint \u2014 pmeta RAM + disk cache are THIS node; S3 is cluster-wide.
      if (ov.meta_resident_bytes || ov.meta_disk_bytes || ov.meta_s3_bytes) {
        container.appendChild(el("div", { className: "lh-section-title", textContent: "Metadata footprint" }));
        var metaCards = el("div", { className: "lh-cards" });
        [
          ["Metadata RAM (node)", fmtBytes(ov.meta_resident_bytes || 0)],
          ["Disk cache (node)", fmtBytes(ov.meta_disk_bytes || 0)],
          ["Metadata on S3 (cluster)", fmtBytes(ov.meta_s3_bytes || 0)],
        ].forEach(function (d) {
          metaCards.appendChild(el("div", { className: "lh-card" }, [
            el("div", { className: "lh-card-label", textContent: d[0] }),
            el("div", { className: "lh-card-value", textContent: d[1] }),
          ]));
        });
        container.appendChild(metaCards);
      }

      // Info row \u2014 data version + compression (Bucket / Fleet are now tiles above).
      var info = el("div", { className: "lh-info-row" });
      info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Data range: <strong>" + (ov.oldest_data ? ov.oldest_data.slice(0, 10) : "—") + " → " + (ov.newest_data ? ov.newest_data.slice(0, 10) : "—") + "</strong>" }));
      info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Retention: <strong>" + (ov.retention_enabled ? ((ov.retention_default || "—") + (ov.retention_rules ? " · " + ov.retention_rules + " rule" + (ov.retention_rules === 1 ? "" : "s") : "")) : "off") + "</strong>" }));
      info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Registry gen: <strong>" + fmtNum(ov.registry_generation || 0) + "</strong>" }));
      if (ov.avg_compression_ratio > 0) info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Compression: <strong>" + ov.avg_compression_ratio.toFixed(1) + "x</strong>" }));
      container.appendChild(info);

      // Ingestion chart
      if (ing.buckets && ing.buckets.length > 0) {
        container.appendChild(el("div", { className: "lh-section-title", textContent: "Ingestion (last 7 days)" }));
        var maxBytes = Math.max.apply(null, ing.buckets.map(function (b) { return b.bytes; }));
        var chart = el("div", { className: "lh-bar-chart" });
        var labels = el("div", { className: "lh-bar-labels" });
        ing.buckets.forEach(function (b) {
          var pct = maxBytes > 0 ? Math.max((b.bytes / maxBytes) * 100, 2) : 2;
          var bar = el("div", { className: "lh-bar", style: "height:" + pct + "%", title: b.timestamp + ": " + fmtBytes(b.bytes) + " (" + b.files + " files)" });
          chart.appendChild(bar);
          labels.appendChild(el("span", { textContent: b.timestamp.slice(5) }));
        });
        container.appendChild(chart);
        container.appendChild(labels);
        container.appendChild(el("div", { className: "lh-info-row", innerHTML: '<span class="lh-info-item">Total ingested: <strong>' + fmtBytes(ing.total_bytes_ingested) + '</strong></span><span class="lh-info-item">Total files: <strong>' + fmtNum(ing.total_files_written) + '</strong></span>' }));
      }

      // Storage class breakdown
      if (ov.storage_by_class && ov.storage_by_class.length > 0) {
        container.appendChild(el("div", { className: "lh-section-title", textContent: "Storage Classes" }));
        var tbl = el("table", { className: "lh-table" });
        tbl.innerHTML = "<thead><tr><th>Class</th><th>Files</th><th>Size</th></tr></thead>";
        var tbody = el("tbody");
        ov.storage_by_class.forEach(function (c) {
          var row = el("tr");
          row.innerHTML = "<td>" + c.class + "</td><td>" + fmtNum(c.files) + "</td><td>" + fmtBytes(c.bytes) + "</td>";
          tbody.appendChild(row);
        });
        tbl.appendChild(tbody);
        container.appendChild(tbl);
      }
    }).catch(function (e) {
      container.innerHTML = '<div class="lh-error">Error: ' + e.message + "</div>";
    });
  }

  function renderBreakdown(container) {
    var LSKEY = "lh_breakdown_labels";
    var selected = null;   // persisted array of label names; null until seeded
    var byName = {};       // name -> "loading" | null (no values) | label object
    var allFields = [];    // [{name, indexed, cardinality}]
    var tenantLabel = null;

    function persist() { try { localStorage.setItem(LSKEY, JSON.stringify(selected)); } catch (e) {} }
    function fetchLabel(name) {
      return fetchJSON("/lakehouse/api/v1/stats/breakdown?group_by=" + encodeURIComponent(name))
        .then(function (j) { return (j && j.labels && j.labels[0]) || null; })
        .catch(function () { return null; });
    }
    function addLabel(name) {
      name = (name || "").trim();
      if (!name || (selected && selected.indexOf(name) !== -1)) return;
      selected.push(name); persist();
      byName[name] = "loading"; render();
      fetchLabel(name).then(function (lb) { byName[name] = lb; render(); });
    }
    function removeLabel(name) {
      var i = selected.indexOf(name);
      if (i !== -1) { selected.splice(i, 1); persist(); render(); }
    }
    // How much data backs a key \u2014 so you can tell which are usable to break down by.
    function usability(f) {
      if (!f.indexed) return "not indexed \u2014 no breakdown";
      if (!f.cardinality) return "indexed \u00b7 no values yet";
      return fmtNum(f.cardinality) + " values";
    }
    function usabilityRank(f) { if (!f.indexed) return 3; if (!f.cardinality) return 2; return 1; }

    function renderRows(values) {
      var maxBytes = 0;
      values.forEach(function (v) { if (v.estimated_bytes > maxBytes) maxBytes = v.estimated_bytes; });
      var chartDiv = el("div", { style: "display:flex;flex-direction:column;gap:4px" });
      values.forEach(function (v) {
        var pct = maxBytes > 0 ? (v.estimated_bytes / maxBytes * 100) : 0;
        var row = el("div", { style: "display:flex;align-items:center;gap:8px;font-size:13px" });
        row.appendChild(el("div", { textContent: v.value + (v.org_id ? " (" + v.org_id + ")" : ""), style: "width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex-shrink:0" }));
        var barOuter = el("div", { style: "flex:1;height:18px;background:var(--color-hover-black,#0000000f);border-radius:3px;overflow:hidden" });
        barOuter.appendChild(el("div", { style: "height:100%;width:" + Math.max(pct, 1) + "%;background:var(--color-primary,#3f51b5);border-radius:3px;transition:width 0.3s" }));
        row.appendChild(barOuter);
        row.appendChild(el("div", { textContent: fmtBytes(v.estimated_bytes), style: "width:70px;text-align:right;flex-shrink:0;font-size:12px;color:var(--color-text-secondary)" }));
        row.appendChild(el("div", { textContent: (v.share_pct || 0).toFixed(1) + "%", style: "width:50px;text-align:right;flex-shrink:0;font-size:12px;color:var(--color-text-secondary)" }));
        chartDiv.appendChild(row);
      });
      return chartDiv;
    }

    function renderBlock(name) {
      var label = byName[name];
      var section = el("div", { style: "margin-bottom:24px" });
      var header = el("div", { style: "display:flex;align-items:baseline;gap:8px;margin-bottom:8px" });
      header.appendChild(el("h3", { textContent: name, style: "font-size:1rem;margin:0" }));
      if (label && label !== "loading") {
        header.appendChild(el("span", { innerHTML: label.type === "promoted" ? '<span class="lh-badge lh-badge-promoted">promoted</span>' : '<span class="lh-badge lh-badge-map">map</span>' }));
        header.appendChild(el("span", { textContent: label.cardinality + " values", style: "font-size:12px;color:var(--color-text-secondary)" }));
      }
      header.appendChild(el("button", { textContent: "\u00d7", title: "Remove", style: "margin-left:auto;border:none;background:none;cursor:pointer;color:var(--color-text-secondary);font-size:16px;line-height:1", onclick: function () { removeLabel(name); } }));
      section.appendChild(header);
      if (label === "loading" || label === undefined) {
        section.appendChild(el("div", { className: "lh-loading", textContent: "Loading\u2026" }));
      } else if (!label || !label.values || label.values.length === 0) {
        section.appendChild(el("div", { className: "lh-empty", textContent: "No values to break down \u2014 this field is not indexed for breakdown (or has no data yet)." }));
      } else {
        section.appendChild(renderRows(label.values));
      }
      return section;
    }

    function render() {
      if (selected === null) return;
      container.innerHTML = "";
      container.appendChild(el("div", { className: "lh-section-title", textContent: "Storage Breakdown by Label" }));

      var avail = allFields.filter(function (f) { return selected.indexOf(f.name) === -1; });
      avail.sort(function (a, b) { var ra = usabilityRank(a), rb = usabilityRank(b); if (ra !== rb) return ra - rb; return (a.cardinality || 0) - (b.cardinality || 0); });

      var pickWrap = el("div", { style: "display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap" });
      var input = el("input", { type: "text", list: "lh-vmui-bd-options", placeholder: "Search a label to break down by\u2026", style: "flex:1;max-width:340px;padding:6px 10px;border:1px solid var(--color-border,#ccc);border-radius:6px;font-size:13px", onkeydown: function (e) { if (e.key === "Enter") { addLabel(input.value); } } });
      var dl = el("datalist", { id: "lh-vmui-bd-options" });
      avail.forEach(function (f) { dl.appendChild(el("option", { value: f.name, label: usability(f) })); });
      pickWrap.appendChild(input);
      pickWrap.appendChild(dl);
      pickWrap.appendChild(el("button", { textContent: "+ Add", style: "padding:6px 14px;cursor:pointer;border:1px solid var(--color-primary,#3f51b5);border-radius:6px;background:var(--color-primary,#3f51b5);color:#fff;font-size:13px", onclick: function () { addLabel(input.value); } }));
      pickWrap.appendChild(el("span", { textContent: avail.length + " labels available", style: "font-size:12px;color:var(--color-text-secondary)" }));
      container.appendChild(pickWrap);

      container.appendChild(el("div", { style: "font-size:12px;color:var(--color-text-secondary,#706f6f);margin-bottom:16px", textContent: "Pick from the dropdown \u2014 each shows its value count so you know which are usable. Defaults via stats.breakdown_labels; add or remove any label \u2014 your selection is remembered." }));

      if (tenantLabel && tenantLabel.values && tenantLabel.values.length > 0) {
        var ts = el("div", { style: "margin-bottom:24px" });
        var th = el("div", { style: "display:flex;align-items:baseline;gap:8px;margin-bottom:8px" });
        th.appendChild(el("h3", { textContent: "by Tenant", style: "font-size:1rem;margin:0" }));
        th.appendChild(el("span", { innerHTML: '<span class="lh-badge lh-badge-promoted">registry</span>' }));
        th.appendChild(el("span", { textContent: tenantLabel.cardinality + " tenants \u00b7 exact bytes", style: "font-size:12px;color:var(--color-text-secondary)" }));
        ts.appendChild(th);
        ts.appendChild(renderRows(tenantLabel.values));
        container.appendChild(ts);
      }

      if (selected.length === 0) {
        container.appendChild(el("div", { className: "lh-empty", textContent: "No breakdown labels \u2014 use the search above to add one." }));
      } else {
        selected.forEach(function (name) { container.appendChild(renderBlock(name)); });
      }
    }

    container.innerHTML = '<div class="lh-loading">Loading storage breakdown\u2026</div>';
    try { var sv = JSON.parse(localStorage.getItem(LSKEY)); if (Array.isArray(sv)) selected = sv; } catch (e) {}

    fetchJSON("/lakehouse/api/v1/stats/breakdown?group_by=tenant").then(function (j) { tenantLabel = (j && j.labels && j.labels[0]) || null; render(); }).catch(function () {});
    fetchJSON("/lakehouse/api/v1/cardinality/fields").then(function (j) { if (j && j.fields) allFields = j.fields.map(function (f) { return { name: f.name, indexed: f.indexed, cardinality: f.cardinality }; }); render(); }).catch(function () {});

    if (selected === null) {
      fetchJSON("/lakehouse/api/v1/stats/breakdown").then(function (data) {
        var lbls = (data && data.labels) || [];
        selected = lbls.map(function (l) { return l.name; });
        lbls.forEach(function (l) { byName[l.name] = l; });
        persist();
        render();
      }).catch(function (e) { container.innerHTML = '<div class="lh-error">Error: ' + e.message + "</div>"; });
    } else {
      selected.forEach(function (name) {
        if (byName[name] === undefined) {
          byName[name] = "loading";
          fetchLabel(name).then(function (lb) { byName[name] = lb; render(); });
        }
      });
      render();
    }
  }

  function renderTenants(container) {
    container.innerHTML = '<div class="lh-loading">Loading tenants\u2026</div>';
    fetchJSON("/lakehouse/api/v1/tenants").then(function (data) {
      container.innerHTML = "";
      var tenants = data.tenants || [];

      // Summary cards
      var cards = el("div", { className: "lh-cards" });
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Tenants" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(data.total_tenants) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Compressed" }),
        el("div", { className: "lh-card-value", textContent: fmtBytes(data.total_bytes) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Total Files" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(data.total_files) }),
      ]));
      container.appendChild(cards);

      if (tenants.length === 0) {
        container.appendChild(el("div", { className: "lh-empty", textContent: "No tenant data available yet." }));
        return;
      }

      var wrapper = el("div", { style: "overflow-x:auto" });
      var tbl = el("table", { className: "lh-table" });
      tbl.innerHTML = "<thead><tr><th>Victoria ID</th><th>Org / Name</th><th>Source</th><th>S3 Prefix</th><th>Files</th><th>Partitions</th><th>Compressed</th><th>Raw Bytes</th><th>Metadata</th><th>Rows</th><th>Compression</th><th>Est. Cost</th><th>Last Write</th><th>Last Query</th><th>Time Range</th></tr></thead>";
      var tbody = el("tbody");
      tenants.forEach(function (t) {
        var row = el("tr", { style: "cursor:pointer" });
        var tenantID = t.account_id + ":" + t.project_id;
        var isDefault = (t.account_id === "0" && t.project_id === "0");
        var hasAlias = !!t.org_id;
        var aliasOnly = (t.source === "alias");
        var orgName = t.org_id || t.name || (isDefault ? "(default)" : "\u2014");
        var s3Prefix = t.account_id + "/" + t.project_id + "/";
        var srcBadge = "registry";
        var srcStyle = "color:#3a3";
        if (aliasOnly) { srcBadge = "alias-only"; srcStyle = "color:#a83;font-style:italic"; }
        else if (t.source === "manifest") { srcBadge = "manifest"; srcStyle = "color:#39c"; }
        var rowStyle = aliasOnly ? "cursor:pointer;opacity:0.7" : "cursor:pointer";
        row.setAttribute("style", rowStyle);
        var timeRange = (t.min_time ? t.min_time.slice(0, 10) : "\u2014") + " \u2192 " + (t.max_time ? t.max_time.slice(0, 10) : "\u2014");
        row.innerHTML =
          "<td><strong>" + tenantID + "</strong>" + (isDefault ? " <span style='color:#888;font-size:0.85em'>(default)</span>" : "") + "</td>" +
          "<td>" + orgName + (hasAlias ? " <span style='color:#888;font-size:0.85em'>alias</span>" : "") + "</td>" +
          "<td><span style='" + srcStyle + ";font-size:0.85em'>" + srcBadge + "</span></td>" +
          "<td><code style='font-size:0.85em'>" + s3Prefix + "</code></td>" +
          "<td>" + fmtNum(t.total_files) + "</td>" +
          "<td>" + fmtNum(t.partitions || 0) + "</td>" +
          "<td>" + fmtBytes(t.total_bytes) + "</td>" +
          "<td>" + fmtBytes(t.raw_bytes) + "</td>" +
          "<td>" + (t.metadata_bytes ? fmtBytes(t.metadata_bytes) : "—") + "</td>" +
          "<td>" + fmtNum(t.total_rows) + "</td>" +
          "<td>" + fmtRatio(t.compression_ratio) + "</td>" +
          "<td>" + fmtUSD(t.monthly_cost_usd) + "</td>" +
          "<td>" + fmtTime(t.last_write_at) + "</td>" +
          "<td>" + fmtTime(t.last_query_at) + "</td>" +
          "<td>" + timeRange + "</td>";
        row.addEventListener("click", function () { renderTenantDetail(container, t.account_id, t.project_id); });
        tbody.appendChild(row);
      });
      tbl.appendChild(tbody);
      wrapper.appendChild(tbl);
      container.appendChild(wrapper);
    }).catch(function (e) {
      container.innerHTML = '<div class="lh-error">Error: ' + e.message + "</div>";
    });
  }

  function renderTenantDetail(container, accountID, projectID) {
    container.innerHTML = '<div class="lh-loading">Loading tenant detail\u2026</div>';
    fetchJSON("/lakehouse/api/v1/tenants/" + accountID + "/" + projectID).then(function (d) {
      container.innerHTML = "";

      // Back button
      container.appendChild(el("button", {
        className: "lh-tab",
        textContent: "\u2190 Back to Tenants",
        onClick: function () { renderTenants(container); },
      }));

      var title = "Tenant: " + accountID + ":" + projectID;
      if (d.org_id || d.name) title += " (" + (d.org_id || d.name) + ")";
      container.appendChild(el("h3", { textContent: title, style: "margin:12px 0 8px" }));

      // Summary cards
      var cards = el("div", { className: "lh-cards" });
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Files" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(d.total_files) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Compressed" }),
        el("div", { className: "lh-card-value", textContent: fmtBytes(d.total_bytes) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Raw Bytes" }),
        el("div", { className: "lh-card-value", textContent: fmtBytes(d.raw_bytes) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Rows" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(d.total_rows) }),
      ]));
      if (d.compression_ratio > 0) {
        cards.appendChild(el("div", { className: "lh-card" }, [
          el("div", { className: "lh-card-label", textContent: "Compression" }),
          el("div", { className: "lh-card-value", textContent: fmtRatio(d.compression_ratio) }),
        ]));
      }
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Partitions" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(d.partitions) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Est. Cost" }),
        el("div", { className: "lh-card-value", textContent: fmtUSD(d.monthly_cost_usd) }),
      ]));
      container.appendChild(cards);

      // Per-tenant policy override block (omitted when no override is configured).
      if (d.policy) {
        var pol = el("div", { style: "margin:16px 0;padding:12px;border:1px solid #ccc;border-radius:4px;background:#fafafa" });
        pol.appendChild(el("div", { style: "font-weight:600;margin-bottom:6px", textContent: "Effective Policy Override" }));
        var lines = [];
        if (d.policy.retention) lines.push(["Retention", d.policy.retention]);
        if (d.policy.max_fields) lines.push(["Max Fields", d.policy.max_fields.toLocaleString()]);
        if (d.policy.max_streams) lines.push(["Max Streams", d.policy.max_streams.toLocaleString()]);
        if (d.policy.max_bytes_per_sec) lines.push(["Ingest Limit", fmtBytes(d.policy.max_bytes_per_sec) + "/s"]);
        if (d.policy.max_rows_per_sec) lines.push(["Row Rate Limit", fmtNum(d.policy.max_rows_per_sec) + "/s"]);
        if (d.policy.lifecycle && d.policy.lifecycle.length) {
          lines.push(["Lifecycle", d.policy.lifecycle.map(function (r) { return r.storage_class + "@" + r.transition_days + "d"; }).join(", ")]);
        }
        var dl = el("dl", { style: "display:grid;grid-template-columns:max-content 1fr;gap:4px 12px;margin:0" });
        lines.forEach(function (pair) {
          dl.appendChild(el("dt", { style: "color:#666", textContent: pair[0] }));
          dl.appendChild(el("dd", { style: "margin:0;font-family:monospace", textContent: pair[1] }));
        });
        pol.appendChild(dl);
        container.appendChild(pol);
      }

      // Info row with timestamps
      var info = el("div", { className: "lh-info-row" });
      if (d.last_write_at) info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Last Write: <strong>" + fmtTime(d.last_write_at) + "</strong>" }));
      if (d.last_query_at) info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Last Query: <strong>" + fmtTime(d.last_query_at) + "</strong>" }));
      if (d.source) info.appendChild(el("span", { className: "lh-info-item", innerHTML: "Source: <strong>" + d.source + "</strong>" }));
      if (info.children.length > 0) container.appendChild(info);

      // File size histogram
      if (d.file_size_histogram && d.file_size_histogram.buckets) {
        container.appendChild(el("div", { className: "lh-section-title", textContent: "File Size Distribution" }));
        var hist = d.file_size_histogram;
        var maxCount = Math.max.apply(null, hist.counts) || 1;
        var histDiv = el("div", { style: "display:flex;gap:4px;align-items:flex-end;height:120px;margin:12px 0" });
        hist.buckets.forEach(function (label, i) {
          var pct = (hist.counts[i] / maxCount * 100) || 0;
          var bar = el("div", { style: "flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%" });
          bar.appendChild(el("div", { textContent: fmtNum(hist.counts[i]), style: "font-size:11px;color:var(--color-text-secondary,#706f6f)" }));
          bar.appendChild(el("div", { style: "width:100%;max-width:60px;height:" + Math.max(pct, 2) + "%;background:var(--color-primary,#3f51b5);border-radius:3px 3px 0 0" }));
          bar.appendChild(el("div", { textContent: label, style: "font-size:10px;color:var(--color-text-secondary,#706f6f);margin-top:4px;text-align:center" }));
          histDiv.appendChild(bar);
        });
        container.appendChild(histDiv);
      }

      // Partition list (show first 30)
      var plist = d.partition_list || [];
      if (plist.length > 0) {
        container.appendChild(el("div", { className: "lh-section-title", textContent: "Partitions (" + plist.length + ")" }));
        var ptbl = el("table", { className: "lh-table" });
        ptbl.innerHTML = "<thead><tr><th>Date</th><th>Hours</th><th>Files</th><th>Size</th></tr></thead>";
        var ptbody = el("tbody");
        var showAll = plist.length <= 30;
        var display = showAll ? plist : plist.slice(0, 30);
        display.forEach(function (p) {
          var row = el("tr");
          row.innerHTML = "<td>" + p.date + "</td><td>" + (p.hours ? p.hours.length : 0) + "/24</td><td>" + fmtNum(p.files) + "</td><td>" + fmtBytes(p.bytes) + "</td>";
          ptbody.appendChild(row);
        });
        ptbl.appendChild(ptbody);
        container.appendChild(ptbl);
        if (!showAll) {
          container.appendChild(el("div", { className: "lh-info-row", textContent: "Showing 30 of " + plist.length + " partitions" }));
        }
      }
    }).catch(function (e) {
      container.innerHTML = "";
      container.appendChild(el("button", {
        className: "lh-tab",
        textContent: "\u2190 Back to Tenants",
        onClick: function () { renderTenants(container); },
      }));
      container.appendChild(el("div", { className: "lh-error", textContent: "Error loading tenant detail: " + e.message }));
    });
  }

  function renderCardinality(container) {
    container.innerHTML = '<div class="lh-loading">Loading cardinality\u2026</div>';
    fetchJSON("/lakehouse/api/v1/cardinality/fields").then(function (data) {
      container.innerHTML = "";
      var fields = data.fields || [];

      // Summary cards
      var cards = el("div", { className: "lh-cards" });
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Total Fields" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(data.total_fields) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "Promoted" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(data.total_promoted) }),
      ]));
      cards.appendChild(el("div", { className: "lh-card" }, [
        el("div", { className: "lh-card-label", textContent: "MAP Columns" }),
        el("div", { className: "lh-card-value", textContent: fmtNum(data.total_map) }),
      ]));
      if (data.high_cardinality_warning && data.high_cardinality_warning.length > 0) {
        cards.appendChild(el("div", { className: "lh-card" }, [
          el("div", { className: "lh-card-label", textContent: "High Cardinality" }),
          el("div", { className: "lh-card-value", innerHTML: '<span class="lh-badge lh-badge-high">' + data.high_cardinality_warning.length + " fields</span>" }),
        ]));
      }
      container.appendChild(cards);

      if (fields.length === 0) {
        container.appendChild(el("div", { className: "lh-empty", textContent: "No field data available yet." }));
        return;
      }

      var tbl = el("table", { className: "lh-table" });
      tbl.innerHTML = "<thead><tr><th>Field Name</th><th>Cardinality</th><th>Storage</th><th>Type</th><th>Bloom Filter</th></tr></thead>";
      var tbody = el("tbody");
      fields.forEach(function (f) {
        var row = el("tr");
        var badge = "";
        if (f.cardinality >= data.cardinality_threshold) {
          badge = ' <span class="lh-badge lh-badge-high">HIGH</span>';
        }
        var typeBadge = f.type === "promoted"
          ? '<span class="lh-badge lh-badge-promoted">promoted</span>'
          : '<span class="lh-badge lh-badge-map">map</span>';
        var cardCell = (f.indexed || f.cardinality > 0)
          ? fmtNum(f.cardinality)
          : '<span style="color:var(--color-text-secondary)" title="Not indexed \u2014 this field is not sketched, so distinct values are not counted (not the same as zero)">\u2014</span>';
        var storeCell = f.storage_bytes ? fmtBytes(f.storage_bytes) : '<span style="color:var(--color-text-secondary)">\u2014</span>';
        row.innerHTML = "<td>" + f.name + badge + "</td><td>" + cardCell + "</td><td>" + storeCell + "</td><td>" + typeBadge + "</td><td>" + (f.has_bloom ? "\u2705" : "\u2014") + "</td>";
        tbody.appendChild(row);
      });
      tbl.appendChild(tbody);
      container.appendChild(tbl);
    }).catch(function (e) {
      container.innerHTML = '<div class="lh-error">Error: ' + e.message + "</div>";
    });
  }

  // ---- Main render ----

  function renderLakehouse(main) {
    main.innerHTML = "";

    var root = el("div", { id: CONTAINER_ID });

    // Sub-tabs
    var tabs = el("div", { className: "lh-tabs" });
    var tabDefs = [
      { id: "overview", label: "Storage Overview" },
      { id: "details", label: "Storage Details" },
      { id: "tenants", label: "Tenants" },
      { id: "cardinality", label: "Cardinality Explorer" },
    ];
    var content = el("div", { className: "lh-content" });

    function switchTab(id) {
      activeTab = id;
      try { localStorage.setItem(SUBTAB_KEY, id); } catch (x) { /* ignore */ }
      Array.prototype.forEach.call(tabs.children, function (t) {
        t.classList.toggle("active", t.getAttribute("data-tab") === id);
      });
      if (id === "overview") renderOverview(content);
      else if (id === "details") renderBreakdown(content);
      else if (id === "tenants") renderTenants(content);
      else if (id === "cardinality") renderCardinality(content);
    }

    tabDefs.forEach(function (t) {
      var btn = el("button", {
        className: "lh-tab" + (activeTab === t.id ? " active" : ""),
        textContent: t.label,
        "data-tab": t.id,
        onClick: function () { switchTab(t.id); },
      });
      tabs.appendChild(btn);
    });

    root.appendChild(tabs);
    root.appendChild(content);
    main.appendChild(root);

    switchTab(activeTab);
  }

  // ---- Public API ----
  function injectStyleOnce() {
    if (document.getElementById("lh-style")) return;
    var styleEl = document.createElement("style");
    styleEl.id = "lh-style";
    styleEl.textContent = STYLE;
    document.head.appendChild(styleEl);
  }

  // mount() renders the full Lakehouse dashboard into a container. THE single
  // source of truth for the UI — used by both the standalone /lakehouse/ui/ page
  // and the VMUI tab integration (vmui-tab.js), so the two can never drift.
  window.LakehouseUI = {
    mount: function (container) { injectStyleOnce(); renderLakehouse(container); },
  };
})();
