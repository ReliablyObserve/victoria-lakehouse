#!/usr/bin/env python3
"""Parametric monthly cost + projected resources: ClickHouse (self-hosted, Cloud),
Elasticsearch/OpenSearch, VictoriaLogs/VictoriaTraces, Victoria Lakehouse, Loki/Tempo, hybrid.
AWS us-east-1 on-demand list prices fetched 2026-09-30. Every input is a named constant with its
source class in docs/cost-estimates.md: MEASURED, SOURCED (vendor docs), or ASSUMED.

Usage:
  python3 scripts/cost/cost_model.py          # cost tables (markdown)
  python3 scripts/cost/cost_model.py --res    # projected resource tables (markdown)
"""
import os
import sys

HOURS = 730

# ---- Prices (SOURCED: AWS / ClickHouse pricing pages, 2026-09-30) -----------------------------
S3_GB_MO, S3_PUT_1K, S3_GET_1K = 0.023, 0.005, 0.0004
EBS_GB_MO = 0.08
XAZ_GB = 0.02
VCPU_H = 0.1632 / 4                        # m7g.xlarge per vCPU-h (incl. 4 GiB)
GIB_EXTRA_H = (0.2142 - 0.1632) / 16       # r7g - m7g per extra GiB-h
CHC_UNIT_H, CHC_TB_MO = 0.2985, 25.30      # ClickHouse Cloud Scale unit (2 vCPU + 8 GiB); storage

# ---- Compression raw->stored (low, mid, high) --------------------------------------------------
COMP = {
    "logs":   {"CH": (8, 12, 20), "VL": (8, 15, 30), "LH": (4, 6.1, 9), "LOKI": (3, 4, 6)},
    "traces": {"CH": (8, 12, 20), "VT": (10, 12, 20), "LH": (6, 9.4, 12), "TEMPO": (3, 5, 8)},
}
ES_INDEX_OVERHEAD = 1.1    # SOURCED: AWS OpenSearch "often 110% of the source"
ES_DISK_FACTOR = 1.45      # SOURCED: AWS simplified formula (overhead + OS 5% + service 20%)
WIRE_COMP = {"low": 3, "mid": 5, "high": 8}   # ASSUMED: OTLP gzip/zstd, Loki snappy, VL zstd

LOGS_SHARE, QPS, HOT_DAYS = 0.6, 1.0, 7
AVG_S3_OBJ_MB = {"LH": 64, "LOKI": 1.5, "TEMPO": 100, "CH": 16}
QUERY_GETS_PER_DAY = 200_000


class R:  # projected resources for one system/scenario
    def __init__(self):
        self.vcpu = self.gib = self.ebs_gb = self.s3_gb = self.xaz_gb_mo = 0.0
        self.s3_req = 0.0          # $ of requests
        self.extra = 0.0           # $ billed outside the above (CH Cloud)

    def cost(self):
        comp = HOURS * (self.vcpu * VCPU_H + max(0.0, self.gib - 4 * self.vcpu) * GIB_EXTRA_H)
        return (self.ebs_gb * EBS_GB_MO + self.s3_gb * S3_GB_MO,
                comp, self.xaz_gb_mo * XAZ_GB, self.s3_req, self.extra)


def s3_req(stored_gb_day, obj_mb):
    return stored_gb_day * 1000 / obj_mb * 30 / 1000 * S3_PUT_1K + QUERY_GETS_PER_DAY * 30 / 1000 * S3_GET_1K


def scenario(gb_day, days, level):
    i = {"low": 0, "mid": 1, "high": 2}[level]
    w = WIRE_COMP[level]
    logs, traces = gb_day * LOGS_SHARE, gb_day * (1 - LOGS_SHARE)
    r = gb_day * 1000 / 86400                      # raw MB/s
    lmb, tmb = logs * 1000 / 86400, traces * 1000 / 86400
    hot = min(days, HOT_DAYS)
    wire_gb_mo = gb_day / w * 30
    out = {}

    # --- ClickHouse self-hosted, ReplicatedMergeTree RF2 --------------------------------------
    ch_gb = logs / COMP["logs"]["CH"][i] * days + traces / COMP["traces"]["CH"][i] * days
    ch_vcpu = 2 * 0.1 * r + 0.3 * r * QPS + 6       # SOURCED ClickStack rule; x2 ingest for RF2; Keeper
    x = R(); x.vcpu, x.gib = ch_vcpu, 4 * ch_vcpu
    x.ebs_gb = ch_gb * 2 / 0.8
    x.xaz_gb_mo = wire_gb_mo * 0.5 + ch_gb / days * 30
    out["ClickHouse self-hosted (EBS, RF2)"] = x
    y = R(); y.vcpu, y.gib = ch_vcpu, 4 * ch_vcpu
    y.ebs_gb = ch_gb * hot / days * 2 / 0.8
    y.s3_gb = ch_gb * (1 - hot / days) * 2          # RF2 without zero-copy => 2 S3 copies (SOURCED)
    y.xaz_gb_mo = x.xaz_gb_mo
    y.s3_req = s3_req(ch_gb / days * 2, AVG_S3_OBJ_MB["CH"])
    out["ClickHouse self-hosted (7d EBS + S3, RF2)"] = y
    k = R(); k_vcpu = 0.1 * r + 0.3 * r * QPS + 6    # one replica (no compute HA) + Keeper
    k.vcpu, k.gib = k_vcpu, 4 * k_vcpu
    k.ebs_gb = ch_gb * hot / days / 0.8               # 7d cache/hot on local EBS, single copy
    k.s3_gb = ch_gb * (1 - hot / days) + ch_gb * 0.0  # single S3 copy; backups are incremental, not modelled
    k.xaz_gb_mo = wire_gb_mo * 0.5
    k.s3_req = s3_req(ch_gb / days, AVG_S3_OBJ_MB["CH"])
    out["ClickHouse self-hosted (1 replica, 7d EBS + S3; no compute HA)"] = k

    # --- ClickHouse Cloud (Scale) ---------------------------------------------------------------
    z = R(); chc_vcpu = max(4.0, 0.1 * r + 0.3 * r * QPS)
    z.vcpu, z.gib = chc_vcpu, 4 * chc_vcpu
    z.extra = HOURS * CHC_UNIT_H * (chc_vcpu / 2) + ch_gb / 1000 * CHC_TB_MO * 2 \
        - HOURS * chc_vcpu * VCPU_H                  # replace EC2 compute with CHC pricing
    z.xaz_gb_mo = wire_gb_mo * 0.5
    z.s3_gb = 0  # billed inside extra at $25.30/TB (data + 1 backup)
    out["ClickHouse Cloud (Scale)"] = z

    # --- Elasticsearch / OpenSearch -------------------------------------------------------------
    es_hot_gb = gb_day * hot * 2 * ES_DISK_FACTOR    # SOURCED: source x (1+1 replica) x 1.45
    es_all_gb = gb_day * days * 2 * ES_DISK_FACTOR
    light_vcpu = lambda gb: max(6.0, gb / 512 * 2)   # SOURCED light: m6g.large 2 vCPU/8 GiB per 512 GiB; min 3 nodes
    e = R(); e.ebs_gb = es_all_gb; e.vcpu = light_vcpu(es_all_gb) + 6; e.gib = 4 * e.vcpu  # + 3 masters
    e.xaz_gb_mo = wire_gb_mo * (0.5 + 1.0)            # client + replica shipping
    out["Elasticsearch/OpenSearch (all hot, EBS, 1 replica)"] = e
    f = R(); f.ebs_gb = es_hot_gb
    f.s3_gb = gb_day * max(0, days - hot) * ES_INDEX_OVERHEAD   # searchable snapshots: 1 copy on S3
    snap_nodes_vcpu = 8 * max(1.0, f.s3_gb / 50_000)              # ASSUMED: 8 vCPU/64 GiB per 50 TB snapshots
    f.vcpu = light_vcpu(es_hot_gb) + 6 + snap_nodes_vcpu
    f.gib = 4 * (light_vcpu(es_hot_gb) + 6) + 8 * snap_nodes_vcpu
    f.xaz_gb_mo = e.xaz_gb_mo
    f.s3_req = s3_req(gb_day * ES_INDEX_OVERHEAD, 512)
    out["OpenSearch (7d hot EBS + searchable snapshots S3)"] = f

    # --- VictoriaLogs + VictoriaTraces HA (2 independent clusters, SOURCED) ---------------------
    vl_gb = logs / COMP["logs"]["VL"][i] * days + traces / COMP["traces"]["VT"][i] * days
    loki_vcpu_per = 0.86 if gb_day < 3000 else 1.18
    if os.environ.get("VL_CPU") == "loki":                                   # SENSITIVITY: DERIVED
        vl_vcpu = max(2.0, lmb * loki_vcpu_per / 2.6 + tmb * 1.0 / 2.6) * 2
        vl_gib = max(8.0, lmb * loki_vcpu_per * 1.5 / 3.7 + tmb * 9.7 / 3.7) * 2
    else:  # DEFAULT, ASSUMED: same ClickStack rule as ClickHouse (both clusters together = CH RF2)
        vl_vcpu = max(4.0, 2 * 0.1 * r + 0.3 * r * QPS)
        vl_gib = 4 * vl_vcpu
    v = R(); v.vcpu, v.gib = vl_vcpu, vl_gib
    v.ebs_gb = vl_gb * 2 / 0.8
    v.xaz_gb_mo = wire_gb_mo * (0.5 + 1.0)
    out["VL/VT (HA: 2 clusters, EBS)"] = v

    # --- Lakehouse (compute ASSUMED) ------------------------------------------------------------
    lh_gb = logs / COMP["logs"]["LH"][i] * days + traces / COMP["traces"]["LH"][i] * days
    l = R(); l.vcpu = max(4.0, 2 * 0.15 * r + 0.3 * r * QPS); l.gib = 4 * l.vcpu
    l.s3_gb = lh_gb
    l.xaz_gb_mo = wire_gb_mo * 0.5
    l.s3_req = s3_req(lh_gb / days, AVG_S3_OBJ_MB["LH"])
    out["Lakehouse (S3)"] = l

    # --- Loki + Tempo (SOURCED sizing guides) ---------------------------------------------------
    lt_gb = logs / COMP["logs"]["LOKI"][i] * days + traces / COMP["traces"]["TEMPO"][i] * days
    # SOURCED Loki tiers: <3 TB/day linear from tier-1 base; 3-30 TB/day tier-2 base (410 vCPU/780 GiB)
    loki_vcpu = (max(8.0, lmb * 0.86) if logs < 3000 else 410.0) * 1.3   # +30% frontend/compactor/caches
    loki_gib = loki_vcpu * (1.2 if logs < 3000 else 780 / 410)
    tempo_vcpu = max(4.0, tmb * 1.0) + 2 + 6
    tempo_gib = max(16.0, tmb * 9.7) + 24 + 24
    t = R(); t.vcpu, t.gib = loki_vcpu + tempo_vcpu, loki_gib + tempo_gib
    t.s3_gb = lt_gb
    t.ebs_gb = traces * 1.0 * 3 / 0.8                # Kafka 24h RF3
    t.xaz_gb_mo = wire_gb_mo * (0.5 + 2.0)
    t.s3_req = s3_req(logs / COMP["logs"]["LOKI"][i], AVG_S3_OBJ_MB["LOKI"]) + \
        s3_req(traces / COMP["traces"]["TEMPO"][i], AVG_S3_OBJ_MB["TEMPO"])
    out["Loki + Tempo (S3)"] = t

    # --- Hybrid: VL/VT hot HA (7d) + Lakehouse S3 for data older than the hot window ----------------
    # Same tiering shape as ClickHouse tiered. VL/VT -> LH age-out is a DESIGN; today LH dual-writes.
    h = R()
    lh_cold_vcpu = max(4.0, 2 * 0.15 * r + 0.1 * r * QPS)
    h.vcpu, h.gib = vl_vcpu + lh_cold_vcpu, vl_gib + 4 * lh_cold_vcpu
    h.ebs_gb = vl_gb * hot / days * 2 / 0.8
    h.s3_gb = lh_gb * (1 - hot / days)
    h.xaz_gb_mo = wire_gb_mo * (0.5 + 1.0)
    h.s3_req = l.s3_req
    out["Hybrid VL/VT hot 7d + Lakehouse S3 >7d"] = h
    return out


CONF = {  # storage basis / compute basis
    "ClickHouse self-hosted (EBS, RF2)": "compression sourced (10x guide) / compute sourced",
    "ClickHouse self-hosted (7d EBS + S3, RF2)": "sourced / sourced",
    "ClickHouse self-hosted (1 replica, 7d EBS + S3; no compute HA)": "sourced / sourced (single node: no HA)",
    "ClickHouse Cloud (Scale)": "sourced / sourced (prices exact)",
    "Elasticsearch/OpenSearch (all hot, EBS, 1 replica)": "sourced (AWS formula) / sourced (light-workload floor; heavy = ~5x)",
    "OpenSearch (7d hot EBS + searchable snapshots S3)": "sourced / hot sourced, snapshot nodes ASSUMED",
    "VL/VT (HA: 2 clusters, EBS)": "vendor claim range / ASSUMED (ClickStack rule, as ClickHouse)",
    "Lakehouse (S3)": "MEASURED compression / compute ASSUMED",
    "Loki + Tempo (S3)": "compression ASSUMED range / compute sourced (guides)",
    "Hybrid VL/VT hot 7d + Lakehouse S3 >7d": "mixed / ASSUMED (VL/VT→LH age-out is a design)",
}


def money(x):
    return f"${x:,.0f}"


def main():
    res = "--res" in sys.argv
    if res:
        print("| Ingest/day | Retention | System | vCPU | RAM GiB | EBS TB | S3 TB | cross-AZ TB/mo |")
        print("|---|---|---|---:|---:|---:|---:|---:|")
    else:
        print("| Ingest/day | Retention | System | Storage | Compute | Network | S3 req | **Total (mid)** | Low–high | Basis (storage / compute) |")
        print("|---|---|---|---:|---:|---:|---:|---:|---:|---|")
    for gb_day in (100, 1000, 10000):
        for days in (30, 365):
            mid, low, high = (scenario(gb_day, days, lv) for lv in ("mid", "low", "high"))
            for name, x in mid.items():
                if res:
                    print(f"| {gb_day/1000:g} TB | {days}d | {name} | {x.vcpu:,.0f} | {x.gib:,.0f} | "
                          f"{x.ebs_gb/1000:,.1f} | {x.s3_gb/1000:,.1f} | {x.xaz_gb_mo/1000:,.1f} |")
                    continue
                st, cp, nw, rq, ex = x.cost()
                tot = st + cp + nw + rq + ex
                lo, hi = sum(low[name].cost()), sum(high[name].cost())
                cpu_show = cp + ex if name.startswith("ClickHouse Cloud") else cp
                st_show = st if not name.startswith("ClickHouse Cloud") else st + (ex - (HOURS * CHC_UNIT_H * (x.vcpu / 2) - HOURS * x.vcpu * VCPU_H))
                cpu_show = cp if not name.startswith("ClickHouse Cloud") else HOURS * CHC_UNIT_H * (x.vcpu / 2)
                print(f"| {gb_day/1000:g} TB | {days}d | {name} | {money(st_show)} | {money(cpu_show)} | {money(nw)} | "
                      f"{money(rq)} | **{money(tot)}** | {money(min(lo, hi))} – {money(max(lo, hi))} | {CONF[name]} |")


if __name__ == "__main__":
    main()
