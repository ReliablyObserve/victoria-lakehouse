#!/usr/bin/env python3
"""HA, production-topology cost + resource model at 50 / 100 / 300 / 500 TB/day ingest
(logs + traces, 60/40). AWS us-east-1 on-demand list prices, 2026-09-30.
Every rule is tagged SOURCED (vendor docs), DERIVED (from a vendor's relative claim),
or ASSUMED (no source; must be measured). Change a constant, re-run.

  python3 scripts/cost/cost_model_scale.py            # markdown tables
"""
import math

HOURS = 730
SCALES_TB_DAY = (50, 100, 300, 500)
RETENTIONS = (30, 365)
LOGS_SHARE, QPS, HOT_DAYS = 0.6, 1.0, 7
WIRE = 5                                   # ASSUMED payload compression on the wire

# ---- Prices (SOURCED) ---------------------------------------------------------------------------
EBS_GB_MO, XAZ_GB = 0.08, 0.02
S3_PUT_1K, S3_GET_1K = 0.005, 0.0004
INST = {  # $/h on-demand us-east-1 (m7g/r7g scale linearly with size)
    "r7g.8xlarge": (32, 256, 8 * 0.2142),
    "m7g.8xlarge": (32, 128, 8 * 0.1632),
    "m7g.xlarge": (4, 16, 0.1632),
}
CHC_UNIT_H, CHC_TB_MO = 0.2985, 25.30      # ClickHouse Cloud Scale (list; PB-scale deals negotiated)


def s3_cost(gb):  # SOURCED tiered S3 Standard
    t = [(50_000, 0.023), (450_000, 0.022), (float("inf"), 0.021)]
    c, left = 0.0, gb
    for size, price in t:
        take = min(left, size); c += take * price; left -= take
        if left <= 0: break
    return c


def nodes(itype, vcpu, gib=0.0, util=0.7):
    v, g, _ = INST[itype]
    return max(1, math.ceil(vcpu / (v * util)), math.ceil(gib / (g * util)))


def ncost(itype, n):
    return n * INST[itype][2] * HOURS


# ---- Compression (mid; SOURCED/MEASURED ranges in docs/cost-estimates.md) ------------------------------------
C = {"CH": 12, "VL": 15, "VT": 12, "LH_L": 6.1, "LH_T": 9.4, "LOKI": 4, "TEMPO": 5, "ES": 1 / 1.1}

# ---- Per-node practical storage limits (ASSUMED; recovery/merge time bound, not a hard cap) ------
import os
CH_NODE_TB = VL_NODE_TB = float(os.environ.get("NODE_TB", "20"))   # sensitivity: NODE_TB=60


def loki_vcpu_gib(logs_tb_day):
    """SOURCED Loki sizing tiers (base replicas, p90); linear beyond ~30 TB/day (DERIVED)."""
    if logs_tb_day < 3:
        v, g = 30, 36
    elif logs_tb_day < 30:
        v, g = 410, 780
    else:
        f = logs_tb_day / 30; v, g = 1175 * f, 2050 * f
    return v * 1.3, g * 1.3                 # ASSUMED +30%: query-frontend, compactor, index-gw, caches


def model(tb_day, days):
    gb_day = tb_day * 1000
    logs, traces = gb_day * LOGS_SHARE, gb_day * (1 - LOGS_SHARE)
    r = gb_day * 1000 / 86400               # MB/s raw
    hot = min(days, HOT_DAYS)
    wire_mo = gb_day / WIRE * 30
    res = {}

    def row(name, readiness, topo, st, cp, nw, rq, vcpu, gib, ebs_gb, s3_gb, xaz_gb, extra_note=""):
        res[name] = dict(readiness=readiness, topo=topo, st=st, cp=cp, nw=nw, rq=rq, vcpu=vcpu, gib=gib,
                         ebs=ebs_gb, s3=s3_gb, xaz=xaz_gb, note=extra_note)

    # ClickHouse self-hosted, sharded, RF2 (SOURCED ClickStack CPU rule; RF2 doubles ingest work)
    ch_gb = (logs + traces) / C["CH"] * days
    ch_vcpu = 2 * 0.1 * r + 0.3 * r * QPS
    ch_cpu_nodes = nodes("r7g.8xlarge", ch_vcpu, 4 * ch_vcpu)
    keeper = ncost("m7g.xlarge", 5)
    # EBS-only: nodes bound by max(CPU, storage/node)
    ebs_all = ch_gb * 2 / 0.8
    n_ebs = max(ch_cpu_nodes, math.ceil(ebs_all / 1000 / CH_NODE_TB))
    n_ebs += n_ebs % 2                                                    # shards x 2 replicas
    row("ClickHouse self-hosted, EBS (sharded, RF2)", "PROVEN (LogHouse >100 PB uncompressed)",
        f"{n_ebs//2} shards × 2 replicas r7g.8xlarge + 5 Keeper", ebs_all * EBS_GB_MO,
        ncost("r7g.8xlarge", n_ebs) + keeper, (wire_mo * 0.5 + ch_gb / days * 30) * XAZ_GB, 0,
        n_ebs * 32, n_ebs * 256, ebs_all, 0, wire_mo * 0.5 + ch_gb / days * 30)
    # Tiered: 7d EBS hot, older on S3 disk; RF2 => 2 S3 copies (no zero-copy, SOURCED)
    ebs_hot = ch_gb * hot / days * 2 / 0.8
    n_t = max(ch_cpu_nodes, math.ceil(ebs_hot / 1000 / CH_NODE_TB)); n_t += n_t % 2
    s3_t = ch_gb * (1 - hot / days) * 2
    row("ClickHouse self-hosted, 7d EBS + S3 (sharded, RF2)", "PROVEN pattern (storage policies/TTL move)",
        f"{n_t//2} shards × 2 replicas r7g.8xlarge + 5 Keeper", ebs_hot * EBS_GB_MO + s3_cost(s3_t),
        ncost("r7g.8xlarge", n_t) + keeper, (wire_mo * 0.5 + ch_gb / days * 30) * XAZ_GB,
        ch_gb / days * 2 * 1000 / 16 * 30 / 1000 * S3_PUT_1K,
        n_t * 32, n_t * 256, ebs_hot, s3_t, wire_mo * 0.5 + ch_gb / days * 30)
    # ClickHouse Cloud: single copy + 1 backup; compute per ClickStack rule (no RF2 ingest doubling)
    chc_vcpu = 0.1 * r + 0.3 * r * QPS
    row("ClickHouse Cloud (Scale, list price)", "PROVEN (SharedMergeTree; list price — PB deals negotiated)",
        f"{math.ceil(chc_vcpu/2)} compute units", ch_gb / 1000 * CHC_TB_MO * 2,
        HOURS * CHC_UNIT_H * chc_vcpu / 2, wire_mo * 0.5 * XAZ_GB, 0,
        chc_vcpu, 4 * chc_vcpu, 0, ch_gb * 2, wire_mo * 0.5)

    # VL/VT HA = 2 independent clusters (SOURCED); compute ASSUMED = ClickStack rule (VL_CPU=loki: derived)
    vl_gb = logs / C["VL"] * days + traces / C["VT"] * days
    lv, lg = loki_vcpu_gib(logs / 1000)
    tmb = traces * 1000 / 86400
    if os.environ.get("VL_CPU") == "loki":
        # SENSITIVITY (pessimistic): Loki sizing / 2.6 CPU, / 3.7 RAM, 50% util (VL capacity docs)
        vl_vcpu = lv / 1.3 / 2.6 + (tmb * 1.0) / 2.6        # DERIVED (per cluster)
        vl_gib = lg / 1.3 / 3.7 + (tmb * 9.7) / 3.7
        vl_util = 0.5
    else:
        # DEFAULT: VL/VT CPU is unmeasured, like Lakehouse, so use the same ClickStack rule and the
        # same 70% util as ClickHouse. Per cluster: ingest 0.1 + half of the query load (total = CH RF2).
        vl_vcpu = (2 * 0.1 * r + 0.3 * r * QPS) / 2
        vl_gib = 4 * vl_vcpu
        vl_util = 0.7
    vl_ebs = vl_gb / 0.8                                                   # per cluster, 20% free (SOURCED)
    n_vl = max(nodes("r7g.8xlarge", vl_vcpu, vl_gib, util=vl_util), math.ceil(vl_ebs / 1000 / VL_NODE_TB))
    row("VL/VT (HA: 2 clusters)", "UNVERIFIED at this scale (no public reference checked)",
        f"2 clusters × {n_vl} r7g.8xlarge", vl_ebs * 2 * EBS_GB_MO, ncost("r7g.8xlarge", n_vl * 2),
        wire_mo * 1.5 * XAZ_GB, 0, n_vl * 2 * 32, n_vl * 2 * 256, vl_ebs * 2, 0, wire_mo * 1.5)

    # Lakehouse — PROJECTED; compute ASSUMED; not ready at this scale (1–50 M file limits)
    lh_gb = (logs / C["LH_L"] + traces / C["LH_T"]) * days
    files_year = (logs / C["LH_L"] + traces / C["LH_T"]) * 1000 / 64 * 365
    ins = nodes("m7g.8xlarge", 0.15 * r); ins += 1                          # N+1 HA
    sel = nodes("m7g.8xlarge", 0.3 * r * QPS) + 1
    comp = nodes("m7g.8xlarge", 0.05 * r)                                   # ASSUMED compaction
    n_lh = ins + sel + comp
    row("Lakehouse (projected; NOT READY at this scale)", f"NOT READY: ~{files_year/1e6:,.0f} M files/yr vs 1–50 M-file limits",
        f"{ins} insert + {sel} select + {comp} compaction m7g.8xlarge", s3_cost(lh_gb),
        ncost("m7g.8xlarge", n_lh), wire_mo * 0.5 * XAZ_GB,
        lh_gb / days * 1000 / 64 * 30 / 1000 * S3_PUT_1K * 3,               # x3: flush + compaction rewrites
        n_lh * 32, n_lh * 128, 0, lh_gb, wire_mo * 0.5)

    # Hybrid: VL/VT 7d HA + Lakehouse all data
    vl_hot_ebs = vl_ebs * hot / days
    n_vlh = max(nodes("r7g.8xlarge", vl_vcpu, vl_gib, util=vl_util), math.ceil(vl_hot_ebs / 1000 / VL_NODE_TB))
    sel_h = nodes("m7g.8xlarge", 0.1 * r * QPS) + 1
    n_lhh = ins + sel_h + comp
    row("Hybrid VL/VT 7d hot + Lakehouse S3 >7d (LH part NOT READY)", "VL/VT unverified + LH not ready",
        f"2 × {n_vlh} r7g.8xlarge + {n_lhh} m7g.8xlarge", vl_hot_ebs * 2 * EBS_GB_MO + s3_cost(lh_gb * (1 - hot / days)),
        ncost("r7g.8xlarge", n_vlh * 2) + ncost("m7g.8xlarge", n_lhh), wire_mo * 1.5 * XAZ_GB,
        lh_gb / days * 1000 / 64 * 30 / 1000 * S3_PUT_1K * 3,
        (n_vlh * 2) * 32 + n_lhh * 32, n_vlh * 2 * 256 + n_lhh * 128, vl_hot_ebs * 2, lh_gb * (1 - hot / days), wire_mo * 1.5)

    # Loki + Tempo (SOURCED guides; linear beyond published tiers)
    lt_s3 = logs / C["LOKI"] * days + traces / C["TEMPO"] * days
    t_vcpu = tmb / 10 * 2 + tmb / 8 * 1 + tmb / 1.5 * 1 + 4                 # distributor, live-store, querier, frontend
    t_gib = tmb / 10 * 2 + tmb / 8 * 12 + tmb / 1.5 * 12 + 24
    kafka_brokers = max(3, math.ceil(tmb * 3 / 50))                         # ASSUMED 50 MB/s per broker, RF3
    kafka_ebs = traces * 3 / 0.8                                            # 24h retention, RF3
    n_lt = nodes("r7g.8xlarge", lv + t_vcpu, lg + t_gib)
    row("Loki + Tempo", "vendor runs PB-scale SaaS (not verified here)",
        f"{n_lt} r7g.8xlarge + Kafka {kafka_brokers} × m7g.8xlarge",
        s3_cost(lt_s3) + kafka_ebs * EBS_GB_MO, ncost("r7g.8xlarge", n_lt) + ncost("m7g.8xlarge", kafka_brokers),
        wire_mo * 2.5 * XAZ_GB,
        (logs / C["LOKI"] * 1000 / 1.5 + traces / C["TEMPO"] * 1000 / 100) * 30 / 1000 * S3_PUT_1K,
        n_lt * 32 + kafka_brokers * 32, n_lt * 256 + kafka_brokers * 128, kafka_ebs, lt_s3, wire_mo * 2.5)

    # OpenSearch: 7d hot EBS (1 replica, x1.45) + searchable snapshots on S3 (SOURCED formula;
    # snapshot-node sizing ASSUMED)
    os_hot = gb_day * hot * 2 * 1.45
    os_snap = gb_day * max(0, days - hot) * 1.1
    n_os_hot = math.ceil(os_hot / 1000 / 8)                                 # 32 vCPU node ~ 8 TB (light rule)
    n_os_snap = math.ceil(os_snap / 1000 / 50) if os_snap else 0            # ASSUMED 1 node per 50 TB
    row("OpenSearch (7d hot + searchable snapshots)", "widely deployed (not verified here)",
        f"{n_os_hot} hot + {n_os_snap} snapshot r7g.8xlarge + 3 masters", os_hot * EBS_GB_MO + s3_cost(os_snap),
        ncost("r7g.8xlarge", n_os_hot + n_os_snap) + ncost("m7g.xlarge", 3), wire_mo * 1.5 * XAZ_GB, 0,
        (n_os_hot + n_os_snap) * 32, (n_os_hot + n_os_snap) * 256, os_hot, os_snap, wire_mo * 1.5)
    return res


def k(x):
    return f"${x/1000:,.0f}k" if x >= 1000 else f"${x:,.0f}"


def pb(gb):
    return f"{gb/1e6:,.2f}"


def main():
    for tb in SCALES_TB_DAY:
        for days in RETENTIONS:
            res = model(tb, days)
            print(f"\n### {tb} TB/day, {days}-day retention\n")
            print("| System | **Total/mo** | Storage | Compute | Network | Topology | vCPU | RAM TiB | EBS PB | S3 PB | cross-AZ PB/mo | Readiness |")
            print("|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---|")
            for name, d in sorted(res.items(), key=lambda kv: kv[1]["st"] + kv[1]["cp"] + kv[1]["nw"] + kv[1]["rq"]):
                tot = d["st"] + d["cp"] + d["nw"] + d["rq"]
                print(f"| {name} | **{k(tot)}** | {k(d['st'])} | {k(d['cp'])} | {k(d['nw'])} | {d['topo']} | "
                      f"{d['vcpu']:,.0f} | {d['gib']/1024:,.1f} | {pb(d['ebs'])} | {pb(d['s3'])} | {pb(d['xaz'])} | {d['readiness']} |")


if __name__ == "__main__":
    main()
