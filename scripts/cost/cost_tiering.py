#!/usr/bin/env python3
"""Long-term retention and S3 storage-class tiering for Lakehouse vs Loki + Tempo vs VL/VT on EBS.

Steady-state monthly cost and cumulative spend (data accumulating from an empty bucket) for several
ingest sizes, retentions and S3 lifecycle policies. AWS us-east-1 list prices, fetched 2026-10-01
from AWS's public price feed. Logs and traces together, 60/40 by raw volume, at 1 query per second.

  python3 scripts/cost/cost_tiering.py

Lakehouse compute is ASSUMED (ClickHouse's published per-MB/s rule) until measured; see
docs/cost-estimates.md for how every input is classified.
"""

# ---- AWS list prices, us-east-1 (SOURCED, AWS price feed 2026-10-01) ----------------------------
S3_STD_TIERS = ((50_000, 0.023), (450_000, 0.022), (float("inf"), 0.021))   # GB bands, $/GB-month
S3_IA, S3_GIR, S3_DEEP = 0.0125, 0.004, 0.00099                          # $/GB-month
PUT_1K = 0.005                                                           # Standard PUT
TRANSITION_1K = {"IA": 0.01, "GIR": 0.02, "DEEP": 0.05}                  # lifecycle transition requests
EBS_GB_MO, XAZ_GB = 0.08, 0.02
VCPU_H, HOURS = 0.1632 / 4, 730                                          # m7g per vCPU-hour

# ---- Workload and system constants (from scripts/cost/cost_model.py, mid estimates) -------------
LOGS_SHARE, QPS, WIRE = 0.6, 1.0, 5
STORED_PER_RAW = {                                       # stored GB per raw GB, one copy
    "LH": LOGS_SHARE / 6.1 + (1 - LOGS_SHARE) / 9.4,     # MEASURED ZSTD Parquet
    "LT": LOGS_SHARE / 4 + (1 - LOGS_SHARE) / 5,         # typical Loki chunks / Tempo vParquet
    "VL": LOGS_SHARE / 15 + (1 - LOGS_SHARE) / 12,       # vendor range, mid
}
OBJ_MB = {"LH": 64.0, "LT": 1.5 * LOGS_SHARE + 100 * (1 - LOGS_SHARE)}   # average object size

INGEST_TB_DAY = (1, 10, 50, 100, 500)
RETENTIONS = (30, 90, 365, 3 * 365, 7 * 365)

# Lifecycle policies: list of (from_day, class). Each object moves at the given age.
POLICIES = {
    "S3 Standard only": [(0, "STD")],
    "Standard 30d → Standard-IA → Glacier IR at 90d (all queryable)": [(0, "STD"), (30, "IA"), (90, "GIR")],
    "Standard 30d → Glacier IR → Deep Archive after 1y (archive)": [(0, "STD"), (30, "GIR"), (365, "DEEP")],
}


def std_cost(gb):
    cost, left, prev = 0.0, gb, 0.0
    for upto, price in S3_STD_TIERS:
        band = min(left, upto - prev)
        if band <= 0:
            break
        cost += band * price
        left -= band
        prev = upto
    return cost


def storage_by_class(stored_gb_day, retention_days, policy):
    """GB held in each class at steady state."""
    out = {"STD": 0.0, "IA": 0.0, "GIR": 0.0, "DEEP": 0.0}
    for i, (start, cls) in enumerate(policy):
        end = policy[i + 1][0] if i + 1 < len(policy) else retention_days
        days = max(0, min(end, retention_days) - start)
        out[cls] += stored_gb_day * days
    return out


def monthly_storage(stored_gb_day, retention_days, policy, obj_mb):
    gb = storage_by_class(stored_gb_day, retention_days, policy)
    cost = std_cost(gb["STD"]) + gb["IA"] * S3_IA + gb["GIR"] * S3_GIR + gb["DEEP"] * S3_DEEP
    objects_month = stored_gb_day * 1000 / obj_mb * 30
    trans = 0.0
    for start, cls in policy[1:]:
        if start < retention_days:
            trans += objects_month / 1000 * TRANSITION_1K[cls]
    puts = objects_month / 1000 * PUT_1K
    return cost, trans + puts, gb


def lh_compute_network(tb_day):
    r = tb_day * 1e6 / 86400                      # raw MB/s
    vcpu = max(4.0, 2 * 0.15 * r + 0.3 * r * QPS)  # ASSUMED (insert x2 + select)
    wire_gb_mo = tb_day * 1000 / WIRE * 30
    return vcpu * VCPU_H * HOURS, wire_gb_mo * 0.5 * XAZ_GB


def money(x):
    return f"${x/1000:,.0f}k" if x >= 100_000 else f"${x:,.0f}"


def retention_label(d):
    return {30: "30 days", 90: "90 days", 365: "1 year", 1095: "3 years", 2555: "7 years"}[d]


def main():
    print("## Lakehouse S3 storage per month, by lifecycle policy\n")
    print("Steady state (retention window full). Includes PUTs and lifecycle-transition requests; "
          "retrieval fees for reading IA/Glacier IR data are extra ($0.01 / $0.03 per GB read).\n")
    for name, pol in POLICIES.items():
        print(f"**{name}**\n")
        print("| Ingest/day | " + " | ".join(retention_label(r) for r in RETENTIONS) + " |")
        print("|---|" + "---:|" * len(RETENTIONS))
        for tb in INGEST_TB_DAY:
            s = tb * 1000 * STORED_PER_RAW["LH"]
            cells = []
            for rd in RETENTIONS:
                c, req, _ = monthly_storage(s, rd, pol, OBJ_MB["LH"])
                cells.append(money(c + req))
            print(f"| {tb} TB | " + " | ".join(cells) + " |")
        print()

    print("## Total monthly cost: Lakehouse with lifecycle vs Loki + Tempo vs VL/VT on EBS\n")
    print("Storage + compute + cross-AZ network + requests. Lakehouse uses the all-queryable policy "
          "(Standard 30d → Standard-IA → Glacier IR at 90d); Loki + Tempo is shown on S3 Standard and "
          "with the same lifecycle; VL/VT HA keeps two EBS copies (no tiering on EBS). Compute for "
          "Loki + Tempo and VL/VT is taken from scripts/cost/cost_model.py at 10 TB/day.\n")
    pol = POLICIES["Standard 30d → Standard-IA → Glacier IR at 90d (all queryable)"]
    tb = 10
    lh_comp, lh_net = lh_compute_network(tb)
    lt_comp, lt_net = 17_492.0, 3_000.0     # from cost_model.py, 10 TB/day (Loki tier-2 baseline)
    lt_kafka = (tb * 1000 * (1 - LOGS_SHARE)) * 3 / 0.8 * EBS_GB_MO
    vl_comp, vl_net = 1_724.0, 1_800.0      # from cost_model.py, 10 TB/day
    print("| 10 TB/day, retention | Lakehouse + lifecycle | Loki + Tempo, S3 Standard | "
          "Loki + Tempo + same lifecycle | VL/VT HA on EBS |")
    print("|---|---:|---:|---:|---:|")
    for rd in RETENTIONS:
        s_lh = tb * 1000 * STORED_PER_RAW["LH"]
        s_lt = tb * 1000 * STORED_PER_RAW["LT"]
        c, req, _ = monthly_storage(s_lh, rd, pol, OBJ_MB["LH"])
        lh = c + req + lh_comp + lh_net
        c1, req1, _ = monthly_storage(s_lt, rd, POLICIES["S3 Standard only"], OBJ_MB["LT"])
        lt_std = c1 + req1 + lt_comp + lt_net + lt_kafka
        c2, req2, _ = monthly_storage(s_lt, rd, pol, OBJ_MB["LT"])
        lt_tier = c2 + req2 + lt_comp + lt_net + lt_kafka
        vl = tb * 1000 * STORED_PER_RAW["VL"] * rd * 2 / 0.8 * EBS_GB_MO + vl_comp + vl_net
        print(f"| {retention_label(rd)} | **{money(lh)}** | {money(lt_std)} | {money(lt_tier)} | {money(vl)} |")
    print()

    print("## Cumulative spend from an empty bucket (Lakehouse, all-queryable lifecycle)\n")
    print("Sum of monthly bills while data accumulates up to the retention window, then steady state. "
          "Storage, compute, network and requests.\n")
    horizons = (12, 36, 84)
    print("| Ingest/day, retention | " + " | ".join(f"first {h // 12} y" for h in horizons) + " |")
    print("|---|" + "---:|" * len(horizons))
    for tb in (1, 10, 100):
        for rd in (365, 3 * 365, 7 * 365):
            s = tb * 1000 * STORED_PER_RAW["LH"]
            comp, net = lh_compute_network(tb)
            cells = []
            for h in horizons:
                total = 0.0
                for m in range(1, h + 1):
                    held = min(m * 30, rd)
                    c, req, _ = monthly_storage(s, held, pol, OBJ_MB["LH"])
                    total += c + req + comp + net
                cells.append(money(total))
            print(f"| {tb} TB/day, {retention_label(rd)} | " + " | ".join(cells) + " |")
    print()


if __name__ == "__main__":
    main()
