#!/usr/bin/env python3
"""Long-term retention and S3 storage-class tiering for Lakehouse vs Loki + Tempo vs VL/VT on EBS.

Every long-retention table in docs/cost-estimates.md, docs/cost-comparison.md and README.md is printed by this script,
including the read terms (retrieval, GET price difference, early deletion) at the read fractions rho in RHOS.

Steady-state monthly cost and cumulative spend (data accumulating from an empty bucket) for several
ingest sizes, retentions and S3 lifecycle policies. AWS us-east-1 list prices, fetched 2026-10-01
from AWS's public price feed. Logs and traces together, 60/40 by raw volume, at 1 query per second.

  python3 scripts/cost/cost_tiering.py

Lakehouse compute is ASSUMED (ClickHouse's published per-MB/s rule) until measured; see
docs/cost-estimates.md for how every input is classified.
"""

# ---- AWS list prices, us-east-1 (SOURCED, AWS price feed 2026-10-01) ----------------------------
# Cumulative GB caps as the AWS price feed states them (first 50 TB = 51,200 GB, next 450 TB up to 512,000 GB).
S3_STD_TIERS = ((51_200, 0.023), (512_000, 0.022), (float("inf"), 0.021))   # cumulative GB caps, $/GB-month
S3_IA, S3_GIR, S3_DEEP = 0.0125, 0.004, 0.00099                          # $/GB-month
PUT_1K = 0.005                                                           # Standard PUT
TRANSITION_1K = {"IA": 0.01, "GIR": 0.02, "DEEP": 0.05}                  # lifecycle transition requests
# Read-side terms (SOURCED, same feed): retrieval $/GB read, GET $/1,000 above Standard's 0.0004, minimum days.
RETRIEVAL_GB = {"IA": 0.01, "GIR": 0.03}
GET_DELTA_1K = {"IA": 0.001 - 0.0004, "GIR": 0.01 - 0.0004}
CLASS_PRICE = {"IA": S3_IA, "GIR": S3_GIR, "DEEP": S3_DEEP}
MIN_DAYS = {"IA": 30, "GIR": 90, "DEEP": 180}
GET_MIB = 8.0   # ASSUMED average ranged GET, applied to every system (Loki chunks are ~1.5 MB, so Loki would issue more GETs)
RHOS = (0.0, 0.01, 0.1, 1.0)   # ASSUMED fraction of cold-class bytes read per month
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


def read_terms(stored_gb_day, retention_days, policy, rho):
    """Monthly cost the storage/request model leaves out: read retrieval, the GET price difference,
    and early deletion (a class residency shorter than its minimum duration is billed up to it)."""
    gb = storage_by_class(stored_gb_day, retention_days, policy)
    retrieval = sum(gb[c] * rho * RETRIEVAL_GB[c] for c in ("IA", "GIR"))
    gets = sum(gb[c] * rho * 1024 / GET_MIB / 1000 * GET_DELTA_1K[c] for c in ("IA", "GIR"))
    early = 0.0
    for i, (start, cls) in enumerate(policy):
        if cls == "STD":
            continue
        end = policy[i + 1][0] if i + 1 < len(policy) else retention_days
        days = max(0, min(end, retention_days) - start)
        if 0 < days < MIN_DAYS[cls]:
            early += stored_gb_day * CLASS_PRICE[cls] * (MIN_DAYS[cls] - days)
    return retrieval + gets + early


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
    print("Steady state (retention window full). Storage, PUTs and lifecycle-transition requests only "
          "(rho = 0): read retrieval, the GET price difference and early deletion are in the sections below.\n")
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

    ladder = POLICIES["Standard 30d → Standard-IA → Glacier IR at 90d (all queryable)"]
    std = POLICIES["S3 Standard only"]
    tb = 10
    lh_comp, lh_net = lh_compute_network(tb)
    lt_comp, lt_net = 17_492.0, 3_000.0     # from cost_model.py, 10 TB/day (Loki tier-2 baseline)
    lt_kafka = (tb * 1000 * (1 - LOGS_SHARE)) * 3 / 0.8 * EBS_GB_MO
    vl_comp, vl_net = 1_724.0, 1_800.0      # from cost_model.py, 10 TB/day

    def lh_total(rd, policy, rho):
        s = tb * 1000 * STORED_PER_RAW["LH"]
        c, req, _ = monthly_storage(s, rd, policy, OBJ_MB["LH"])
        return c + req + read_terms(s, rd, policy, rho) + lh_comp + lh_net

    def lt_total(rd, policy, rho):
        s = tb * 1000 * STORED_PER_RAW["LT"]
        c, req, _ = monthly_storage(s, rd, policy, OBJ_MB["LT"])
        return c + req + read_terms(s, rd, policy, rho) + lt_comp + lt_net + lt_kafka

    def vl_total(rd):
        return tb * 1000 * STORED_PER_RAW["VL"] * rd * 2 / 0.8 * EBS_GB_MO + vl_comp + vl_net

    print("## Totals including reads, 10 TB/day: Lakehouse on Standard vs the example ladder\n")
    print("Storage + compute + cross-AZ network + requests + read retrieval + GET price difference + early deletion. "
          "rho = fraction of cold-class bytes read per month (ASSUMED); GET size 8 MiB (ASSUMED).\n")
    print("| Retention | Lakehouse Standard (default) | Ladder rho=0 | Ladder rho=0.01 | Ladder rho=0.1 | Ladder rho=1 |")
    print("|---|---:|---:|---:|---:|---:|")
    for rd in RETENTIONS:
        cells = [money(lh_total(rd, std, 0))] + [money(lh_total(rd, ladder, r)) for r in RHOS]
        print(f"| {retention_label(rd)} | **{cells[0]}** | " + " | ".join(cells[1:]) + " |")
    print()

    print("## Break-even rho of the ladder against Standard (Lakehouse, 10 TB/day)\n")
    print("| Retention | rho* |")
    print("|---|---:|")
    for rd in RETENTIONS[1:]:
        base, top = lh_total(rd, ladder, 0), lh_total(rd, ladder, 1)
        print(f"| {retention_label(rd)} | {(lh_total(rd, std, 0) - base) / (top - base):.2f} |")
    print()

    print("## Standard against Standard, 10 TB/day (storage + compute + cross-AZ network + requests)\n")
    print("Loki + Tempo compute and VL/VT compute are taken from cost_model.py at 10 TB/day.\n")
    print("| Retention | Lakehouse, Standard | Loki + Tempo, Standard | VL/VT HA on EBS |")
    print("|---|---:|---:|---:|")
    for rd in RETENTIONS:
        print(f"| {retention_label(rd)} | **{money(lh_total(rd, std, 0))}** | {money(lt_total(rd, std, 0))} | {money(vl_total(rd))} |")
    print()

    print("## Both on the example ladder with reads, 10 TB/day\n")
    print("Loki + Tempo uses its own stored bytes and ~41 MB average objects; the same rho and the same 8 MiB GET size "
          "are applied to both (Loki chunks are ~1.5 MB, so its real GET count would be higher).\n")
    print("| Retention | Lakehouse rho=0 | Lakehouse rho=0.01 | Lakehouse rho=0.1 | Loki + Tempo rho=0.01 | Loki + Tempo rho=0.1 |")
    print("|---|---:|---:|---:|---:|---:|")
    for rd in RETENTIONS:
        print(f"| {retention_label(rd)} | {money(lh_total(rd, ladder, 0))} | {money(lh_total(rd, ladder, 0.01))} | "
              f"{money(lh_total(rd, ladder, 0.1))} | {money(lt_total(rd, ladder, 0.01))} | {money(lt_total(rd, ladder, 0.1))} |")
    print()

    arch = POLICIES["Standard 30d → Glacier IR → Deep Archive after 1y (archive)"]
    print("## Early deletion the archive policy's 90-day column leaves out (Glacier IR residency 60 d against 90 d)\n")
    print("| Ingest/day | $ per month |")
    print("|---|---:|")
    for tb_ in INGEST_TB_DAY:
        s = tb_ * 1000 * STORED_PER_RAW["LH"]
        print(f"| {tb_} TB | {money(read_terms(s, 90, arch, 0))} |")
    print()

    print("## Cumulative spend from an empty bucket (Lakehouse), Standard then the example ladder at rho=0\n")
    print("Sum of monthly bills while data accumulates up to the retention window, then steady state. "
          "Storage, compute, network and requests (no read terms).\n")
    horizons = (12, 36, 84)
    for label, pol in (("S3 Standard (the default)", std), ("Example ladder, rho=0 (best case)", ladder)):
        print(f"**{label}**\n")
        print("| Ingest/day, retention | " + " | ".join(f"first {h // 12} y" for h in horizons) + " |")
        print("|---|" + "---:|" * len(horizons))
        for tb_ in (1, 10, 100):
            for rd in (365, 3 * 365, 7 * 365):
                s = tb_ * 1000 * STORED_PER_RAW["LH"]
                comp, net = lh_compute_network(tb_)
                cells = []
                for h in horizons:
                    total = 0.0
                    for m in range(1, h + 1):
                        held = min(m * 30, rd)
                        c, req, _ = monthly_storage(s, held, pol, OBJ_MB["LH"])
                        total += c + req + comp + net
                    cells.append(money(total))
                print(f"| {tb_} TB/day, {retention_label(rd)} | " + " | ".join(cells) + " |")
        print()


if __name__ == "__main__":
    main()
