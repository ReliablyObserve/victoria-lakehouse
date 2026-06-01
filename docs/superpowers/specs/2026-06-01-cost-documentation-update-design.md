# Cost Documentation Update — Design Spec

**Date:** 2026-06-01  
**Scope:** Priority 1 — Cost accuracy, resource metrics, and code/docs parity  
**Deliverable:** Single PR updating README and cost-estimates.md with verified resource metrics

---

## Problem Statement

The current README cost table shows total monthly costs but lacks:
1. **Resource metrics breakdown** — CPU, memory, network traffic per scenario
2. **Cost composition** — which costs are compute, storage, network
3. **Data format clarity** — "Proprietary+Parquet hybrid" label for VL/VT EBS + Lakehouse S3 combination
4. **Source attribution** — where numbers come from (benchmarks, production, Helm defaults)

Decision-makers need to understand not just total cost, but the *composition* — storage vs compute dominance at different scales.

---

## Design: Two-Document Update

### 1. README.md — Enhanced Cost Comparison Table

**Current table:** 8 columns (Scenario, Standalone LH, VL/VT EBS, Hybrid, Loki+Tempo)  
**New rows to add:**
- **CPU (vCPU-months)** — total cores needed per scenario, sourced from perf benchmarks
- **Memory (GB)** — total RAM needed, from Helm chart defaults + stack measurements
- **Network traffic (GB/month)** — ingest + query patterns, compared across all 3
- **Storage breakdown** — EBS cost + S3 cost shown separately in parens
- **Resource cost composition** — (Compute %, Storage %, Network %) to show dominance

**Data format row update:**
- Change "Open Parquet" for Hybrid to "**Proprietary+Parquet**" (VL/VT EBS hot + LH S3 cold)

**Links added:**
- Each row links to `cost-estimates.md#resource-cost-breakdown` for detailed methodology
- Performance.md linked for CPU/memory derivation
- Benchmark docs linked for network assumptions

### 2. cost-estimates.md — New Section: Resource Cost Breakdown

**Add after "Annual Savings Summary":**

#### New Section: "Resource Cost Breakdown"

**Subsection: CPU Requirements**
- Per-scenario vCPU calculation from benchmarks
- Derivation: ingest volume → vCPU needed (based on throughput tests)
- Assumptions: instance type (m5.xlarge equiv), regional pricing

**Subsection: Memory Requirements**
- Per-scenario RAM from Helm defaults (cache_memory_request, cache_memory_limit)
- Cross-reference to actual production stack measurements
- Per-pod breakdown for multi-node scenarios

**Subsection: Network Traffic**
- Ingest traffic (raw bytes/month × compression ratio → S3 PUT traffic)
- Query traffic (S3 GET requests × avg response size)
- Cross-AZ replication cost (for Hybrid and VL/VT EBS multi-AZ)
- Per-solution comparison (LH vs VL/VT vs Loki/Tempo)

**Subsection: Cost Composition Table**
Shows for the 500 GB/day scenario:
```
| Solution | Compute | Storage | Network | Other | Total |
|----------|---------|---------|---------|-------|-------|
| VL/VT EBS | $X/mo | $Y/mo | $Z/mo | - | $total |
| Hybrid | $X/mo | $Y/mo | $Z/mo | - | $total |
| Lakehouse | $X/mo | $Y/mo | $Z/mo | - | $total |
| Loki+Tempo | $X/mo | $Y/mo | $Z/mo | - | $total |
```

**Subsection: Measurement Sources**
- Benchmarks: link to `benchmarks/` directory with specific file references
- Helm defaults: link to `charts/victoria-lakehouse/values.yaml` line numbers
- Production data: note which metrics come from actual LH stack observability
- Estimation method: where inferred, show the formula (e.g., "ingest volume × 0.15 = GET requests")

---

## Data Sources

### From Code/Benchmarks (to verify):
1. **Helm charts** (`charts/victoria-lakehouse/values.yaml`)
   - `resources.requests.cpu`, `resources.limits.cpu`
   - `cache.memory_limit`, `cache.memory_request`
   - Pod counts per deployment

2. **Performance docs** (`docs/performance.md`)
   - Throughput benchmarks (GB/s ingest, queries/s)
   - CPU/memory utilization under load

3. **Benchmarks directory** (`benchmarks/`)
   - ZSTD compression ratios (already in cost-estimates.md)
   - S3 PUT/GET cost derivation

4. **Configuration docs** (`docs/configuration.md`)
   - Default tuning for CPU/memory scaling

### From External (to cross-reference):
1. **Loki docs** — CPU/memory requirements, network assumptions
2. **Tempo docs** — CPU/memory requirements, trace write amplification
3. **AWS pricing** (us-east-1) — EC2, S3, networking rates

---

## Verification Strategy

**For each number added:**
1. **Show the source** — code file, benchmark result, or production measurement
2. **Show the derivation** — if calculated, show the formula (e.g., "500 GB/day ÷ 24h ÷ 3600s = X MB/s ingest → Y vCPU")
3. **Note confidence level** — "measured", "benchmarked", "estimated from Loki docs", or "theoretical"
4. **Link to proof** — GitHub line numbers, benchmark script output, etc.

---

## PR Scope

**Single PR with:**
- Modified: `README.md` (enhanced cost table)
- Modified: `docs/cost-estimates.md` (new Resource Cost Breakdown section)
- No code changes
- Clear commit message: "docs: add resource metrics and cost composition to cost comparison"

**Out of Scope (Priority 2+):**
- Memory/CPU tuning documentation updates
- Network optimization deep dive
- Full docs audit for parity across all files

---

## Success Criteria

1. ✅ README cost table has 5 new rows (CPU, Memory, Network, Storage breakdown, Composition)
2. ✅ cost-estimates.md has new "Resource Cost Breakdown" section with formulas + sources
3. ✅ All numbers have source attribution (code link, benchmark ref, or "measured")
4. ✅ Hybrid column explicitly labeled "Proprietary+Parquet"
5. ✅ Bidirectional links between README → cost-estimates.md → performance.md
6. ✅ Derivations are honest about measured vs estimated
7. ✅ No contradictions between README and cost-estimates.md numbers

---

## Next Steps (After Priority 1)

**Priority 2:** Resource metrics verification (CPU/memory config parity)  
**Priority 3:** Full docs audit across `/docs` subdirectories

