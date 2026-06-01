# Config Parity Audit (Priority 2) — Design Spec

**Date:** 2026-06-01  
**Scope:** Priority 2 — Config/docs/Helm alignment + best-practice recommendations  
**Deliverable:** Updated configuration.md, best-practice recommendations doc, code update candidates (for approval)

---

## Problem Statement

Victoria Lakehouse has configuration defaults spread across three sources:
1. **Code** (`internal/config/config.go`) — actual runtime defaults
2. **Docs** (`docs/configuration.md`) — user-facing guidance
3. **Helm** (`charts/victoria-lakehouse/values.yaml`) — deployment defaults

These often diverge:
- Docs lag behind code changes
- Helm values may not reflect code defaults
- Best practices evolve faster than documentation updates
- Users unsure which source to trust

**Goal:** Make code the source of truth, align docs to it, and recommend unified best-practice values across all three sources.

---

## Design: Three-Phase Audit

### Phase 1: Extract & Compare

**Extract all configuration from three sources:**

1. **Code defaults** (`internal/config/config.go`)
   - Parse Go constants, struct defaults, initialization code
   - Document each setting: name, default value, data type, purpose
   - Note any runtime overrides (env vars, flags, dynamic calculation)

2. **Documentation** (`docs/configuration.md`)
   - Extract all documented defaults
   - Extract all documented ranges/constraints
   - Extract all documented trade-offs

3. **Helm Chart** (`charts/victoria-lakehouse/values.yaml`)
   - Extract all chart defaults
   - Extract all configurable values (values.schema.json constraints)
   - Note any template rendering logic

4. **Production Reality** (optional, if observability data available)
   - Current deployment configs from running clusters
   - Actual utilization vs configured values

**Output: Comparison Matrix**
```
| Setting | Code Default | Docs Value | Helm Value | Production | Notes |
|---------|--------------|------------|-----------|-----------|-------|
| cache.memory_limit | 512MB | 512MB | 4Gi | varies | MISMATCH: docs vs helm units |
| ingestion.throughput | 80 MB/s | "50-80 MB/s" | unlimited | measured | Docs are range, code/helm are fixed |
| ... | ... | ... | ... | ... | ... |
```

### Phase 2: Gap Analysis & Classification

**For each mismatch in the matrix, classify it:**

**Gap Types:**
1. **Documentation Lag** — Code changed, docs not updated (docs should follow code)
2. **Helm Divergence** — Code and docs align, but Helm differs (Helm may have good reason, needs review)
3. **Intentional Override** — Docs explicitly recommend different value than code (design choice, needs validation)
4. **Bug or Inconsistency** — Value should be the same everywhere but isn't (needs fixing)
5. **Unit Mismatch** — Same value in different units (512MB vs 0.5GB, needs standardization)

**Classification Output:**
- List of all gaps with type + severity (critical, important, minor)
- Gaps grouped by category (cache, ingestion, query, etc.)
- Critical gaps that block best-practice recommendations

### Phase 3: Best-Practice Recommendations

**For each configuration setting, determine optimal value using three sources:**

1. **Production Deployment Data** (if available)
   - Actual values running in production clusters
   - Measured performance metrics (throughput, latency, resource utilization)
   - Known issues or tuning lessons learned

2. **Priority 1 Benchmarks** (from cost documentation)
   - Throughput benchmarks: 50-80 MB/s per vCPU
   - Compression ratios: 6.1x (Parquet), 3.5x (Loki), 55-70x (VL/VT)
   - Network patterns: S3 requests, cross-AZ replication
   - Cost implications: which settings drive cost trade-offs

3. **External References**
   - VictoriaLogs performance tuning guide
   - AWS/cloud provider best practices
   - CNCF observability benchmarks
   - Industry standards (e.g., Kubernetes resource requests)

**Recommendation Output:**
```
| Setting | Current Value | Recommended Value | Rationale | Source | Risk Level |
|---------|---------------|-------------------|-----------|--------|-----------|
| cache.memory_limit | 512MB | 1GB | Benchmark shows 512MB insufficient for >100GB retention | perf.md benchmark | Medium |
| ingestion.buffer | 256MB | 512MB | Production data shows buffer overflows at 500GB/day | production | High |
| query.max_concurrent | 32 | 64 | Throughput benchmark shows 32 leaves CPU headroom unused | Priority 1 | Low |
```

---

## Scope: All Configuration Categories

### Categories Covered

1. **Storage/S3** — bucket, region, credentials, lifecycle, tiering
2. **Cache** — L1 (memory), L2 (disk), bloom filters, eviction
3. **Ingestion** — WAL, buffers, compression, throughput limits, write amplification
4. **Query** — concurrency, timeouts, performance tuning, resource limits
5. **Replication/HA** — cross-AZ, peer sync, consensus, failover
6. **Observability** — metrics, logging, tracing, telemetry

### What's Not Covered (Out of Scope)

- Security settings (auth, encryption, TLS) — separate audit
- Deployment orchestration (Kubernetes-specific) — separate audit
- Application-level business logic settings — separate audit

---

## Deliverables

### 1. Updated `docs/configuration.md`

**Changes:**
- All documented defaults updated to match code (source of truth)
- Add "Code Default" column to all settings tables
- Add "Helm Equivalent" column for deployments
- Clarify which settings are hardcoded vs runtime-configurable
- Note any environment variable or flag overrides

**Structure:**
```markdown
## Cache Settings

| Setting | Code Default | Helm Value | Type | Description |
|---------|--------------|-----------|------|-------------|
| memory_limit | 512MB | 4Gi | string | L1 in-memory cache size |
| ...
```

### 2. Best-Practice Recommendations Document

**File:** `docs/best-practice-config-recommendations.md` (new)

**Contents:**
- Executive summary of critical gaps
- Full recommendation matrix (all settings)
- Justification for each recommendation
- Implementation roadmap (urgent → important → nice-to-have)
- Validation checklist (how to verify each recommendation works)

**Example section:**
```markdown
## Cache Memory Limit (IMPORTANT)

**Current:** 512MB (code), 4Gi (Helm)
**Recommended:** 1GB (code) + 4Gi (Helm default for typical deployments)
**Rationale:** Priority 1 benchmarks show 512MB insufficient for >100GB retention; Helm default of 4Gi is appropriate for typical production. Code default is overly conservative for documented use cases.
**Source:** performance.md throughput benchmarks, production deployment data
**Risk:** Low — backward compatible, affects only cache hit ratio
**Validation:** Monitor cache hit ratio before/after; should improve for retained datasets
```

### 3. Code Update Candidates Document

**File:** `docs/superpowers/specs/config-code-updates.md` (new)

**Contents:**
- Specific lines in `internal/config/config.go` where changes are recommended
- Before/after code snippets
- Rationale for each change
- Impact analysis (breaking change, performance, backward compat)
- Test coverage needed

**Format for user review:**
```markdown
## Update: cache.memory_limit

**File:** internal/config/config.go
**Lines:** 45-47
**Current:**
```go
const MemoryLimit = "512MB"
```
**Recommended:**
```go
const MemoryLimit = "1GB"  // Increased from 512MB — benchmarks show insufficient for >100GB datasets
```
**Impact:** Non-breaking (Helm overrides this anyway); affects cache hit ratio
**Validation:** Existing tests pass; new test: cache hit rate increases 20%+ for typical workloads
```

### 4. Verification Checklist

**For each recommendation:**
- ✓ Mismatch identified (code vs docs vs Helm)
- ✓ Gap type classified (lag, divergence, bug, etc.)
- ✓ Best practice researched (≥2 sources)
- ✓ Rationale documented
- ✓ Risk assessed
- ✓ Implementation candidate prepared
- ✓ User review checkpoint

---

## Success Criteria

1. ✅ All config settings extracted and compared (code, docs, Helm, production)
2. ✅ All gaps classified and documented
3. ✅ All gaps have best-practice recommendations with 2+ source references
4. ✅ No placeholders or "TBD" in recommendations
5. ✅ `docs/configuration.md` updated to match code defaults
6. ✅ Code update candidates prepared for user approval (no changes made yet)
7. ✅ Each recommendation traceable to source data (perf benchmark, production, external ref)

---

## Next Steps (After Priority 2)

**Priority 3:** Full docs audit across `/docs` subdirectories for consistency with updated configuration.md and best practices

---
