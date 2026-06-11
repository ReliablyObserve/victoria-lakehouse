# Dedicated columns

Hot OpenTelemetry attributes are stored as first-class typed Parquet columns
instead of inside the resource/log/span attribute maps. This shrinks files and
makes equality filters on those attributes fast, while staying VL/VT-compatible
and standard-Parquet readable.

## Two tiers

**Tier 1 — strict OTel columns (built in).** A fixed set of OpenTelemetry
semantic-convention attributes is promoted out of the maps into typed columns:
high-byte low-cardinality descriptors (`k8s.cluster.name`, `telemetry.sdk.*`,
`cloud.*`, `os.type`, …) are dictionary-encoded — the compression win — and
high-cardinality id-like keys (`container.id`, `service.instance.id`, `url.full`,
`client.address`, …) are plain-encoded with a split-block bloom filter for
selective row-group pruning. Encoding is chosen per cardinality class: a bloom
only helps where a value is in few row groups, so low-cardinality columns are
never bloomed (a bloom there never skips and only wastes space).

**Tier 2 — custom attributes (operator-configured).** Deployments declare their
own non-OTel attributes for promotion via config (see below). Each maps to one of
eight reserved spare slot columns per signal; the name→slot binding is written to
each file's Parquet footer so files stay self-describing and portable.

## VL/VT compatibility & dual-read

Promotion is invisible to queries. A read emits each promoted column under the
exact field name it had as a map attribute, so LogsQL/the query layer sees the
same field whether the value is stored in a column (new files) or a map (older,
pre-promotion files). Old and new files coexist: queries read both identically
(dual-read), and old files migrate forward as they age through compaction (a
schema-version fingerprint keeps the compactor from mixing layouts).

## Configuration (Tier 2)

```yaml
logs:
  config:
    promoted_attributes:
      - { name: "tenant_id",    bloom: true }   # high-cardinality → slot + bloom
      - { name: "feature_flag", bloom: false }  # low-cardinality → slot, dict only
traces:
  config:
    promoted_attributes:
      - { name: "deploy_color", bloom: false }
```

- `name` — the attribute key as it appears in ingested data.
- `bloom` — request a bloom filter on the slot. Set it only for
  high-cardinality keys queried by equality; a bloom on a low-cardinality key
  is wasted space.
- Up to 8 custom attributes per signal; excess is ignored with a startup warning.

## Where the benefit lands

- **Size**: measured −9.5% (logs) / −8.0% (traces) on real data — see
  [benchmarks](../benchmarks/dedicated-columns.md).
- **Query**: a needle/equality filter on a promoted high-cardinality column is
  served from metadata + bloom (label aggregates, row-group pruning) instead of
  scanning the attribute map. The benefit is specific to filters on promoted
  columns; metadata-served counts and full-text scans are unaffected.
- **External analytics**: promoted columns + standard split-block blooms are read
  natively by DuckDB/pyarrow/Spark for their own predicate pushdown.
