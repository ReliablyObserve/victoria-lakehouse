# Parity suite

Hot VictoriaLogs/VictoriaTraces against Lakehouse, on the compose stack in `docker-compose.yml`
(`docker compose -f tests/parity/docker-compose.yml --profile test run --rm --no-deps -T parity-tests`).
Use your own `COMPOSE_PROJECT_NAME`; the stack publishes no host ports.

## Layer and tenant helpers (`layer_controls_test.go`)

A parity case that must hold in every data layer uses these; they are small and shared.

| Helper | What it does |
|---|---|
| `waitLeftBuffer` (`buffer_layer_test.go`) | waits until the tenant's rows left Lakehouse's insert buffer: from then on they are read from Parquet only. Before it, the buffer layer (upstream's engine) answers. |
| `recompactHourPartition(t, base, at)` | merges the objects of the hour partition with `POST /lakehouse/compaction/recompact`, retrying while the segment guard holds the objects back. Takes the minimum number of objects it must merge (two per tenant). |
| `restartComposeServices(t, base, mode, services)` | restarts services of this compose project through the Docker Engine API, proves the container's `StartedAt` changed, then waits for `/health` and for the insert buffer endpoint. The socket is a powerful handle, so this is enforced by the helper's checks (`compose_guard_test.go`, unit-tested in `compose_guard_unit_test.go`): only `lakehouse-logs` and `lakehouse-traces`; the target's compose project and config files must equal the suite's own container's; exactly one match; only GET list/inspect and POST restart of a verified container are ever sent. |
| `pickQuietHour(t, accounts)` | an hour (within hot's 48 h retention) in which none of the case tenants holds an object, so a case's partition holds only its own objects and reruns on one stack stay clean. |
| `requireBuffered`, `requireFlushedOnly`, `requireCompactedOnce`, `partitionObjects` | layer proofs: buffer rows of a tenant, and its objects by level in S3 (listed with the stack's S3 credentials). A cell calls them before and after its compares. |
| `tenantForm`, `numericTenant`, `aliasTenant` | a tenant as numeric `AccountID/ProjectID` or as a string OrgID. `f.header(cold)` gives the OrgID header for Lakehouse and the numeric headers for hot, which has no aliases. |
| `post`, `getWith`, `pushSpanAs` | requests with explicit tenant headers. |

The compose file gives each Lakehouse binary one static alias (`-lakehouse.tenant.alias`):
`parity-logs-orgid` = 7312:0 and `parity-traces-orgid` = 7302:0. Use a tenant no other test reads.

Layers of a case, in order: buffer, parquet (flushed), compacted, restart. See
`sort_all_columns_parity_test.go` for the full pattern (layer x tenant form x signal).
