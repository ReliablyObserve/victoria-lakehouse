# Upstream Sync

Lakehouse embeds VictoriaLogs and VictoriaTraces as Go libraries and serves their own HTTP
handlers, so moving to a new upstream release is not a dependency bump in the usual sense:
the patches that attach the cold tier are generated against one exact upstream tree, the
embedded web UI and the internal peer protocol come with the release, and every native
endpoint has to keep behaving as it does upstream.

This page is the procedure, in the order it is done, followed by the daily sync probe that
performs its mechanical half automatically and opens a pull request with the result.

## The pins

### Three pins, one source of truth

The `Makefile` is the single source of truth for what is embedded:

| Variable | What it pins | How it is chosen |
| --- | --- | --- |
| `VL_VERSION_LOGS` | the VictoriaLogs release the logs binary embeds | free to track the newest VictoriaLogs release |
| `VT_VERSION` | the VictoriaTraces release the traces binary embeds | the VictoriaTraces release being moved to |
| `VL_COMMIT_TRACES` | the VictoriaLogs commit the traces binary embeds | **derived, never chosen** |

`VL_COMMIT_TRACES` is the VictoriaLogs commit that VictoriaTraces' own `go.mod` requires at
`VT_VERSION`. The traces binary links VictoriaTraces against exactly the VictoriaLogs it was
built and tested with:

```
$ git -C lakehouse-traces/deps/VictoriaTraces show v0.11.0:go.mod | grep VictoriaLogs
	github.com/VictoriaMetrics/VictoriaLogs v1.121.1-0.20260617051904-6ae2da3c11f3 // v1.51.0
```

The last component of that pseudo-version (`6ae2da3c11f3`) is the pin. It usually lags
`VL_VERSION_LOGS`, and must not be lifted to it because a newer commit happens to compile —
the two-pin model is deliberate. `TestVLCommitTracesPinIsDerivedFromVT` holds the Makefile
to the rule, and `TestVLSurface_LogsPinSupersetOfTracesPin` requires the logs pin to expose
everything the traces pin does, so the extracted upstream inventory never under-reports
the traces surface.

### The five places a pin lives

A pin is written in five places, and each one outside the Makefile is held to it by a test.
A stale copy is not harmless: a Dockerfile cloning an older tree fails the image build with
a "patch failed" hunk error that reads like a broken patch, and a stale Compose tag
silently benchmarks and parity-tests the previous release.

| Place | What it holds | Kept equal by |
| --- | --- | --- |
| `Makefile` | the three pins | source of truth |
| `go.mod`, `lakehouse-traces/go.mod` | the requirements — root: `VictoriaLogs <VL_VERSION_LOGS>`; traces: VictoriaTraces' own VictoriaLogs requirement verbatim, and `VictoriaTraces <VT_VERSION>` — plus the versions each binary reports on `/lakehouse/info` (`vlCompat` in `cmd/lakehouse-logs/main.go`, `vtCompat` in `lakehouse-traces/main.go`) | `TestVLCompatMatchesGoMod`, `TestVTCompatMatchesGoMod` |
| `Dockerfile.logs`, `Dockerfile.traces`, `Dockerfile.datagen` | `ARG VL_VERSION`, `ARG VL_COMMIT`, `ARG VT_VERSION` defaults | `TestDockerfilePinsMatchMakefile`, `TestDockerfilePatchRefsExist` |
| Compose files: `deployment/docker/docker-compose-e2e.yml`, `docker-compose-benchmark.yml`, `docker-compose-benchmark.gp3.yml`, `tests/parity/docker-compose.yml` | the hot-tier images `victoriametrics/victoria-logs:<tag>` and `victoriametrics/victoria-traces:<tag>` | `TestComposeImagePinsMatchMakefile`, `TestComposeHealthchecksDoNotAssumeAShell` |
| Workflow `env:` blocks under `.github/workflows/` | the three pins, feeding deps cache keys and release image `--build-arg`s | `TestWorkflowPinsMatchMakefile` |

The Dockerfile, Compose and workflow gates live in `tests/conformance/pins_test.go`; the two
reported-version gates sit next to each binary. The same file pins the base image of
`deployment/docker/Dockerfile.upstream-probe` — the static busybox the Compose healthchecks
run inside the distroless upstream images — by tag **and** digest
(`TestUpstreamProbeBaseImagePinnedByDigest`). To move that image, take the index digest from
`docker buildx imagetools inspect busybox:<tag>` and change the Dockerfile and the test
constant in one commit.

There is no other version manifest. The daily check used to compare releases against a
`.upstream-versions.json` that nothing else read and that had drifted to values matching no
tag; it is gone, and the probe below reads the Makefile.

## A bump, in order

Every step names the check that proves it done. Run go commands with `GOWORK=off` — the two
modules embed different VictoriaLogs trees, and `go.work` exists for editors only.

### 1. Pins

Edit the three Makefile variables, deriving `VL_COMMIT_TRACES` as above, then the other four
places. The pin gates list anything missed:

```
$ GOWORK=off go test ./tests/conformance/ -run 'Dockerfile|Compose|Workflow' -count=1
```

### 2. Fresh upstream trees

```
$ rm -rf deps lakehouse-traces/deps
$ make deps-logs deps-traces deps-vt
```

Each deps target clones the pinned tree, copies in the `*.src` overlay files and runs
`git apply` for every patch; the first patch that no longer applies stops `make`.

### 3. Patches

Regenerate against the new tree rather than re-implementing anything locally — the recipe
(stop at the failing `git apply`, hand-apply with fuzz, `git diff` back into the patch,
re-clone, apply cleanly) is the "Workflow" section of `patches/README.md`.

Two things a clean `git apply` does not tell you:

- **The `*.src` files are not diffs.** They are whole files added to the upstream tree, and
  they mirror upstream code rather than patch it: `external.go` declares the storage interface
  the dispatch patch routes every `app/vlstorage/main.go` (or `app/vtstorage/main.go`) storage
  call to, and `external_query.go` repeats what the native query path in
  `lib/logstorage/storage_search.go` does before running pipes (subquery preprocessing for
  `join`, for instance). They keep compiling when that upstream code changes — which is exactly
  how behaviour drifts silently. Diff the mirrored files across the bump and port every
  relevant change; a new storage function in `main.go` needs a method on `ExternalStorage` and
  a dispatch hunk:

  ```
  $ git -C deps/VictoriaLogs fetch --depth 1 origin tag <old> tag <new>
  $ git -C deps/VictoriaLogs diff <old> <new> -- app/vlstorage/main.go lib/logstorage/storage_search.go
  ```

- **`patches/vl-logs/` and `patches/vl-traces/` must stay byte-equal**, file for file, except
  where the two VictoriaLogs pins sit on either side of an upstream change to a context line.
  `scripts/ci/check_patches_equal.sh` enforces it; a legitimate difference is declared, with
  the upstream change that caused it, in `patches/vl-traces/DIVERGENCE.md`. A declaration
  only ever excuses context — the added and removed lines must match — and a row for a pair
  that is equal again fails the guard, so the list empties itself once the pins converge.

### 4. Go modules

```
$ GOWORK=off go mod edit -require=github.com/VictoriaMetrics/VictoriaLogs@<VL_VERSION_LOGS>
$ (cd lakehouse-traces && GOWORK=off go mod edit \
    -require=github.com/VictoriaMetrics/VictoriaLogs@<VictoriaTraces' own requirement, verbatim> \
    -require=github.com/VictoriaMetrics/VictoriaTraces@<VT_VERSION>)
$ GOWORK=off go mod tidy && (cd lakehouse-traces && GOWORK=off go mod tidy)
```

Both modules `replace` the upstream modules with the `deps/` checkouts, so the requirement
lines do not select the code — but tidy resolves the new trees' own requirements, and those
move shared libraries (the VictoriaMetrics library carries security fixes), raise the `go`
directive or add a toolchain line. Review every line tidy changes. Then move `vlCompat` and
`vtCompat` to the new releases.

### 5. Build

```
$ GOWORK=off go build ./... && GOWORK=off go vet ./...
$ (cd lakehouse-traces && GOWORK=off go build ./... && GOWORK=off go vet ./...)
$ make test
```

### 6. Embedded web UI

vmui is VictoriaLogs' own UI, embedded with the Lakehouse tab injected. Re-sync it from the
tree each binary embeds:

```
$ make sync-vmui sync-vmui-traces
```

The `internal/ui` drift tests require the embedded `index.html` to equal the vendored one and
every asset it references to exist.

### 7. Protocol inventory

`make conformance-gen` regenerates `tests/conformance/inventory.generated.yaml`, including the
internal peer-protocol versions (`/internal/select/*`, `/internal/delete/*`) extracted per
module. If either moved, peers on different versions reject each other's requests, and a
multi-node deployment has to upgrade all nodes together — say so in the changelog.

### 8. Registry rows

```
$ make conformance-check
```

The drift gate must report **0 unmapped, 0 stale, 0 pending-bump**: every upstream route,
pipe, filter, stats function and TraceQL function in the new inventory has a row in
`tests/conformance/registry/rows/`, rows for items upstream removed are deleted or marked
`expect: absent`, and rows carrying `since:` for a feature introduced in the bumped range are
now live. Flag warnings are soft; the triage rule for which flags earn a row is in
`tests/conformance/README.md`.

### 9. Query-grammar sweep

Part of `make conformance-check`: `TestRepoLogsQLLiteralsParse` parses every LogsQL literal
in the repository (sources, tests, scripts, docs, dashboards, alerts, workflows) with the new
parser, `TestRows_QueriesParseWithUpstream` does the same for registry rows, and
`TestLogsQLPipeGrammarContract` pins which forms the parser accepts after a pipe. A grammar
change upstream fails here, not in a user's dashboard.

### 10. Verification round

Against the new hot-tier images and the new cold binaries:

```
$ docker compose -f tests/parity/docker-compose.yml build
$ docker compose -f tests/parity/docker-compose.yml up -d
$ docker compose -f tests/parity/docker-compose.yml --profile test run --rm parity-tests
$ make e2e
```

Every parity test must run — a skip is not a pass — and the outcomes are diffed row by row
against the same suites on the current release. Any pass that becomes a failure is either an
upstream change (its row gains `since:` and the changelog names it) or a Lakehouse regression
fixed in the same pull request.

### 11. Benchmark

```
$ scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" --iterations 20 --warmup 3
```

Compare every cell with the recorded baseline in [the full-scope S3 benchmark](./benchmarks/full-scope-s3.md).
Every timed response is validated; a cell that did not return the expected rows does not
count as a latency.

## The daily sync probe

`.github/workflows/upstream-check.yaml` runs `scripts/ci/upstream_sync_probe.sh` every day at
08:00 UTC and on demand. On a throwaway clone it performs steps 1, 2, 4 and 5, checks every
patch of step 3 (it does not regenerate any), runs the automated gates of steps 7–9, and
reports what the bump would cost before anyone starts it.

### What it does

1. Reads the current pins from the Makefile.
2. Resolves the candidate releases: the `vl_version` / `vt_version` inputs of a manual run,
   otherwise the newest non-prerelease GitHub release of each project. When neither is newer
   it reports "up to date" and stops. An explicitly requested version is always probed, older
   or not — that is how to price a downgrade.
3. Clones the repository, checks out `upstream-sync/vl-<version>-vt-<version>`, and rewrites
   the Makefile, Dockerfile, Compose and workflow pins — only the version token, keeping
   quoting and trailing comments, and quoting a commit that a YAML parser would otherwise read
   as a number.
4. Runs `deps-vt`, derives `VL_COMMIT_TRACES` from the candidate VictoriaTraces `go.mod`, then
   `deps-logs` and `deps-traces`. The deps recipes are read from `make -n`, so the probe cannot
   drift from the Makefile, and every `git apply` is tried with `git apply --check --verbose`
   first — a failing patch is recorded together with the context it searched for.
5. Moves the `go.mod` requirements and `vlCompat` / `vtCompat`, runs `go mod tidy`, then builds
   and vets both modules, and lists every other `go.mod` line tidy changed.
6. Regenerates the inventory, lists upstream items added and removed and any protocol version
   that moved, and runs the conformance package (drift, pins, grammar) and the two
   reported-version gates. A gate whose name matches no test fails rather than passing
   silently.
7. Commits the result on the branch as `github-actions[bot]`.

A step whose prerequisite failed is reported as not run, never as passed.

| Exit | Verdict |
| --- | --- |
| 0 | a clean bump is possible, or already up to date |
| 10 | a patch no longer applies — regenerate it |
| 20 | a module no longer tidies, builds or vets |
| 30 | a conformance or reported-version gate fails — registry rows or a pin site need a human |
| 1 | the probe could not run (clone, network, a pin it could not derive) |

The result matrix — pins, a ✅/❌ per patch with the failing hunk, tidy/build/vet per module
with the first error, the surface diff, the gate verdicts and drift summary, the upstream
compare links, and a checklist of the steps above that remain — is the run's step summary,
its `upstream-sync-matrix` artifact, and the body of the sync pull request.

To run it locally (needs git, make, Go and network; your working tree is never touched):

```
$ scripts/ci/upstream_sync_probe.sh --vl v1.53.0 --vt v0.12.0 \
    --workdir /tmp/upstream-sync --keep --out /tmp/upstream-sync/matrix.md
```

### The pull request

`scripts/ci/upstream_sync_publish.sh` turns the result into **one pull request per version
pair**, on the probe's branch, titled `Upstream sync: VictoriaLogs <version>, VictoriaTraces
<version>` and labelled `dependencies` and `upstream-sync`. On later runs:

- an open pull request for the pair is edited, never duplicated;
- a result identical to the one already pushed (same files, same base) is not pushed again,
  so the pull request's CI does not re-run every morning for nothing;
- a new result replaces the previous probe commit, with a lease on exactly the commit it
  observed;
- **once anyone else commits on the branch** — regenerating patches, adding rows — the probe
  never rewrites it again. It only refreshes the pull request body with the new matrix and a
  note that the matrix describes a fresh probe, not the branch.

Findings — patches to regenerate, a build break, missing rows — do not fail the run; they are
what the pull request is for. The run fails when the probe itself could not run, or when a
pull request is due and the token below is missing. The `dry_run` input probes and reports
without touching any branch or pull request.

GitHub disables scheduled workflows in a repository with no activity for 60 days. If the
daily run stops, re-enable it from the workflow's page in the Actions tab.

### Granting the token

The probe runs with a read-only workflow token (`permissions: contents: read`, no persisted
checkout credentials). Pushing the branch and opening the pull request use the repository
secret **`UPSTREAM_SYNC_TOKEN`** instead, for two reasons:

- the repository keeps **"Allow GitHub Actions to create and approve pull requests" off**, so
  `GITHUB_TOKEN` cannot open one — in a public repository every workflow run carries that
  token, and it should not be able to;
- a pull request opened with `GITHUB_TOKEN` does not trigger `pull_request` workflows, so the
  sync pull request's own CI would never run.

**Fine-grained personal access token** (the simplest option):

1. GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens →
   Generate new token.
2. Resource owner: the organization. Repository access: **Only select repositories** → this
   repository.
3. Repository permissions: **Contents: Read and write** and **Pull requests: Read and write**
   (Metadata: Read-only is added automatically). Nothing else.
4. Choose an expiration the organization's token policy allows, and note when it runs out: an
   expired token turns the daily run red at the publish step.
5. Repository → Settings → Secrets and variables → Actions → New repository secret, named
   `UPSTREAM_SYNC_TOKEN`.

**GitHub App** (no personal token to rotate): create an app with the same two repository
permissions and install it on this repository only. Installation tokens expire after an hour,
so they cannot be stored as a secret; store the app ID and private key as secrets instead,
mint the token in the job with `actions/create-github-app-token`, and pass its output to the
publish step's `GH_TOKEN` in place of `secrets.UPSTREAM_SYNC_TOKEN`.

Either way, `Contents: write` can push to any branch that is not protected: keep the default
branch protected, so the token's reach is the `upstream-sync/*` branches it is meant for.

The publish step applies the `dependencies` and `upstream-sync` labels but never creates
them, so create `upstream-sync` once with `gh label create upstream-sync`. A missing label is
noted in the run summary and never blocks the pull request.
