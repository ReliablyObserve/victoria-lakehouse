# PR proof comments: the artifact contract and the trust model

The PR proof workflows (`pr-quality`, `pr-proof`) run pull request code and produce Markdown reports. One workflow,
`.github/workflows/pr-proof-comment.yaml`, posts them as four sticky pull request comments. It runs **base-branch code
only**, on `workflow_run`, so it also works for fork and Dependabot pull requests, which a job running on the
`pull_request` event cannot comment on. Everything the producers hand over is **untrusted data**.

| marker | kind | posted from the artifact of the workflow |
|---|---|---|
| `<!-- lh-pr-quality -->` | `pr-quality` | `PR Quality` (`.github/workflows/pr-quality.yaml`) |
| `<!-- lh-perf-smoke -->` | `perf-smoke` | `PR proof` (`.github/workflows/pr-proof.yaml`) |
| `<!-- lh-api-matrix -->` | `api-matrix` | `PR proof` |
| `<!-- lh-visual-proof -->` | `visual-proof` | `PR proof` |

The first three markers are the constants of `scripts/bench/ab` (a unit test compares them). The code is
`scripts/ci/proof_comment.py`; its tests are `scripts/ci/tests/test_proof_comment.py`.

## What a producer must do

1. Be a workflow with the exact `name:` `PR Quality` or `PR proof` (that is what `workflow_run.workflows` matches, not the
   file name), in `.github/workflows/pr-quality.yaml` or `.github/workflows/pr-proof.yaml`, triggered by `pull_request`.
   The commenter also checks the path of the run in the API. A workflow with another name or path is ignored. The names,
   the two paths (`PRODUCERS`) and the artifact name are the only places to change when a producer is renamed; a test pins
   the names in the workflow.
2. Upload **one** artifact named `lh-proof-comment` (`actions/upload-artifact`) even when the job failed
   (`if: always()`), with this layout and nothing else:

```
manifest.json
pr-quality.md | perf-smoke.md | api-matrix.md | visual-proof.md      one per comment, named <kind>.md
images/<name>.png                                                    only the PNG files a body shows
```

`PR Quality` may post only the kind `pr-quality`. `PR proof` may post `perf-smoke`, `api-matrix` and `visual-proof`.
Only a manifest that posts `visual-proof` may carry images, and only the `visual-proof` body may show them: publishing
replaces the pull request's whole `pr-<N>/` image folder, so two producers must never publish for the same head.

**Emit every kind you own, on every run.** A comment kind that a later head does not emit would keep the previous head's verdict
on screen. When a run uploads no artifact, or one that fails validation, the commenter replaces the producer's older comments
with a "no valid proof for this head" banner (see "A head without valid proof"). When a job fails after producing partial
results, upload the artifact anyway (`if: always()`) with the verdict `incomplete` or `skipped` and a short body, which says
more than the banner does.

### manifest.json

The schema is `scripts/ci/proof-comment-manifest.schema.json` (JSON Schema 2020-12). The enforcement is
`proof_comment.parse_manifest`, and a unit test keeps the two in step. Unknown keys are refused.

```json
{
  "schema_version": 1,
  "pr_number": 123,
  "head_sha": "<40 lower-case hex: github.event.pull_request.head.sha>",
  "comments": [{"kind": "perf-smoke", "verdict": "pass"}],
  "images": [{"name": "logs-search-1h.png", "sha256": "<hex>", "bytes": 48211}],
  "allowed_issue_refs": [500]
}
```

* `verdict` is one of `pass`, `fail`, `incomplete`, `skipped`. It is informational (the log line); the body carries the
  verdict-first text.
* `pr_number` and `head_sha` are claims: they must equal what the GitHub API says (see the cross-checks).
* `images[].name` matches `[a-z0-9][a-z0-9_.-]{0,60}.png`; at most 80 images, 300 000 bytes each, 8 MiB for everything.
  Only plain PNG is accepted (see "Images"); it is what the montage emits.
* `allowed_issue_refs`: up to 20 issue or PR numbers a body may write as `#N` and have auto-linked (the known-difference
  and gap links). Any other `#N` is neutralised.

### Body Markdown

A body is Markdown, at most 256 KiB. It is sanitised, so write only:

* text, tables, lists, headings, code spans and fences, block quotes;
* `<details>`, `<summary>`, `<sub>` and their closing tags, with no attributes;
* in the `visual-proof` body, an image as `![alt](image:<name>.png)` where `<name>.png` is in the manifest
  (`scripts/proof/comment.py --image-base image:` writes `image:/<name>.png`, which is accepted). The commenter replaces it
  with the `pr-visuals` URL of this pull request and head;
* links as `[text](url)` or bare URLs **to this repository only**: `actions/runs/<id>`, `issues/<n>`, `pull/<n>`,
  `commit/<sha>` and `blob/<40-hex-sha>/<path>` (no `.` or `..` segment);
* `#N` for the numbers in `allowed_issue_refs`.

Everything else is neutralised, not rejected: `<` and `[` `]` become entities (no HTML, no other links, no other
images, no reference-style links), `@name` and `@org/team` get a zero-width space (nobody is notified), other `#N`,
`GH-N` and `owner/repo#N` get one (no cross-reference), other URLs and `www.` lose their autolink. A body cannot carry or
spoof a `<!--` marker: the commenter adds the marker of the kind itself, and drops the body's own first line when it is
exactly that marker (the existing renderers start with it). The bodies that `scripts/bench/ab/comment.py` renders pass
through unchanged apart from that line (`RealRenderers` in the tests). Sanitising a second time changes nothing. The
final comment is at most 65 000 characters; a longer body is cut at a line boundary, open code fences and `<details>`
are closed, and a note says it was cut.

### Images

The files are hosted on `raw.githubusercontent.com`, so an image must carry nothing but pixels. The commenter walks every
chunk and checks its CRC. Allowed: `IHDR`, `PLTE`, `IDAT`, `IEND` and the fixed-size `pHYs` (9 bytes, unit 0 or 1), `sRGB`
(1 byte, 0..3), `gAMA` (4) and `cHRM` (32), each at most once, before the first `IDAT`, `IDAT` chunks consecutive.
`PLTE` only on palette images (the specification also allows a suggested palette on truecolour; up to 768 bytes nothing
reads is refused here). `tRNS` is exact: 2 bytes on grey, 6 on truecolour, and on palette images after `PLTE` with at most
one byte per palette entry; never on images that have an alpha channel. Refused: every other chunk (`tEXt`, `iTXt`,
`zTXt`, `eXIf`, `iCCP`, `tIME` and private chunks can hold arbitrary data), interlacing, any byte after `IEND`, and any
`IDAT` stream that does not inflate to exactly `height * (1 + ceil(width * bits per pixel / 8))` bytes (so nothing hides
behind the zlib stream), with every row's filter byte in 0..4. Width and height are at most 8192 and the picture at most
16 million pixels. Re-encode with PIL (the montage tool does) if a capture tool writes a text or profile chunk. JPEG is not
accepted.

## What the commenter checks

`validate` (token: `actions: read`, `contents: read`, `pull-requests: read`) does, in order, and fails closed:

1. Format-checks every value of the event (`run id`, `head sha`, `head repository`, `path`, listed PR numbers).
2. Skips (success, nothing posted) when the run is not a `pull_request` run, was cancelled or skipped, or is not one of
   the two producer paths (any other workflow that happens to carry a listed name is dropped here).
3. Reads the run from the API and requires the same event, head SHA, repository, head repository, status `completed`
   and path as the event says. A pull request can edit its own copy of a producer workflow, so the **path** decides which
   kinds the artifact may post.
4. Finds the open pull requests of this repository whose head is exactly that commit **and** that head repository
   (`commits/{sha}/pulls`, then the newest open pull requests as a fallback). It does not use `workflow_run.pull_requests`
   to find the PR (it is empty for forks). No such PR (closed, or the head moved on) is a quiet skip. Two pull requests can
   share a head; both are "the PR" for the notices below, and the manifest picks one for a normal report.
5. Lists the artifacts of that run (a re-run lists those of every attempt): none named `lh-proof-comment` means **no
   proof**; otherwise the artifact with the highest id (the last attempt's) is used, and it must belong to that run and head
   and be at most 20 MiB or it is rejected. The event's `run_attempt` is recorded in the bundle.
6. Downloads the zip with `gh api` (which drops the token on a cross-host redirect) and reads it **in memory, never
   extracting**: at most 120 entries, only the allowlisted names above, no directories except `images/`, no symlink or
   special file, no absolute path, `..`, backslash or duplicate, no encrypted entry, per-entry and total caps enforced on
   the bytes actually read (a lying size header does not help), a corrupt entry is a clean rejection. Anything else
   rejects the whole artifact.
7. Parses the manifest strictly (no duplicate keys, no `NaN`, exact keys and types) and requires its `head_sha` to equal
   the event's and its `pr_number` to be one of the PRs of step 4 (and listed by the event when the event lists any). The
   `.md` files and the images must be exactly those the manifest lists, each image a strict PNG whose size and SHA-256
   match.
8. Writes the **bundle**: the sanitised bodies, `meta.json`, and only the images a body shows, renamed
   `<head sha 12>-<name>.png`. It is uploaded for the two jobs below.

### A head without valid proof

Steps 5 to 7 end in one of two states. "No proof" (step 5) and "rejected" (steps 5 to 7) both mean the **current head has no
valid report**. The older report of the same producer must not stay on screen as if it were this head's, and a pull request
could cause that on purpose by skipping the upload. So the bundle then holds only API facts (the PRs of step 4, the kinds of
that producer path, the run id and conclusion, a fixed reason code), and the `comment` job **edits the producer's existing
comments** into `No valid proof for head <sha>`, with the reason, a link to the producing run and to the commenter's own log.
It creates nothing when no comment exists, touches only the producer's kinds, and obeys the same head and run-id checks as
a normal report. Nothing taken from the artifact reaches such a banner. A cancelled run, a closed PR and a moved head are
still plain skips.

`publish` (token: `contents: write`, `pull-requests: read`) re-checks the bundle (file set, regular files, PNG bytes) and
that the pull request is still open at that head, then runs `scripts/proof/visual/publish.py` from the default branch:
one parentless commit on the data-only branch `pr-visuals`, folder `pr-<N>/`, pushed with a lease. It writes nothing else.

`comment` (token: `contents: read`, `pull-requests: write`) re-checks the bundle, resolves the image placeholders
(or drops them when `publish` did not succeed), truncates, and upserts each comment:

* find comments by **marker at the start of the body and author `github-actions[bot]` of type Bot**; edit the oldest,
  delete the other duplicates; create one if none (a human comment that quotes a marker is never touched);
* **superseded head**: right before every write the PR is read again; if it is closed or its head is no longer the
  manifest's, nothing is written. A comment carries a hidden `lh-proof-run: <run id> <sha>`; an older run never overwrites
  the comment of a newer run;
* a deleted comment between find and edit falls back to creating one.

Closed pull request, moved head, cancelled run: nothing is posted and the job succeeds with a notice. A mismatch between the
event and the GitHub API (wrong run, repository, head repository, status or path) fails the job and posts nothing. Only the 403 answer
"Resource not accessible by integration" (a read-only token, as for Dependabot) is a warning that leaves the job green with
nothing posted; any other 403 (rate limit, abuse detection) fails the job, so a banner is never silently dropped. A run id
in an existing comment's hidden metadata that is above the running commenter's own run id cannot be real and is ignored (a
same-repository workflow can write any hidden text as the bot), so a forged id cannot freeze a comment.

Every comment ends with a footer that names the head, the producer's verdict and a link to the producing run with the
conclusion the API reports; a producer verdict `pass` on a run that concluded anything but `success` or `neutral` is flagged.
An unclosed `<details>`, `<summary>`, `<sub>` or code fence is closed before the footer, so a body cannot hide it. Links to
this repository's `issues/N` and `pull/N` count as `#N` references: only the numbers in `allowed_issue_refs` are kept.

## Permissions and untrusted inputs

| job | token scopes | why | untrusted input it handles |
|---|---|---|---|
| (workflow) | `permissions: {}` | nothing by default | - |
| `validate` | `actions: read`, `contents: read`, `pull-requests: read` | list/download the artifact, check out this one script, read the PR list | the artifact zip (parsed in memory by Python); `head_repository.full_name` and the run path (format-checked, compared to the API); the fork's head SHA |
| `publish` | `contents: write`, `pull-requests: read` | push to `pr-visuals` | the bundle (re-validated: PNG bytes, names) |
| `prune` (`pr-visuals-prune.yaml`, schedule and manual only) | `contents: write`, `pull-requests: read` | drop the folders of PRs that are not open | none (the list of open PRs from the API) |
| `comment` | `contents: read`, `pull-requests: write` | read the script, write PR comments (`issues: write` is not needed for PR comments) | the bundle (re-validated, sanitised Markdown) |
| `dry-run` (manual) | `contents: read` | render a committed fixture | `inputs.fixture`, `inputs.pr_number` (the path must stay under `scripts/ci/tests/testdata/proof_comment/`) |

Rules, enforced by `test_proof_comment.py` and `test_workflow_permissions.py`: no checkout of pull request code (the
checkouts are the default branch, sparse, without credentials); no `${{ }}` expression inside a `run:` script (event data,
outputs and inputs go through `env:`); Python always runs isolated (`-I`); every action is pinned by commit SHA; the exact
permissions above are the only ones allowed for this workflow, and a new scope, job or `ref` on a checkout fails the test.

### The `pr-visuals` branch

`pr-visuals` is a data-only orphan branch: PNG files only, never merged. Its commits are made by `github-actions[bot]` and
are **unsigned** (CI cannot sign; the alternative is a signing key in CI), an exception to the repository's signed-commit
rule for a branch that carries no code. Every change is one parentless commit, so removed images become unreachable at once.
Per pull request the folder is replaced with each new head (at most 80 files of 300 KB), and the files are named with the head
SHA, so a comment never shows another head's screenshot. Folders of pull requests that are no longer open are removed by
`.github/workflows/pr-visuals-prune.yaml` (weekly and on demand; it runs `publish.py prune` with the list of open pull
requests, never on a pull request event). The listing is fully paginated, a failed call or a non-numeric entry stops the job, and
`publish.py prune` removes only directories named `pr-<digits>` that are not in the list. Neither `publish` nor `prune` uses a
concurrency group: GitHub keeps one running and one pending job per group and cancels the pending one when another is queued
(`queue: max` is not part of the workflow syntax), which would drop publishes. `publish.py` rebuilds on the new tip and pushes
with a lease (4 attempts), which keeps concurrent publishes and the prune correct. Nothing may require the branch to be
fast-forward only: `publish.py` pushes a parentless commit with a lease.

**Image hosting risk.** A fork pull request can cause up to 80 files of 300 KB to be hosted on `raw.githubusercontent.com`
under this repository's name. The strict PNG check (above) is what limits this to pictures: no text, profile or private
chunk, no trailing data, no stream that is not exactly the pixels. Compensating controls: the branch is data-only, commits
are parentless and never merged, `main` still needs a pull request and signatures, and the weekly prune bounds it.

## Dry run

```bash
GITHUB_REPOSITORY=ReliablyObserve/victoria-lakehouse \
  python3 -I scripts/ci/proof_comment.py dry-run --source scripts/ci/tests/testdata/proof_comment/valid --pr 12345
```

It reads a fixture directory or zip (only from `scripts/ci/tests/testdata/proof_comment/`), applies the same validation and
sanitising, prints the comments it would post and makes no API call. The same runs from the Actions tab as
`PR proof comment` > `Run workflow` (job `dry-run`). A workflow dispatch needs the workflow file on the default branch, so
until the file is merged the unit tests are the evidence.

## Limits and open points

* `workflow_run` only starts workflows that exist on the default branch. The first real run is after merge.
* **Obligation of the producer pull requests.** When `pr-proof.yaml` lands (the perf-smoke, api-matrix and visual-proof
  producers), the same pull request must carry a contract test that builds the artifact from the real renderers and passes
  it through `scripts/ci/proof_comment.py` (manifest, bodies, images, `allowed_issue_refs`), so a change in a renderer
  cannot silently turn the comments into "no valid proof" banners. The same holds for `PR Quality`.
* The `dangerous-triggers` audit of the pinned `zizmor` is silenced by an inline comment on the `workflow_run:` line, with the
  reason, not by a line number in `.github/zizmor.yml`.
* `workflow_run.workflows` matches the producer's `name:`. A test fails when a producer file that exists is not named `PR Quality`
  or `PR proof`, so a rename cannot silently disable the comments. Until the producers are merged under those names the
  commenter is inert for them.
* Fenced code shows `&lt;` and `&#91;` literally: the sanitiser escapes everywhere, because deciding what is code the way
  GitHub does (indented fences, fences in lists or quotes) is where a mistake would let HTML through.
* Dependabot: a workflow that Dependabot triggers gets a read-only `GITHUB_TOKEN` unless the workflow sets `permissions:`
  (documented by GitHub for `pull_request`); this workflow sets them explicitly. Whether the `workflow_run` that follows a
  Dependabot pull request keeps those write scopes is verified on the first such pull request.
* JPEG is not accepted. The `publish` step is the existing `scripts/proof/visual/publish.py`, which is PNG only.
* Whether a ruleset blocks `github-actions[bot]` pushing `pr-visuals` is verified on the first publish.
