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

**Always emit every kind you own.** A comment kind that a later head does not emit keeps the previous head's verdict on
screen, and so does a run that dies before the upload. Emit each kind on every run, with the verdict `skipped` or
`incomplete` and a short "did not complete" body when the job failed (the harness already renders such a body), so a stale
verdict never stays. The commenter itself posts nothing when a run has no artifact.

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
  Only PNG is accepted: it is what the Playwright capture and the montage emit.
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

## What the commenter checks

`validate` (token: `actions: read`, `contents: read`, `pull-requests: read`) does, in order, and fails closed:

1. Format-checks every value of the event (`run id`, `head sha`, `head repository`, `path`, listed PR numbers).
2. Skips (success, nothing posted) when the run is not a `pull_request` run, was cancelled or skipped, or is not one of
   the two producer paths (any other workflow that happens to carry a listed name is dropped here).
3. Reads the run from the API and requires the same event, head SHA, repository, head repository, status `completed`
   and path as the event says. A pull request can edit its own copy of a producer workflow, so the **path** decides which
   kinds the artifact may post.
4. Lists the artifacts of that run: none named `lh-proof-comment` is a quiet skip; two is a rejection; the artifact must
   belong to that run and head, and be at most 20 MiB.
5. Downloads the zip with `gh api` (which drops the token on a cross-host redirect) and reads it **in memory, never
   extracting**: at most 120 entries, only the allowlisted names above, no directories except `images/`, no symlink or
   special file, no absolute path, `..`, backslash or duplicate, no encrypted entry, per-entry and total caps enforced on
   the bytes actually read (a lying size header does not help). Anything else rejects the whole artifact.
6. Parses the manifest strictly (no duplicate keys, no `NaN`, exact keys and types) and requires its `head_sha` to equal
   the event's. The `.md` files and the images must be exactly those the manifest lists, each image must be a PNG
   (signature, `IHDR` with sane dimensions, `IEND`) whose size and SHA-256 match.
7. Finds the open pull requests of this repository whose head is exactly that commit **and** that head repository
   (`commits/{sha}/pulls`, then the newest open pull requests as a fallback). It does not use `workflow_run.pull_requests`
   to find the PR (it is empty for forks); the manifest's `pr_number` must be one of the matches, and be listed by the
   event when the event lists any. Two pull requests can share a head; the manifest picks one of those.
8. Writes the **bundle**: the sanitised bodies, `meta.json`, and only the images a body shows, renamed
   `<head sha 12>-<name>.png`. It is uploaded for the two jobs below.

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

Missing artifact, closed pull request, moved head, cancelled run: nothing is posted and the job succeeds with a notice.
A contract violation (bad zip, wrong SHA, wrong PR number, forbidden kind, bad image) fails the job and posts nothing.

## Permissions and untrusted inputs

| job | token scopes | why | untrusted input it handles |
|---|---|---|---|
| (workflow) | `permissions: {}` | nothing by default | - |
| `validate` | `actions: read`, `contents: read`, `pull-requests: read` | list/download the artifact, check out this one script, read the PR list | the artifact zip (parsed in memory by Python); `head_repository.full_name` and the run path (format-checked, compared to the API); the fork's head SHA |
| `publish` | `contents: write`, `pull-requests: read` | push to `pr-visuals` | the bundle (re-validated: PNG bytes, names) |
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
SHA, so a comment never shows another head's screenshot. Folders of closed pull requests are removed by the clean-up
workflow (`publish.py remove|prune`), which is a separate change.

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
* Dependabot: a workflow that Dependabot triggers gets a read-only `GITHUB_TOKEN` unless the workflow sets `permissions:`
  (documented by GitHub for `pull_request`); this workflow sets them explicitly. Whether the `workflow_run` that follows a
  Dependabot pull request keeps those write scopes is verified on the first such pull request.
* JPEG is not accepted. The `publish` step is the existing `scripts/proof/visual/publish.py`, which is PNG only.
