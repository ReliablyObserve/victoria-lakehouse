# Release notes

Every GitHub release of Victoria Lakehouse starts with notes in the style of the
[VictoriaLogs releases](https://github.com/VictoriaMetrics/VictoriaLogs/releases), written from
`CHANGELOG.md`, followed by the artifact, Docker image and Helm chart instructions:

```text
Released at 2026-10-22

**Update note 1:** ...

* SECURITY: ...

* FEATURE: ...

* BUGFIX: ...

## Release Artifacts
...
```

## How they are produced

The build job of the release workflow (`.github/workflows/auto-release.yaml`) runs
`scripts/ci/changelog_fragments.py assemble` and then `scripts/ci/release_notes.py --version X.Y.Z` on the
commit it releases, and puts the output in front of the artifacts section. The script uses the standard
library only, reads no network and renders the same text for the same input. The source is the
`changelog.d/` fragments present at the release commit (see `changelog.d/README.md` in the repository):
`assemble` builds the `## [X.Y.Z]` section of the workspace's `CHANGELOG.md` from them, and the notes are
rendered from that section. The section reaches `main` only through the release-metadata pull request that
follows the release, which also deletes the fragments. When a release is cut before the previous
release-metadata pull request has merged, the tree still holds the previous release's fragments; `assemble`
leaves out every fragment file (same path and same bytes) that the previous release tag already had
(`--after-tag`, the newest plain `vX.Y.Z` tag reachable from the parent commit, from `changelog_fragments.py previous-tag`; any other tag is ignored), so a fragment is released once; a later fragment that
reuses the name of a released one has other bytes and is released. A fragment merged after the release commit is not in the
checkout and lands in the next release. If the tag cannot be read, all fragments are released. A fragment the previous release
took whose release section is already in `CHANGELOG.md` (the file was left behind) stops the release instead of
being skipped for ever, and so does any text under `[Unreleased]`, a fragment that repeats the lead-in of a
released section, and a section larger than 100000 bytes (a GitHub release body holds about 125000 characters).

To render the notes of any released version, or to preview the entries of a pull request:

```bash
python3 scripts/ci/release_notes.py --version 0.157.6
python3 scripts/ci/changelog_fragments.py check                              # validate the changelog.d fragments
```

Warnings (an unknown subsection) go to stderr; the notes go to stdout.

## How the CHANGELOG maps to the notes

| CHANGELOG subsection | Release notes |
|----------------------|---------------|
| `### Security` | `* SECURITY: ...` |
| `### Added`, `### Changed`, `### Performance` | `* FEATURE: ...` |
| `### Deprecated` | `* FEATURE: deprecated: ...` |
| `### Removed` | `* FEATURE: removed: ...` |
| `### Fixed` | `* BUGFIX: ...` |
| `### Update notes

Behaviour that an operator has to act on when upgrading (a flag, an endpoint or a default that
changes, a format that is no longer read) goes INSIDE the bullet of the change, as an indented second
paragraph that opens with a bold `**Update note.**`, with the issue or pull request number. A
`### Update notes` subsection is not written by hand any more: `CHANGELOG.md` is only written by the
release from the fragments, and the gate refuses a hand-filed subsection. A separate fragment kind for
update notes was deferred.

```markdown
- **The `/delete/run_task` endpoint accepts only `POST` (#1234).** It answered `GET` before.

  **Update note.** Change scripts that call it with `GET` to use `POST`.
```

The notes show the paragraph under its bullet. (A `### Update notes` subsection that an older
`CHANGELOG.md` section already has still renders as `**Update note:** ...` paragraphs before the
bullets, one `**Update note 1:** ...`, `**Update note 2:** ...` each when there are several.)

## Existing releases

Releases published before this change keep their old body, which starts at `## Release Artifacts`.
`scripts/ci/release_notes_backfill.py` re-renders the notes of those releases and keeps the artifacts block of
the existing body byte for byte. It is a dry run unless it gets `--apply` and `LH_BACKFILL_APPLY=yes`:

```bash
env -u GITHUB_TOKEN python3 scripts/ci/release_notes_backfill.py --out-dir /tmp/notes-backfill      # plan and report.md, edits nothing
env -u GITHUB_TOKEN LH_BACKFILL_APPLY=yes python3 scripts/ci/release_notes_backfill.py \
    --out-dir /tmp/notes-backfill --apply --max 10                                                  # edits, resumable
```

* The notes are rendered with `release_notes.py` from the repository at the release tag: `CHANGELOG.md` and
  the `changelog.d/` fragments at the tag, minus what the previous release tag already had, which is what the
  release job does. No tag has its own `## [X.Y.Z]` section (the release-metadata pull request writes it
  after the tag), so the entries of a release are what `[Unreleased]` and the fragments hold at the tag and
  did not hold at the previous tag. A section at the tag, if there is one, is used as it stands.
  Releases before v0.167.12 were cut with hand-written text under `[Unreleased]`, which the release assembler
  refuses; the backfill merges that text itself (`legacy_assemble` in the backfill script only; the release
  path does not use it) and renders with `release_notes.py`.
* A release is left alone, and the report says why, when it is immutable or was published at or after the
  cutoff (2026-10-23 06:00 UTC), has no `## Release Artifacts` marker, already starts with `Released at`,
  has other text before the marker, or has no entry at its tag.
* Risks are only reported: unbalanced backticks or fences, an unclosed HTML comment, a body above 60,000
  bytes, `@mentions`, and `#N` references to numbers that are not issues or pull requests here.
* Apply refuses an immutable release, re-reads the live body and stops if it is not the planned one, writes
  the old body to `backup/TAG.md` first, verifies the body after the edit and logs each release in
  `apply.log.jsonl`. Undo one release with `gh release edit TAG --notes-file backup/TAG.md`.

## How the release workflow is organised

The release workflow is split by what each part may write, so that the job that builds never holds
a token that can write:

| Job | Permissions | What it does |
|-----|-------------|--------------|
| `release-decide` | read | Decides whether to release (is any commit since the last release tag a product change without a `[skip release]` marker; see below) and which version (validated: only `vMAJOR.MINOR.PATCH` tags count, the `pr` input must be a number), and publishes the decision. |
| `release-build` | read, no secrets | Builds the eight binaries, the four multi-arch image archives (default and FIPS, for logs and traces), the Helm chart, the release notes and the files of the release-metadata pull request, and uploads them as workflow artifacts. |
| `release-publish` | packages and contents write | Copies the artifacts: the images and the chart to GHCR (and the images to Docker Hub when enabled), and creates the GitHub release as a draft with the binaries and `checksums.txt`. Runs no build tool. |
| `release-metadata` | contents and pull requests write, the personal token, environment `release` | Pushes the tag, publishes the draft release, and opens the release-metadata pull request from the four prepared files. Runs no repository code. |

The release stays a draft until the tag exists, so the tag is always pushed with the personal token
and never created by the release itself. Releases are still serialized (one run at a time, in the
`auto-release` concurrency group).

### Which merges release

The decision looks at every commit since the last release tag, not at the pushed commit alone. Runs
are serialised and GitHub keeps one pending run per group, so when merges land in a burst the run of
a middle merge can be cancelled; the next run that completes still releases it, even when its own
commit is docs, CI, a test or a `[skip release]` merge. A merge is released when it changes anything
that ships (code, the Go modules or toolchain, a changed image base digest or tag, the upstream pins)
and neither its subject nor its pull request title carries `[skip release]`. A release's own metadata
merge never releases. Re-running a finished run releases nothing. The version bump is still taken from
the labels and subject of the pushed commit's pull request. See `tests/conformance/README.md`
("Release decision") for the full rules.

### Dry run

A dry run builds everything and pushes nothing: no image, chart, release, tag or pull request. It
runs from any branch, which is the way to check a change to the release workflow before it merges:

```bash
gh workflow run auto-release.yaml --ref <branch> -f dry_run=true
gh run list --workflow auto-release.yaml --limit 3
gh run view <run-id>            # the release-publish and release-metadata jobs are skipped
```

The run summary lists the version it would release, pushes the four image archives with the real
push script to a throwaway registry on the runner and compares their manifests with the build
output, and prints what publish and metadata would do (images and tags, chart, release assets, tag,
pull-request branch and files, and the release notes). A dry run uses its own concurrency group, so
it never delays or replaces a real release.
