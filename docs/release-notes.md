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
`scripts/ci/release_notes.py --version Unreleased` on the commit it releases and puts the output in
front of the artifacts section. The script uses the standard library only, reads no network and renders the same text
for the same input. The `[Unreleased]` section is the source because the section of the new
version is created only by the release-metadata pull request that follows the release. Releases
are cut one at a time (merge, wait for the release, merge the release-metadata pull request),
so `[Unreleased]` holds exactly the entries of that release. When a release is cut before the
previous release-metadata pull request has merged, `[Unreleased]` still holds the previous
release's entries as well; the script leaves out every bullet that the `[Unreleased]` section had
at the previous release tag (`--after-tag`, which the job takes from `git describe`), so they are
not repeated. If that tag cannot be read, all entries are rendered and a warning is printed.

To render the notes of any released version, or to preview the entries of a pull request:

```bash
python3 scripts/ci/release_notes.py --version 0.157.6
python3 scripts/ci/release_notes.py --version Unreleased --date 2026-10-22
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
| `### Update notes` | `**Update note:** ...` paragraphs, before the bullets |

- Upstream uses only the `SECURITY`, `FEATURE` and `BUGFIX` prefixes, so changes and removals are
  `FEATURE` bullets that say what happened.
- A bullet is one line in the notes: the lines of a wrapped bullet are joined, and the bold
  lead-in opens the text (after the prefix, and after `removed: ` or `deprecated: ` for those two
  sections). Nested bullets and further paragraphs of a bullet stay under it.
- `#123` becomes a link to that issue or pull request of this repository. Code spans, existing
  links, URLs and references to other repositories are left alone.
- A subsection the script does not know (for example `### CI`) is rendered under `FEATURE` and
  reported as a warning; text is never dropped. Prose before the first subsection is kept
  right after `Released at`.
- A release without entries reads `Released at <date>` and `No user-visible changes.`

## Update notes

Behaviour that an operator has to act on when upgrading (a flag, an endpoint or a default that
changes, a format that is no longer read) is written as one bullet each under a
`### Update notes` subsection of the same `[Unreleased]` section, with the issue or pull request
number:

```markdown
## [Unreleased]

### Update notes

- The `/delete/run_task` endpoint accepts only `POST`; change scripts that call it with `GET` (#1234).
```

One note renders as `**Update note:** ...`; several render as `**Update note 1:** ...`,
`**Update note 2:** ...`. The change itself still gets its own `### Changed` or `### Fixed`
bullet; the update note is the short instruction to the operator.

## Existing releases

Releases published before this change keep their old body. A backfill re-renders the notes from
the version's `CHANGELOG.md` section and keeps the artifacts block of the existing body; it is run
by the repository owner with `gh release edit`, one release at a time.

## How the release workflow is organised

The release workflow is split by what each part may write, so that the job that builds never holds
a token that can write:

| Job | Permissions | What it does |
|-----|-------------|--------------|
| `release-decide` | read | Decides whether to release and which version (validated: only `vMAJOR.MINOR.PATCH` tags count, the `pr` input must be a number), and publishes the decision. |
| `release-build` | read, no secrets | Builds the eight binaries, the four multi-arch image archives (default and FIPS, for logs and traces), the Helm chart, the release notes and the files of the release-metadata pull request, and uploads them as workflow artifacts. |
| `release-publish` | packages and contents write | Copies the artifacts: the images and the chart to GHCR (and the images to Docker Hub when enabled), and creates the GitHub release as a draft with the binaries and `checksums.txt`. Runs no build tool. |
| `release-metadata` | contents and pull requests write, the personal token, environment `release` | Pushes the tag, publishes the draft release, and opens the release-metadata pull request from the four prepared files. Runs no repository code. |

The release stays a draft until the tag exists, so the tag is always pushed with the personal token
and never created by the release itself. Releases are still serialized (one run at a time, in the
`auto-release` concurrency group).

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
