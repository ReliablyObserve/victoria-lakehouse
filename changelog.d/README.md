# Changelog fragments

A change is documented by one small file here, not by an edit of `CHANGELOG.md`. The release
turns the fragments into the new `## [X.Y.Z] - DATE` section of `CHANGELOG.md` and removes them in
its release-metadata pull request, so every fragment is released exactly once and two pull requests
never touch the same lines.

## File

`changelog.d/<PR-or-slug>.<kind>.md`

- `<PR-or-slug>`: lower-case letters, digits, `.`, `_` and `-`, for example `1234` or `peer-auth-redirects`.
  One pull request may add several fragments (`1234.fixed.md`, `1234-docs.changed.md`).
- `<kind>`: `added`, `changed`, `deprecated`, `removed`, `fixed` or `security`. It becomes the
  `### Added`, `### Changed`, `### Deprecated`, `### Removed`, `### Fixed` or `### Security` heading.
- Content: the bullet exactly as it should read in `CHANGELOG.md`, in the repository's style: one item,
  starting with `- **bold lead-in.**`; a following paragraph or a sub-list of the same item is
  indented by two spaces; no heading and no second item at the left margin.

```markdown
- **A cold read no longer answers 200 with rows missing when an object it selected could not be read (#503).** `processOneFile` ...
```

An `added` fragment's bold lead-in is what the feature catalog matches (`changelog_bullets:`), as an
`### Added` bullet of `CHANGELOG.md` was before.

## Rules

- The changelog gate decides from the pull request's files and commit subjects, as it did for `[Unreleased]`
  bullets, and it does not read the `[skip release]` marker (that is for the release decision only). A fragment
  is required unless every changed file is under `docs/`, `.github/`, `scripts/ci/`, `scripts/proof/` or
  `website/`, is `README.md`, `CHANGELOG.md` or `LICENSE`, or is a Go unit test (`*_test.go` under `cmd/`,
  `internal/`, `lakehouse-traces/`), and no commit subject is `feat`, `fix`, `perf` or `revert` (with a scope
  other than `ci`, `build`, `deps`, `deps-dev`, `chore`). Dependency-only commits are skipped. A file under
  `tests/` that is not a unit test, for example the conformance registry, does need a fragment.
- A fragment is released with the release whose tag contains it, and is not edited or deleted after that
  release (the gate refuses it); add a new fragment for a correction. A fragment whose bold lead-in is already
  in a released section of `CHANGELOG.md` is refused: the change would be released twice.
- A fragment is at most 16 KiB, a regular file, with no unclosed `<!--`.
- `CHANGELOG.md` is not edited by hand: `## [Unreleased]` stays empty, and a bullet filed by hand under a
  released section is refused, except the bullet of a fragment the same pull request deletes (how a fragment
  whose metadata pull request could not merge is cleared). `changelog.d/README.md` stays a regular file.
- A fragment is never edited after it was released; the release removes it.
- Check locally: `python3 scripts/ci/changelog_fragments.py check`.

## Release

`scripts/ci/changelog_fragments.py assemble` runs in the release job on the release commit. It
releases the fragments present there, except those the previous release tag already had, groups them
under the Keep a Changelog headings in file-name order, and lists them for the metadata job, which
deletes them in the release-metadata pull request. A fragment merged after the release commit is
not in that release and lands in the next one.

## Recovery

Two situations wedge a release until a fragment is removed, and both are fixed by a small pull request that
only deletes the file (use a `docs:` or `chore:` subject: a `feat:`, `fix:` or `perf:` subject asks for a new
fragment):

- **The same bold lead-in in two pull requests**, or a stale branch that re-adds the lead-in of a change a
  later release already took. The gate refuses the second one when it can see the first; when both were
  merged in parallel, the release stops with the two file names. Delete the duplicate (or reword its lead-in).
- **A fragment left behind**: its release section exists in `CHANGELOG.md` but the file is still here, or the
  release-metadata pull request was closed and never merged. Delete the file when the section holds its bullet,
  or file the bullet by hand under the section of the release that took it and delete the file in the same
  pull request. The gate allows exactly that, and nothing else, under a released section.
