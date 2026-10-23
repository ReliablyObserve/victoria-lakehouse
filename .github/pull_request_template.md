## Summary

<!-- Brief description of what this PR does and why -->

## Changes

- 

## Changelog

<!-- A change that ships adds a fragment, changelog.d/<PR-or-slug>.<kind>.md (see changelog.d/README.md); do not edit CHANGELOG.md -->
- [ ] Added a changelog fragment (the gate decides from the changed files, see changelog.d/README.md; it does not read `[skip release]`)

## Test Plan

- [ ] Unit tests pass (`make test`)
- [ ] Integration tests pass (if applicable)
- [ ] Manual testing performed

## Feature catalog (required for feature PRs)

- [ ] `tests/conformance/registry/features/*.yaml` entry added/updated (rows, tests, docs, highlight)
- [ ] Registry rows + regression tests cover the new/changed behavior
- [ ] `make conformance-gen` run; `docs/features.md`, `UPSTREAM_COVERAGE.md`, README highlights committed current

## Checklist

- [ ] Code follows project conventions
- [ ] No secrets or credentials in code
- [ ] Metrics added/updated for new functionality
- [ ] Dashboard updated if new metrics added
