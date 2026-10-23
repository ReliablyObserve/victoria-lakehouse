---
title: Security posture
sidebar_position: 14
---

# Security posture

The automated supply-chain and code checks that run on this repository, how to read their results,
and which results are expected to stay low. For the product's own security settings (container
hardening, authentication, tenant isolation) see [Security](security.md). To report a vulnerability,
see [SECURITY.md](https://github.com/ReliablyObserve/victoria-lakehouse/blob/main/SECURITY.md).

## Checks

| Check | Workflow | When | Result goes to | Blocks a merge |
| --- | --- | --- | --- | --- |
| OpenSSF Scorecard | `scorecard.yaml` | push to `main`, weekly, manual | public Scorecard API and a workflow artifact (`scorecard`) | no |
| Semgrep (Go, Dockerfile, GitHub Actions, OWASP Top Ten, secrets rule packs) | `semgrep.yaml` | pull request, weekly, manual | job summary and a workflow artifact (`semgrep-sarif`) | no (report only) |
| Dependency review (vulnerable dependency added or upgraded, HIGH or CRITICAL) | `dependency-review.yaml` | pull request | job log and summary | not a required check |
| Workflow lint (actionlint, zizmor) | `workflow-lint.yaml` | pull request, push to `main` | job log | not a required check |
| gosec, govulncheck, Trivy, gitleaks; CodeQL | `security.yaml`; `codeql.yaml` | see the workflows | job logs; code scanning | gosec, govulncheck, trivy and gitleaks are required checks |

The Scorecard, Semgrep and dependency-review workflows are not required status checks and are not
skipped by the CI scope; their findings are not uploaded to code scanning, so they do not enter the
code-scanning merge gate.

## Workflow rules enforced by tests

`scripts/ci/tests` fails a pull request that breaks these rules for any workflow:

- every external action is pinned to a full commit SHA with a version comment;
- tools installed in CI are pinned and their downloads verified (`--require-hashes` locks,
  checksums);
- every workflow states its token permissions (at the workflow level or on each job), and none uses
  `permissions: write-all`;
- `id-token: write` (an OIDC token) appears only in the workflows listed with a reason in
  `scripts/ci/tests/test_workflow_permissions.py`.

## Reading the Scorecard

- Result and per-check reasons: <https://scorecard.dev/viewer/?uri=github.com/ReliablyObserve/victoria-lakehouse>.
  The badge in the README reads the same result. Results are published from the `main` workflow run
  only; a run from another branch checks the workflow and produces the artifact, but does not
  publish.
- The workflow artifact `scorecard` holds the JSON of the last run, including the details of every
  check.
- Scorecard evaluates a check from public repository settings. Some checks need a token with more
  rights than the workflow token (branch protection details), and then report what they can see.

### Checks expected to stay low

| Check | Why |
| --- | --- |
| Code-Review | The project has a single maintainer, who merges his own pull requests. Scorecard counts changes approved by someone other than the author. |
| Contributors | Scorecard looks for contributors from several organisations. |
| CII-Best-Practices | Needs the maintainer to register the project for the OpenSSF Best Practices badge. This has not been done. |

This table lists expectations, not scores. Read the actual numbers from the viewer above.

## Semgrep: report only

The Semgrep job never fails on findings. It installs Semgrep from a hashed lock
(`.github/semgrep-requirements.txt`, compiled from `.github/semgrep-requirements.in`), downloads the
rule packs from the Semgrep registry at run time (it needs network access to `semgrep.dev`; no
account, token or metrics), and prints the finding counts in the job summary. The SARIF file is
kept as an artifact for 30 days. Findings are triaged before the job is made blocking; an accepted finding is to get an inline
`nosemgrep` comment with its reason, not a silent rule exclusion.

## Dependency review

`dependency-review.yaml` compares the dependency graph of the pull request with its base and fails
on a newly introduced dependency with a known HIGH or CRITICAL advisory. It needs the repository's
dependency graph (enabled). License checks are off. Vulnerabilities already present on `main` are
tracked by Dependabot alerts and `govulncheck`.
