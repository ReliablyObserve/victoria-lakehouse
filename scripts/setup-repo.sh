#!/usr/bin/env bash
set -euo pipefail

# Setup script for ReliablyObserve/victoria-lakehouse GitHub repository
# Run after creating the repo: ./scripts/setup-repo.sh
#
# Prerequisites:
# - gh CLI authenticated with admin access
# - Repository already created on GitHub

REPO="ReliablyObserve/victoria-lakehouse"

echo "=== Setting up branch protection for $REPO ==="

# Enable branch protection on main
gh api repos/$REPO/branches/main/protection \
  --method PUT \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["test", "lint", "build (linux, amd64)", "docker", "security / govulncheck"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true,
    "require_last_push_approval": false
  },
  "restrictions": null,
  "required_linear_history": false,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true
}
EOF

echo "Branch protection enabled on main"
echo "  - Require PR reviews (1 reviewer, CODEOWNERS enforced)"
echo "  - Dismiss stale reviews on new push"
echo "  - Require CI status checks to pass"
echo "  - No force pushes allowed"
echo "  - Require conversation resolution"
echo "  - Admins CAN bypass (enforce_admins: false)"

# Create labels for auto-release
echo ""
echo "=== Creating labels ==="

LABELS=(
  "release:major:B60205:Major version bump"
  "release:minor:0E8A16:Minor version bump"
  "release:patch:1D76DB:Patch version bump"
  "no-release:EDEDED:Skip auto-release"
  "breaking-change:B60205:Breaking change"
  "feature:0E8A16:New feature"
  "bugfix:D93F0B:Bug fix"
  "performance:FBCA04:Performance improvement"
  "documentation:0075CA:Documentation only"
  "maintenance:EDEDED:Maintenance/chore"
  "dependencies:0366D6:Dependency update"
  "size/XS:C2E0C6:Extra small change"
  "size/S:C2E0C6:Small change"
  "size/M:FBCA04:Medium change"
  "size/L:E99695:Large change"
  "size/XL:B60205:Extra large change"
  "scope/config:D4C5F9:Config changes"
  "scope/storage:D4C5F9:Storage layer"
  "scope/s3:D4C5F9:S3 integration"
  "scope/cache:D4C5F9:Cache layer"
  "scope/manifest:D4C5F9:Manifest"
  "scope/schema:D4C5F9:Schema registry"
  "scope/metrics:D4C5F9:Metrics/observability"
  "scope/startup:D4C5F9:Startup/shutdown"
  "scope/protocol:D4C5F9:Binary protocol"
  "scope/discovery:D4C5F9:Service discovery"
  "scope/peercache:D4C5F9:Peer cache"
  "scope/helm:D4C5F9:Helm chart"
  "scope/ci:D4C5F9:CI/CD workflows"
  "scope/dashboards:D4C5F9:Grafana dashboards"
  "scope/docs:D4C5F9:Documentation"
  "scope/tests:D4C5F9:Test suite"
)

for entry in "${LABELS[@]}"; do
  IFS=: read -r name color desc <<< "$entry"
  gh label create "$name" --repo "$REPO" --color "$color" --description "$desc" --force 2>/dev/null || true
done

echo "Labels created"

echo ""
echo "=== Repository setup complete ==="
echo ""
echo "Manual steps remaining:"
echo "  1. Add secrets in Settings > Secrets:"
echo "     - GHCR_PACKAGE_ADMIN_TOKEN (for public package visibility)"
echo "     - RELEASE_PR_TOKEN (optional, for bot PRs to trigger CI)"
echo "  2. Enable 'Automatically delete head branches' in Settings > General"
echo "  3. Set 'Squash and merge' as default merge method"
echo "  4. Enable Dependabot alerts in Settings > Code security"
