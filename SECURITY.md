# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Victoria Lakehouse, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

### How to Report

Email: **security@reliablyobserve.com**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 48 hours
- **Initial assessment**: Within 5 business days
- **Fix timeline**: Depends on severity, typically within 30 days for critical issues

### What to Expect

1. We will acknowledge your report and begin investigation
2. We will work with you to understand the scope and impact
3. We will develop and test a fix
4. We will release the fix and credit you (unless you prefer anonymity)

## Security Practices

### Code Security

- All file operations use restricted permissions (0o600 files, 0o750 directories)
- No secrets in code or configuration defaults
- S3 credentials handled via standard AWS SDK credential chain (IAM roles, env vars, config files)
- All HTTP endpoints validate input parameters
- ZSTD decompression has size limits to prevent decompression bombs

### Dependency Management

- Dependencies reviewed before adoption
- Dependabot enabled for automated vulnerability scanning
- Go module checksums verified via `go.sum`

### CI/CD Security

- CodeQL analysis on all pull requests
- gosec security linting enforced
- No secrets in CI logs or artifacts
