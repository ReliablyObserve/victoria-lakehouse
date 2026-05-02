# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure with Go module
- Config namespace (`--lakehouse.*`) with production-ready defaults
- S3 `io.ReaderAt` adapter for parquet-go integration
- Stub `ParquetS3Storage` implementing storage interface
- HTTP server with `/health`, `/ready`, `/manifest/range`, `/lakehouse/info` endpoints
- Phased startup warmup (init -> disk recovery -> S3 refresh -> ready)
- CI/CD workflows (test, lint, build, security, auto-release)
- Dockerfile with multi-stage build
- Helm chart skeleton
- PR labeler, dependabot, CODEOWNERS
