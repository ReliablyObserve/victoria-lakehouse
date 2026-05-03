// Package e2e contains end-to-end tests for Victoria Lakehouse.
//
// These tests run against a live Docker Compose stack with:
//   - lakehouse-logs at localhost:19428 (VictoriaLogs-compatible select API)
//   - lakehouse-traces at localhost:20428 (VictoriaTraces-compatible + Jaeger API)
//   - MinIO at localhost:9000 (S3-compatible storage)
//   - Grafana at localhost:3000
//
// Data is seeded by datagen: 5000 logs + 1000 trace spans over 48 hours,
// across 5 services (api-gateway, user-service, order-service, payment-service,
// notification-service) with levels INFO, WARN, ERROR, DEBUG.
//
// Run with:
//
//	go test -tags=e2e -v -count=1 -timeout=10m ./tests/e2e/
package e2e
