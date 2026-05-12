// Package e2e contains end-to-end tests for Victoria Lakehouse.
//
// These tests run against a live Docker Compose stack with:
//   - lakehouse-logs at localhost:29428 (VictoriaLogs-compatible select API)
//   - lakehouse-traces at localhost:20428 (VictoriaTraces-compatible + Jaeger API)
//   - MinIO at localhost:29000 (S3-compatible storage)
//   - vlselect at localhost:29471 (multi-level select fan-out)
//   - loki-vl-proxy at localhost:23100 (Loki API compatibility)
//   - Grafana at localhost:3003
//
// Data is seeded by datagen:
//   - Tenant 0/0 (default): 5000 logs + 1000 traces over 48 hours
//   - Tenant 1/1 (test): 1000 logs + 200 traces over 48 hours
//
// Services: api-gateway, user-service, order-service, payment-service,
// notification-service with levels INFO, WARN, ERROR, DEBUG.
//
// All ports use a "2" prefix to avoid conflicts with other docker-compose stacks.
//
// Run with:
//
//	go test -tags=e2e -v -count=1 -timeout=10m ./tests/e2e/
package e2e
