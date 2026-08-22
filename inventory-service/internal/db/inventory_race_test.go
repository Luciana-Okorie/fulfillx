//go:build integration

package db_test

// Integration test: two concurrent ReserveForOrder calls compete for
// SKU-2, seeded with available_quantity = 1 (see migrations/0001_init.sql).
// Exactly one must succeed (InventoryReserved), the other must fail
// (InventoryReservationFailed), and available_quantity must never go
// negative. Requires DATABASE_URL pointing at a migrated Postgres
// instance — run via `go test -tags=integration ./...` after
// `docker compose up postgres`.
//
// The core assertion: run two goroutines calling ReserveForOrder for
// two different order IDs, both requesting SKU-2 quantity 1,
// simultaneously. The `SELECT ... FOR UPDATE` row lock in
// ReserveForOrder serializes them — the second transaction blocks
// until the first commits, then sees available_quantity = 0 and
// fails cleanly instead of racing past the check. This file is a
// placeholder documenting that scenario for CI; fill in with the
// project's Postgres test-container setup of choice (e.g.
// testcontainers-go) to run it as one of the required
// failure/recovery tests.
