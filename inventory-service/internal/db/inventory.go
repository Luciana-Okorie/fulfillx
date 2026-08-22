package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/luciana-okorie/fulfillx/inventory-service/internal/models"
	"github.com/luciana-okorie/fulfillx/inventory-service/internal/telemetry"
)

type InventoryRepo struct {
	pool *pgxpool.Pool
}

func NewInventoryRepo(pool *pgxpool.Pool) *InventoryRepo {
	return &InventoryRepo{pool: pool}
}

// ReserveForOrder is where the two hard distributed-systems problems
// in this service get solved:
//
//  1. Duplicate delivery: OrderCreated arrives at-least-once (the
//     upstream outbox worker can republish after a crash). We check
//     processed_events for (order_id, event_type) FIRST, inside the
//     same transaction as the reservation itself, so "already
//     processed" and "reserve inventory" can never disagree.
//
//  2. Overselling: two orders racing for the last unit of a SKU.
//     We take `SELECT ... FOR UPDATE` row locks on every SKU touched,
//     in sorted order (to avoid a classic lock-ordering deadlock
//     between two orders that share SKUs), before checking
//     availability. The second transaction to reach the lock simply
//     waits until the first commits or rolls back, then sees the
//     updated (or reverted) quantity. Only one of them can succeed.
func (r *InventoryRepo) ReserveForOrder(ctx context.Context, event models.OrderCreated) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var alreadyProcessed bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE order_id = $1 AND event_type = 'OrderCreated')`,
		event.OrderID,
	).Scan(&alreadyProcessed)
	if err != nil {
		return fmt.Errorf("check processed_events: %w", err)
	}
	if alreadyProcessed {
		// Duplicate delivery of an event we've already handled.
		// Nothing to do — this is the idempotency guarantee working
		// as intended, not an error.
		return tx.Commit(ctx)
	}

	skus := make([]string, 0, len(event.Items))
	qtyBySKU := map[string]int{}
	for _, it := range event.Items {
		skus = append(skus, it.SKU)
		qtyBySKU[it.SKU] += it.Quantity
	}
	sort.Strings(skus) // consistent lock order across all transactions -> no deadlocks

	available := map[string]int{}
	for _, sku := range skus {
		var qty int
		err = tx.QueryRow(ctx,
			`SELECT available_quantity FROM inventory WHERE sku = $1 FOR UPDATE`, sku,
		).Scan(&qty)
		if err != nil {
			return fmt.Errorf("lock inventory row %s: %w", sku, err)
		}
		available[sku] = qty
	}

	var failedSKUs []string
	for _, sku := range skus {
		if available[sku] < qtyBySKU[sku] {
			failedSKUs = append(failedSKUs, sku)
		}
	}

	now := time.Now().UTC()

	if len(failedSKUs) > 0 {
		payload, _ := json.Marshal(models.InventoryReservationFailed{
			OrderID:    event.OrderID,
			Reason:     "insufficient_inventory",
			FailedSKUs: failedSKUs,
			OccurredAt: now,
		})
		if err := r.writeOutboxAndMarkProcessed(ctx, tx, event.OrderID, "InventoryReservationFailed", payload); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	for _, sku := range skus {
		_, err = tx.Exec(ctx,
			`UPDATE inventory
			 SET available_quantity = available_quantity - $1,
			     reserved_quantity  = reserved_quantity + $1,
			     updated_at = now()
			 WHERE sku = $2`,
			qtyBySKU[sku], sku,
		)
		if err != nil {
			return fmt.Errorf("update inventory %s: %w", sku, err)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO reservations (order_id, sku, quantity) VALUES ($1, $2, $3)`,
			event.OrderID, sku, qtyBySKU[sku],
		)
		if err != nil {
			return fmt.Errorf("insert reservation %s: %w", sku, err)
		}
	}

	payload, _ := json.Marshal(models.InventoryReserved{
		OrderID:    event.OrderID,
		Items:      event.Items,
		OccurredAt: now,
	})
	if err := r.writeOutboxAndMarkProcessed(ctx, tx, event.OrderID, "InventoryReserved", payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *InventoryRepo) writeOutboxAndMarkProcessed(ctx context.Context, tx pgx.Tx, orderID, eventType string, payload []byte) error {
	carrier := &telemetry.TextCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, trace_context)
		 VALUES ('order', $1, $2, $3, $4)`,
		orderID, eventType, payload, carrier.Value,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO processed_events (order_id, event_type) VALUES ($1, 'OrderCreated')`,
		orderID,
	)
	if err != nil {
		return fmt.Errorf("insert processed_events: %w", err)
	}
	return nil
}

