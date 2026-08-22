package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/models"
	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/telemetry"
)

type FulfillmentRepo struct {
	pool *pgxpool.Pool
}

func NewFulfillmentRepo(pool *pgxpool.Pool) *FulfillmentRepo {
	return &FulfillmentRepo{pool: pool}
}

// CreateForOrder handles PaymentAuthorized the same way every other
// consumer in this system handles its inbound event: idempotency
// check, do the work, write the outbox event, all in one transaction.
// It also immediately advances to OrderReadyForShipment — in a
// system with real warehouse/carrier integrations this would be two
// separate steps (create the fulfillment job, then a later signal
// when it's actually packed), but for this simplified version the
// two events are emitted together to keep the demo deterministic.
func (r *FulfillmentRepo) CreateForOrder(ctx context.Context, event models.PaymentAuthorized) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var alreadyProcessed bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE order_id = $1 AND event_type = 'PaymentAuthorized')`,
		event.OrderID,
	).Scan(&alreadyProcessed)
	if err != nil {
		return fmt.Errorf("check processed_events: %w", err)
	}
	if alreadyProcessed {
		return tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO fulfillments (order_id, status) VALUES ($1, 'READY_FOR_SHIPMENT')
		 ON CONFLICT (order_id) DO NOTHING`,
		event.OrderID,
	)
	if err != nil {
		return fmt.Errorf("insert fulfillment: %w", err)
	}

	now := time.Now().UTC()

	createdPayload, _ := json.Marshal(models.FulfillmentCreated{OrderID: event.OrderID, OccurredAt: now})
	if err := insertOutbox(ctx, tx, event.OrderID, "FulfillmentCreated", createdPayload); err != nil {
		return err
	}

	readyPayload, _ := json.Marshal(models.OrderReadyForShipment{OrderID: event.OrderID, OccurredAt: now})
	if err := insertOutbox(ctx, tx, event.OrderID, "OrderReadyForShipment", readyPayload); err != nil {
		return err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO processed_events (order_id, event_type) VALUES ($1, 'PaymentAuthorized')`,
		event.OrderID,
	)
	if err != nil {
		return fmt.Errorf("insert processed_events: %w", err)
	}

	return tx.Commit(ctx)
}

func insertOutbox(ctx context.Context, tx pgx.Tx, orderID, eventType string, payload []byte) error {
	carrier := &telemetry.TextCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	_, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, trace_context)
		 VALUES ('order', $1, $2, $3, $4)`,
		orderID, eventType, payload, carrier.Value,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event %s: %w", eventType, err)
	}
	return nil
}
