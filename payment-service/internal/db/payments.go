package db

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/luciana-okorie/fulfillx/payment-service/internal/models"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/telemetry"
)

type PaymentRepo struct {
	pool *pgxpool.Pool
}

func NewPaymentRepo(pool *pgxpool.Pool) *PaymentRepo {
	return &PaymentRepo{pool: pool}
}

// AuthorizeForOrder is the payment-side mirror of the inventory
// service's ReserveForOrder: one transaction that (a) checks
// processed_events first so a duplicated InventoryReserved can never
// charge twice, (b) "authorizes" the payment, and (c) writes the
// resulting event to the outbox — all atomically.
func (r *PaymentRepo) AuthorizeForOrder(ctx context.Context, event models.InventoryReserved) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var alreadyProcessed bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE order_id = $1 AND event_type = 'InventoryReserved')`,
		event.OrderID,
	).Scan(&alreadyProcessed)
	if err != nil {
		return fmt.Errorf("check processed_events: %w", err)
	}
	if alreadyProcessed {
		// Idempotency guarantee doing its job: this is a Kafka
		// redelivery of an event we already turned into a charge (or
		// a decline). Processing it again must NEVER charge twice, so
		// we simply no-op.
		return tx.Commit(ctx)
	}

	var total int64
	for _, it := range event.Items {
		total += it.UnitPriceCents * int64(it.Quantity)
	}

	now := time.Now().UTC()
	authorized := simulateAuthorization(event.OrderID)

	var failureReason string
	if !authorized {
		failureReason = "issuer_declined"
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO payments (order_id, amount_cents, authorized, failure_reason)
		 VALUES ($1, $2, $3, NULLIF($4, ''))`,
		event.OrderID, total, authorized, failureReason,
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}

	var payload []byte
	var eventType string
	if authorized {
		eventType = "PaymentAuthorized"
		payload, _ = json.Marshal(models.PaymentAuthorized{
			OrderID:     event.OrderID,
			AmountCents: total,
			OccurredAt:  now,
		})
	} else {
		eventType = "PaymentFailed"
		payload, _ = json.Marshal(models.PaymentFailed{
			OrderID:    event.OrderID,
			Reason:     failureReason,
			OccurredAt: now,
		})
	}

	if err := writeOutboxAndMarkProcessed(ctx, tx, event.OrderID, eventType, payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// simulateAuthorization stands in for a real payment processor call.
// It's deterministic (hashed from order_id) rather than random so
// that replaying the same OrderID in a test or a demo always
// produces the same outcome — a random simulateAuthorization would
// make the idempotency guarantee impossible to test reliably, since
// "processed twice" and "processed once" would look identical by
// coincidence some of the time.
func simulateAuthorization(orderID string) bool {
	h := fnv.New32a()
	h.Write([]byte(orderID))
	// ~95% authorized, ~5% declined - enough to exercise the
	// PaymentFailed path in tests/demos without most orders failing.
	return h.Sum32()%20 != 0
}

func writeOutboxAndMarkProcessed(ctx context.Context, tx pgx.Tx, orderID, eventType string, payload []byte) error {
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
		`INSERT INTO processed_events (order_id, event_type) VALUES ($1, 'InventoryReserved')`,
		orderID,
	)
	if err != nil {
		return fmt.Errorf("insert processed_events: %w", err)
	}
	return nil
}
