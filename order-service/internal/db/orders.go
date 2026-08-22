package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/luciana-okorie/fulfillx/order-service/internal/models"
	"github.com/luciana-okorie/fulfillx/order-service/internal/telemetry"
)

type OrderRepo struct {
	pool *pgxpool.Pool
}

func NewOrderRepo(pool *pgxpool.Pool) *OrderRepo {
	return &OrderRepo{pool: pool}
}

// CreateOrderWithOutbox is the heart of the transactional outbox
// pattern: the order row, its items, and the OrderCreated outbox
// event are written in ONE database transaction. If the process
// crashes after COMMIT but before the outbox worker publishes to
// Kafka, the event is simply still sitting in outbox_events,
// unpublished — nothing is lost, and nothing is double-created,
// because we never publish directly from the request handler.
func (r *OrderRepo) CreateOrderWithOutbox(ctx context.Context, req models.CreateOrderRequest) (*models.Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	// If we return before tx.Commit, this rollback is a no-op after
	// commit succeeds and a safety net if any step below fails.
	defer tx.Rollback(ctx)

	var total int64
	for _, it := range req.Items {
		total += it.UnitPriceCents * int64(it.Quantity)
	}

	var orderID string
	var createdAt, updatedAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (customer_id, status, total_amount_cents)
		 VALUES ($1, 'PENDING', $2)
		 RETURNING id, created_at, updated_at`,
		req.CustomerID, total,
	).Scan(&orderID, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}

	for _, it := range req.Items {
		_, err = tx.Exec(ctx,
			`INSERT INTO order_items (order_id, sku, quantity, unit_price_cents)
			 VALUES ($1, $2, $3, $4)`,
			orderID, it.SKU, it.Quantity, it.UnitPriceCents,
		)
		if err != nil {
			return nil, fmt.Errorf("insert order_item: %w", err)
		}
	}

	event := models.OrderCreated{
		OrderID:    orderID,
		CustomerID: req.CustomerID,
		Items:      req.Items,
		TotalCents: total,
		OccurredAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// Capture the current trace context (set by otelhttp's server span
	// around this HTTP request) as a W3C traceparent string, stored
	// alongside the event. The outbox worker runs in a different
	// goroutine, possibly seconds later, so this is the only way for
	// its eventual Kafka publish span to be linked back to the
	// request that created the order.
	carrier := &telemetry.TextCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, trace_context)
		 VALUES ('order', $1, 'OrderCreated', $2, $3)`,
		orderID, payload, carrier.Value,
	)
	if err != nil {
		return nil, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &models.Order{
		ID:               orderID,
		CustomerID:       req.CustomerID,
		Status:           models.StatusPending,
		TotalAmountCents: total,
		Items:            req.Items,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
	}, nil
}

func (r *OrderRepo) UpdateStatus(ctx context.Context, orderID, status string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE orders SET status = $1, updated_at = now() WHERE id = $2`,
		status, orderID,
	)
	return err
}
func (r *OrderRepo) GetOrder(ctx context.Context, id string) (*models.Order, error) {
	var o models.Order
	err := r.pool.QueryRow(ctx,
		`SELECT id, customer_id, status, total_amount_cents, created_at, updated_at
		 FROM orders WHERE id = $1`, id,
	).Scan(&o.ID, &o.CustomerID, &o.Status, &o.TotalAmountCents, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx,
		`SELECT sku, quantity, unit_price_cents FROM order_items WHERE order_id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var it models.OrderItem
		if err := rows.Scan(&it.SKU, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, err
		}
		o.Items = append(o.Items, it)
	}

	return &o, nil
}

// SaveIdempotentResponse records the response we returned for a given
// idempotency key, so a retried request with the same key gets the
// exact same response instead of creating a second order. This is
// the durable fallback behind the faster Redis-based check.
func (r *OrderRepo) SaveIdempotentResponse(ctx context.Context, key, requestHash string, status int, body []byte, orderID string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash, response_status, response_body, order_id)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (key) DO NOTHING`,
		key, requestHash, status, body, orderID,
	)
	return err
}

type IdempotentRecord struct {
	RequestHash    string
	ResponseStatus int
	ResponseBody   []byte
}

func (r *OrderRepo) GetIdempotentResponse(ctx context.Context, key string) (*IdempotentRecord, error) {
	var rec IdempotentRecord
	err := r.pool.QueryRow(ctx,
		`SELECT request_hash, response_status, response_body FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&rec.RequestHash, &rec.ResponseStatus, &rec.ResponseBody)
	if err != nil {
		return nil, err
	}
	return &rec, nil
}
