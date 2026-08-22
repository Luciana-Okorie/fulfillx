package outbox

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/telemetry"
)

// Worker polls outbox_events for unpublished rows and publishes them
// to Kafka, then marks them published — in that order. This means a
// crash between "Kafka publish" and "mark published" can cause a
// duplicate publish (at-least-once delivery), which is why every
// downstream consumer must be idempotent (dedupe on event id/order
// id). It can NEVER cause a lost event, which is the property that
// actually matters here.
type Worker struct {
	pool     *pgxpool.Pool
	writer   *kafka.Writer
	interval time.Duration
	batch    int
	tracer   trace.Tracer
}

func NewWorker(pool *pgxpool.Pool, brokers []string, topic string) *Worker {
	return &Worker{
		pool: pool,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{}, // partition by key (order id) so per-order ordering is preserved
		},
		interval: 500 * time.Millisecond,
		batch:    50,
		tracer:   otel.Tracer("fulfillment-service.outbox"),
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.writer.Close()
			return
		case <-ticker.C:
			if err := w.publishBatch(ctx); err != nil {
				log.Printf("outbox worker: %v", err)
			}
		}
	}
}

func (w *Worker) publishBatch(ctx context.Context) error {
	rows, err := w.pool.Query(ctx,
		`SELECT id, aggregate_id, event_type, payload, COALESCE(trace_context, '')
		 FROM outbox_events
		 WHERE published = FALSE
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		w.batch,
	)
	if err != nil {
		return err
	}

	type row struct {
		id            int64
		aggregateID   string
		eventType     string
		payload       []byte
		traceContext  string
	}
	var toPublish []row

	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.aggregateID, &r.eventType, &r.payload, &r.traceContext); err != nil {
			rows.Close()
			return err
		}
		toPublish = append(toPublish, r)
	}
	rows.Close()

	for _, r := range toPublish {
		// Resume the trace that started back at the HTTP request (or
		// upstream consumer, in other services) using the traceparent
		// persisted on the row, then start a child span for this
		// publish specifically.
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, &telemetry.TextCarrier{Value: r.traceContext})
		msgCtx, span := w.tracer.Start(msgCtx, "outbox.publish "+r.eventType)
		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("event.type", r.eventType),
			attribute.String("order.id", r.aggregateID),
		)

		headers := []kafka.Header{
			{Key: "event_type", Value: []byte(r.eventType)},
			{Key: "outbox_id", Value: []byte(itoa(r.id))},
		}
		// Inject the (possibly new, child) span context into the
		// Kafka headers so the consuming service can continue the
		// same trace.
		otel.GetTextMapPropagator().Inject(msgCtx, telemetry.KafkaHeaderCarrier{Headers: &headers})

		msg := kafka.Message{
			Key:     []byte(r.aggregateID),
			Value:   r.payload,
			Headers: headers,
		}
		if err := w.writer.WriteMessages(ctx, msg); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			// Stop on first failure; unpublished rows stay unpublished
			// and will be retried next tick. Order is preserved.
			return err
		}
		span.End()

		if _, err := w.pool.Exec(ctx,
			`UPDATE outbox_events SET published = TRUE, published_at = now() WHERE id = $1`, r.id,
		); err != nil {
			return err
		}
	}
	return nil
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
