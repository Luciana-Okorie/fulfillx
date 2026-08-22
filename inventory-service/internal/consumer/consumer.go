package consumer

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/luciana-okorie/fulfillx/inventory-service/internal/db"
	"github.com/luciana-okorie/fulfillx/inventory-service/internal/models"
	"github.com/luciana-okorie/fulfillx/inventory-service/internal/telemetry"
)

// OrderCreatedConsumer reads from the order-events topic and reserves
// inventory for each order. Kafka's own delivery guarantee here is
// at-least-once (we commit offsets after processing, not before —
// see Run), so ReserveForOrder's internal idempotency check is what
// actually prevents double-reservation, not the consumer loop.
type OrderCreatedConsumer struct {
	reader *kafka.Reader
	repo   *db.InventoryRepo
	tracer trace.Tracer
}

func New(brokers []string, topic, groupID string, repo *db.InventoryRepo) *OrderCreatedConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID, // consumer group -> restart resumes from last committed offset
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &OrderCreatedConsumer{reader: r, repo: repo, tracer: otel.Tracer("inventory-service.consumer")}
}

func (c *OrderCreatedConsumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutting down
			}
			log.Printf("inventory consumer: fetch error: %v", err)
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			// Deliberately do NOT commit the offset on failure (e.g. a
			// Postgres outage). Kafka will redeliver this message on
			// restart/rebalance. Combined with ReserveForOrder's
			// idempotency check, redelivery is always safe. A
			// production version would route to a dead-letter topic
			// after N retries instead of blocking the partition
			// forever — noted as a stretch goal in the README.
			log.Printf("inventory consumer: handle error for offset %d: %v", msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("inventory consumer: commit error: %v", err)
		}
	}
}

func (c *OrderCreatedConsumer) handle(ctx context.Context, msg kafka.Message) error {
	// Resume the trace carried in the Kafka headers (originally
	// injected by the Order Service's outbox worker) so this
	// consumer's work shows up as a child span of the same trace that
	// started at the client's POST /orders request.
	ctx = otel.GetTextMapPropagator().Extract(ctx, telemetry.KafkaHeaderCarrier{Headers: &msg.Headers})
	ctx, span := c.tracer.Start(ctx, "consume OrderCreated")
	defer span.End()

	var event models.OrderCreated
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// A malformed message can never be processed successfully —
		// retrying it forever would wedge the partition. Log and skip
		// (in production: route to a dead-letter topic) rather than
		// blocking every order behind it.
		log.Printf("inventory consumer: skipping malformed message at offset %d: %v", msg.Offset, err)
		return nil
	}
	span.SetAttributes(attribute.String("order.id", event.OrderID))
	return c.repo.ReserveForOrder(ctx, event)
}
