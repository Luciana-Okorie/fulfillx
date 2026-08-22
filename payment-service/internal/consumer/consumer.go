package consumer

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/luciana-okorie/fulfillx/payment-service/internal/db"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/models"
	"github.com/luciana-okorie/fulfillx/payment-service/internal/telemetry"
)

type InventoryReservedConsumer struct {
	reader *kafka.Reader
	repo   *db.PaymentRepo
	tracer trace.Tracer
}

func New(brokers []string, topic, groupID string, repo *db.PaymentRepo) *InventoryReservedConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &InventoryReservedConsumer{reader: r, repo: repo, tracer: otel.Tracer("payment-service.consumer")}
}

func (c *InventoryReservedConsumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("payment consumer: fetch error: %v", err)
			continue
		}

		eventType := headerValue(msg.Headers, "event_type")
		// The inventory-events topic carries both InventoryReserved
		// and InventoryReservationFailed. Payment only acts on the
		// former; a failed reservation ends the order here (no
		// payment to authorize) rather than being an error.
		if eventType != "InventoryReserved" {
			c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			log.Printf("payment consumer: handle error for offset %d: %v", msg.Offset, err)
			continue // do not commit; Kafka redelivers, idempotency check makes retry safe
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("payment consumer: commit error: %v", err)
		}
	}
}

func (c *InventoryReservedConsumer) handle(ctx context.Context, msg kafka.Message) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, telemetry.KafkaHeaderCarrier{Headers: &msg.Headers})
	ctx, span := c.tracer.Start(ctx, "consume InventoryReserved")
	defer span.End()

	var event models.InventoryReserved
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("payment consumer: skipping malformed message at offset %d: %v", msg.Offset, err)
		return nil
	}
	span.SetAttributes(attribute.String("order.id", event.OrderID))
	return c.repo.AuthorizeForOrder(ctx, event)
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
