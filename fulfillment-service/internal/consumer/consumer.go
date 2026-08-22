package consumer

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/db"
	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/models"
	"github.com/luciana-okorie/fulfillx/fulfillment-service/internal/telemetry"
)

type PaymentAuthorizedConsumer struct {
	reader *kafka.Reader
	repo   *db.FulfillmentRepo
	tracer trace.Tracer
}

func New(brokers []string, topic, groupID string, repo *db.FulfillmentRepo) *PaymentAuthorizedConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &PaymentAuthorizedConsumer{reader: r, repo: repo, tracer: otel.Tracer("fulfillment-service.consumer")}
}

func (c *PaymentAuthorizedConsumer) Run(ctx context.Context) {
	defer c.reader.Close()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fulfillment consumer: fetch error: %v", err)
			continue
		}

		eventType := headerValue(msg.Headers, "event_type")
		// payment-events also carries PaymentFailed; nothing for
		// fulfillment to do with that, so just move past it.
		if eventType != "PaymentAuthorized" {
			c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.handle(ctx, msg); err != nil {
			log.Printf("fulfillment consumer: handle error for offset %d: %v", msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("fulfillment consumer: commit error: %v", err)
		}
	}
}

func (c *PaymentAuthorizedConsumer) handle(ctx context.Context, msg kafka.Message) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, telemetry.KafkaHeaderCarrier{Headers: &msg.Headers})
	ctx, span := c.tracer.Start(ctx, "consume PaymentAuthorized")
	defer span.End()

	var event models.PaymentAuthorized
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("fulfillment consumer: skipping malformed message at offset %d: %v", msg.Offset, err)
		return nil
	}
	span.SetAttributes(attribute.String("order.id", event.OrderID))
	return c.repo.CreateForOrder(ctx, event)
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
