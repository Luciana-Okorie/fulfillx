package statusconsumer

import (
	"context"
	"encoding/json"
	"log"

	kafka "github.com/segmentio/kafka-go"

	"github.com/luciana-okorie/fulfillx/order-service/internal/db"
)

// statusByEventType maps every downstream event this service cares
// about to the order status it implies. This is what lets
// GET /orders/:id/status reflect the true state of an order as it
// moves through inventory, payment, and fulfillment — the Order
// Service is the one place a client (or the API Gateway's WebSocket
// stream) can check without knowing about the other three services.
var statusByEventType = map[string]string{
	"InventoryReserved":          "INVENTORY_RESERVED",
	"InventoryReservationFailed": "FAILED",
	"PaymentAuthorized":          "PAYMENT_AUTHORIZED",
	"PaymentFailed":              "FAILED",
	"FulfillmentCreated":         "FULFILLMENT_CREATED",
	"OrderReadyForShipment":      "FULFILLED",
}

type genericEvent struct {
	OrderID string `json:"order_id"`
}

// Consumer subscribes to every downstream topic (inventory, payment,
// fulfillment events) using one reader per topic, since kafka-go
// readers are single-topic. All events for a given order land on the
// same partition within each topic (keyed by order id upstream), so
// per-order ordering holds even with several readers running
// concurrently.
type Consumer struct {
	readers []*kafka.Reader
	repo    *db.OrderRepo
}

func New(brokers []string, topics []string, groupID string, repo *db.OrderRepo) *Consumer {
	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		readers = append(readers, kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}))
	}
	return &Consumer{readers: readers, repo: repo}
}

func (c *Consumer) Run(ctx context.Context) {
	for _, reader := range c.readers {
		go c.consumeLoop(ctx, reader)
	}
	<-ctx.Done()
	for _, reader := range c.readers {
		reader.Close()
	}
}

func (c *Consumer) consumeLoop(ctx context.Context, reader *kafka.Reader) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("status consumer: fetch error on %s: %v", reader.Config().Topic, err)
			continue
		}

		eventType := headerValue(msg.Headers, "event_type")
		status, known := statusByEventType[eventType]
		if !known {
			reader.CommitMessages(ctx, msg)
			continue
		}

		var event genericEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("status consumer: skipping malformed message: %v", err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		// Setting status is naturally idempotent (re-applying the
		// same status update twice is harmless), so unlike the other
		// services this consumer doesn't need its own
		// processed_events table — the operation itself is safe to
		// repeat, which is the simplest form of idempotency there is.
		if err := c.repo.UpdateStatus(ctx, event.OrderID, status); err != nil {
			log.Printf("status consumer: update failed for order %s: %v", event.OrderID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("status consumer: commit error: %v", err)
		}
	}
}

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
