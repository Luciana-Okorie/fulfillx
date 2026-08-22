package wsstream

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	kafka "github.com/segmentio/kafka-go"
)

// Hub fans out order-lifecycle events to WebSocket clients subscribed
// to a specific order id. This is the stretch-goal piece: a customer
// watching `GET /ws/orders/{id}` sees each stage —
// InventoryReserved -> PaymentAuthorized -> FulfillmentCreated ->
// OrderReadyForShipment — arrive in real time instead of polling
// GET /orders/:id/status.
type Hub struct {
	mu          sync.Mutex
	subscribers map[string][]*websocket.Conn // order_id -> connections
	upgrader    websocket.Upgrader
}

func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string][]*websocket.Conn),
		upgrader: websocket.Upgrader{
			// Demo-scope CORS: accept any origin. A production gateway
			// would check against a configured allowlist here.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

type statusEvent struct {
	OrderID   string `json:"order_id"`
	EventType string `json:"event_type"`
}

// ServeWS upgrades the connection and registers it under the order id
// path parameter. The connection is cleaned up on close/error.
func (h *Hub) ServeWS(orderID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := h.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("wsstream: upgrade failed: %v", err)
			return
		}

		h.mu.Lock()
		h.subscribers[orderID] = append(h.subscribers[orderID], conn)
		h.mu.Unlock()

		// Block reading (and discarding) client frames so we notice
		// disconnects promptly; this hub is server-push only.
		go func() {
			defer h.remove(orderID, conn)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}
}

func (h *Hub) remove(orderID string, conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	conns := h.subscribers[orderID]
	for i, c := range conns {
		if c == conn {
			h.subscribers[orderID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	conn.Close()
}

func (h *Hub) broadcast(orderID string, payload []byte) {
	h.mu.Lock()
	conns := append([]*websocket.Conn{}, h.subscribers[orderID]...)
	h.mu.Unlock()

	for _, conn := range conns {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			h.remove(orderID, conn)
		}
	}
}

// Consumer reads every downstream topic and forwards matching events
// to the Hub. It never blocks order processing itself — this is a
// pure fan-out reader, one consumer group per gateway instance so
// horizontally scaling the gateway doesn't compete for partitions
// with the actual processing services.
type Consumer struct {
	readers []*kafka.Reader
	hub     *Hub
}

func NewConsumer(brokers []string, topics []string, groupID string, hub *Hub) *Consumer {
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
	return &Consumer{readers: readers, hub: hub}
}

func (c *Consumer) Run(ctx context.Context) {
	for _, reader := range c.readers {
		go c.loop(ctx, reader)
	}
	<-ctx.Done()
	for _, reader := range c.readers {
		reader.Close()
	}
}

func (c *Consumer) loop(ctx context.Context, reader *kafka.Reader) {
	for {
		// Gateway fan-out is best-effort: ReadMessage (not
		// FetchMessage+Commit) auto-commits, so a dropped connection
		// here never blocks or replays into the actual business
		// services — it only affects what the WebSocket clients see.
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("wsstream consumer: read error on %s: %v", reader.Config().Topic, err)
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(msg.Value, &raw); err != nil {
			continue
		}
		orderID, _ := raw["order_id"].(string)
		if orderID == "" {
			continue
		}

		eventType := headerValue(msg.Headers, "event_type")
		out, _ := json.Marshal(statusEvent{OrderID: orderID, EventType: eventType})
		c.hub.broadcast(orderID, out)
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
