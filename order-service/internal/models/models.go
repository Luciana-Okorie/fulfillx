package models

import "time"

type OrderStatus string

const (
	StatusPending            OrderStatus = "PENDING"
	StatusInventoryReserved  OrderStatus = "INVENTORY_RESERVED"
	StatusPaymentAuthorized  OrderStatus = "PAYMENT_AUTHORIZED"
	StatusFulfilled          OrderStatus = "FULFILLED"
	StatusFailed             OrderStatus = "FAILED"
)

type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type CreateOrderRequest struct {
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
}

type Order struct {
	ID               string      `json:"id"`
	CustomerID       string      `json:"customer_id"`
	Status           OrderStatus `json:"status"`
	TotalAmountCents int64       `json:"total_amount_cents"`
	Items            []OrderItem `json:"items"`
	CreatedAt        time.Time   `json:"created_at"`
	UpdatedAt        time.Time   `json:"updated_at"`
}

// OrderCreated is the event published to Kafka (via the outbox) when
// an order is successfully created. Downstream services (inventory,
// payment, fulfillment) consume this — see the architecture diagram
// in the root README.
type OrderCreated struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	TotalCents int64       `json:"total_amount_cents"`
	OccurredAt time.Time   `json:"occurred_at"`
}
