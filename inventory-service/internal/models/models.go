package models

import "time"

type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// OrderCreated mirrors the event published by the Order Service.
// Keeping a local copy (rather than a shared module) is a deliberate
// choice for this project: each service owns its view of the event
// contract it consumes, so the two services can evolve independently
// as long as the wire format (documented in the root event-contracts
// doc) doesn't break. A shared Go module is the alternative,
// trading independence for less duplication.
type OrderCreated struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	TotalCents int64       `json:"total_amount_cents"`
	OccurredAt time.Time   `json:"occurred_at"`
}

type InventoryReserved struct {
	OrderID    string      `json:"order_id"`
	Items      []OrderItem `json:"items"`
	OccurredAt time.Time   `json:"occurred_at"`
}

type InventoryReservationFailed struct {
	OrderID    string   `json:"order_id"`
	Reason     string   `json:"reason"`
	FailedSKUs []string `json:"failed_skus"`
	OccurredAt time.Time `json:"occurred_at"`
}
