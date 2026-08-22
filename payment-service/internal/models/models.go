package models

import "time"

type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type InventoryReserved struct {
	OrderID    string      `json:"order_id"`
	Items      []OrderItem `json:"items"`
	OccurredAt time.Time   `json:"occurred_at"`
}

type PaymentAuthorized struct {
	OrderID    string    `json:"order_id"`
	AmountCents int64    `json:"amount_cents"`
	OccurredAt time.Time `json:"occurred_at"`
}

type PaymentFailed struct {
	OrderID    string    `json:"order_id"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}
