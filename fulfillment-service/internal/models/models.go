package models

import "time"

type PaymentAuthorized struct {
	OrderID     string    `json:"order_id"`
	AmountCents int64     `json:"amount_cents"`
	OccurredAt  time.Time `json:"occurred_at"`
}

type FulfillmentCreated struct {
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

type OrderReadyForShipment struct {
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
}
