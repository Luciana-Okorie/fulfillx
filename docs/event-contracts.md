# FulfillX — Event Contracts

Every event is a JSON message on Kafka. The Kafka message **key** is
always the `order_id` (as a string), so all events for one order land
on the same partition and are delivered in order to any single
consumer. Every message also carries an `event_type` header — this is
what lets one topic safely carry more than one event type (e.g.
`inventory-events` carries both `InventoryReserved` and
`InventoryReservationFailed`) without consumers needing to inspect
the payload just to route.

Delivery is **at-least-once** everywhere. Every consumer in this
system is written assuming any event may arrive more than once — see
each service's `processed_events` table.

---

## Topic: `order-events`

Published by: Order Service. Consumed by: Inventory Service, Order
Service's own status-consumer (no-op for this topic today, included
for symmetry).

### `OrderCreated`
```json
{
  "order_id": "uuid",
  "customer_id": "string",
  "items": [
    { "sku": "string", "quantity": 1, "unit_price_cents": 1500 }
  ],
  "total_amount_cents": 1500,
  "occurred_at": "2026-08-20T10:00:00Z"
}
```

---

## Topic: `inventory-events`

Published by: Inventory Service. Consumed by: Payment Service, Order
Service's status-consumer.

### `InventoryReserved`
```json
{
  "order_id": "uuid",
  "items": [ { "sku": "string", "quantity": 1, "unit_price_cents": 1500 } ],
  "occurred_at": "2026-08-20T10:00:01Z"
}
```

### `InventoryReservationFailed`
```json
{
  "order_id": "uuid",
  "reason": "insufficient_inventory",
  "failed_skus": ["SKU-2"],
  "occurred_at": "2026-08-20T10:00:01Z"
}
```
Terminal for the order — Payment Service ignores this event type
entirely (it only acts on `InventoryReserved`); the Order Service's
status-consumer marks the order `FAILED`.

---

## Topic: `payment-events`

Published by: Payment Service. Consumed by: Fulfillment Service,
Order Service's status-consumer.

### `PaymentAuthorized`
```json
{
  "order_id": "uuid",
  "amount_cents": 1500,
  "occurred_at": "2026-08-20T10:00:02Z"
}
```

### `PaymentFailed`
```json
{
  "order_id": "uuid",
  "reason": "issuer_declined",
  "occurred_at": "2026-08-20T10:00:02Z"
}
```
Terminal for the order — same pattern as `InventoryReservationFailed`.
Note this is a **simulated** decline (deterministic per `order_id`,
not a real processor call); see `payment-service/README.md`.

---

## Topic: `fulfillment-events`

Published by: Fulfillment Service. Consumed by: Order Service's
status-consumer, API Gateway's WebSocket fan-out.

### `FulfillmentCreated`
```json
{ "order_id": "uuid", "occurred_at": "2026-08-20T10:00:03Z" }
```

### `OrderReadyForShipment`
```json
{ "order_id": "uuid", "occurred_at": "2026-08-20T10:00:03Z" }
```
Both are emitted together by the current Fulfillment Service (see its
README for why); a real warehouse integration would separate them in
time.

---

## Compatibility rules

- **Adding an optional field** to any payload is backward compatible
  — existing consumers that don't know about it simply ignore it
  (Go's `encoding/json` skips unknown fields by default).
- **Removing or renaming a field**, or **changing a field's type**, is
  a breaking change and requires either a new event type (e.g.
  `OrderCreatedV2`) or a coordinated multi-service deploy. Given each
  service defines its own copy of the event structs (rather than a
  shared module — see `docs/architecture.md`), a breaking change is
  caught at the consuming service's own compile/test time, not
  silently at runtime.
- **New event types** on an existing topic are safe to introduce as
  long as every consumer of that topic checks the `event_type` header
  and explicitly ignores types it doesn't recognize (every consumer in
  this codebase already does this — see e.g.
  `payment-service/internal/consumer/consumer.go`).
