# FulfillX — Database Schema

Each service owns its own Postgres database (`fulfillx_order`,
`fulfillx_inventory`, `fulfillx_payment`, `fulfillx_fulfillment`).
Nothing outside a service's own code ever writes to its tables.

## Order Service — `fulfillx_order`

```mermaid
erDiagram
    orders ||--o{ order_items : contains
    orders {
        uuid id PK
        text customer_id
        text status
        bigint total_amount_cents
        timestamptz created_at
        timestamptz updated_at
    }
    order_items {
        uuid id PK
        uuid order_id FK
        text sku
        int quantity
        bigint unit_price_cents
    }
    idempotency_keys {
        text key PK
        text request_hash
        int response_status
        jsonb response_body
        uuid order_id FK
        timestamptz created_at
    }
    outbox_events {
        bigserial id PK
        text aggregate_type
        uuid aggregate_id
        text event_type
        jsonb payload
        boolean published
        timestamptz created_at
        timestamptz published_at
    }
```

`orders.status` is written by two different code paths: the order
handler sets it to `PENDING` at creation, and the status-consumer
(reading inventory/payment/fulfillment events) advances it from
there. This is a deliberate exception to "one writer per table" — the
status-consumer only ever calls a single-column `UPDATE`, which is
naturally idempotent, so having two writers doesn't create a
correctness problem the way it would for `total_amount_cents` or
`order_items`.

## Inventory Service — `fulfillx_inventory`

```mermaid
erDiagram
    inventory ||--o{ reservations : "reserved via"
    inventory {
        text sku PK
        int available_quantity
        int reserved_quantity
        timestamptz updated_at
    }
    reservations {
        uuid id PK
        uuid order_id
        text sku FK
        int quantity
        timestamptz created_at
    }
    processed_events {
        uuid order_id PK
        text event_type PK
        timestamptz processed_at
    }
    outbox_events {
        bigserial id PK
        text aggregate_type
        uuid aggregate_id
        text event_type
        jsonb payload
        boolean published
        timestamptz created_at
        timestamptz published_at
    }
```

`available_quantity >= 0` is a database-level `CHECK` constraint, not
just an application-level assumption — even a bug that skipped the
row lock would be stopped from writing negative inventory at the
schema layer.

## Payment Service — `fulfillx_payment`

```mermaid
erDiagram
    payments {
        uuid order_id PK
        bigint amount_cents
        boolean authorized
        text failure_reason
        timestamptz created_at
    }
    processed_events {
        uuid order_id PK
        text event_type PK
        timestamptz processed_at
    }
    outbox_events {
        bigserial id PK
        text aggregate_type
        uuid aggregate_id
        text event_type
        jsonb payload
        boolean published
        timestamptz created_at
        timestamptz published_at
    }
```

## Fulfillment Service — `fulfillx_fulfillment`

```mermaid
erDiagram
    fulfillments {
        uuid order_id PK
        text status
        timestamptz created_at
        timestamptz updated_at
    }
    processed_events {
        uuid order_id PK
        text event_type PK
        timestamptz processed_at
    }
    outbox_events {
        bigserial id PK
        text aggregate_type
        uuid aggregate_id
        text event_type
        jsonb payload
        boolean published
        timestamptz created_at
        timestamptz published_at
    }
```

## Recurring pattern: `outbox_events` + `processed_events`

Three of the four databases have an almost-identical pair of tables.
That repetition is intentional, not an oversight — it's the same
correctness pattern applied at every hop in the chain:

- `outbox_events` — write-side guarantee: "this event will be
  published, even if the service crashes right after committing."
- `processed_events` — read-side guarantee: "this event has already
  been handled, so reprocessing it is always safe."

A shared library could factor these out, but each service keeps its
own copy deliberately — see the note in
`inventory-service/internal/models/models.go` about services owning
their own view of the contracts they depend on, rather than sharing
a Go module that would couple their deploy cadence together.
