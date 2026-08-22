# FulfillX — Inventory Service

Consumes `OrderCreated` from Kafka, reserves inventory, and publishes
`InventoryReserved` or `InventoryReservationFailed` via its own
transactional outbox — same pattern as the Order Service.

```
order-events topic
        │
        ▼
OrderCreatedConsumer.handle()
        │
        ▼
ReserveForOrder()  ← everything below happens in ONE Postgres transaction
    │
    ├─ already processed this order_id? → commit, no-op (idempotency)
    ├─ SELECT ... FOR UPDATE on every SKU, sorted order (no deadlocks,
    │  no oversell — see below)
    ├─ enough stock? → decrement, insert reservation, outbox InventoryReserved
    └─ not enough?    → outbox InventoryReservationFailed
        │
        ▼
inventory-events topic (published async by this service's own outbox worker)
```

## How overselling is prevented

Two customers hit "buy" on the last unit of `SKU-2` at the same
instant. Both requests reach `ReserveForOrder` and both start a
transaction. Here's what stops both from succeeding:

`SELECT available_quantity FROM inventory WHERE sku = $1 FOR UPDATE`
takes a row lock. The **second** transaction to reach that line
doesn't error — it **blocks**, waiting for the first transaction to
commit or roll back. Once the first commits (having decremented
`available_quantity` from 1 to 0), the second transaction's `SELECT
... FOR UPDATE` finally returns, sees `0`, and fails the reservation
cleanly. Only one of the two ever gets `InventoryReserved`.

This is the standard way to serialize "check-then-act" logic in
Postgres — the row lock is what turns a race into a queue of one.

**Why sort the SKUs before locking:** if an order needs `[SKU-A,
SKU-B]` and a concurrent order needs `[SKU-B, SKU-A]`, locking in
whatever order the items happen to arrive can deadlock — transaction
1 holds A and waits for B while transaction 2 holds B and waits for
A. Always locking in the same (sorted) order across every
transaction eliminates that: everyone queues for A before B, so
there's no cycle to deadlock on.

## Why the consumer is also idempotent, not just relying on Kafka

Kafka's delivery guarantee here is at-least-once: the offset is
committed *after* successful processing (see `consumer.Run`), so a
crash between "reserved inventory" and "commit offset" means the
same message gets redelivered. If `ReserveForOrder` just blindly
decremented on every call, that redelivery would double-reserve.

The `processed_events` table (checked and written inside the same
transaction as the reservation) is what actually makes redelivery
safe: the second delivery sees `already processed` and no-ops. This
is the same idea as the Order Service's idempotency-key table, one
layer downstream — every consumer in this system dedupes on the
event it's consuming, not just the producer deduping on the way out.

## Failure scenarios this design handles

| Scenario | What happens |
|---|---|
| Kafka redelivers `OrderCreated` | `processed_events` check no-ops the second delivery |
| Two orders race for the last unit | Row lock serializes them; one wins, one gets `InventoryReservationFailed` |
| Service crashes after reserving, before committing offset | Kafka redelivers on restart; idempotency check makes it a no-op |
| Postgres unavailable mid-reserve | Transaction fails, offset isn't committed, message is retried once Postgres recovers |
| Malformed message on the topic | Logged and skipped (would route to a dead-letter topic in production) rather than wedging the partition forever |

## Running locally

```bash
docker compose up --build
```

Seed data (`migrations/0001_init.sql`) includes `SKU-2` with
`available_quantity = 1` specifically to make the last-unit race easy
to test manually: fire two `OrderCreated`-shaped messages at the
`order-events` topic for `SKU-2` at the same time and confirm exactly
one reservation succeeds.

## What's next

- Payment Service: consumes `InventoryReserved`, simulates
  authorization, idempotent on replayed events (same pattern as
  here).
- Dead-letter topic + admin replay endpoint (stretch goal) once
  there's a real failure mode worth routing around instead of just
  retrying forever.
