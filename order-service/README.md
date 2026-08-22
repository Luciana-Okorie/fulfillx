# FulfillX — Order Service

The first of four services in FulfillX, a distributed, event-driven
order fulfillment platform. This service owns order creation and is
the entry point for the whole pipeline:

```
POST /orders
     │
     ├─ INSERT order + order_items + outbox_event   (one Postgres transaction)
     │
     └─ 201 response to client
              │
     (async)  ▼
     outbox worker polls outbox_events, publishes OrderCreated to Kafka
              │
              ▼
     Inventory Service consumes OrderCreated  (next service to build)
```

## Why the transactional outbox

The naive approach — `INSERT order; COMMIT; publish to Kafka` — has a
window where the database commit succeeds but the process crashes
before the Kafka publish. The order exists, but no downstream service
ever hears about it. It just silently stalls forever.

The outbox pattern closes that window by writing the event as a row
in the *same transaction* as the order. Either both commit or neither
does — there is no in-between state to crash into. A separate worker
then polls for unpublished rows and pushes them to Kafka, retrying
indefinitely until it succeeds, then marks the row published.

**Trade-off:** this makes delivery *at-least-once*, not exactly-once
— if the worker crashes between "Kafka ack" and "mark published,"
the same event gets republished on restart. That's why every
consumer downstream must be idempotent (see the architecture-level
README once Inventory/Payment/Fulfillment exist). At-least-once +
idempotent consumers = effectively-once, which is the achievable
guarantee in a distributed system — true exactly-once delivery
doesn't exist.

## Why idempotency has two layers

- **Redis (`SETNX` lock)** — fast path. Stops two near-simultaneous
  requests with the same `Idempotency-Key` from both reaching
  Postgres. Cheap, but not durable — a Redis flush loses the lock.
- **Postgres (`idempotency_keys` table)** — durable source of truth.
  Every retry checks here first; if a response was already recorded
  for that key, it's returned verbatim and no new order is created.

Redis alone isn't safe (data loss risk); Postgres alone is safe but
slower under bursty retries. Using both gets correctness from
Postgres and low latency from Redis.

## Why Postgres row-level transactions, not application logic

Order + order_items + outbox_event must either all exist or none do.
Enforcing that in application code (create order, then create items,
then create the event, with manual rollback on failure) is exactly
the kind of multi-step invariant transactions exist to guarantee
atomically. Doing it by hand invites partial-write bugs under crashes
or concurrent requests.

## Redis rate limiting

`100 requests/minute/customer`, fixed-window counter via `INCR` +
`EXPIRE`. Returns `429` when exceeded. **Fails open** — if Redis is
unreachable, requests are allowed through rather than blocking order
creation on a non-critical-path dependency. A fixed window has a
known edge-case (bursts can double near a window boundary); a sliding
window or token bucket would fix that at the cost of more Redis
round-trips, and is a reasonable v2.

## Running locally

```bash
docker compose up --build
```

Then:

```bash
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{"customer_id":"cust_1","items":[{"sku":"SKU-1","quantity":2,"unit_price_cents":1500}]}'

# Re-send the identical request+key — same order, no duplicate:
curl -X POST localhost:8080/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{"customer_id":"cust_1","items":[{"sku":"SKU-1","quantity":2,"unit_price_cents":1500}]}'
```

## What's next (Day 2 of this service's build)

- Inventory Service: consumes `OrderCreated`, reserves stock with
  `SELECT ... FOR UPDATE` to prevent overselling under concurrency.
- OpenTelemetry spans across the HTTP handler → outbox insert → Kafka
  publish, so a single order is traceable end to end.
- Kubernetes manifests (Deployment/Service/ConfigMap/Secret/probes)
  once there's more than one service to orchestrate.
