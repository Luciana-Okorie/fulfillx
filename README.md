# FulfillX

A distributed, event-driven order fulfillment platform: four
independently deployable Go services connected by Kafka, each with
its own Postgres database, plus an API Gateway with a real-time
WebSocket status stream.

```
API Gateway → Order Service → Kafka → Inventory Service → Kafka
                                    → Payment Service → Kafka
                                    → Fulfillment Service
```

See `docs/architecture.md` for the full diagram and request flow,
`docs/db-schema.md` for each service's tables, `docs/api.md` for the
client-facing REST/WebSocket contract, and `docs/event-contracts.md`
for the Kafka wire formats every service depends on.

## Running the whole stack

```bash
docker compose up --build
```

This brings up Postgres (one database per service, auto-migrated via
`infra/postgres-init`), Redis, a single-broker Kafka (KRaft mode, no
Zookeeper), and all five services. Health check endpoints:

```bash
curl localhost:8000/healthz   # API Gateway
curl localhost:8080/readyz    # Order Service
curl localhost:8081/readyz    # Inventory Service
curl localhost:8082/readyz    # Payment Service
curl localhost:8083/readyz    # Fulfillment Service
```

Place an order through the gateway and watch it flow through the
whole chain:

```bash
curl -s -X POST localhost:8000/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-1" \
  -d '{"customer_id":"cust_1","items":[{"sku":"SKU-1","quantity":1,"unit_price_cents":1000}]}' \
  | tee /tmp/order.json

ORDER_ID=$(jq -r .id /tmp/order.json)
watch -n1 "curl -s localhost:8080/orders/$ORDER_ID/status"
```

Or watch it in real time over WebSocket instead of polling — see
`docs/api.md` for the exact frame format.

Open `http://localhost:16686` for the Jaeger UI and search for
service `order-service` (or any of the others) to see the full trace
for that order — one trace ID spanning the HTTP request through all
four services, described in the "How to trace one order" section
below.

## Kubernetes

```bash
kubectl apply -f k8s/
```

Applies the namespace, ConfigMap/Secret, infra (Postgres, Redis,
Kafka), and all five services with readiness/liveness probes,
resource requests/limits, and an HPA on the Order Service. The
manifests assume images are already built and pushed as
`fulfillx/<service>:latest` — see each service's Dockerfile.

**Production note:** running Postgres and Kafka as in-cluster
Deployments (as these manifests do) is convenient for a self-contained
demo, but a managed Postgres (RDS/Cloud SQL) and managed Kafka
(MSK/Confluent Cloud) would be the real call for anything handling
actual customer orders — stateful infra on Kubernetes is
operationally heavier than most teams want to own.

## CI/CD

Each service has its own `.github/workflows/ci.yml`
(lint → unit tests → build image) for fast per-PR feedback. The root
`.github/workflows/ci.yml` runs the same matrix across all five
services and adds the one thing per-service CI can't express: it
brings up the entire stack with `docker compose` and pushes a real
order through Order → Inventory → Payment → Fulfillment, failing the
build if it doesn't reach `FULFILLED`. That's the actual regression
test for the distributed behavior this project is about — unit tests
alone can't catch a broken event contract between two services.

The `deploy` job is a stub — wiring it to real cluster credentials is
intentionally left out of a demo repo.

## Design decisions and trade-offs

### Why the transactional outbox, everywhere

Every service that publishes an event writes it to an `outbox_events`
row in the *same transaction* as its own state change, then a
background worker polls and publishes. The alternative — publish to
Kafka directly inside the request/consumer handler — has an
unavoidable crash window between "database commit" and "Kafka ack"
where the state change happened but nothing downstream ever finds
out. See `order-service/README.md` for the full walkthrough; the same
reasoning is why Inventory, Payment, and Fulfillment all do it too,
not just the client-facing Order Service.

**Trade-off:** at-least-once delivery, not exactly-once. The outbox
worker can crash between "Kafka ack" and "mark published," causing a
duplicate publish on restart. This is why every consumer is
idempotent — see the next section — rather than trying to chase
exactly-once delivery, which doesn't really exist in a system with
independent failure domains.

### Why every consumer has a `processed_events` table

Kafka redelivers on consumer crash/rebalance; the outbox worker can
also double-publish (above). Either way, "the same event arrives
twice" is not an edge case here — it's an expected, routine
occurrence. Each consuming service checks `processed_events` for
`(order_id, event_type)` inside the *same transaction* as the actual
work, so "already handled" and "do the work" can never disagree with
each other, even across a crash.

### Why overselling is prevented with a row lock, not application code

Inventory Service's `SELECT ... FOR UPDATE`, taken on every SKU in
sorted order before checking availability, is what actually prevents
two concurrent orders from both succeeding on the last unit. See
`inventory-service/README.md` for the full explanation, including why
sorting the lock order matters (deadlock avoidance when two orders
share SKUs).

### Why Postgres, not MongoDB

The core invariants in this system — an order's items existing
alongside its outbox event, an inventory decrement happening alongside
its reservation record, a payment being recorded alongside the event
that announces it — are exactly the kind of multi-row atomicity
transactions exist for. Enforcing them by hand across separate writes
(as you'd have to without multi-document transactions) reintroduces
the partial-write problem the outbox pattern exists to solve.
Postgres's `SELECT ... FOR UPDATE` is also what makes the oversell
fix simple; achieving the same guarantee without real row locks would
mean building a distributed lock (e.g. via Redis) for something a
relational database already does natively.

### Where Redis helps, and what happens when it's unavailable

Redis serves two purposes, both in the Order Service:

- **Idempotency fast path** (`SETNX` lock) — avoids a Postgres round
  trip on every retried request. If Redis is down, the lock
  acquisition fails and the request is rejected with a 500 rather
  than silently skipping the idempotency check — losing this layer
  should be loud, not silent, since it's a correctness guarantee, not
  just a cache.
- **Rate limiting** (fixed-window counter) — **fails open** if Redis
  is unreachable, since a rate limit is a protective measure, not a
  correctness guarantee; letting extra traffic through briefly is a
  much smaller problem than blocking all order creation on a
  non-critical dependency being down.

That asymmetry — fail closed for idempotency, fail open for rate
limiting — is deliberate and is the kind of distinction worth being
able to explain in an interview: not every dependency failure should
be handled the same way, and the right answer depends on what
correctness property that dependency is protecting.

### Why Kafka instead of synchronous HTTP between services

A synchronous call chain (Order → Inventory → Payment → Fulfillment,
each waiting on the last) means the whole chain's latency is additive
and any one service being briefly slow or down blocks order creation
entirely. With Kafka, Order Service returns `201` the moment the
order and its outbox event are committed — inventory reservation,
payment, and fulfillment all happen asynchronously, and if Payment
Service is down for five minutes, `InventoryReserved` events simply
queue up in Kafka and get processed once it's back, rather than every
order placed in that window failing outright.

### How to trace one order across four services

Every hop carries a W3C `traceparent`. The HTTP request into Order
Service starts a trace (via `otelhttp`); when that request writes its
`OrderCreated` outbox row, the current trace context is captured into
a `trace_context` column on the row itself — necessary because the
outbox worker publishes from a different goroutine, seconds or
minutes later, and the only way to keep it in the same trace is to
persist the traceparent alongside the event. The worker resumes that
trace, opens a child span for the Kafka publish, and injects the
(possibly updated) context into the Kafka message headers next to the
existing `event_type` header. Each downstream consumer (Inventory,
Payment, Fulfillment) extracts the traceparent from those headers,
opens its own child span, and — if it writes its own outbox event —
repeats the same capture-into-a-column trick for the next hop. The
result: one trace ID, visible end to end in Jaeger
(`localhost:16686` when running via `docker compose`), spanning the
whole `POST /orders` → `OrderCreated` → `InventoryReserved` →
`PaymentAuthorized` → `FulfillmentCreated` → `OrderReadyForShipment`
chain. See `internal/telemetry/carriers.go` in any service for the
two carrier implementations (`KafkaHeaderCarrier`, `TextCarrier`) that
make this work.

## Failure scenarios and how this system handles them

| Scenario | What happens |
|---|---|
| Kafka redelivers an event | `processed_events` check no-ops the reprocessing |
| Two orders race for the last unit of a SKU | `SELECT...FOR UPDATE` row lock serializes them; exactly one wins |
| Service crashes after committing DB, before Kafka publish | Impossible to lose the event — it's already in `outbox_events`, waiting for the worker |
| Service crashes after Kafka ack, before marking outbox row published | Event is republished on restart; downstream idempotency makes this safe |
| Postgres briefly unavailable | Transaction fails, consumer doesn't commit its Kafka offset, message is retried once Postgres recovers |
| Redis unavailable (idempotency) | Order creation fails closed (500) rather than risk a duplicate |
| Redis unavailable (rate limiting) | Requests are allowed through (fail open) rather than blocking on a non-critical dependency |
| Malformed message on any topic | Logged and skipped rather than wedging the partition forever (a real dead-letter topic is the production fix — see gaps) |

## Known gaps (honest accounting, not just a feature list)

This is what a "production-readiness" README should actually contain
— not a claim that everything's done, but a clear map of what's real
and what's a documented stretch:

- **No dead-letter topic or admin replay endpoint.** Malformed
  messages are currently logged and skipped in place; a real DLQ plus
  a replay endpoint (stretch goal in the brief) isn't built.
- **The WebSocket stream is best-effort**, not guaranteed delivery —
  documented in `docs/api.md`. Fine for a live status indicator, not
  something to build billing logic on top of.
- **Metrics are not implemented**, only traces. OTel spans exist
  across every hop (HTTP → outbox → Kafka → consumer), but there's no
  Prometheus/OTLP metrics export for request rates, queue depth, or
  outbox lag — the "structured telemetry" half of the observability
  ask from the brief is still open.
- **No automated chaos/failure-injection tests** (killing a service
  mid-event, temporarily blocking Postgres) — the failure-mode table
  above is architecturally true given how the code is written, but
  it's asserted, not tested by an automated suite. The integration
  smoke test in root CI proves the happy path end-to-end; it doesn't
  yet inject failures.
- **Payment authorization is simulated**, deterministically, not a
  real processor integration — by design for this project, called out
  explicitly so it's not mistaken for a real payments implementation.
