# FulfillX — API Documentation

All client traffic goes through the **API Gateway** (`:8000`), which
proxies REST calls to the Order Service and serves the WebSocket
status stream directly. The Order Service is also reachable directly
on `:8080` for local debugging.

## `POST /orders`

Create an order.

**Headers**
| Header | Required | Notes |
|---|---|---|
| `Content-Type` | yes | `application/json` |
| `Idempotency-Key` | recommended | see below |
| `X-Customer-ID` | no | used for rate limiting; falls back to remote address |

**Body**
```json
{
  "customer_id": "cust_123",
  "items": [
    { "sku": "SKU-1", "quantity": 2, "unit_price_cents": 1500 }
  ]
}
```

**Responses**
| Status | Meaning |
|---|---|
| `201 Created` | Order created; body is the order (`id`, `status: "PENDING"`, ...) |
| `200/201 + Idempotent-Replay: true` | Same key + same body as a prior request — the original response is returned verbatim, no new order created |
| `409 Conflict` | Same `Idempotency-Key` reused with a **different** body, or a concurrent request with the same key is still in flight (check `Retry-After`) |
| `400 Bad Request` | Missing `customer_id` or empty `items` |
| `429 Too Many Requests` | Rate limit exceeded (100 req/min/customer) |

**Idempotency contract:** send the same `Idempotency-Key` on retry
(e.g. after a timeout where you're not sure the first request
landed) with the *identical* request body. You will get back the
original order, never a duplicate. Reusing a key with a different
body is rejected rather than silently processing the new body or
silently returning the old response — either would hide a client bug.

## `GET /orders/{id}`

Returns the full order, including line items.

| Status | Meaning |
|---|---|
| `200 OK` | Order found |
| `404 Not Found` | No such order |

## `GET /orders/{id}/status`

Lightweight status-only endpoint, for polling.

```json
{
  "order_id": "…",
  "status": "PAYMENT_AUTHORIZED",
  "updated_at": "2026-08-20T10:15:32Z"
}
```

`status` progresses through: `PENDING` → `INVENTORY_RESERVED` →
`PAYMENT_AUTHORIZED` → `FULFILLMENT_CREATED` → `FULFILLED`, or jumps
to `FAILED` at any point inventory or payment can't be satisfied. See
`event-contracts.md` for exactly which event causes which transition.

## `GET /ws/orders/{id}` (WebSocket, via API Gateway)

Real-time alternative to polling `/status`. Upgrade the connection,
then receive one JSON frame per lifecycle event for that order:

```json
{ "order_id": "…", "event_type": "InventoryReserved" }
{ "order_id": "…", "event_type": "PaymentAuthorized" }
{ "order_id": "…", "event_type": "FulfillmentCreated" }
{ "order_id": "…", "event_type": "OrderReadyForShipment" }
```

This connection is server-push only — the gateway does not expect or
act on any client messages, and treats a broken connection as a
signal to stop pushing (see `api-gateway/internal/wsstream`).

**Note:** this is best-effort. If the gateway restarts or drops a
message, the client will not get a retroactive replay of missed
events — poll `GET /orders/:id/status` for the current state if you
need a guaranteed-consistent read, and treat the WebSocket purely as
a low-latency notification channel on top of it.

## Health endpoints (every service)

| Path | Meaning |
|---|---|
| `GET /healthz` | Process is up (liveness) |
| `GET /readyz` | Process is up **and** its database connection is healthy (readiness) — Order/Inventory/Payment/Fulfillment only |
