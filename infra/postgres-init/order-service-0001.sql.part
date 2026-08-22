-- FulfillX Order Service schema
-- Run against a fresh Postgres database.

CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, INVENTORY_RESERVED, PAYMENT_AUTHORIZED, FULFILLED, FAILED
    total_amount_cents BIGINT NOT NULL,
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku         TEXT NOT NULL,
    quantity    INT NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL
);

-- Belt-and-suspenders idempotency record. Redis holds the fast-path
-- lock (see internal/idempotency), this table is the durable source
-- of truth so a Redis flush can never cause a duplicate order.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,       -- hash of the request body, to detect key reuse with a different payload
    response_status INT,
    response_body   JSONB,
    order_id        UUID REFERENCES orders(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox. Written in the SAME transaction as the order
-- insert, so a crash between "commit" and "publish to Kafka" is
-- impossible to observe: either both rows exist, or neither does.
CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,       -- e.g. 'order'
    aggregate_id    UUID NOT NULL,       -- the order id
    event_type      TEXT NOT NULL,       -- e.g. 'OrderCreated'
    payload         JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    trace_context   TEXT,        -- W3C traceparent captured at insert time, so the outbox worker can resume the same trace when it publishes to Kafka
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

-- The outbox worker polls this index for unpublished rows in order.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (id)
    WHERE published = FALSE;

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);
