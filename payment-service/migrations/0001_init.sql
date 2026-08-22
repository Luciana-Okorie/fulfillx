-- FulfillX Payment Service schema

-- Simulated payment authorizations. A real integration would call out
-- to a processor and store its transaction id; here `authorized` is
-- decided deterministically from order_id so the demo is reproducible
-- (see internal/db/payments.go).
CREATE TABLE IF NOT EXISTS payments (
    order_id        UUID PRIMARY KEY,
    amount_cents    BIGINT NOT NULL,
    authorized      BOOLEAN NOT NULL,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumer-side idempotency for InventoryReserved: guarantees that
-- replaying the same event (Kafka's at-least-once delivery) can never
-- charge — or fail-charge — the same order twice.
CREATE TABLE IF NOT EXISTS processed_events (
    order_id     UUID NOT NULL,
    event_type   TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    published      BOOLEAN NOT NULL DEFAULT FALSE,
    trace_context  TEXT,        -- W3C traceparent captured at insert time, so the outbox worker can resume the same trace when it publishes to Kafka minutes later
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (id)
    WHERE published = FALSE;
