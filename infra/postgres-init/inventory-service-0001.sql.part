-- FulfillX Inventory Service schema

CREATE TABLE IF NOT EXISTS inventory (
    sku                 TEXT PRIMARY KEY,
    available_quantity  INT NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity   INT NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed data for local testing.
INSERT INTO inventory (sku, available_quantity) VALUES
    ('SKU-1', 10),
    ('SKU-2', 1)   -- deliberately scarce, for the "last unit" race test
ON CONFLICT (sku) DO NOTHING;

CREATE TABLE IF NOT EXISTS reservations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL,
    sku         TEXT NOT NULL REFERENCES inventory(sku),
    quantity    INT NOT NULL CHECK (quantity > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumer-side idempotency: the outbox worker delivers OrderCreated
-- at-least-once, so the same event can arrive twice (or more) after a
-- crash/retry. This table is the dedupe key. We process an event and
-- record it in the SAME transaction as the inventory update, so a
-- crash mid-processing can never leave "reserved inventory" without
-- "marked processed," or vice versa.
CREATE TABLE IF NOT EXISTS processed_events (
    order_id    UUID NOT NULL,
    event_type  TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type)
);

-- This service's own transactional outbox, for InventoryReserved /
-- InventoryReservationFailed, published downstream to Payment.
CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    published       BOOLEAN NOT NULL DEFAULT FALSE,
    trace_context   TEXT,        -- W3C traceparent captured at insert time, so the outbox worker can resume the same trace when it publishes to Kafka
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (id)
    WHERE published = FALSE;
