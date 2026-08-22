-- FulfillX Fulfillment Service schema

CREATE TABLE IF NOT EXISTS fulfillments (
    order_id    UUID PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'CREATED', -- CREATED, READY_FOR_SHIPMENT
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_events (
    order_id     UUID NOT NULL,
    event_type   TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type)
);

-- Even though this is the last service in the chain, it still
-- publishes through an outbox rather than directly, for the same
-- reason every other service does: FulfillmentCreated and
-- OrderReadyForShipment are what the Order Service (and eventually
-- the API Gateway's WebSocket stream) consume to reflect final
-- status, and a lost event here would leave an order stuck showing
-- "payment authorized" forever.
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
