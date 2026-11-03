-- PostgreSQL 15+; apply via Flyway/Liquibase before using the library.
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(200) NOT NULL,
    aggregate_id VARCHAR(200) NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_undelivered
    ON outbox_events (created_at, id) WHERE delivered_at IS NULL;
