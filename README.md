# OutboxKit

A small **PostgreSQL transactional outbox** library for Java 21. Write a business change and an outbox event in the same database transaction, then publish committed events with **at-least-once** delivery.

**Status:** 0.1.0-SNAPSHOT, experimental, not published to Maven Central. No runtime dependencies: provide your JDBC driver and DataSource.

## Apply schema

Use [schema-postgresql.sql](src/main/resources/schema-postgresql.sql) through Flyway/Liquibase.

## Write events atomically

```java
var event = OutboxEvent.create("wallet", walletId, "BALANCE_CHANGED", jsonPayload);

try (Connection conn = dataSource.getConnection()) {
    conn.setAutoCommit(false);
    try {
        updateBusinessRecord(conn); // same connection and transaction
        new JdbcOutboxStore().enqueue(conn, event);
        conn.commit();
    } catch (Exception failure) {
        conn.rollback();
        throw failure;
    }
}
```

**Do not** send an event from a separate transaction, and do not set `autoCommit=true` for `enqueue`. Spring projects should pass the transaction-bound JDBC connection.

## Dispatch events

```java
EventPublisher kafkaPublisher = event -> broker.publish(
    event.eventType(), event.payload(), event.id().toString());

int delivered = new OutboxDispatcher(dataSource, kafkaPublisher).dispatchBatch(50);
```

Use an application scheduler to call `dispatchBatch` repeatedly. The consumer must deduplicate by `event.id()`. Publishing may succeed while the DB transaction later rolls back, causing duplicates on retry.

## Guarantees and limits

- Multiple workers claim different pending rows with PostgreSQL `FOR UPDATE SKIP LOCKED`.
- Publish + DB acknowledgment run **inside one transaction**. A batch failure rolls back all its delivery acknowledgments.
- At-least-once delivery only; **no exactly-once guarantee**. A poison event currently blocks its batch until repaired.
- Slow brokers hold database row locks: use small batches and bounded publisher timeouts.
- No Kafka adapter, retention policy, retry/backoff schedule, dead-letter queue or metrics in v0.1.
- Payloads are plaintext: do not store secrets in the outbox.

## Build and test

```bash
# Tests require local PostgreSQL / environment credentials:
export TEST_PG_URL=jdbc:postgresql://localhost:5432/outboxkit
export TEST_PG_USER=test
export TEST_PG_PASSWORD=test
mvn verify
```

GitHub Actions provisions a PostgreSQL service automatically. See [architecture](docs/ARCHITECTURE.md), [roadmap](docs/ROADMAP.md). GPL-3.0 (LICENSE).
