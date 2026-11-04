package dev.cayatceek.outboxkit;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

class PostgresOutboxIntegrationTest {
    private static PGSimpleDataSource dataSource;

    @BeforeAll
    static void setUpSchema() throws Exception {
        dataSource = new PGSimpleDataSource();
        dataSource.setURL(System.getenv().getOrDefault("TEST_PG_URL",
            "jdbc:postgresql://localhost:5432/outboxkit"));
        dataSource.setUser(System.getenv().getOrDefault("TEST_PG_USER", "test"));
        dataSource.setPassword(System.getenv().getOrDefault("TEST_PG_PASSWORD", "test"));

        try (Connection conn = dataSource.getConnection(); var statement = conn.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS ledger (id UUID PRIMARY KEY)");
            statement.execute("""
                CREATE TABLE IF NOT EXISTS outbox_events (
                  id UUID PRIMARY KEY, aggregate_type VARCHAR(200) NOT NULL,
                  aggregate_id VARCHAR(200) NOT NULL, event_type VARCHAR(200) NOT NULL,
                  payload TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
                  delivered_at TIMESTAMPTZ)
                """);
        }
    }

    @BeforeEach
    void clearDb() throws Exception {
        try (Connection conn = dataSource.getConnection(); var statement = conn.createStatement()) {
            statement.execute("TRUNCATE TABLE outbox_events, ledger");
        }
    }

    @Test
    void enqueueSharesTransactionWithBusinessWrite() throws Exception {
        JdbcOutboxStore store = new JdbcOutboxStore();
        UUID businessId = UUID.randomUUID();
        try (Connection conn = dataSource.getConnection()) {
            assertThrows(IllegalStateException.class,
                () -> store.enqueue(conn, OutboxEvent.create("ledger", businessId.toString(), "CREATED", "{}")));
            conn.setAutoCommit(false);
            try (var statement = conn.prepareStatement("INSERT INTO ledger (id) VALUES (?)")) {
                statement.setObject(1, businessId);
                statement.executeUpdate();
            }
            store.enqueue(conn, OutboxEvent.create("ledger", businessId.toString(), "CREATED", "{}"));
            conn.rollback();
        }
        assertEquals(0, count("ledger"));
        assertEquals(0, count("outbox_events"));
    }

    @Test
    void committedBusinessTransactionPersistsBothBusinessAndEvent() throws Exception {
        UUID ledgerId = UUID.randomUUID();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (var statement = conn.prepareStatement("INSERT INTO ledger (id) VALUES (?)")) {
                statement.setObject(1, ledgerId);
                statement.executeUpdate();
            }
            new JdbcOutboxStore().enqueue(conn, OutboxEvent.create(
                "ledger", ledgerId.toString(), "CREATED", "{}"));
            conn.commit();
        }
        assertEquals(1, count("ledger"));
        assertEquals(1, count("outbox_events"));
    }

    @Test
    void uncheckedPublisherFailureDoesNotCommitDeliveryAcknowledgment() throws Exception {
        enqueueCommitted(OutboxEvent.create("wallet", "w-failure", "UPDATED", "{}"));
        OutboxDispatcher dispatcher = new OutboxDispatcher(dataSource, event -> {
            throw new AssertionError("simulated unchecked publisher failure");
        });
        assertThrows(AssertionError.class, () -> dispatcher.dispatchBatch(1));
        assertEquals(1, countPending());
        assertEquals(1, new OutboxDispatcher(dataSource, event -> { }).dispatchBatch(1));
        assertEquals(0, countPending());
    }

    @Test
    void committedEventIsPublishedOnceAndMarkedDelivered() throws Exception {
        OutboxEvent event = OutboxEvent.create("wallet", "w-1", "BALANCE_CHANGED", "{\"amount\":10}");
        enqueueCommitted(event);
        List<OutboxEvent> published = new ArrayList<>();
        OutboxDispatcher dispatcher = new OutboxDispatcher(dataSource, published::add);
        assertEquals(1, dispatcher.dispatchBatch(100));
        assertEquals(0, dispatcher.dispatchBatch(100));
        assertEquals(List.of(event.id()), published.stream().map(OutboxEvent::id).toList());
        assertEquals(0, countPending());
    }

    @Test
    void failedPublishRollsBackAndCanBeRetried() throws Exception {
        OutboxEvent event = OutboxEvent.create("wallet", "w-2", "CHANGED", "{}");
        enqueueCommitted(event);
        OutboxDispatcher failing = new OutboxDispatcher(dataSource, e -> {
            throw new java.io.IOException("simulated broker failure");
        });
        assertThrows(java.io.IOException.class, () -> failing.dispatchBatch(10));
        assertEquals(1, countPending());
        List<UUID> seen = new ArrayList<>();
        assertEquals(1, new OutboxDispatcher(dataSource, e -> seen.add(e.id())).dispatchBatch(10));
        assertEquals(List.of(event.id()), seen);
        assertEquals(0, countPending());
    }

    @Test
    void publishedThenFailedBatchMayBeDeliveredAgain() throws Exception {
        OutboxEvent one = new OutboxEvent(UUID.randomUUID(), "wallet", "w-3", "A", "{}",
            Instant.parse("2026-01-01T00:00:00Z"));
        OutboxEvent two = new OutboxEvent(UUID.randomUUID(), "wallet", "w-3", "B", "{}",
            Instant.parse("2026-01-01T00:00:01Z"));
        enqueueCommitted(one);
        enqueueCommitted(two);
        List<UUID> sideEffects = new ArrayList<>();
        OutboxDispatcher first = new OutboxDispatcher(dataSource, e -> {
            sideEffects.add(e.id());
            if (e.id().equals(two.id())) throw new SQLException("simulated second publish failure");
        });
        assertThrows(SQLException.class, () -> first.dispatchBatch(10));
        assertEquals(2, countPending());
        assertEquals(2, new OutboxDispatcher(dataSource, e -> sideEffects.add(e.id())).dispatchBatch(10));
        assertEquals(4, sideEffects.size());
        assertEquals(sideEffects.get(0), sideEffects.get(2));
        assertEquals(0, countPending());
    }

    @Test
    void concurrentDispatchersSkipRowsLockedByAnotherWorker() throws Exception {
        OutboxEvent earlier = new OutboxEvent(UUID.randomUUID(), "wallet", "w-4", "FIRST", "{}",
            Instant.parse("2026-02-01T00:00:00Z"));
        OutboxEvent later = new OutboxEvent(UUID.randomUUID(), "wallet", "w-4", "SECOND", "{}",
            Instant.parse("2026-02-01T00:00:01Z"));
        enqueueCommitted(earlier);
        enqueueCommitted(later);
        var enteredPublisher = new java.util.concurrent.CountDownLatch(1);
        var releasePublisher = new java.util.concurrent.CountDownLatch(1);
        var seen = java.util.concurrent.ConcurrentHashMap.<UUID>newKeySet();

        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var first = executor.submit(() -> new OutboxDispatcher(dataSource, event -> {
                enteredPublisher.countDown();
                if (!releasePublisher.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new java.io.IOException("Publisher latch timed out");
                }
                seen.add(event.id());
            }).dispatchBatch(1));
            try {
                assertTrue(enteredPublisher.await(5, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals(1, new OutboxDispatcher(dataSource, event -> seen.add(event.id()))
                    .dispatchBatch(1));
            } finally {
                releasePublisher.countDown();
            }
            assertEquals(1, first.get(5, java.util.concurrent.TimeUnit.SECONDS));
        }
        assertEquals(java.util.Set.of(earlier.id(), later.id()), seen);
        assertEquals(0, countPending());
    }

    private static void enqueueCommitted(OutboxEvent event) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            new JdbcOutboxStore().enqueue(conn, event);
            conn.commit();
        }
    }

    private static long count(String table) throws Exception {
        // Only fixed internal table names: never pass user input here.
        try (Connection conn = dataSource.getConnection();
             var statement = conn.createStatement();
             var result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            result.next();
            return result.getLong(1);
        }
    }

    private static long countPending() throws Exception {
        try (Connection conn = dataSource.getConnection();
             var statement = conn.createStatement();
             var result = statement.executeQuery(
                 "SELECT COUNT(*) FROM outbox_events WHERE delivered_at IS NULL")) {
            result.next();
            return result.getLong(1);
        }
    }
}
