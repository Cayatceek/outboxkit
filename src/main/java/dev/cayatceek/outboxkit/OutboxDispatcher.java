package dev.cayatceek.outboxkit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * PostgreSQL-specific transactional dispatcher. Row locks plus SKIP LOCKED
 * allow parallel workers without claiming the same row concurrently.
 *
 * Holds DB locks while publishing: use short broker timeouts and small batches.
 */
public final class OutboxDispatcher {
    private static final String LOCK_DUE_SQL = """
        SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
        FROM outbox_events
        WHERE delivered_at IS NULL
        ORDER BY created_at, id
        LIMIT ?
        FOR UPDATE SKIP LOCKED
        """;

    private static final String MARK_DELIVERED_SQL = """
        UPDATE outbox_events SET delivered_at = CURRENT_TIMESTAMP
        WHERE id = ? AND delivered_at IS NULL
        """;

    private final DataSource dataSource;
    private final EventPublisher publisher;

    public OutboxDispatcher(DataSource dataSource, EventPublisher publisher) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
    }

    /**
     * Returns number of committed events. If publisher fails, the batch rolls back.
     * Previously published events may therefore be delivered again on retry.
     */
    public int dispatchBatch(int limit) throws Exception {
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("limit must be between 1 and 1000");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<OutboxEvent> events = loadLocked(connection, limit);
                for (OutboxEvent event : events) {
                    publisher.publish(event);
                    markDelivered(connection, event.id());
                }
                connection.commit();
                return events.size();
            } catch (Exception failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        }
    }

    private static List<OutboxEvent> loadLocked(Connection connection, int limit) throws SQLException {
        List<OutboxEvent> result = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(LOCK_DUE_SQL)) {
            statement.setInt(1, limit);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    result.add(new OutboxEvent(
                        rows.getObject("id", UUID.class),
                        rows.getString("aggregate_type"),
                        rows.getString("aggregate_id"),
                        rows.getString("event_type"),
                        rows.getString("payload"),
                        rows.getTimestamp("created_at").toInstant()));
                }
            }
        }
        return result;
    }

    private static void markDelivered(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(MARK_DELIVERED_SQL)) {
            statement.setObject(1, id);
            if (statement.executeUpdate() != 1) {
                throw new SQLException("Cannot mark outbox event delivered: " + id);
            }
        }
    }
}
