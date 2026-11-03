package dev.cayatceek.outboxkit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Inserts an event on the SAME connection/transaction as the business write.
 * This class must never open its own transaction or commit a caller transaction.
 */
public final class JdbcOutboxStore {
    public void enqueue(Connection connection, OutboxEvent event) throws SQLException {
        Objects.requireNonNull(connection, "connection");
        Objects.requireNonNull(event, "event");
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("Outbox enqueue requires a caller-owned transaction (autoCommit=false)");
        }
        String sql = """
            INSERT INTO outbox_events
                (id, aggregate_type, aggregate_id, event_type, payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, event.id());
            statement.setString(2, event.aggregateType());
            statement.setString(3, event.aggregateId());
            statement.setString(4, event.eventType());
            statement.setString(5, event.payload());
            statement.setTimestamp(6, Timestamp.from(event.createdAt()));
            int inserted = statement.executeUpdate();
            if (inserted != 1) throw new SQLException("Expected one outbox row; inserted " + inserted);
        }
    }
}
