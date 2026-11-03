package dev.cayatceek.outboxkit;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable event record. Event ID is also a deduplication key for subscribers. */
public record OutboxEvent(
    UUID id, String aggregateType, String aggregateId, String eventType,
    String payload, Instant createdAt
) {
    public OutboxEvent {
        Objects.requireNonNull(id, "id");
        aggregateType = nonBlank(aggregateType, "aggregateType");
        aggregateId = nonBlank(aggregateId, "aggregateId");
        eventType = nonBlank(eventType, "eventType");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(createdAt, "createdAt");
    }

    public static OutboxEvent create(String aggregateType, String aggregateId,
                                      String eventType, String payload) {
        return new OutboxEvent(UUID.randomUUID(), aggregateType, aggregateId,
            eventType, payload, Instant.now());
    }

    private static String nonBlank(String value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isBlank()) throw new IllegalArgumentException(label + " cannot be blank");
        return value;
    }
}
