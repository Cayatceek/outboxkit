package dev.cayatceek.outboxkit;

/**
 * Adapter for Kafka, RabbitMQ, HTTP, etc.
 * Must handle duplicate event IDs: outbox delivery is at-least-once, not exactly-once.
 */
@FunctionalInterface
public interface EventPublisher {
    void publish(OutboxEvent event) throws Exception;
}
