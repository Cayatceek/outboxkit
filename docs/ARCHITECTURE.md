# Transactional outbox guarantees

Writer and outbox insertion **must** use the same JDBC transaction to prevent business/notification divergence.

Dispatcher locks undelivered rows with `FOR UPDATE SKIP LOCKED` while a broker adapter publishes, then marks them delivered and commits. Two active workers cannot lock the same pending row simultaneously.

**Failure window:** the broker may acknowledge an event, but the DB transaction can fail before `delivered_at` commits. Such events will be published again. Consumers must deduplicate by stable event ID and apply business changes idempotently. The dispatcher intentionally does not claim exactly-once semantics.

Long publisher calls hold row locks. Later releases should support lease-based claiming, retry policies and dead-letter handling. This implementation is PostgreSQL-specific and requires proper connection/transaction management.
