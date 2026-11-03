# Roadmap

- [x] Atomic outbox insert using caller's business transaction
- [x] PostgreSQL SKIP LOCKED dispatcher and explicit at-least-once semantics
- [x] Real PostgreSQL service integration tests in GitHub Actions
- [ ] Per-event retry/backoff and poison-event handling
- [ ] Dead-letter queue, retention and schema migrations
- [ ] Kafka / RabbitMQ adapters and Spring Boot auto-configuration
- [ ] Bounded publisher call timeouts and metrics
- [ ] Multi-worker load/performance and crash-injection tests
