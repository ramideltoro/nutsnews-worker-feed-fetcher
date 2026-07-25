# Architecture Notes

## Scope

The feed fetcher owns the worker-uplift service boundary that consumes `feedFetchRequest` messages on the contracted `fetch` route and prepares the route boundary for future `feedFetchResult` output to canonicalization. Issue #95 is a deployable shell only; feed HTTP fetching, RSS/Atom parsing, durable fetch metadata, candidate fan-out, retry classification, and DLQ policy are implemented by later issues.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Input route boundary: `getWorkerRoute("fetch")`
- Future output route boundary: `getWorkerRoute("canonicalization")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle

## Bootstrap Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Start the shared broker lifecycle and assert fetch/canonicalization topology.
4. Register a `fetch` consumer through the shared runtime message processor.
5. Validate incoming envelopes and feed-fetch payloads before any handler work.
6. Claim the durable idempotency interface before delegating to the injected handler.
7. Drain in-flight handlers before broker shutdown.

The local bootstrap handler does not perform network fetches, parse feeds, persist feed metadata, call AI providers, translate content, or publish articles.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- HTTP client;
- DNS policy;
- durable state/idempotency;
- fetch work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies real database and RabbitMQ values later.

## Safety Bounds

`NUTSNEWS_FETCHER_CONCURRENCY` caps concurrent feed-fetch handlers. `NUTSNEWS_FETCHER_PREFETCH` must be greater than or equal to concurrency. HTTP policy settings define connect/read/total timeouts, redirect count, and response-size limits before the implementation issue wires real fetch behavior.

`NUTSNEWS_FETCHER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.
