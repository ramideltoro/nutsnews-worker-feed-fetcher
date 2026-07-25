# Architecture Notes

## Scope

The feed fetcher owns the worker-uplift service boundary that consumes `feedFetchRequest` messages on the contracted `fetch` route and publishes normalized candidate requests to canonicalization. Issue #95 created the deployable shell. Issue #96 adds conditional HTTP fetching, RSS/Atom parsing, durable fetch metadata, idempotent candidate publication, and bounded diagnostics. Retry/DLQ hardening is intentionally left to the next issue.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Input route boundary: `getWorkerRoute("fetch")`
- Output route boundary: `getWorkerRoute("canonicalization")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink
- Shutdown: stop accepting deliveries, wait for in-flight handlers, cancel consumers, close broker lifecycle

## Fetch Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Start the shared broker lifecycle and assert fetch/canonicalization topology.
4. Register a `fetch` consumer through the shared runtime message processor.
5. Validate incoming envelopes and feed-fetch payloads before any handler work.
6. Claim the durable idempotency interface before delegating to the injected handler.
7. Evaluate DNS policy and reject unsupported protocols, localhost, and private resolved addresses.
8. Fetch with configured user agent, conditional headers, timeout bounds, redirect limit, content-type allowlist, and response-size limit.
9. Treat `304 Not Modified` and unchanged content fingerprints as successful no-fanout refreshes.
10. Parse RSS 2.x and Atom with namespace stripping, CDATA/text cleanup, relative link resolution, optional malformed-date tolerance, language, excerpt, and image hints.
11. Record fetch outcome metadata and bounded candidate references without raw feed bodies.
12. Publish one contract-valid canonicalization message per newly claimed candidate ID.
13. Drain in-flight handlers before broker shutdown.

The fetcher does not write production article rows, call AI providers, translate content, or publish articles.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- HTTP client;
- DNS policy;
- durable state/idempotency;
- fetch work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies real database and RabbitMQ values later.

## Safety Bounds

`NUTSNEWS_FETCHER_CONCURRENCY` caps concurrent feed-fetch handlers. `NUTSNEWS_FETCHER_PREFETCH` must be greater than or equal to concurrency. HTTP policy settings define connect/read/total timeouts, redirect count, accepted content types, and response-size limits.

`NUTSNEWS_FETCHER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.

## Idempotency

Runtime message idempotency acks repeated deliveries after a successful handler completion. Fetch metadata stores `ETag`, `Last-Modified`, and a content fingerprint per feed. Candidate publication uses stable IDs derived from feed ID, source item ID, canonical URL, and content fingerprint; already published candidate IDs are skipped on replay.
