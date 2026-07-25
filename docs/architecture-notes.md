# Architecture Notes

## Scope

The feed fetcher owns the worker-uplift service boundary that consumes `feedFetchRequest` messages on the contracted `fetch` route and publishes normalized candidate requests to canonicalization. Issue #95 created the deployable shell. Issue #96 adds conditional HTTP fetching, RSS/Atom parsing, durable fetch metadata, idempotent candidate publication, and bounded diagnostics. Issue #97 adds explicit failure classification, retry/DLQ decisions, SSRF-safe redirects, bounded `Retry-After` evidence, and hostile-feed tests.

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
7. Evaluate DNS policy and reject unsupported protocols, localhost, loopback, link-local, metadata, private literal addresses, and protected resolved addresses.
8. Fetch with configured user agent, conditional headers, timeout bounds, redirect limit, per-redirect DNS policy checks, content-type allowlist, and streaming response-size limit.
9. Treat `304 Not Modified` and unchanged content fingerprints as successful no-fanout refreshes.
10. Classify DNS, connect, TLS, timeout, redirect, HTTP status, content type, oversize, malformed XML, parser, and contract-validation failures before returning retry or terminal failure to the runtime.
11. Parse RSS 2.x and Atom with namespace stripping, CDATA/text cleanup, relative link resolution, optional malformed-date tolerance, language, excerpt, and image hints.
12. Pre-validate canonicalization commands before recording successful item references.
13. Record fetch outcome metadata, bounded candidate references, and redacted failure context without raw feed bodies.
14. Publish one contract-valid canonicalization message per newly claimed candidate ID.
15. Drain in-flight handlers before broker shutdown.

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

`NUTSNEWS_FETCHER_CONCURRENCY` caps concurrent feed-fetch handlers. `NUTSNEWS_FETCHER_PREFETCH` must be greater than or equal to concurrency. HTTP policy settings define connect/read/total timeouts, redirect count, accepted content types, streaming response-size limits, and the maximum safe `Retry-After` delay retained for transient failures.

Retryable classes are limited to transient DNS resolution, connect timeout, read/total timeout, transient HTTP status codes (`408`, `425`, `429`, and `5xx`), and unknown network/client failures. Permanent classes include SSRF policy blocks, unsupported protocols, redirect policy failures, redirect limit/location failures, non-transient HTTP status codes, unsupported content types, oversized responses, malformed XML, unsupported/parser feed errors, TLS failures, and canonicalization contract-validation failures. The shared runtime controls bounded retry tiers and sends exhausted retryable failures to the fetch DLQ.

This repository has no backend-owned trusted-feed allowlist source yet. Protected destinations remain denied by default on initial requests and redirects until a later backend integration supplies an explicit approval mechanism.

`NUTSNEWS_FETCHER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.

## Idempotency

Runtime message idempotency acks repeated deliveries after a successful handler completion. Fetch metadata stores `ETag`, `Last-Modified`, and a content fingerprint per feed. Candidate publication uses stable IDs derived from feed ID, source item ID, canonical URL, and content fingerprint; already published candidate IDs are skipped on replay.
