# Architecture Notes

## Scope

The feed fetcher owns the worker-uplift service boundary that consumes `feedFetchRequest` messages on the contracted `fetch` route and publishes normalized candidate requests to canonicalization. Issue #95 created the deployable shell. Issue #96 adds conditional HTTP fetching, RSS/Atom parsing, durable fetch metadata, idempotent candidate publication, and bounded diagnostics. Issue #97 adds explicit failure classification, retry/DLQ decisions, SSRF-safe redirects, bounded `Retry-After` evidence, and hostile-feed tests.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@1.0.0`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@1.0.0`
- Input route boundary: `getWorkerRoute("fetch")`
- Output route boundary: `getWorkerRoute("canonicalization")`
- Health: separate liveness, startup, and readiness probes; readiness requires an active `fetch` main-queue consumer and the dependency-mode-appropriate state-store adapter
- Metrics: bounded Prometheus text from the shared runtime sink plus build, dependency/store mode, expected-active, state readiness, last-success, distinct liveness/startup/readiness, and canonical exact-once fetch lifecycle counter/fixed-bucket latency histogram families
- Shutdown: mark draining, cancel the consumer, close the processor gate, wait for accepted handlers, and close the broker lifecycle; a drain timeout force-closes the transport and still finalizes lifecycle/health state

## Fetch Flow

1. Validate value-free configuration and secret presence by variable name.
2. Assert exact contracts/runtime package versions.
3. Bind diagnostic HTTP and install shutdown handling before dependency initialization.
4. Probe the state store within `NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS`; remain fail-closed without broker consumption when it is unhealthy, fails, or times out.
5. Start the shared broker lifecycle against the protected backend's pre-provisioned fetch/canonicalization topology, perform no runtime configure operation, and register a `fetch` consumer through the shared runtime message processor.
6. Validate incoming envelopes and feed-fetch payloads before any handler work.
7. Claim the durable idempotency interface before delegating to the injected handler.
8. Evaluate DNS policy and reject unsupported protocols, localhost, loopback, link-local, metadata, private literal addresses, and protected resolved addresses.
9. Fetch with configured user agent, conditional headers, timeout bounds, redirect limit, per-redirect DNS policy checks, content-type allowlist, and streaming response-size limit.
10. Treat `304 Not Modified` and unchanged content fingerprints as successful no-fanout refreshes.
11. Classify DNS, connect, TLS, timeout, redirect, HTTP status, content type, oversize, malformed XML, parser, and contract-validation failures before returning retry or terminal failure to the runtime.
12. Parse RSS 2.x and Atom with namespace stripping, CDATA/text cleanup, relative link resolution, optional malformed-date tolerance, language, excerpt, and image hints.
13. Pre-validate canonicalization commands before publishing.
14. Claim each candidate, publisher-confirm its canonicalization message, and record its publication marker; on replay, skip candidates already marked published.
15. Commit successful fetch outcome metadata and the new content fingerprint only after the complete fan-out succeeds; otherwise retry without hiding the unpublished remainder behind unchanged detection.
16. Cancel consumer delivery before closing the processor gate, then drain accepted handlers before broker shutdown; force-close and report the original timeout if graceful drain expires.

The fetcher does not write production article rows, call AI providers, translate content, or publish articles.

## Dependency Interfaces

The repository defines narrow interfaces for:

- broker transport;
- HTTP client;
- DNS policy;
- durable state/idempotency;
- fetch work handler.

Local doubles back tests and health probes without production dependencies. Backend-owned deployment configuration supplies database and RabbitMQ values; configuration presence alone does not satisfy readiness.

RabbitMQ exchanges, queues, bindings, retry tiers, and DLQs are declared only by the protected backend baseline. The runtime stage identity retains `configure="^$"` and uses its bounded read/write permissions only. A missing queue fails consumer registration, and a missing or unroutable exchange fails the mandatory publisher-confirm path; neither condition causes the service to widen privileges or create topology.

Production mode constructs a bounded PostgreSQL pool from the protected runtime URL and probes backend-owned `worker_uplift_fetcher.state_contract` version 2 plus every required table, claim-token index, and validated five-minute lease constraint before broker consumption. It also requires production identities from the HTTP, DNS, RabbitMQ, and state-store adapters; a mixed or unknown set keeps consumption disabled and is exported truthfully. A missing URL at adapter construction selects an explicit `unsupported` store. A missing, stale, or unhealthy contract keeps consumption disabled; liveness, startup, and metrics remain available. Test mode alone may use the `local-memory` adapter.

The adapter implements Runtime 1 leased inbox claims; transactional outcome/version/health persistence; and a durable canonicalization outbox. Every successful claim or reclaim gets a fresh opaque token, and completion/failure/release uses token compare-and-set even when RabbitMQ redelivers the same message ID. Lease acquisition, expiry, and reclaim comparisons use PostgreSQL `statement_timestamp()` and cannot exceed 300 seconds. Outcome rows carry a 30-day `redact_after` boundary with an ordered cleanup index. The complete validated broker command is committed before publication. Only a proven pre-delivery/unroutable broker failure releases the record to `retrying`; unknown confirms, channel failures, and ambiguous post-confirm persistence retain the fenced lease until expiry. A new token owner republishes the stored stable command, and confirmation is checked by candidate, idempotency key, token, and message ID. The handler advances successful fetch metadata only after all candidate publications are confirmed, so a partial-fan-out retry skips completed candidates and resumes the missing remainder. The HTTP reconciler requires bearer authentication plus protected apply confirmation; dry-run lists bounded aged rows without claiming, while apply atomically claims and settles exactly one row before selecting the next, even if a caller requests a larger batch.

The application does not run DDL. Migration 001 is the base test/reference schema and migration 002 is the additive claim-fencing contract. The backend repository owns the production sequence: keep the shadow consumer disabled, apply equivalent version-2 DDL, deploy this Runtime 1 image, verify version-2 readiness, and only then consider cutover. The version-2 constraints intentionally reject tokenless leases, so a pre-1.0 Runtime process must not remain active after migration.

## Safety Bounds

`NUTSNEWS_FETCHER_CONCURRENCY` caps concurrent feed-fetch handlers. `NUTSNEWS_FETCHER_PREFETCH` must be greater than or equal to concurrency and defaults to the same value so broker delivery does not queue a second request-timeout wave behind the active handlers during shutdown. Consumer cancellation is bounded, queued work is rejected on forced close, and the total stop operation remains bounded by `NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS`. PostgreSQL pool size, query/connect timeout, and claim lease duration are bounded; the database timeout cannot exceed the startup-probe timeout in production. Configuration also requires the lease to cover both total feed timeout plus a PostgreSQL checkpoint and RabbitMQ confirm plus PostgreSQL finalization, with a 5-second safety margin. HTTP policy settings define connect/read/total timeouts, redirect count, accepted content types, streaming response-size limits, and the maximum safe `Retry-After` delay retained for transient failures.

Retryable classes are limited to transient DNS resolution, connect timeout, read/total timeout, transient HTTP status codes (`408`, `425`, `429`, and `5xx`), and unknown network/client failures. Permanent classes include SSRF policy blocks, unsupported protocols, redirect policy failures, redirect limit/location failures, non-transient HTTP status codes, unsupported content types or declared charsets, oversized responses, malformed XML, unsupported/parser feed errors, TLS failures, and canonicalization contract-validation failures. The shared runtime controls bounded retry tiers and sends exhausted retryable failures to the fetch DLQ.

An exception escaping the shared processor is never negatively acknowledged as an unrecoverable drop. The RabbitMQ transport publisher-confirms a transfer to the contracted retry tier, or to the terminal DLQ when attempts are exhausted, before acknowledging the original delivery. A transfer failure requeues the original delivery, and each successful processor-exception disposition emits exactly one bounded `runtime.message.retry` or `runtime.message.dlq` event.

This repository has no backend-owned trusted-feed allowlist source yet. Protected destinations remain denied by default on initial requests and redirects until a later backend integration supplies an explicit approval mechanism.

`NUTSNEWS_FETCHER_SHADOW_MODE` remains required so bootstrap deployment cannot become the production legacy ingestion path by accident.

A `production` or `prod` environment label cannot select test dependency mode. Production dependency mode requires a durable PostgreSQL state store and production HTTP, DNS, and RabbitMQ adapter identities; configuration and readiness fail closed before queue ownership when those modes disagree.

`expected_active` is derived from that cutover control and remains false while shadow mode is required. It gates ownership-sensitive paging, not readiness: a production-shadow instance is ready when its durable state, network dependencies, broker, and consumer are usable. The signal is exported with dependency/deployment mode, actual state-store and adapter mode, service version, and immutable build revision. Worker last-success advances only after the shared runtime emits a terminally successful accepted or duplicate-message event.

The canonical fetch-stage metrics map the shared processor's single completion event to one of `success`, `duplicate`, `invalid`, `retry`, `dlq`, or `failure`. All six bounded outcomes are pre-seeded; `failure` is reserved for an explicitly reported terminal processing failure and does not absorb dependency, health, shutdown, invalid-input, retry, or DLQ events. Both success and duplicate are terminal successes and advance last-success. The latency histogram observes the processor's measured completion duration once in seconds using fixed cumulative buckets through the 30-second SLO boundary and up to 300 seconds; missing durations never manufacture observations.

Configured structured-log, telemetry, and metric sinks are independently wrapped as best-effort observers before broker, processor, ingestion, health, or shutdown use. Rejected observations cannot change acknowledgement, idempotency, retry, or DLQ state, and one rejected sink cannot starve the remaining fan-out. Every event is forwarded once to Runtime 1. Dependency duration has one path: a real finite duration on `runtime.dependency.observed`; no service wrapper exposes the removed direct-latency method. Runtime 1 owns the fixed-bucket seconds histograms, one bounded health family with per-check latency, and bounded build/deployment/expected-active/last-success identity. No millisecond summaries or duplicate health compatibility shims remain. Failed or disabled collection emits an explicit bounded status series.

## Idempotency

Runtime message idempotency acks repeated deliveries after a successful handler completion. Runtime 1 receives a fresh opaque PostgreSQL claim token on every successful claim or reclaim; the message ID remains audit metadata and cannot authorize completion. A stale token cannot complete, fail, or release a newer execution. If a claim response is lost, redelivery observes the live claim as in-progress. If a completion response is lost after commit, Runtime 1 release observes the processed row and preserves completion. Candidate publication uses stable IDs derived from feed ID, source item ID, canonical URL, and content fingerprint; each publisher-confirmed candidate is a durable replay-safe checkpoint, already confirmed candidates are skipped, and retry/reconciliation republishes the original outbox command. Concurrent reconciliation apply is safe because selection and fresh-token acquisition are atomic and only one candidate is held at a time. Source-side cutover blockers are resolved; production remains blocked on backend schema v2, signed immutable image evidence, deployment verification, and protected ownership approval.
