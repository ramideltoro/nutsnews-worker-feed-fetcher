# nutsnews-worker-feed-fetcher

Deployable worker-uplift feed fetcher service shell for NutsNews.

## Responsibility

Own the fetcher service boundary that will consume contracted feed-fetch requests, safely download RSS/Atom feeds, and publish normalized feed-fetch results for canonicalization without touching legacy ingestion.

Issue #95 bootstraps the deployable shell. Issue #96 adds bounded conditional HTTP fetching, RSS/Atom parsing, idempotent fetch metadata, and contract-valid canonicalization candidate publication. Issue #97 hardens retry classification, SSRF redirect checks, bounded `Retry-After` evidence, and durable DLQ diagnostics.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-feed-fetcher:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

The image runs as a non-root user, exposes port `8080`, and serves:

- `GET /live`
- `GET /startup`
- `GET /ready`
- `GET /metrics`
- `GET /config-schema`

## Runtime Dependencies

The service consumes exact immutable worker-uplift package versions:

- `@ramideltoro/nutsnews-worker-contracts@1.0.0`
- `@ramideltoro/nutsnews-worker-runtime@1.0.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

`/ready` is unhealthy whenever the `fetch` main queue has zero active consumers. Consumer cancellation and channel-drop recovery emit bounded structured runtime events and Prometheus consumer-state metrics.

Production state is deliberately fail-closed and durable. In `production` dependency mode the application constructs the PostgreSQL adapter only from the protected runtime URL, probes backend-owned state contract version 2, and does not register the RabbitMQ consumer unless that contract, its claim-fencing indexes and lease constraints, and every required table are healthy. The HTTP client, DNS policy, RabbitMQ transport, and state store must each identify as a production adapter; any test/unknown mixture exports `adapter="mixed"` and keeps readiness unhealthy and consumption disabled. Missing adapter configuration selects an explicit `unsupported` store. Local `test` mode uses the explicitly ephemeral `local-memory` adapter only.

The diagnostic HTTP server binds before state-store probing or broker startup. State-store startup probing is bounded by `NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS`; an unhealthy, failed, or timed-out probe keeps consumption disabled and readiness unhealthy while liveness, startup, and metrics remain queryable.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_FETCHER_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_FETCHER_DATABASE_URL`
- `NUTSNEWS_FETCHER_DATABASE_POOL_MAX`
- `NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS`
- `NUTSNEWS_FETCHER_RABBITMQ_URL`
- `NUTSNEWS_FETCHER_CONCURRENCY`
- `NUTSNEWS_FETCHER_PREFETCH`
- `NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_SHADOW_MODE`
- `NUTSNEWS_FETCHER_BUILD_REVISION`
- `NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_READ_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_MAX_REDIRECTS`
- `NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES`
- `NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS`
- `NUTSNEWS_FETCHER_ACCEPTED_CONTENT_TYPES`

`NUTSNEWS_FETCHER_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

`NUTSNEWS_ENVIRONMENT=production` (or `prod`) requires `NUTSNEWS_FETCHER_DEPENDENCY_MODE=production`. Production dependency mode, in turn, requires the durable PostgreSQL state-store adapter and production HTTP, DNS, and RabbitMQ adapter identities before readiness can pass or a consumer can register.

The default prefetch equals concurrency so a single maximum-duration HTTP request wave fits inside the graceful shutdown budget with time left for state and broker settlement. Claim leases are generated from PostgreSQL time and cannot exceed 300 seconds. Configuration is rejected unless the lease covers both the total feed timeout plus one PostgreSQL checkpoint deadline and a candidate RabbitMQ-confirm plus PostgreSQL-finalization deadline, each with a 5-second settlement margin. Intake cancellation begins before the HTTP server waits for active diagnostics, and a forced shutdown rejects queued work rather than starting it after the deadline. The protected backend RabbitMQ baseline owns topology declaration. The fetcher deliberately performs no exchange, queue, or binding configuration because its stage identity has `configure="^$"`; missing pre-provisioned topology instead fails closed at consume or mandatory publish. Concurrent reconnect/publish paths share one channel-open operation so they cannot leave duplicate consumers behind.

`expected_active=0` describes production ownership and gates paging while the worker is shadowed; it is not a readiness failure. Production-shadow readiness is healthy when the PostgreSQL state contract, HTTP/DNS dependencies, RabbitMQ lifecycle, and active consumer are usable.

The metrics endpoint exports bounded service identity and operating-state signals:

- `nutsnews_worker_build_info`
- `nutsnews_worker_deployment_info`
- `nutsnews_worker_expected_active`
- `nutsnews_worker_state_store_ready`
- `nutsnews_worker_last_success_timestamp_seconds`
- `nutsnews_worker_health_probe`
- `nutsnews_worker_metrics_enabled`
- `nutsnews_worker_telemetry_collection_ready`
- `nutsnews_worker_processing_duration_seconds`
- `nutsnews_worker_dependency_duration_seconds`
- `nutsnews_worker_uplift_stage_events_total`
- `nutsnews_worker_uplift_stage_latency_seconds`

State-store readiness details and structured startup health telemetry include the expected/actual store mode, all four dependency adapter modes, their aggregate mode, dependency/deployment mode, service version, build revision, and `expectedActive`. Distinct liveness, startup, and readiness series are emitted by the Runtime 1 health probes and transition with health evaluation, service startup, consumer cancellation, and shutdown. A disabled or failed collector returns explicit bounded `metrics_enabled=0` or `telemetry_collection_ready=0` state instead of an empty scrape. Secrets and per-feed/message identifiers are not metric labels.

Each completed fetch delivery contributes exactly one bounded lifecycle outcome: `success`, `duplicate`, `invalid`, `retry`, `dlq`, or `failure`. The `failure` series is pre-seeded with the other shared outcomes and is reserved for an explicitly reported terminal processing failure; invalid input, retry disposition, and DLQ disposition remain distinct. Dependency, health, and shutdown failures are not delivery completions. Successful and duplicate terminal completions both advance the monotonic last-success timestamp and form the success numerator. Measured completion latency is exported in seconds with cumulative `0.005`, `0.01`, `0.025`, `0.05`, `0.1`, `0.25`, `0.5`, `1`, `2.5`, `5`, `10`, `30`, `60`, `120`, and `300` second buckets, followed by `+Inf`, `_sum`, and `_count`. The canonical stage families use only `environment`, `service="fetch"`, `outcome`, and the fixed histogram `le` boundary; delivery identifiers remain structured-log metadata only.

Every configured log, telemetry, and metric sink is independently best effort before it reaches the broker, processor, ingestion handler, health probes, or shutdown controller. Sink rejection cannot change acknowledgement, idempotency, retry, or DLQ decisions, and a failed sink cannot prevent another sink from observing the same event. Runtime 1 receives every event exactly once and owns the bounded message, dependency, health, build, deployment, expected-active, and last-success families. Dependency latency is observed exclusively through duration-bearing `runtime.dependency.observed` events; duration-less observations do not manufacture zero-second samples, and no legacy duration summaries or duplicate health shims remain.

## Service Boundary

The service registers the contracted `fetch` consumer route and `canonicalization` publish route through the shared runtime broker lifecycle. The message processor validates worker envelopes and feed-fetch payloads, applies the durable idempotency interface, delegates work to the fetch handler, and drains in-flight deliveries during shutdown.

The fetch handler:

- sends conditional `If-None-Match` and `If-Modified-Since` headers from durable fetch metadata;
- enforces configured user agent, timeout, redirect, content-type, and streaming response-size bounds;
- blocks unsupported protocols, loopback, link-local, metadata, and private destinations on the initial URL and each redirect;
- classifies DNS, connect, TLS, timeout, redirect, HTTP status, content type, oversize, malformed XML, parser, and contract-validation failures;
- retries only transient failure classes through the bounded shared retry tiers and records safe `Retry-After` values up to `NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS`;
- sends permanent and exhausted failures to the fetch DLQ with redacted feed-health diagnostics for replay triage;
- parses RSS 2.x and Atom with namespace, CDATA, relative link, date, language, excerpt, and image-hint support;
- records fetch outcomes without raw feed bodies;
- publishes one contract-valid canonicalization message per new candidate identity;
- skips fan-out for `304 Not Modified`, duplicate deliveries, and unchanged content fingerprints.

Successful fetch metadata, including the content fingerprint used for unchanged detection, is committed only after every newly claimed candidate has a confirmed publication and a recorded publication marker. If a partial fan-out fails, the delivery is retried: already published candidate IDs are skipped and the remaining candidates are published before the feed fingerprint advances. Unexpected processor exceptions are transferred with publisher confirmation to the contracted retry tier or terminal DLQ before the original delivery is acknowledged, with exactly one bounded retry/DLQ lifecycle event for the disposition.

The repository includes test interfaces and local doubles for:

- broker transport;
- HTTP client;
- DNS policy;
- durable state/idempotency;
- fetch work handler.

The PostgreSQL adapter uses the reviewed backend-owned `worker_uplift_fetcher` schema for leased inbox claims, fetch-version metadata, append-only outcome history, feed-health projections, and durable canonicalization outbox commands. Fetch outcomes receive an explicit 30-day `redact_after` boundary and cleanup index. Every successful inbox or outbox claim receives a fresh opaque token, including a reclaimed delivery with the same RabbitMQ message ID. Completion, failure, and ambiguous-response release are compare-and-set operations on that token; stale executions cannot finalize a newer claim. PostgreSQL `statement_timestamp()` is authoritative for acquisition, expiry, and reclamation. Only a broker outcome that proves the message was not delivered moves the outbox record to `retrying`. A confirm timeout, channel failure, unknown publish result, or lost post-confirm database response retains the fenced lease until expiry; a later owner republishes the original stored command and stable message/idempotency identity for downstream idempotent handling.

Fan-out is safely checkpointed candidate by candidate: the durable command precedes publication, RabbitMQ confirmation is bounded, and the token-fenced publication marker follows it within the configured lease budget. A long delivery may cross its inbox lease only between replay-safe checkpoints; Runtime 1 then prevents the stale delivery from completing the reclaimed inbox claim. Reconciliation dry-run is read-only. Apply atomically claims exactly one eligible row with `FOR UPDATE SKIP LOCKED` immediately before publishing it; the state-store boundary refuses batch preclaiming regardless of caller input. It finalizes that row before selecting another and stops fail-closed without reopening the row on any ambiguous settlement.

The service never creates or migrates production schema. `migrations/001_worker_uplift_fetcher_state.sql` is the base integration-test/reference contract and `migrations/002_fetcher_claim_fencing.sql` advances it to version 2. `ramideltoro/nutsnews-backend` must stop or keep the shadow consumer disabled, apply the equivalent additive version-2 migration, and then deploy this Runtime 1 image; a version-1 process cannot write tokenless leases through the version-2 constraints. Readiness intentionally remains unhealthy until the backend-owned schema, role/grants, and secret injection satisfy version 2. CI covers same-message lease reclamation with distinct tokens, stale-token rejection, ambiguous claim/completion responses, atomic concurrent replay claims, metadata/outcome persistence, and stored-command confirmation.

The source-side Runtime 1 and reconciliation blockers are resolved. Production ownership remains blocked only on the backend migration/deployment sequence, immutable-image evidence, protected cutover approval, and the repository guardrail that still requires shadow mode.

The repository does not write production article rows, call AI providers, translate content, or publish user-facing articles.

## Development

```sh
export NODE_AUTH_TOKEN="<GitHub classic PAT with read:packages>"
npm ci
npm run ci
docker build --secret id=npm_token,env=NODE_AUTH_TOKEN -t nutsnews-worker-feed-fetcher:local .
```

`npm run ci` runs linting, strict type checking, unit tests, integration tests, build, CycloneDX SBOM generation, and a production dependency audit.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

The publish workflow creates a SHA-tagged GHCR image, SBOM/provenance attestations, and a keyless cosign signature. Backend deployment must resolve the published manifest digest and pin `ghcr.io/ramideltoro/nutsnews-worker-feed-fetcher@sha256:<digest>`; the SHA tag alone is release evidence, not the immutable deployment reference. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI when package access is granted to this repository. Workflows use least-privilege permissions, request `packages: read` for package install jobs, and request `packages: write` only for image publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
