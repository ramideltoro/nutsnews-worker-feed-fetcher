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

- `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- `@ramideltoro/nutsnews-worker-runtime@0.4.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_FETCHER_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_FETCHER_DATABASE_URL`
- `NUTSNEWS_FETCHER_RABBITMQ_URL`
- `NUTSNEWS_FETCHER_CONCURRENCY`
- `NUTSNEWS_FETCHER_PREFETCH`
- `NUTSNEWS_FETCHER_SHADOW_MODE`
- `NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_READ_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS`
- `NUTSNEWS_FETCHER_MAX_REDIRECTS`
- `NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES`
- `NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS`
- `NUTSNEWS_FETCHER_ACCEPTED_CONTENT_TYPES`

`NUTSNEWS_FETCHER_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

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

The repository includes test interfaces and local doubles for:

- broker transport;
- HTTP client;
- DNS policy;
- durable state/idempotency;
- fetch work handler.

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

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI when package access is granted to this repository. Workflows use least-privilege permissions, request `packages: read` for package install jobs, and request `packages: write` only for image publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.
