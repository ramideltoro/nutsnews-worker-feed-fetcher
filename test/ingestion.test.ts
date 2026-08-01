import {
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import { FetcherDefinitePublishError } from "../src/dependencies.js";
import { createFeedFetchWorkHandler } from "../src/ingestion.js";
import { SequenceFetcherIdFactory } from "../src/ids.js";
import { createFetcherPrometheusMetricsSink } from "../src/metrics.js";
import { createFetcherService } from "../src/service.js";
import {
  InMemoryFetcherStateStore,
  LocalBrokerTransport,
  LocalHttpClient,
  createLocalFetcherDependencies,
  createMinimalFetchDelivery
} from "../src/test-doubles.js";

const rssFeed = `<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>World Source</title>
    <item>
      <guid>guid-001</guid>
      <title>Story One</title>
      <link>https://articles.example.test/world/story-one</link>
      <pubDate>Thu, 23 Jul 2026 04:05:06 GMT</pubDate>
      <description>Short summary</description>
    </item>
    <item>
      <guid>guid-002</guid>
      <title>Story Two</title>
      <link>https://articles.example.test/world/story-two</link>
    </item>
  </channel>
</rss>`;

describe("feed fetch ingestion handler", () => {
  it("fetches RSS conditionally, records metadata, and publishes contract-valid canonicalization requests", async () => {
    const context = createIngestionContext(rssResponse());

    await context.service.start();
    const result = await context.broker.deliverFetch();

    expect(result).toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(context.http.requests[0]).toMatchObject({
      headers: {},
      userAgent: "NutsNewsWorkerFetcher/0.1",
      maxRedirects: 3
    });
    expect(context.state.outcomes[0]).toMatchObject({
      feedId: "feed-world",
      fetchStatus: "success",
      httpStatus: 200,
      etag: "\"rss-v1\"",
      itemCount: 2
    });
    expect(context.broker.published).toHaveLength(2);
    expect(context.broker.published[0]?.envelope.route).toBe("canonicalization");
    expect(validateStagePayload(context.broker.published[0]?.payload).ok).toBe(true);
    expect(context.broker.published[0]?.payload).toMatchObject({
      feedId: "feed-world",
      sourceItemId: "guid-001",
      originalUrl: "https://articles.example.test/world/story-one",
      canonicalUrl: "https://articles.example.test/world/story-one",
      title: "Story One",
      dedupeStatus: "new"
    });
    expect(context.telemetry.events.some((event) =>
      event.attributes?.feedId === "feed-world" &&
      event.attributes.fetchStatus === "success" &&
      event.attributes.itemCount === 2
    )).toBe(true);
    expect(context.metrics.collect()).toMatch(/nutsnews_worker_dependency_duration_seconds_count\{[^\n]*queue="nutsnews.worker.fetch.v1"[^\n]*outcome="success",dependency="feed-fetch"\} 1/u);
    expect(context.metrics.collect()).toMatch(/nutsnews_worker_dependency_duration_seconds_sum\{[^\n]*queue="nutsnews.worker.fetch.v1"[^\n]*outcome="success",dependency="feed-fetch"\} 0\.012/u);
    expect(context.metrics.collect()).not.toContain("_duration_ms");
    expect(context.metrics.collect()).toMatch(new RegExp(`nutsnews_worker_last_success_timestamp_seconds\\{[^\\n]+\\} ${String(Date.parse("2026-07-23T00:00:00.000Z") / 1_000)}`, "u"));

    await context.service.stop();
  });

  it("does not publish candidates for repeated delivery or the same fetched version", async () => {
    const context = createIngestionContext(rssResponse());

    await context.service.start();
    const delivery = createMinimalFetchDelivery();

    await context.broker.deliverFetch(delivery);
    await context.broker.deliverFetch(delivery);

    expect(context.broker.published).toHaveLength(2);

    context.http.requests.splice(0);
    await context.broker.deliverFetch(createMinimalFetchDelivery({
      envelope: {
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3991",
        causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3991",
        idempotencyKey: "scheduler:feed:feed-world:20260723t000100000z"
      },
      payload: {
        idempotencyKey: "scheduler:feed:feed-world:20260723t000100000z",
        stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3992"
      }
    }));

    expect(context.http.requests[0]?.headers).toMatchObject({
      "if-none-match": "\"rss-v1\"",
      "if-modified-since": "Thu, 23 Jul 2026 04:00:00 GMT"
    });
    expect(context.broker.published).toHaveLength(2);
    expect(context.state.outcomes.at(-1)).toMatchObject({
      fetchStatus: "unchanged",
      diagnosticSample: "content-fingerprint-match",
      itemCount: 0
    });

    await context.service.stop();
  });

  it("retries partial fan-out without committing a fingerprint or losing unpublished candidates", async () => {
    const state = new InMemoryFetcherStateStore();
    const broker = new FailOnceOnNthPublishBroker(2);
    const context = createIngestionContext(rssResponse(), state, undefined, broker);
    const delivery = createMinimalFetchDelivery();

    await context.service.start();

    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "handler-error"
    });
    expect(context.broker.published).toHaveLength(1);
    expect(await state.getFeedMetadata("feed-world")).toBeUndefined();
    expect(state.outcomes).toHaveLength(0);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.message.retry" &&
      event.outcome === "retry" &&
      event.attributes?.reason === "handler-error"
    )).toBe(true);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.dependency.observed" &&
      event.attributes?.event === "fetcher.feed.completed"
    )).toBe(false);

    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(context.broker.published).toHaveLength(2);
    expect(context.broker.published.map((command) => command.payload.sourceItemId)).toEqual([
      "guid-001",
      "guid-002"
    ]);
    expect(await state.getFeedMetadata("feed-world")).toMatchObject({
      feedId: "feed-world",
      etag: "\"rss-v1\""
    });
    expect(state.outcomes.map((outcome) => outcome.fetchStatus)).toEqual([
      "success"
    ]);

    await context.service.stop();
  });

  it("retains the candidate lease for an ambiguous publish outcome", async () => {
    const state = new PublicationFailureTrackingStateStore();
    const broker = new AmbiguousOnceOnNthPublishBroker(2);
    const context = createIngestionContext(rssResponse(), state, undefined, broker);
    const delivery = createMinimalFetchDelivery();

    await context.service.start();
    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "handler-error"
    });
    expect(context.broker.published).toHaveLength(1);
    expect(state.publishFailureRecords).toBe(0);
    await expect(state.listPendingCandidatePublications({
      maxItems: 100,
      minAgeSeconds: 0
    })).resolves.toEqual([]);

    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "retry"
    });
    expect(context.broker.published).toHaveLength(1);
    expect(state.publishFailureRecords).toBe(0);

    await context.service.stop();
  });

  it("does not reopen a candidate after confirmed publish finalization is ambiguous at the last attempt", async () => {
    const state = new AmbiguousFinalizationStateStore();
    const context = createIngestionContext(rssResponse(), state);
    const delivery = createMinimalFetchDelivery({
      envelope: {
        attempt: {
          count: 4,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z",
          lastAttemptAt: "2026-07-23T00:20:00.000Z"
        }
      }
    });

    await context.service.start();
    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "dlq",
      reason: "handler-error"
    });
    expect(context.broker.published).toHaveLength(2);
    expect(state.publishFailureRecords).toBe(0);
    await expect(state.listPendingCandidatePublications({
      maxItems: 100,
      minAgeSeconds: 0
    })).resolves.toEqual([]);

    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "dlq"
    });
    expect(context.broker.published).toHaveLength(2);
    expect(state.publishFailureRecords).toBe(0);

    await context.service.stop();
  });

  it("routes state-store incidents through DLQ lifecycle telemetry without changing feed health", async () => {
    const state = new UnavailableFeedMetadataStateStore();
    const context = createIngestionContext(rssResponse(), state);

    await context.service.start();
    await expect(context.broker.deliverFetch(createMinimalFetchDelivery({
      envelope: {
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3971",
        idempotencyKey: "scheduler:feed:feed-world:20260723t000400000z",
        attempt: {
          count: 4,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z",
          lastAttemptAt: "2026-07-23T00:03:00.000Z"
        }
      },
      payload: {
        idempotencyKey: "scheduler:feed:feed-world:20260723t000400000z"
      }
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "handler-error"
    });

    expect(context.http.requests).toHaveLength(0);
    expect(state.outcomes).toHaveLength(0);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.message.dlq" &&
      event.outcome === "dlq" &&
      event.attributes?.reason === "handler-error"
    )).toBe(true);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.dependency.observed" &&
      event.attributes?.event === "fetcher.feed.completed"
    )).toBe(false);

    await context.service.stop();
  });

  it("handles 304 Not Modified without candidate fan-out", async () => {
    const state = new InMemoryFetcherStateStore();

    await state.recordFetchOutcome({
      feedId: "feed-world",
      feedUrl: "https://feeds.example.test/world.xml",
      fetchedAt: "2026-07-23T00:00:00.000Z",
      fetchStatus: "success",
      httpStatus: 200,
      etag: "\"rss-v1\"",
      lastModified: "Thu, 23 Jul 2026 04:00:00 GMT",
      contentFingerprint: "abc123",
      bodyBytes: 100,
      itemCount: 1,
      durationMs: 5
    });

    const context = createIngestionContext({
      statusCode: 304,
      headers: {},
      body: new Uint8Array(),
      bodyBytes: 0,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 8
    }, state);

    await context.service.start();
    await context.broker.deliverFetch();

    expect(context.http.requests[0]?.headers).toMatchObject({
      "if-none-match": "\"rss-v1\"",
      "if-modified-since": "Thu, 23 Jul 2026 04:00:00 GMT"
    });
    expect(context.broker.published).toHaveLength(0);
    expect(context.state.outcomes.at(-1)).toMatchObject({
      fetchStatus: "unchanged",
      httpStatus: 304,
      itemCount: 0
    });

    await context.service.stop();
  });

  it("decodes declared response charset before parsing candidates", async () => {
    const body = Buffer.from(`<?xml version="1.0" encoding="ISO-8859-1"?>
      <rss><channel><title>Source</title><item><guid>guid-cafe</guid><title>Caf\xe9</title><link>https://articles.example.test/cafe</link></item></channel></rss>`, "latin1");
    const context = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "application/rss+xml ; charset = \"iso-8859-1\" ; boundary=ignored"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 6
    });

    await context.service.start();
    await context.broker.deliverFetch();

    expect(context.broker.published[0]?.payload).toMatchObject({
      title: "Caf\u00e9"
    });

    await context.service.stop();
  });

  it("classifies an unsupported declared charset as a permanent source failure", async () => {
    const body = bytes(rssFeed);
    const context = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "application/rss+xml; charset=x-nutsnews-unsupported"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 6
    });

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "dlq",
      reason: "unsupported-charset"
    });

    expect(context.broker.published).toHaveLength(0);
    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "permanent_failure",
      failure: {
        failureClass: "content_type",
        code: "unsupported-charset",
        retryable: false,
        action: "dlq",
        diagnosticSample: "x-nutsnews-unsupported"
      }
    });

    await context.service.stop();
  });

  it("returns terminal failure for unsupported content types without raw body persistence", async () => {
    const body = bytes("<html><body>not a feed</body></html>");
    const context = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "text/html"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 4
    });

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "dlq",
      reason: "unsupported-content-type"
    });

    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "permanent_failure",
      diagnosticSample: "text/html"
    });
    expect(JSON.stringify(context.state.outcomes)).not.toContain("not a feed");

    await context.service.stop();
  });

  it("retries transient HTTP failures with safe bounded Retry-After evidence", async () => {
    const context = createIngestionContext({
      statusCode: 429,
      headers: {
        "content-type": "text/plain",
        "retry-after": "120"
      },
      body: new Uint8Array(),
      bodyBytes: 0,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 9
    });

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "retry",
      reason: "http-429",
      retryAfterMs: 120_000
    });

    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "transient_failure",
      httpStatus: 429,
      failure: {
        failureClass: "http_status",
        code: "http-429",
        retryable: true,
        action: "retry",
        retryAfterMs: 120_000
      }
    });

    await context.service.stop();
  });

  it("records an unclassified remote transport failure against feed health", async () => {
    const state = new InMemoryFetcherStateStore();
    const http = new UnavailableRemoteFeedHttpClient();
    const context = createIngestionContext(rssResponse(), state, undefined, undefined, http);

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "retry",
      reason: "unknown-fetch-error"
    });

    expect(state.outcomes).toHaveLength(1);
    expect(state.outcomes[0]).toMatchObject({
      fetchStatus: "transient_failure",
      failure: {
        failureClass: "unknown",
        code: "unknown-fetch-error",
        retryable: true
      }
    });
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.message.retry" &&
      event.attributes?.reason === "unknown-fetch-error"
    )).toBe(true);

    await context.service.stop();
  });

  it("ignores unsafe Retry-After values above the configured bound", async () => {
    const context = createIngestionContext({
      statusCode: 503,
      headers: {
        "retry-after": "7200"
      },
      body: new Uint8Array(),
      bodyBytes: 0,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 9
    });

    await context.service.start();
    const result = await context.broker.deliverFetch();

    expect(result).toMatchObject({
      action: "retry",
      reason: "http-503"
    });
    expect("retryAfterMs" in result).toBe(false);
    expect(context.state.outcomes[0]?.failure).toMatchObject({
      failureClass: "http_status",
      code: "http-503",
      retryable: true,
      action: "retry"
    });
    expect(context.state.outcomes[0]?.failure).not.toHaveProperty("retryAfterMs");

    await context.service.stop();
  });

  it("routes exhausted transient failures to the fetch DLQ with retryable feed-health context", async () => {
    const context = createIngestionContext({
      statusCode: 503,
      headers: {},
      body: new Uint8Array(),
      bodyBytes: 0,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 9
    });

    await context.service.start();
    await expect(context.broker.deliverFetch(createMinimalFetchDelivery({
      envelope: {
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3981",
        idempotencyKey: "scheduler:feed:feed-world:20260723t000200000z",
        attempt: {
          count: 4,
          max: 4,
          firstAttemptAt: "2026-07-23T00:00:00.000Z",
          lastAttemptAt: "2026-07-23T00:20:00.000Z"
        }
      },
      payload: {
        idempotencyKey: "scheduler:feed:feed-world:20260723t000200000z",
        stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3982"
      }
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "http-503"
    });

    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "transient_failure",
      failure: {
        failureClass: "http_status",
        code: "http-503",
        retryable: true,
        action: "dlq"
      }
    });

    await context.service.stop();
  });

  it("blocks protected feed destinations before opening an HTTP request", async () => {
    const context = createIngestionContext(rssResponse());

    await context.service.start();
    await expect(context.broker.deliverFetch(createMinimalFetchDelivery({
      envelope: {
        idempotencyKey: "scheduler:feed:feed-world:20260723t000300000z"
      },
      payload: {
        idempotencyKey: "scheduler:feed:feed-world:20260723t000300000z",
        feedUrl: "http://169.254.169.254/latest/meta-data"
      }
    }))).resolves.toMatchObject({
      action: "dlq",
      reason: "blocked-metadata-address"
    });

    expect(context.http.requests).toHaveLength(0);
    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "permanent_failure",
      failure: {
        failureClass: "security",
        code: "blocked-metadata-address",
        retryable: false,
        action: "dlq",
        safeFeedUrl: "http://169.254.169.254/[path-redacted]"
      }
    });

    await context.service.stop();
  });

  it("classifies malformed XML as a permanent parser failure", async () => {
    const body = bytes("<rss><channel><item></rss>");
    const context = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "application/rss+xml"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 7
    });

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "dlq",
      reason: "malformed-xml"
    });
    expect(context.broker.published).toHaveLength(0);
    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "permanent_failure",
      failure: {
        failureClass: "malformed_xml",
        code: "malformed-xml",
        retryable: false,
        action: "dlq"
      }
    });

    await context.service.stop();
  });

  it("classifies candidate validation failures as terminal without successful item-ref persistence", async () => {
    const body = bytes(`<?xml version="1.0"?>
      <rss><channel><title>World Source</title><item><guid>unsafe</guid><title>Unsafe Link</title><link>https://articles.example.test/story?api_key=secret</link></item></channel></rss>`);
    const context = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "application/rss+xml"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 7
    });

    await context.service.start();
    await expect(context.broker.deliverFetch()).resolves.toMatchObject({
      action: "dlq",
      reason: "canonicalization-payload-validation"
    });

    expect(context.broker.published).toHaveLength(0);
    expect(context.state.outcomes).toHaveLength(1);
    expect(context.state.outcomes[0]).toMatchObject({
      fetchStatus: "permanent_failure",
      failure: {
        failureClass: "validation",
        code: "canonicalization-payload-validation",
        retryable: false,
        action: "dlq"
      }
    });
    expect(JSON.stringify(context.state.outcomes)).not.toContain("secret");

    await context.service.stop();
  });

  it("keeps ack, retry, and DLQ outcomes independent from ingestion telemetry rejection", async () => {
    const rejectingTelemetry: RuntimeTelemetrySink = {
      emit: () => Promise.reject(new Error("telemetry unavailable"))
    };
    const success = createIngestionContext(rssResponse(), new InMemoryFetcherStateStore(), rejectingTelemetry);

    await success.service.start();
    await expect(success.broker.deliverFetch()).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    expect(success.state.outcomes).toHaveLength(1);
    expect(success.broker.published).toHaveLength(2);
    await success.service.stop();

    const retry = createIngestionContext({
      statusCode: 429,
      headers: {
        "retry-after": "120"
      },
      body: new Uint8Array(),
      bodyBytes: 0,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 9
    }, new InMemoryFetcherStateStore(), rejectingTelemetry);

    await retry.service.start();
    await expect(retry.broker.deliverFetch()).resolves.toMatchObject({
      action: "retry",
      reason: "http-429"
    });
    expect(retry.state.outcomes[0]?.fetchStatus).toBe("transient_failure");
    await retry.service.stop();

    const body = bytes("<html><body>not a feed</body></html>");
    const dlq = createIngestionContext({
      statusCode: 200,
      headers: {
        "content-type": "text/html"
      },
      body,
      bodyBytes: body.byteLength,
      finalUrl: "https://feeds.example.test/world.xml",
      durationMs: 4
    }, new InMemoryFetcherStateStore(), rejectingTelemetry);

    await dlq.service.start();
    await expect(dlq.broker.deliverFetch()).resolves.toMatchObject({
      action: "dlq",
      reason: "unsupported-content-type"
    });
    expect(dlq.state.outcomes[0]?.fetchStatus).toBe("permanent_failure");
    await dlq.service.stop();
  });
});

function createIngestionContext(
  response: LocalHttpClient["response"],
  state = new InMemoryFetcherStateStore(),
  telemetryOverride?: RuntimeTelemetrySink,
  brokerOverride?: LocalBrokerTransport,
  httpOverride?: LocalHttpClient
) {
  const config = loadFetcherConfig({
    NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
  });
  const http = httpOverride ?? new LocalHttpClient();
  const broker = brokerOverride ?? new LocalBrokerTransport();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const dependencies = createLocalFetcherDependencies({
    httpClient: http,
    stateStore: state,
    brokerTransport: broker
  });

  http.response = response;

  const metrics = createFetcherPrometheusMetricsSink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host
    },
    config,
    stateStore: dependencies.stateStore
  });
  const telemetryFanout = {
    emit: async (event: Parameters<typeof telemetry.emit>[0]): Promise<void> => {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const configuredTelemetry = telemetryOverride ?? telemetryFanout;

  const workHandler = createFeedFetchWorkHandler({
    config,
    dependencies,
    telemetry: configuredTelemetry,
    idFactory: new SequenceFetcherIdFactory([
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3901",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3902",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3903",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3904",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3905",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3906",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3907",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3908",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3909",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3910",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3911",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3912"
    ])
  });
  const service = createFetcherService({
    config,
    dependencies: {
      ...dependencies,
      workHandler
    },
    telemetry: configuredTelemetry,
    metrics
  });

  return {
    broker,
    http,
    metrics,
    service,
    state,
    telemetry
  };
}

class FailOnceOnNthPublishBroker extends LocalBrokerTransport {
  private publishCalls = 0;
  private failed = false;

  constructor(private readonly failOnCall: number) {
    super();
  }

  override publish(
    command: Parameters<LocalBrokerTransport["publish"]>[0]
  ): ReturnType<LocalBrokerTransport["publish"]> {
    this.publishCalls += 1;

    if (!this.failed && this.publishCalls === this.failOnCall) {
      this.failed = true;
      return Promise.reject(new FetcherDefinitePublishError("simulated partial fan-out failure"));
    }

    return super.publish(command);
  }
}

class AmbiguousOnceOnNthPublishBroker extends LocalBrokerTransport {
  private publishCalls = 0;
  private failed = false;

  constructor(private readonly failOnCall: number) {
    super();
  }

  override publish(
    command: Parameters<LocalBrokerTransport["publish"]>[0]
  ): ReturnType<LocalBrokerTransport["publish"]> {
    this.publishCalls += 1;

    if (!this.failed && this.publishCalls === this.failOnCall) {
      this.failed = true;
      return Promise.reject(new Error("simulated ambiguous publish outcome"));
    }

    return super.publish(command);
  }
}

class PublicationFailureTrackingStateStore extends InMemoryFetcherStateStore {
  publishFailureRecords = 0;

  override markCandidatePublishFailed(
    ...args: Parameters<InMemoryFetcherStateStore["markCandidatePublishFailed"]>
  ): ReturnType<InMemoryFetcherStateStore["markCandidatePublishFailed"]> {
    this.publishFailureRecords += 1;
    return super.markCandidatePublishFailed(...args);
  }
}

class AmbiguousFinalizationStateStore extends PublicationFailureTrackingStateStore {
  private finalizationCalls = 0;

  override markCandidatePublished(
    ...args: Parameters<InMemoryFetcherStateStore["markCandidatePublished"]>
  ): ReturnType<InMemoryFetcherStateStore["markCandidatePublished"]> {
    this.finalizationCalls += 1;

    if (this.finalizationCalls === 2) {
      return Promise.reject(new Error("simulated lost finalization response"));
    }

    return super.markCandidatePublished(...args);
  }
}

class UnavailableFeedMetadataStateStore extends InMemoryFetcherStateStore {
  override getFeedMetadata(
    feedId: string
  ): ReturnType<InMemoryFetcherStateStore["getFeedMetadata"]> {
    void feedId;
    const error = new Error("Internal PostgreSQL hostname lookup failed.") as NodeJS.ErrnoException;

    error.code = "ENOTFOUND";
    return Promise.reject(error);
  }
}

class UnavailableRemoteFeedHttpClient extends LocalHttpClient {
  override request(
    request: Parameters<LocalHttpClient["request"]>[0]
  ): ReturnType<LocalHttpClient["request"]> {
    this.requests.push(request);
    return Promise.reject(new Error("Remote feed transport failed without a platform error code."));
  }
}

function rssResponse(): LocalHttpClient["response"] {
  const body = bytes(rssFeed);

  return {
    statusCode: 200,
    headers: {
      "content-type": "application/rss+xml; charset=utf-8",
      "etag": "\"rss-v1\"",
      "last-modified": "Thu, 23 Jul 2026 04:00:00 GMT"
    },
    body,
    bodyBytes: body.byteLength,
    finalUrl: "https://feeds.example.test/world.xml",
    durationMs: 12
  };
}

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}
