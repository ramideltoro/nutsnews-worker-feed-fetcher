import {
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import { createFeedFetchWorkHandler } from "../src/ingestion.js";
import { SequenceFetcherIdFactory } from "../src/ids.js";
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
        "content-type": "application/rss+xml; charset=iso-8859-1"
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
});

function createIngestionContext(response: LocalHttpClient["response"], state = new InMemoryFetcherStateStore()) {
  const config = loadFetcherConfig({
    NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
  });
  const http = new LocalHttpClient();
  const broker = new LocalBrokerTransport();
  const telemetry = createBufferedRuntimeTelemetrySink();
  const dependencies = createLocalFetcherDependencies({
    httpClient: http,
    stateStore: state,
    brokerTransport: broker
  });

  http.response = response;

  const workHandler = createFeedFetchWorkHandler({
    config,
    dependencies,
    telemetry,
    idFactory: new SequenceFetcherIdFactory([
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3901",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3902",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3903",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3904",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3905",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3906"
    ])
  });
  const service = createFetcherService({
    config,
    dependencies: {
      ...dependencies,
      workHandler
    },
    telemetry
  });

  return {
    broker,
    http,
    service,
    state,
    telemetry
  };
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
