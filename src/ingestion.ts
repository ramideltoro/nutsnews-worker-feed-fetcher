import { TextDecoder } from "node:util";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerPublishCommand,
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHandlerResult,
  type RuntimeMessageContext,
  type RuntimeTelemetryOutcome,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherConfig } from "./config.js";
import type {
  FetcherCandidateReference,
  FetcherDependencies,
  FetcherFetchOutcome,
  FetcherHttpResponse,
  FetcherWorkHandler,
  FetcherWorkTools
} from "./dependencies.js";
import {
  FeedParseError,
  parseFeedXml,
  type ParsedFeedItem
} from "./feed-parser.js";
import {
  createCryptoFetcherIdFactory,
  sha256Hex,
  stableCandidateId,
  type FetcherIdFactory
} from "./ids.js";
import { FetcherHttpError } from "./network.js";

export interface FeedFetchWorkHandlerOptions {
  readonly config: FetcherConfig;
  readonly dependencies: FetcherDependencies;
  readonly idFactory?: FetcherIdFactory;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
}

interface FeedFetchRequest {
  readonly feedId: string;
  readonly feedUrl: string;
  readonly pipelineRunId: string;
  readonly traceparent: string;
  readonly maxItems: number;
  readonly timeoutMs: number;
}

const FETCH_QUEUE = getWorkerRoute("fetch").mainQueue.name;
const DIAGNOSTIC_SAMPLE_BYTES = 256;
const DEFAULT_MAX_ITEMS = 35;
const MAX_ITEMS = 500;

export function createFeedFetchWorkHandler(options: FeedFetchWorkHandlerOptions): FetcherWorkHandler {
  const idFactory = options.idFactory ?? createCryptoFetcherIdFactory();

  return {
    name: "feed-fetch-work-handler",
    handle: (context, tools) => handleFeedFetch(context, tools, options, idFactory)
  };
}

async function handleFeedFetch(
  context: RuntimeMessageContext,
  tools: FetcherWorkTools,
  options: FeedFetchWorkHandlerOptions,
  idFactory: FetcherIdFactory
): Promise<RuntimeHandlerResult> {
  const request = feedFetchRequestFromContext(context, options.config);
  const startedAtMs = options.dependencies.clock.now().getTime();
  const fetchedAt = runtimeNow(options.dependencies.clock);

  try {
    const feedUrl = new URL(request.feedUrl);
    const dnsDecision = await options.dependencies.dnsPolicy.evaluate(feedUrl);

    if (!dnsDecision.allowed) {
      await recordAndObserve(options, {
        feedId: request.feedId,
        feedUrl: request.feedUrl,
        fetchedAt,
        fetchStatus: "permanent_failure",
        bodyBytes: 0,
        itemCount: 0,
        durationMs: elapsedMs(options, startedAtMs),
        diagnosticSample: dnsDecision.reason
      }, "failure");

      return {
        status: "terminal-failure",
        reason: dnsDecision.reason
      };
    }

    const metadata = await options.dependencies.stateStore.getFeedMetadata(request.feedId);
    const response = await options.dependencies.httpClient.request({
      url: feedUrl,
      headers: conditionalHeaders(metadata),
      userAgent: options.config.fetchPolicy.userAgent,
      connectTimeoutMs: options.config.fetchPolicy.connectTimeoutMs,
      readTimeoutMs: options.config.fetchPolicy.readTimeoutMs,
      totalTimeoutMs: request.timeoutMs,
      maxRedirects: options.config.fetchPolicy.maxRedirects,
      maxResponseBytes: options.config.fetchPolicy.maxResponseBytes
    });

    if (response.statusCode === 304) {
      await recordAndObserve(options, {
        feedId: request.feedId,
        feedUrl: request.feedUrl,
        fetchedAt,
        fetchStatus: "unchanged",
        httpStatus: response.statusCode,
        ...(metadata?.etag === undefined ? {} : {
          etag: metadata.etag
        }),
        ...(metadata?.lastModified === undefined ? {} : {
          lastModified: metadata.lastModified
        }),
        ...(metadata?.contentFingerprint === undefined ? {} : {
          contentFingerprint: metadata.contentFingerprint
        }),
        bodyBytes: response.bodyBytes,
        itemCount: 0,
        durationMs: response.durationMs
      }, "success");

      return {
        status: "ok"
      };
    }

    if (isTransientStatus(response.statusCode)) {
      await recordHttpFailure(options, request, response, fetchedAt, "transient_failure", "retry");

      return {
        status: "retry",
        reason: `http-${String(response.statusCode)}`
      };
    }

    if (response.statusCode < 200 || response.statusCode >= 300) {
      await recordHttpFailure(options, request, response, fetchedAt, "permanent_failure", "failure");

      return {
        status: "terminal-failure",
        reason: `http-${String(response.statusCode)}`
      };
    }

    if (!isAcceptedContentType(response.headers["content-type"], options.config.fetchPolicy.acceptedContentTypes)) {
      await recordHttpFailure(options, request, response, fetchedAt, "permanent_failure", "failure");

      return {
        status: "terminal-failure",
        reason: "unsupported-content-type"
      };
    }

    const contentFingerprint = sha256Hex(response.body);

    if (metadata?.contentFingerprint === contentFingerprint) {
      await recordAndObserve(options, {
        feedId: request.feedId,
        feedUrl: request.feedUrl,
        fetchedAt,
        fetchStatus: "unchanged",
        httpStatus: response.statusCode,
        ...responseMetadata(response, contentFingerprint),
        bodyBytes: response.bodyBytes,
        itemCount: 0,
        durationMs: response.durationMs,
        diagnosticSample: "content-fingerprint-match"
      }, "success");

      return {
        status: "ok"
      };
    }

    const parsed = parseFeedXml(decodeBody(response), response.finalUrl, request.feedId);
    const itemRefs = parsed.items
      .slice(0, request.maxItems)
      .map((item) => candidateReference(request.feedId, contentFingerprint, item));

    await recordAndObserve(options, {
      feedId: request.feedId,
      feedUrl: request.feedUrl,
      fetchedAt,
      fetchStatus: "success",
      httpStatus: response.statusCode,
      ...responseMetadata(response, contentFingerprint),
      bodyBytes: response.bodyBytes,
      itemCount: itemRefs.length,
      durationMs: response.durationMs,
      itemRefs
    }, "success");

    for (const itemRef of itemRefs) {
      const claim = await options.dependencies.stateStore.claimCandidate(itemRef.candidateId, {
        feedId: request.feedId,
        sourceItemId: itemRef.sourceItemId,
        contentFingerprint,
        firstSeenAt: fetchedAt
      });

      if (claim.status === "already-published") {
        continue;
      }

      const command = createCanonicalizationPublishCommand(context, request, itemRef, idFactory, options.config, contentFingerprint, fetchedAt);
      const receipt = await tools.publish(command);
      await options.dependencies.stateStore.markCandidatePublished(itemRef.candidateId, {
        publishedAt: runtimeNow(options.dependencies.clock),
        messageId: receipt.messageId,
        idempotencyKey: command.envelope.idempotencyKey
      });
    }

    return {
      status: "ok"
    };
  } catch (error: unknown) {
    return handleFetchError(error, options, request, fetchedAt, elapsedMs(options, startedAtMs));
  }
}

async function handleFetchError(
  error: unknown,
  options: FeedFetchWorkHandlerOptions,
  request: FeedFetchRequest,
  fetchedAt: string,
  durationMs: number
): Promise<RuntimeHandlerResult> {
  if (error instanceof FeedParseError) {
    await recordAndObserve(options, {
      feedId: request.feedId,
      feedUrl: request.feedUrl,
      fetchedAt,
      fetchStatus: "permanent_failure",
      bodyBytes: 0,
      itemCount: 0,
      durationMs,
      diagnosticSample: diagnosticSample(error.message)
    }, "failure");

    return {
      status: "terminal-failure",
      reason: error.name
    };
  }

  if (error instanceof FetcherHttpError && error.name === "ResponseTooLargeError") {
    await recordAndObserve(options, {
      feedId: request.feedId,
      feedUrl: request.feedUrl,
      fetchedAt,
      fetchStatus: "permanent_failure",
      bodyBytes: 0,
      itemCount: 0,
      durationMs,
      diagnosticSample: diagnosticSample(error.name)
    }, "failure");

    return {
      status: "terminal-failure",
      reason: error.name
    };
  }

  await recordAndObserve(options, {
    feedId: request.feedId,
    feedUrl: request.feedUrl,
    fetchedAt,
    fetchStatus: "transient_failure",
    bodyBytes: 0,
    itemCount: 0,
    durationMs,
    diagnosticSample: diagnosticSample(classifyError(error))
  }, "retry");

  return {
    status: "retry",
    reason: classifyError(error)
  };
}

function feedFetchRequestFromContext(context: RuntimeMessageContext, config: FetcherConfig): FeedFetchRequest {
  const limits = recordValue(context.payload.limits);
  const payloadTimeoutMs = integerValue(limits?.timeoutMs);
  const payloadMaxItems = integerValue(limits?.maxItems);

  return {
    feedId: stringValue(context.payload.feedId, "feedId"),
    feedUrl: stringValue(context.payload.feedUrl, "feedUrl"),
    pipelineRunId: stringValue(context.payload.pipelineRunId, "pipelineRunId"),
    traceparent: stringValue(context.payload.traceparent, "traceparent"),
    timeoutMs: Math.min(payloadTimeoutMs ?? config.fetchPolicy.totalTimeoutMs, config.fetchPolicy.totalTimeoutMs),
    maxItems: Math.min(Math.max(payloadMaxItems ?? DEFAULT_MAX_ITEMS, 0), MAX_ITEMS)
  };
}

function createCanonicalizationPublishCommand(
  context: RuntimeMessageContext,
  request: FeedFetchRequest,
  itemRef: FetcherCandidateReference,
  idFactory: FetcherIdFactory,
  config: FetcherConfig,
  contentFingerprint: string,
  producedAt: string
): BrokerPublishCommand {
  const route = getWorkerRoute("canonicalization");
  const messageId = idFactory.uuid();
  const idempotencyKey = `fetcher:canonicalization:${itemRef.candidateId}:${contentFingerprint.slice(0, 24)}`;
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.canonicalArticleCandidate,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: request.pipelineRunId,
    stageExecutionId: idFactory.uuid(),
    sourceMessageId: context.envelope.messageId,
    idempotencyKey,
    traceparent: request.traceparent,
    producedAt,
    candidateId: itemRef.candidateId,
    feedId: request.feedId,
    sourceItemId: itemRef.sourceItemId,
    originalUrl: itemRef.originalUrl,
    canonicalUrl: itemRef.canonicalUrl,
    title: itemRef.title,
    sourceName: itemRef.sourceName,
    ...(itemRef.publishedAt === undefined ? {} : {
      publishedAt: itemRef.publishedAt
    }),
    dedupeStatus: "new"
  } as const;
  const validation = validateStagePayload(payload);

  if (!validation.ok) {
    throw new Error(`Invalid canonicalization payload: ${validation.issues.map((issue) => `${issue.path}:${issue.code}`).join(",")}`);
  }

  return {
    envelope: assertWorkerEnvelope({
      schemaId: route.schemaId,
      schemaVersion: 1,
      route: "canonicalization",
      messageId,
      causationId: context.envelope.messageId,
      correlationId: context.envelope.correlationId,
      traceparent: context.envelope.traceparent,
      ...(context.envelope.tracestate === undefined ? {} : {
        tracestate: context.envelope.tracestate
      }),
      idempotencyKey,
      aggregate: {
        type: "candidate",
        id: itemRef.candidateId,
        version: 1
      },
      occurredAt: producedAt,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: producedAt
      },
      producer: {
        name: "fetcher",
        version: config.serviceVersion,
        instanceId: config.host
      },
      payloadRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/feed-fetcher/${encodeURIComponent(request.feedId)}/${encodeURIComponent(itemRef.candidateId)}`,
        mediaType: "application/json",
        sizeBytes: getStagePayloadSizeBytes(payload)
      }
    }),
    payload
  };
}

function candidateReference(
  feedId: string,
  contentFingerprint: string,
  item: ParsedFeedItem
): FetcherCandidateReference {
  const candidateId = stableCandidateId([
    feedId,
    item.sourceItemId,
    item.canonicalUrl,
    contentFingerprint
  ]);

  return {
    candidateId,
    sourceItemId: item.sourceItemId,
    originalUrl: item.originalUrl,
    canonicalUrl: item.canonicalUrl,
    title: item.title,
    sourceName: item.sourceName,
    ...(item.publishedAt === undefined ? {} : {
      publishedAt: item.publishedAt
    }),
    ...(item.excerpt === undefined ? {} : {
      excerpt: item.excerpt
    }),
    ...(item.imageUrl === undefined ? {} : {
      imageUrl: item.imageUrl
    }),
    ...(item.language === undefined ? {} : {
      language: item.language
    })
  };
}

async function recordHttpFailure(
  options: FeedFetchWorkHandlerOptions,
  request: FeedFetchRequest,
  response: FetcherHttpResponse,
  fetchedAt: string,
  fetchStatus: FetcherFetchOutcome["fetchStatus"],
  outcome: RuntimeTelemetryOutcome
): Promise<void> {
  await recordAndObserve(options, {
    feedId: request.feedId,
    feedUrl: request.feedUrl,
    fetchedAt,
    fetchStatus,
    httpStatus: response.statusCode,
    bodyBytes: response.bodyBytes,
    itemCount: 0,
    durationMs: response.durationMs,
    diagnosticSample: response.headers["content-type"] ?? `http-${String(response.statusCode)}`
  }, outcome);
}

async function recordAndObserve(
  options: FeedFetchWorkHandlerOptions,
  outcome: FetcherFetchOutcome,
  telemetryOutcome: RuntimeTelemetryOutcome
): Promise<void> {
  await options.dependencies.stateStore.recordFetchOutcome(outcome);
  options.metrics?.recordDependencyLatency(FETCH_QUEUE, outcome.durationMs, telemetryOutcome);
  await emitRuntimeTelemetry(options.telemetry, {
    name: "runtime.dependency.observed",
    level: telemetryOutcome === "success" ? "info" : telemetryOutcome === "retry" ? "warn" : "error",
    at: outcome.fetchedAt,
    stage: "fetch",
    queue: FETCH_QUEUE,
    durationMs: outcome.durationMs,
    outcome: telemetryOutcome,
    attributes: {
      event: "fetcher.feed.completed",
      dependency: "feed-fetch",
      feedId: outcome.feedId,
      fetchStatus: outcome.fetchStatus,
      statusClass: statusClass(outcome.httpStatus),
      bytes: outcome.bodyBytes,
      itemCount: outcome.itemCount
    }
  });
}

function responseMetadata(response: FetcherHttpResponse, contentFingerprint: string): Pick<FetcherFetchOutcome, "etag" | "lastModified" | "contentFingerprint"> {
  return {
    ...(response.headers.etag === undefined ? {} : {
      etag: response.headers.etag
    }),
    ...(response.headers["last-modified"] === undefined ? {} : {
      lastModified: response.headers["last-modified"]
    }),
    contentFingerprint
  };
}

function conditionalHeaders(metadata: Awaited<ReturnType<FetcherDependencies["stateStore"]["getFeedMetadata"]>>): Readonly<Record<string, string>> {
  return {
    ...(metadata?.etag === undefined ? {} : {
      "if-none-match": metadata.etag
    }),
    ...(metadata?.lastModified === undefined ? {} : {
      "if-modified-since": metadata.lastModified
    })
  };
}

function isTransientStatus(statusCode: number): boolean {
  return statusCode === 408 || statusCode === 425 || statusCode === 429 || (statusCode >= 500 && statusCode <= 599);
}

function isAcceptedContentType(contentType: string | undefined, accepted: readonly string[]): boolean {
  if (contentType === undefined) {
    return true;
  }

  const normalized = contentType.split(";")[0]?.trim().toLowerCase();

  return normalized === undefined || accepted.includes(normalized);
}

function decodeBody(response: FetcherHttpResponse): string {
  const contentType = response.headers["content-type"];
  const charset = contentType?.match(/charset=([^;]+)/iu)?.[1]?.trim();
  const decoder = new TextDecoder(charset ?? "utf-8", {
    fatal: false
  });

  return decoder.decode(response.body);
}

function statusClass(httpStatus: number | undefined): string {
  if (httpStatus === undefined) {
    return "none";
  }

  return `${String(Math.floor(httpStatus / 100))}xx`;
}

function elapsedMs(options: FeedFetchWorkHandlerOptions, startedAtMs: number): number {
  return Math.max(0, options.dependencies.clock.now().getTime() - startedAtMs);
}

function recordValue(value: unknown): Readonly<Record<string, unknown>> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : undefined;
}

function stringValue(value: unknown, key: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Feed fetch payload is missing ${key}.`);
  }

  return value;
}

function integerValue(value: unknown): number | undefined {
  return Number.isInteger(value) ? value as number : undefined;
}

function classifyError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-fetch-error";
}

function diagnosticSample(value: string): string {
  return value.replace(/(token|secret|password|api[_-]?key)=\S+/giu, "$1=[redacted]").slice(0, DIAGNOSTIC_SAMPLE_BYTES);
}
