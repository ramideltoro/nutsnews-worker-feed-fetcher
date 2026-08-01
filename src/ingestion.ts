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
  type RuntimeHandlerResult,
  type RuntimeMessageContext,
  type RuntimeTelemetryOutcome,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherConfig } from "./config.js";
import type {
  FetcherCandidateReference,
  FetcherDependencies,
  FetcherFailureClass,
  FetcherFailureDetails,
  FetcherFetchOutcome,
  FetcherHttpResponse,
  FetcherWorkHandler,
  FetcherWorkTools
} from "./dependencies.js";
import { isFetcherDefinitePublishError } from "./dependencies.js";
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
import { bestEffortTelemetrySink } from "./telemetry-safety.js";

export interface FeedFetchWorkHandlerOptions {
  readonly config: FetcherConfig;
  readonly dependencies: FetcherDependencies;
  readonly idFactory?: FetcherIdFactory;
  readonly telemetry?: RuntimeTelemetrySink;
}

interface FeedFetchRequest {
  readonly feedId: string;
  readonly feedUrl: string;
  readonly pipelineRunId: string;
  readonly traceparent: string;
  readonly maxItems: number;
  readonly timeoutMs: number;
  readonly attemptCount: number;
  readonly maxAttempts: number;
}

interface FetcherFailureDecision {
  readonly failureClass: FetcherFailureClass;
  readonly code: string;
  readonly fetchStatus: Extract<FetcherFetchOutcome["fetchStatus"], "transient_failure" | "permanent_failure">;
  readonly telemetryOutcome: Extract<RuntimeTelemetryOutcome, "retry" | "failure">;
  readonly retryable: boolean;
  readonly diagnosticSample: string;
  readonly retryAfterMs?: number;
}

class FetcherPayloadValidationError extends Error {
  readonly issueRefs: readonly string[];

  constructor(issueRefs: readonly string[]) {
    super(`Canonicalization payload failed validation: ${issueRefs.join(",")}`);
    this.name = "FetcherPayloadValidationError";
    this.issueRefs = issueRefs;
  }
}

class FetcherUnsupportedCharsetError extends Error {
  readonly charset: string;

  constructor(charset: string, cause: unknown) {
    super(`Feed response declares an unsupported charset: ${charset}.`, {
      cause
    });
    this.name = "FetcherUnsupportedCharsetError";
    this.charset = charset;
  }
}

class FetcherInternalOperationError extends Error {
  readonly operation: string;

  constructor(operation: string, cause: unknown) {
    super(`Internal fetcher operation failed: ${operation}.`, {
      cause
    });
    this.name = "FetcherInternalOperationError";
    this.operation = operation;
  }
}

class FetcherSourceOperationError extends Error {
  readonly operation: string;

  constructor(operation: string, cause: unknown) {
    super(`Feed source operation failed: ${operation}.`, {
      cause
    });
    this.name = "FetcherSourceOperationError";
    this.operation = operation;
  }
}

const FETCH_QUEUE = getWorkerRoute("fetch").mainQueue.name;
const DIAGNOSTIC_SAMPLE_BYTES = 256;
const DEFAULT_MAX_ITEMS = 35;
const MAX_ITEMS = 500;

export function createFeedFetchWorkHandler(options: FeedFetchWorkHandlerOptions): FetcherWorkHandler {
  const idFactory = options.idFactory ?? createCryptoFetcherIdFactory();
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const safeOptions = telemetry === undefined ? options : {
    ...options,
    telemetry
  };

  return {
    name: "feed-fetch-work-handler",
    handle: (context, tools) => handleFeedFetch(context, tools, safeOptions, idFactory)
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
    const dnsDecision = await runSourceOperation("resolve-feed-host", () =>
      options.dependencies.dnsPolicy.evaluate(feedUrl)
    );

    if (!dnsDecision.allowed) {
      const failure = permanentFailure("security", dnsDecision.reason, dnsDecision.reason);

      await recordFailure(options, request, fetchedAt, elapsedMs(options, startedAtMs), failure);

      return resultFromFailure(failure);
    }

    const metadata = await runInternalOperation("read-feed-metadata", () =>
      options.dependencies.stateStore.getFeedMetadata(request.feedId)
    );
    const response = await runSourceOperation("retrieve-feed", () =>
      options.dependencies.httpClient.request({
        url: feedUrl,
        headers: conditionalHeaders(metadata),
        userAgent: options.config.fetchPolicy.userAgent,
        connectTimeoutMs: options.config.fetchPolicy.connectTimeoutMs,
        readTimeoutMs: options.config.fetchPolicy.readTimeoutMs,
        totalTimeoutMs: request.timeoutMs,
        maxRedirects: options.config.fetchPolicy.maxRedirects,
        maxResponseBytes: options.config.fetchPolicy.maxResponseBytes,
        initialDnsDecision: dnsDecision,
        redirectPolicy: options.dependencies.dnsPolicy
      })
    );

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
      const failure = transientFailure("http_status", `http-${String(response.statusCode)}`, response.headers["content-type"] ?? `http-${String(response.statusCode)}`, retryAfterMs(response.headers["retry-after"], fetchedAt, options.config.fetchPolicy.maxRetryAfterMs));

      await recordHttpFailure(options, request, response, fetchedAt, failure);

      return resultFromFailure(failure);
    }

    if (response.statusCode < 200 || response.statusCode >= 300) {
      const failure = permanentFailure("http_status", `http-${String(response.statusCode)}`, response.headers["content-type"] ?? `http-${String(response.statusCode)}`);

      await recordHttpFailure(options, request, response, fetchedAt, failure);

      return resultFromFailure(failure);
    }

    if (!isAcceptedContentType(response.headers["content-type"], options.config.fetchPolicy.acceptedContentTypes)) {
      const failure = permanentFailure("content_type", "unsupported-content-type", response.headers["content-type"] ?? "missing-content-type");

      await recordHttpFailure(options, request, response, fetchedAt, failure);

      return resultFromFailure(failure);
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
    const publishCommands = itemRefs.map((itemRef) =>
      createCanonicalizationPublishCommand(context, request, itemRef, idFactory, options.config, contentFingerprint, fetchedAt)
    );

    for (const [index, itemRef] of itemRefs.entries()) {
      const command = publishCommands[index];

      if (command === undefined) {
        throw new Error("Candidate publish command missing for item reference.");
      }

      const claim = await runInternalOperation("claim-candidate", () =>
        options.dependencies.stateStore.claimCandidate(itemRef.candidateId, {
          feedId: request.feedId,
          sourceItemId: itemRef.sourceItemId,
          contentFingerprint,
          firstSeenAt: fetchedAt,
          command
        })
      );

      if (claim.status === "already-published") {
        continue;
      }

      if (claim.status === "in-progress") {
        return {
          status: "retry",
          reason: "candidate-publication-in-progress",
          retryAfterMs: claim.retryAfterMs
        };
      }

      let receipt: Awaited<ReturnType<FetcherWorkTools["publish"]>>;

      try {
        receipt = await runInternalOperation("publish-candidate", () =>
          tools.publish(claim.command)
        );
      } catch (error: unknown) {
        if (isFetcherDefinitePublishError(error)) {
          await runInternalOperation("record-candidate-publication-failure", () =>
            options.dependencies.stateStore.markCandidatePublishFailed(itemRef.candidateId, {
              failedAt: runtimeNow(options.dependencies.clock),
              idempotencyKey: claim.command.envelope.idempotencyKey,
              claimToken: claim.claimToken,
              reason: error instanceof Error ? error.name : "CandidatePublishError"
            })
          );
        }

        // Unknown publish outcomes can already have reached the queue. Retain
        // the fenced lease so redelivery cannot immediately duplicate them.
        throw error;
      }

      // Keep finalization outside the publish catch. A confirmed publish whose
      // database response is lost is ambiguous and must never be reclassified
      // as an immediately retryable broker failure.
      await runInternalOperation("record-candidate-publication", () =>
        options.dependencies.stateStore.markCandidatePublished(itemRef.candidateId, {
          publishedAt: receipt.confirmedAt,
          messageId: receipt.messageId,
          idempotencyKey: claim.command.envelope.idempotencyKey,
          claimToken: claim.claimToken
        })
      );
    }

    // Commit the fingerprint only after every candidate has a confirmed,
    // durable publication record. A retry after partial fan-out can then
    // revisit this body, skip already-published candidates, and publish the
    // remainder instead of incorrectly treating the feed as unchanged.
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
  const failure = classifyFailure(error);

  if (failure === undefined) {
    throw error;
  }

  await recordFailure(options, request, fetchedAt, durationMs, failure);

  return resultFromFailure(failure);
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
    maxItems: Math.min(Math.max(payloadMaxItems ?? DEFAULT_MAX_ITEMS, 0), MAX_ITEMS),
    attemptCount: context.envelope.attempt.count,
    maxAttempts: context.envelope.attempt.max
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
    throw new FetcherPayloadValidationError(validation.issues.map((issue) => `${issue.path}:${issue.code}`));
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
  failure: FetcherFailureDecision
): Promise<void> {
  await recordFailure(options, request, fetchedAt, response.durationMs, failure, {
    httpStatus: response.statusCode,
    bodyBytes: response.bodyBytes
  });
}

async function recordFailure(
  options: FeedFetchWorkHandlerOptions,
  request: FeedFetchRequest,
  fetchedAt: string,
  durationMs: number,
  failure: FetcherFailureDecision,
  observed: {
    readonly httpStatus?: number;
    readonly bodyBytes?: number;
  } = {}
): Promise<void> {
  await recordAndObserve(options, {
    feedId: request.feedId,
    feedUrl: request.feedUrl,
    fetchedAt,
    fetchStatus: failure.fetchStatus,
    ...(observed.httpStatus === undefined ? {} : {
      httpStatus: observed.httpStatus
    }),
    bodyBytes: observed.bodyBytes ?? 0,
    itemCount: 0,
    durationMs,
    diagnosticSample: diagnosticSample(failure.diagnosticSample),
    failure: failureDetails(request, failure, observed.httpStatus)
  }, failure.telemetryOutcome);
}

async function recordAndObserve(
  options: FeedFetchWorkHandlerOptions,
  outcome: FetcherFetchOutcome,
  telemetryOutcome: RuntimeTelemetryOutcome
): Promise<void> {
  await runInternalOperation("record-feed-outcome", () =>
    options.dependencies.stateStore.recordFetchOutcome(outcome)
  );
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
      itemCount: outcome.itemCount,
      ...(outcome.failure === undefined ? {} : {
        failureClass: outcome.failure.failureClass,
        failureCode: outcome.failure.code,
        failureAction: outcome.failure.action,
        retryable: outcome.failure.retryable,
        ...(outcome.failure.retryAfterMs === undefined ? {} : {
          retryAfterMs: outcome.failure.retryAfterMs
        })
      })
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
  const charsetMatch = contentType?.match(/(?:^|;)\s*charset\s*=\s*(?:"([^"]*)"|([^;\s]*))/iu);
  const charset = (charsetMatch?.[1] ?? charsetMatch?.[2] ?? "utf-8").trim();
  let decoder: TextDecoder;

  try {
    decoder = new TextDecoder(charset, {
      fatal: false
    });
  } catch (error: unknown) {
    throw new FetcherUnsupportedCharsetError(charset, error);
  }

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

function resultFromFailure(failure: FetcherFailureDecision): RuntimeHandlerResult {
  if (failure.retryable) {
    return failure.retryAfterMs === undefined
      ? {
          status: "retry",
          reason: failure.code
        }
      : {
          status: "retry",
          reason: failure.code,
          retryAfterMs: failure.retryAfterMs
        };
  }

  return {
    status: "terminal-failure",
    reason: failure.code
  };
}

function failureDetails(
  request: FeedFetchRequest,
  failure: FetcherFailureDecision,
  httpStatus: number | undefined
): FetcherFailureDetails {
  return {
    failureClass: failure.failureClass,
    code: failure.code,
    retryable: failure.retryable,
    action: failure.retryable && request.attemptCount < request.maxAttempts ? "retry" : "dlq",
    safeFeedUrl: safeFeedUrl(request.feedUrl),
    diagnosticSample: diagnosticSample(failure.diagnosticSample),
    ...(httpStatus === undefined ? {} : {
      httpStatus
    }),
    ...(failure.retryAfterMs === undefined ? {} : {
      retryAfterMs: failure.retryAfterMs
    })
  };
}

function classifyFailure(error: unknown): FetcherFailureDecision | undefined {
  if (error instanceof FetcherInternalOperationError) {
    return undefined;
  }

  if (error instanceof FetcherSourceOperationError) {
    return classifyKnownSourceFailure(error.cause)
      ?? transientFailure("unknown", "unknown-fetch-error", errorName(error.cause));
  }

  return classifyKnownSourceFailure(error);
}

function classifyKnownSourceFailure(error: unknown): FetcherFailureDecision | undefined {

  if (error instanceof FetcherUnsupportedCharsetError) {
    return permanentFailure("content_type", "unsupported-charset", error.charset);
  }

  if (error instanceof FeedParseError) {
    if (error.code === "malformed-xml") {
      return permanentFailure("malformed_xml", "malformed-xml", error.message);
    }

    return permanentFailure("parser", error.code, error.message);
  }

  if (error instanceof FetcherPayloadValidationError) {
    return permanentFailure("validation", "canonicalization-payload-validation", error.issueRefs.join(","));
  }

  if (error instanceof FetcherHttpError) {
    return classifyFetcherHttpError(error);
  }

  const code = nestedErrorCode(error);

  if (code === "EAI_AGAIN") {
    return transientFailure("dns", "dns-temporary-failure", code);
  }

  if (code === "ENOTFOUND" || code === "ENODATA") {
    return permanentFailure("dns", "dns-name-not-found", code);
  }

  if (code !== undefined && isTlsFailureCode(code)) {
    return permanentFailure("tls", "tls-failure", code);
  }

  return undefined;
}

function classifyFetcherHttpError(error: FetcherHttpError): FetcherFailureDecision {
  if (error.name === "ResponseTooLargeError") {
    return permanentFailure("oversize", "response-too-large", error.name);
  }

  if (error.name === "RedirectBlockedError") {
    const policyReason = stringDiagnostic(error.diagnostics.policyReason);

    return permanentFailure("security", policyReason ?? "redirect-blocked", policyReason ?? error.name);
  }

  if (error.name === "RedirectLimitError" || error.name === "RedirectLocationError") {
    return permanentFailure("redirect", error.name === "RedirectLimitError" ? "redirect-limit-exceeded" : "redirect-location-missing", error.name);
  }

  if (error.name === "ConnectTimeoutError") {
    return transientFailure("connect", "connect-timeout", error.name);
  }

  if (error.name === "ReadTimeoutError" || error.name === "TotalTimeoutError") {
    return transientFailure("timeout", error.name === "ReadTimeoutError" ? "read-timeout" : "total-timeout", error.name);
  }

  return transientFailure("unknown", "http-client-error", error.name);
}

function transientFailure(
  failureClass: FetcherFailureClass,
  code: string,
  sample: string,
  retryAfterMsValue?: number
): FetcherFailureDecision {
  const base = {
    failureClass,
    code,
    fetchStatus: "transient_failure",
    telemetryOutcome: "retry",
    retryable: true,
    diagnosticSample: sample
  } as const;

  return retryAfterMsValue === undefined
    ? base
    : {
        ...base,
        retryAfterMs: retryAfterMsValue
      };
}

function permanentFailure(
  failureClass: FetcherFailureClass,
  code: string,
  sample: string
): FetcherFailureDecision {
  return {
    failureClass,
    code,
    fetchStatus: "permanent_failure",
    telemetryOutcome: "failure",
    retryable: false,
    diagnosticSample: sample
  };
}

function retryAfterMs(header: string | undefined, nowIso: string, maxRetryAfterMs: number): number | undefined {
  if (header === undefined) {
    return undefined;
  }

  const trimmed = header.trim();

  if (trimmed.length === 0) {
    return undefined;
  }

  const seconds = Number(trimmed);

  if (Number.isSafeInteger(seconds) && seconds > 0) {
    const delayMs = seconds * 1_000;

    return delayMs <= maxRetryAfterMs ? delayMs : undefined;
  }

  const retryAtMs = Date.parse(trimmed);
  const nowMs = Date.parse(nowIso);

  if (Number.isNaN(retryAtMs) || Number.isNaN(nowMs) || retryAtMs <= nowMs) {
    return undefined;
  }

  const delayMs = retryAtMs - nowMs;

  return delayMs <= maxRetryAfterMs ? delayMs : undefined;
}

function nestedErrorCode(error: unknown): string | undefined {
  const direct = stringProperty(error, "code");

  if (direct !== undefined) {
    return direct;
  }

  const cause = error instanceof Error ? error.cause : undefined;

  return stringProperty(cause, "code");
}

function stringProperty(value: unknown, key: string): string | undefined {
  const record = recordValue(value);
  const property = record?.[key];

  return typeof property === "string" && property.length > 0 ? property : undefined;
}

function stringDiagnostic(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function isTlsFailureCode(code: string): boolean {
  return code.startsWith("ERR_TLS") ||
    code.startsWith("CERT_") ||
    code.startsWith("DEPTH_ZERO_SELF_SIGNED_CERT") ||
    code.startsWith("SELF_SIGNED_CERT");
}

function errorName(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown";
}

function safeFeedUrl(value: string): string {
  try {
    const url = new URL(value);
    const path = url.pathname === "/" || url.pathname.length === 0 ? "/" : "/[path-redacted]";
    const query = url.search.length === 0 ? "" : "?[query-redacted]";

    return `${url.protocol}//${url.host}${path}${query}`;
  } catch {
    return diagnosticSample(value);
  }
}

async function runInternalOperation<T>(operation: string, execute: () => Promise<T>): Promise<T> {
  try {
    return await execute();
  } catch (error: unknown) {
    if (error instanceof FetcherInternalOperationError) {
      throw error;
    }

    throw new FetcherInternalOperationError(operation, error);
  }
}

async function runSourceOperation<T>(operation: string, execute: () => T | Promise<T>): Promise<T> {
  try {
    return await execute();
  } catch (error: unknown) {
    if (error instanceof FetcherSourceOperationError) {
      throw error;
    }

    throw new FetcherSourceOperationError(operation, error);
  }
}

function diagnosticSample(value: string): string {
  return value.replace(/(token|secret|password|api[_-]?key)=\S+/giu, "$1=[redacted]").slice(0, DIAGNOSTIC_SAMPLE_BYTES);
}
