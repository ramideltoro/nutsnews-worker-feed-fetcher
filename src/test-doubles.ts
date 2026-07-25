import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createInMemoryIdempotencyStore,
  type BrokerConsumerHandle,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeIdempotencyClaimContext,
  type RuntimeIdempotencyClaimResult,
  type RuntimeIdempotencyCompletion,
  type RuntimeIdempotencyFailure,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  FetcherDependencies,
  FetcherCandidateClaim,
  FetcherCandidateClaimResult,
  FetcherCandidatePublication,
  FetcherDependencyProbe,
  FetcherDnsPolicy,
  FetcherDnsPolicyDecision,
  FetcherDurableStateStore,
  FetcherFeedMetadata,
  FetcherFetchOutcome,
  FetcherHttpClient,
  FetcherHttpRequest,
  FetcherHttpResponse,
  FetcherWorkTools,
  FetcherWorkHandler
} from "./dependencies.js";

export class ManualFetcherClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class LocalHttpClient implements FetcherHttpClient {
  readonly name: string = "local-http-client";
  status: FetcherDependencyProbe["status"] = "ok";
  readonly requests: FetcherHttpRequest[] = [];
  response: FetcherHttpResponse = {
    statusCode: 200,
    headers: {},
    body: new Uint8Array(),
    bodyBytes: 0,
    finalUrl: "https://feeds.example.test/world.xml",
    durationMs: 0
  };

  probe(): FetcherDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local HTTP client ready" : "local HTTP client degraded"
    };
  }

  request(request: FetcherHttpRequest): Promise<FetcherHttpResponse> {
    this.requests.push(request);
    return Promise.resolve(this.response);
  }
}

export class LocalDnsPolicy implements FetcherDnsPolicy {
  readonly name: string = "local-dns-policy";
  status: FetcherDependencyProbe["status"] = "ok";

  probe(): FetcherDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local DNS policy ready" : "local DNS policy degraded"
    };
  }

  evaluate(url: URL): FetcherDnsPolicyDecision {
    if (url.protocol !== "https:" && url.protocol !== "http:") {
      return {
        allowed: false,
        reason: "blocked-unsupported-protocol"
      };
    }

    if (url.hostname === "localhost") {
      return {
        allowed: false,
        reason: "blocked-localhost"
      };
    }

    if (url.hostname === "127.0.0.1" || url.hostname === "[::1]") {
      return {
        allowed: false,
        reason: "blocked-loopback-address"
      };
    }

    if (url.hostname === "169.254.169.254") {
      return {
        allowed: false,
        reason: "blocked-metadata-address"
      };
    }

    return {
      allowed: true,
      reason: "allowed"
    };
  }
}

export class InMemoryFetcherStateStore implements FetcherDurableStateStore {
  readonly name: string = "local-durable-state";
  status: FetcherDependencyProbe["status"] = "ok";
  readonly outcomes: FetcherFetchOutcome[] = [];
  private readonly feedMetadata = new Map<string, FetcherFeedMetadata>();
  private readonly candidates = new Map<string, FetcherCandidatePublication>();
  private readonly store;

  constructor(clock: RuntimeClock = new ManualFetcherClock()) {
    this.store = createInMemoryIdempotencyStore(clock);
  }

  probe(): FetcherDependencyProbe {
    return {
      status: this.status,
      summary: this.status === "ok" ? "local durable state ready" : "local durable state degraded"
    };
  }

  claim(idempotencyKey: string, context: RuntimeIdempotencyClaimContext): Promise<RuntimeIdempotencyClaimResult> {
    return this.store.claim(idempotencyKey, context);
  }

  markCompleted(idempotencyKey: string, completion: RuntimeIdempotencyCompletion): Promise<void> {
    return this.store.markCompleted(idempotencyKey, completion);
  }

  markFailed(idempotencyKey: string, failure: RuntimeIdempotencyFailure): Promise<void> {
    return this.store.markFailed(idempotencyKey, failure);
  }

  getFeedMetadata(feedId: string): Promise<FetcherFeedMetadata | undefined> {
    return Promise.resolve(this.feedMetadata.get(feedId));
  }

  recordFetchOutcome(outcome: FetcherFetchOutcome): Promise<void> {
    this.outcomes.push(outcome);

    if (outcome.fetchStatus === "success" || outcome.fetchStatus === "unchanged") {
      this.feedMetadata.set(outcome.feedId, {
        feedId: outcome.feedId,
        ...(outcome.etag === undefined ? {} : {
          etag: outcome.etag
        }),
        ...(outcome.lastModified === undefined ? {} : {
          lastModified: outcome.lastModified
        }),
        ...(outcome.contentFingerprint === undefined ? {} : {
          contentFingerprint: outcome.contentFingerprint
        }),
        fetchedAt: outcome.fetchedAt
      });
    }

    return Promise.resolve();
  }

  claimCandidate(candidateId: string, claim: FetcherCandidateClaim): Promise<FetcherCandidateClaimResult> {
    void claim;
    const existing = this.candidates.get(candidateId);

    if (existing !== undefined) {
      return Promise.resolve({
        status: "already-published",
        publishedAt: existing.publishedAt,
        messageId: existing.messageId
      });
    }

    return Promise.resolve({
      status: "claimed"
    });
  }

  markCandidatePublished(candidateId: string, publication: FetcherCandidatePublication): Promise<void> {
    this.candidates.set(candidateId, publication);
    return Promise.resolve();
  }
}

export class LocalFetcherWorkHandler implements FetcherWorkHandler {
  readonly name: string = "local-fetch-work-handler";
  readonly handled: RuntimeMessageContext[] = [];
  result: RuntimeHandlerResult = {
    status: "ok"
  };
  handleGate: Promise<void> | undefined;
  onHandleStart: (() => void) | undefined;

  async handle(context: RuntimeMessageContext, tools: FetcherWorkTools): Promise<RuntimeHandlerResult> {
    void tools;
    this.onHandleStart?.();
    await this.handleGate;
    this.handled.push(context);

    return this.result;
  }
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly published: BrokerPublishCommand[] = [];
  readonly assertedRoutes: WorkerRoute[] = [];
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();
  private deliveryCount = 0;
  private connected = false;
  private closed = false;

  get inFlightDeliveryCount(): number {
    return this.deliveryCount;
  }

  connect(): Promise<void> {
    this.connected = true;
    this.closed = false;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertedRoutes.splice(0, this.assertedRoutes.length, ...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return Promise.resolve({
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<BrokerConsumerHandle> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.consumers.set(stage, handler);

    return Promise.resolve({
      stage,
      cancel: () => {
        this.consumers.delete(stage);
        return Promise.resolve();
      }
    });
  }

  async deliver(stage: WorkerStage, delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
    const handler = this.consumers.get(stage);

    if (handler === undefined) {
      throw new Error(`No local consumer is registered for ${stage}.`);
    }

    this.deliveryCount += 1;

    try {
      return await handler(delivery);
    } finally {
      this.deliveryCount = Math.max(0, this.deliveryCount - 1);
    }
  }

  deliverFetch(delivery: RuntimeMessageDelivery = createMinimalFetchDelivery()): Promise<RuntimeMessageProcessingResult> {
    return this.deliver("fetch", delivery);
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    this.connected = false;
    this.consumers.clear();
    return Promise.resolve();
  }
}

export interface LocalFetcherDependencyOptions {
  readonly clock?: RuntimeClock;
  readonly httpClient?: FetcherHttpClient;
  readonly dnsPolicy?: FetcherDnsPolicy;
  readonly stateStore?: FetcherDurableStateStore;
  readonly brokerTransport?: RuntimeBrokerTransport;
  readonly workHandler?: FetcherWorkHandler;
}

export function createLocalFetcherDependencies(options: LocalFetcherDependencyOptions = {}): FetcherDependencies {
  const clock = options.clock ?? new ManualFetcherClock();

  return {
    clock,
    httpClient: options.httpClient ?? new LocalHttpClient(),
    dnsPolicy: options.dnsPolicy ?? new LocalDnsPolicy(),
    stateStore: options.stateStore ?? new InMemoryFetcherStateStore(clock),
    brokerTransport: options.brokerTransport ?? new LocalBrokerTransport(),
    workHandler: options.workHandler ?? new LocalFetcherWorkHandler()
  };
}

export function createMinimalFetchPayload(overrides: Readonly<Record<string, unknown>> = {}): Readonly<Record<string, unknown>> {
  const now = "2026-07-23T00:00:00.000Z";

  return {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.feedFetchRequest,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3602",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3603",
    idempotencyKey: "scheduler:feed:feed-world:20260723t000000000z",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    feedId: "feed-world",
    feedUrl: "https://feeds.example.test/world.xml",
    shardIndex: 0,
    shardCount: 1,
    fetchReason: "scheduled",
    limits: {
      timeoutMs: 15_000,
      maxItems: 35
    },
    ...overrides
  };
}

export function createMinimalFetchEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("fetch");
  const now = "2026-07-23T00:00:00.000Z";

  return assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3620",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3610",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3610",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "scheduler:feed:feed-world:20260723t000000000z",
    aggregate: {
      type: "feed",
      id: "feed-world",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: now
    },
    producer: {
      name: "scheduler",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/scheduler/feed-world",
      mediaType: "application/json",
      sizeBytes: 256
    },
    ...overrides
  });
}

export function createMinimalFetchDelivery(
  overrides: {
    readonly envelope?: Partial<WorkerMessageEnvelope>;
    readonly payload?: Readonly<Record<string, unknown>>;
  } = {}
): RuntimeMessageDelivery {
  return {
    envelope: createMinimalFetchEnvelope(overrides.envelope ?? {}),
    payload: createMinimalFetchPayload(overrides.payload),
    receivedAt: "2026-07-23T00:00:01.000Z"
  };
}
