import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getWorkerRoute,
  validateStagePayload,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  type BrokerConsumerHandle,
  type BrokerConsumerStatus,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeClock,
  type RuntimeHandlerResult,
  type RuntimeMessageContext,
  type RuntimeMessageDelivery,
  type RuntimeMessageProcessingResult
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  FetcherBrokerTransport,
  FetcherDependencies,
  FetcherDependencyAdapterMode,
  FetcherDependencyProbe,
  FetcherDnsPolicy,
  FetcherDnsPolicyDecision,
  FetcherDurableStateStore,
  FetcherHttpClient,
  FetcherHttpRequest,
  FetcherHttpResponse,
  FetcherWorkTools,
  FetcherWorkHandler
} from "./dependencies.js";
import { InMemoryFetcherStateStore } from "./state-store.js";

export { InMemoryFetcherStateStore };

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
  readonly adapterMode: FetcherDependencyAdapterMode;
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

  constructor(adapterMode: FetcherDependencyAdapterMode = "test") {
    this.adapterMode = adapterMode;
  }

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
  readonly adapterMode: FetcherDependencyAdapterMode;
  status: FetcherDependencyProbe["status"] = "ok";

  constructor(adapterMode: FetcherDependencyAdapterMode = "test") {
    this.adapterMode = adapterMode;
  }

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

export class LocalBrokerTransport implements FetcherBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly adapterMode: FetcherDependencyAdapterMode;
  readonly published: BrokerPublishCommand[] = [];
  readonly assertedRoutes: WorkerRoute[] = [];
  private readonly consumers = new Map<WorkerStage, BrokerDeliveryHandler>();
  private readonly consumerStateListeners = new Set<() => void>();
  private deliveryCount = 0;
  private connected = false;
  private closed = false;
  cancelGate: Promise<void> | undefined;
  cancelCalls = 0;

  constructor(adapterMode: FetcherDependencyAdapterMode = "test") {
    this.adapterMode = adapterMode;
  }

  get inFlightDeliveryCount(): number {
    return this.deliveryCount;
  }

  consumerStatus(stage: WorkerStage): BrokerConsumerStatus {
    const active = this.consumers.has(stage);

    return {
      stage,
      queue: getWorkerRoute(stage).mainQueue.name,
      state: active ? "active" : "inactive",
      activeConsumers: active ? 1 : 0,
      reason: active ? "local-consumer-registered" : "local-consumer-inactive",
      changedAt: new Date(0).toISOString()
    };
  }

  onConsumerStateChange(listener: () => void): () => void {
    this.consumerStateListeners.add(listener);

    return () => {
      this.consumerStateListeners.delete(listener);
    };
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
    this.notifyConsumerStateChange();

    return Promise.resolve({
      stage,
      cancel: async () => {
        this.cancelCalls += 1;
        await this.cancelGate;
        this.consumers.delete(stage);
        this.notifyConsumerStateChange();
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
    this.notifyConsumerStateChange();
    return Promise.resolve();
  }

  simulateChannelDrop(stage: WorkerStage = "fetch"): void {
    this.consumers.delete(stage);
    this.notifyConsumerStateChange();
  }

  private notifyConsumerStateChange(): void {
    for (const listener of this.consumerStateListeners) {
      listener();
    }
  }
}

export interface LocalFetcherDependencyOptions {
  readonly clock?: RuntimeClock;
  readonly httpClient?: FetcherHttpClient;
  readonly dnsPolicy?: FetcherDnsPolicy;
  readonly stateStore?: FetcherDurableStateStore;
  readonly brokerTransport?: FetcherBrokerTransport;
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

export function createMinimalCanonicalizationCommand(overrides: {
  readonly candidateId?: string;
  readonly messageId?: string;
  readonly idempotencyKey?: string;
} = {}): BrokerPublishCommand {
  const route = getWorkerRoute("canonicalization");
  const now = "2026-07-23T00:00:00.000Z";
  const candidateId = overrides.candidateId ?? "candidate-world-one";
  const messageId = overrides.messageId ?? "018f1598-2dd5-7c4f-9f92-8f7a7f8b3630";
  const idempotencyKey = overrides.idempotencyKey ?? `fetcher:canonicalization:${candidateId}:fingerprint-v1`;
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.canonicalArticleCandidate,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3601",
    stageExecutionId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3631",
    sourceMessageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3620",
    idempotencyKey,
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    producedAt: now,
    candidateId,
    feedId: "feed-world",
    sourceItemId: "guid-001",
    originalUrl: "https://articles.example.test/world/story-one",
    canonicalUrl: "https://articles.example.test/world/story-one",
    title: "Story One",
    sourceName: "World Source",
    dedupeStatus: "new"
  } as const;

  if (!validateStagePayload(payload).ok) {
    throw new Error("Minimal canonicalization command fixture is invalid.");
  }

  return {
    envelope: assertWorkerEnvelope({
      schemaId: route.schemaId,
      schemaVersion: 1,
      route: "canonicalization",
      messageId,
      causationId: payload.sourceMessageId,
      correlationId: payload.pipelineRunId,
      traceparent: payload.traceparent,
      idempotencyKey,
      aggregate: {
        type: "candidate",
        id: candidateId,
        version: 1
      },
      occurredAt: now,
      attempt: {
        count: 1,
        max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
        firstAttemptAt: now
      },
      producer: {
        name: "fetcher",
        version: "0.1.0",
        instanceId: "fetcher-test"
      },
      payloadRef: {
        kind: "backend-record",
        uri: `backend://worker-uplift/feed-fetcher/feed-world/${candidateId}`,
        mediaType: "application/json",
        sizeBytes: Buffer.byteLength(JSON.stringify(payload), "utf8")
      }
    }),
    payload
  };
}
