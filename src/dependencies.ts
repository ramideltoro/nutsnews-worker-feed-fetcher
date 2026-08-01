import type {
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock,
  RuntimeHandlerResult,
  RuntimeIdempotencyStore,
  RuntimeMessageContext
} from "@ramideltoro/nutsnews-worker-runtime";

export interface FetcherDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export type FetcherDependencyAdapterMode = "test" | "production" | "unknown";
export type FetcherAggregateAdapterMode = "in_memory" | "mixed" | "production" | "unknown";

export interface FetcherDependencyAdapterIdentity {
  readonly stateStore: FetcherDependencyAdapterMode;
  readonly httpClient: FetcherDependencyAdapterMode;
  readonly dnsPolicy: FetcherDependencyAdapterMode;
  readonly broker: FetcherDependencyAdapterMode;
  readonly aggregate: FetcherAggregateAdapterMode;
}

export interface FetcherHttpRequest {
  readonly url: URL;
  readonly headers?: Readonly<Record<string, string>>;
  readonly initialDnsDecision?: FetcherDnsPolicyDecision;
  readonly redirectPolicy?: Pick<FetcherDnsPolicy, "evaluate">;
  readonly userAgent: string;
  readonly connectTimeoutMs: number;
  readonly readTimeoutMs: number;
  readonly totalTimeoutMs: number;
  readonly maxRedirects: number;
  readonly maxResponseBytes: number;
}

export interface FetcherHttpResponse {
  readonly statusCode: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
  readonly bodyBytes: number;
  readonly finalUrl: string;
  readonly durationMs: number;
}

export interface FetcherHttpClient {
  readonly name: string;
  readonly adapterMode: FetcherDependencyAdapterMode;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  request(request: FetcherHttpRequest): Promise<FetcherHttpResponse>;
}

export type FetcherDnsPolicyReason =
  | "allowed"
  | "blocked-loopback-address"
  | "blocked-link-local-address"
  | "blocked-metadata-address"
  | "blocked-private-address"
  | "blocked-localhost"
  | "blocked-unsupported-protocol";

export interface FetcherDnsPolicyDecision {
  readonly allowed: boolean;
  readonly reason: FetcherDnsPolicyReason;
  readonly resolvedAddresses?: readonly FetcherResolvedAddress[];
}

export interface FetcherResolvedAddress {
  readonly address: string;
  readonly family: 4 | 6;
}

export interface FetcherDnsPolicy {
  readonly name: string;
  readonly adapterMode: FetcherDependencyAdapterMode;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  evaluate(url: URL): FetcherDnsPolicyDecision | Promise<FetcherDnsPolicyDecision>;
}

export interface FetcherBrokerTransport extends RuntimeBrokerTransport {
  readonly name: string;
  readonly adapterMode: FetcherDependencyAdapterMode;
}

export type FetcherStateStoreMode = "local-memory" | "postgresql" | "unsupported";

export interface FetcherDurableStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  readonly mode: FetcherStateStoreMode;
  readonly adapter: string;
  readonly durable: boolean;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  getFeedMetadata(feedId: string): Promise<FetcherFeedMetadata | undefined>;
  recordFetchOutcome(outcome: FetcherFetchOutcome): Promise<void>;
  claimCandidate(candidateId: string, claim: FetcherCandidateClaim): Promise<FetcherCandidateClaimResult>;
  markCandidatePublished(candidateId: string, publication: FetcherCandidatePublication): Promise<void>;
  markCandidatePublishFailed(candidateId: string, failure: FetcherCandidatePublicationFailure): Promise<void>;
  listPendingCandidatePublications(query: FetcherPendingPublicationQuery): Promise<readonly FetcherPendingCandidatePublication[]>;
  close?(): Promise<void>;
}

export interface FetcherFeedMetadata {
  readonly feedId: string;
  readonly etag?: string;
  readonly lastModified?: string;
  readonly contentFingerprint?: string;
  readonly fetchedAt?: string;
}

export type FetcherFetchStatus =
  | "success"
  | "unchanged"
  | "transient_failure"
  | "permanent_failure";

export type FetcherFailureClass =
  | "dns"
  | "connect"
  | "tls"
  | "timeout"
  | "redirect"
  | "http_status"
  | "content_type"
  | "oversize"
  | "malformed_xml"
  | "parser"
  | "validation"
  | "security"
  | "unknown";

export interface FetcherFailureDetails {
  readonly failureClass: FetcherFailureClass;
  readonly code: string;
  readonly retryable: boolean;
  readonly action: "retry" | "dlq";
  readonly safeFeedUrl: string;
  readonly diagnosticSample: string;
  readonly httpStatus?: number;
  readonly retryAfterMs?: number;
}

export interface FetcherFetchOutcome {
  readonly feedId: string;
  readonly feedUrl: string;
  readonly fetchedAt: string;
  readonly fetchStatus: FetcherFetchStatus;
  readonly httpStatus?: number;
  readonly etag?: string;
  readonly lastModified?: string;
  readonly contentFingerprint?: string;
  readonly bodyBytes: number;
  readonly itemCount: number;
  readonly durationMs: number;
  readonly itemRefs?: readonly FetcherCandidateReference[];
  readonly diagnosticSample?: string;
  readonly failure?: FetcherFailureDetails;
}

export interface FetcherCandidateReference {
  readonly candidateId: string;
  readonly sourceItemId: string;
  readonly originalUrl: string;
  readonly canonicalUrl: string;
  readonly title: string;
  readonly sourceName: string;
  readonly publishedAt?: string;
  readonly excerpt?: string;
  readonly imageUrl?: string;
  readonly language?: string;
}

export interface FetcherCandidateClaim {
  readonly feedId: string;
  readonly sourceItemId: string;
  readonly contentFingerprint: string;
  readonly firstSeenAt: string;
  readonly claimOwnerKey: string;
  readonly command: BrokerPublishCommand;
}

export type FetcherCandidateClaimResult =
  | {
      readonly status: "claimed";
      readonly command: BrokerPublishCommand;
    }
  | {
      readonly status: "already-published";
      readonly publishedAt: string;
      readonly messageId: string;
    }
  | {
      readonly status: "in-progress";
      readonly retryAfterMs: number;
    };

export interface FetcherCandidatePublication {
  readonly publishedAt: string;
  readonly messageId: string;
  readonly idempotencyKey: string;
  readonly claimOwnerKey: string;
}

export interface FetcherCandidatePublicationFailure {
  readonly failedAt: string;
  readonly idempotencyKey: string;
  readonly claimOwnerKey: string;
  readonly reason: string;
}

export interface FetcherPendingPublicationQuery {
  readonly maxItems: number;
  readonly minAgeSeconds: number;
}

export interface FetcherPendingCandidatePublication {
  readonly candidateId: string;
  readonly claimOwnerKey: string;
  readonly command: BrokerPublishCommand;
  readonly createdAt: string;
}

export interface FetcherWorkTools {
  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt>;
}

export interface FetcherWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext, tools: FetcherWorkTools): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface FetcherDependencies {
  readonly clock: RuntimeClock;
  readonly httpClient: FetcherHttpClient;
  readonly dnsPolicy: FetcherDnsPolicy;
  readonly stateStore: FetcherDurableStateStore;
  readonly brokerTransport: FetcherBrokerTransport;
  readonly workHandler: FetcherWorkHandler;
}

export function fetcherDependencyAdapterIdentity(dependencies: {
  readonly stateStore: Pick<FetcherDurableStateStore, "mode" | "durable">;
  readonly httpClient: Pick<FetcherHttpClient, "adapterMode">;
  readonly dnsPolicy: Pick<FetcherDnsPolicy, "adapterMode">;
  readonly brokerTransport: Pick<FetcherBrokerTransport, "adapterMode">;
}): FetcherDependencyAdapterIdentity {
  const modes = {
    stateStore: stateStoreAdapterMode(dependencies.stateStore),
    httpClient: dependencies.httpClient.adapterMode,
    dnsPolicy: dependencies.dnsPolicy.adapterMode,
    broker: dependencies.brokerTransport.adapterMode
  };
  const values = Object.values(modes);
  const aggregate: FetcherAggregateAdapterMode = values.every((mode) => mode === "production")
    ? "production"
    : values.every((mode) => mode === "test")
      ? "in_memory"
      : values.every((mode) => mode === "unknown")
        ? "unknown"
        : "mixed";

  return {
    ...modes,
    aggregate
  };
}

function stateStoreAdapterMode(
  stateStore: Pick<FetcherDurableStateStore, "mode" | "durable">
): FetcherDependencyAdapterMode {
  if (stateStore.mode === "postgresql" && stateStore.durable) {
    return "production";
  }

  if (stateStore.mode === "local-memory" && !stateStore.durable) {
    return "test";
  }

  return "unknown";
}
