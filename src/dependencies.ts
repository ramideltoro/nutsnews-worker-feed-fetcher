import type {
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

export interface FetcherHttpRequest {
  readonly url: URL;
  readonly headers?: Readonly<Record<string, string>>;
  readonly timeoutMs: number;
  readonly maxResponseBytes: number;
}

export interface FetcherHttpResponse {
  readonly statusCode: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly bodyBytes: number;
}

export interface FetcherHttpClient {
  readonly name: string;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  request(request: FetcherHttpRequest): Promise<FetcherHttpResponse>;
}

export type FetcherDnsPolicyReason =
  | "allowed"
  | "blocked-private-address"
  | "blocked-localhost"
  | "blocked-unsupported-protocol";

export interface FetcherDnsPolicyDecision {
  readonly allowed: boolean;
  readonly reason: FetcherDnsPolicyReason;
}

export interface FetcherDnsPolicy {
  readonly name: string;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  evaluate(url: URL): FetcherDnsPolicyDecision | Promise<FetcherDnsPolicyDecision>;
}

export interface FetcherDurableStateStore extends RuntimeIdempotencyStore {
  readonly name: string;
  probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
}

export interface FetcherWorkHandler {
  readonly name: string;
  handle(context: RuntimeMessageContext): RuntimeHandlerResult | Promise<RuntimeHandlerResult>;
}

export interface FetcherDependencies {
  readonly clock: RuntimeClock;
  readonly httpClient: FetcherHttpClient;
  readonly dnsPolicy: FetcherDnsPolicy;
  readonly stateStore: FetcherDurableStateStore;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly workHandler: FetcherWorkHandler;
}
