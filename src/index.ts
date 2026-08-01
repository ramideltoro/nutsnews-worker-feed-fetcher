import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadFetcherConfig,
  type FetcherConfig
} from "./config.js";
import type {
  FetcherBrokerTransport,
  FetcherDnsPolicy,
  FetcherDurableStateStore,
  FetcherHttpClient
} from "./dependencies.js";
import { fetcherDependencyAdapterIdentity } from "./dependencies.js";
import { createFetcherHttpServer } from "./http.js";
import { createFeedFetchWorkHandler } from "./ingestion.js";
import {
  DefaultFetcherDnsPolicy,
  NodeFetcherHttpClient
} from "./network.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-transport.js";
import {
  createFetcherOutboxReconciler
} from "./reconciliation.js";
import { createFetcherService } from "./service.js";
import {
  createLocalFetcherDependencies
} from "./test-doubles.js";
import {
  createFetcherPrometheusMetricsSink,
  type FetcherMetricIdentity
} from "./metrics.js";
import { createFetcherStateStore } from "./state-store.js";
import {
  bestEffortFetcherMetricsSink,
  bestEffortTelemetryFlusher,
  combineBestEffortTelemetrySinks
} from "./telemetry-safety.js";

export {
  FETCHER_CLAIM_SETTLEMENT_SAFETY_MS,
  FETCHER_CONFIG_SCHEMA,
  FETCHER_SERVICE_NAME,
  FETCHER_SERVICE_VERSION,
  loadFetcherConfig,
  type FetcherConfig,
  type FetcherDeploymentMode
} from "./config.js";
export type {
  FetcherCandidateClaim,
  FetcherCandidateClaimResult,
  FetcherCandidatePublication,
  FetcherCandidatePublicationFailure,
  FetcherClaimedPendingCandidatePublication,
  FetcherCandidateReference,
  FetcherAggregateAdapterMode,
  FetcherBrokerTransport,
  FetcherDependencies,
  FetcherDependencyAdapterIdentity,
  FetcherDependencyAdapterMode,
  FetcherDependencyProbe,
  FetcherDnsPolicy,
  FetcherDnsPolicyDecision,
  FetcherDurableStateStore,
  FetcherFeedMetadata,
  FetcherFetchOutcome,
  FetcherFetchStatus,
  FetcherHttpClient,
  FetcherHttpRequest,
  FetcherHttpResponse,
  FetcherPendingCandidatePublication,
  FetcherPendingPublicationQuery,
  FetcherStateStoreMode,
  FetcherWorkTools,
  FetcherWorkHandler
} from "./dependencies.js";
export {
  FetcherDefinitePublishError,
  FETCHER_MAX_CLAIM_LEASE_MS,
  fetcherDependencyAdapterIdentity,
  isFetcherDefinitePublishError
} from "./dependencies.js";
export {
  createFetcherHttpServer,
  type FetcherHttpServer
} from "./http.js";
export {
  createFeedFetchWorkHandler,
  type FeedFetchWorkHandlerOptions
} from "./ingestion.js";
export {
  FETCHER_METRIC_LABELS,
  FETCHER_STAGE_LATENCY_BUCKETS_SECONDS,
  FETCHER_STAGE_METRIC_OUTCOMES,
  createFetcherPrometheusMetricsSink,
  type FetcherBaseMetricsSink,
  type FetcherPrometheusMetricsSink,
  type FetcherPrometheusMetricsSinkOptions,
  type FetcherStageMetricOutcome
} from "./metrics.js";
export {
  SequenceFetcherIdFactory,
  createCryptoFetcherIdFactory,
  sha256Base64Url,
  sha256Hex,
  stableCandidateId,
  type FetcherIdFactory
} from "./ids.js";
export {
  DefaultFetcherDnsPolicy,
  FetcherHttpError,
  NodeFetcherHttpClient
} from "./network.js";
export {
  FeedParseError,
  parseFeedXml,
  type ParsedFeed,
  type ParsedFeedItem
} from "./feed-parser.js";
export {
  createFetcherService,
  type FetcherService
} from "./service.js";
export {
  PayloadRabbitMqTransport
} from "./rabbitmq-transport.js";
export {
  FETCHER_RECONCILIATION_CONFIRMATION,
  FETCHER_RECONCILIATION_PATH,
  createFetcherFailClosedReconciler,
  createFetcherOutboxReconciler,
  type FetcherReconciliationReport,
  type FetcherReconciliationRequest,
  type FetcherReconciler
} from "./reconciliation.js";
export {
  FETCHER_PRODUCTION_STATE_STORE_MODE,
  FetcherStateStoreUnavailableError,
  InMemoryFetcherStateStore,
  UnsupportedProductionFetcherStateStore,
  createFetcherStateStore,
  expectedFetcherStateStoreMode
} from "./state-store.js";
export {
  FETCHER_POSTGRES_SCHEMA,
  FETCHER_POSTGRES_STATE_CONTRACT_VERSION,
  FETCHER_FETCH_OUTCOME_RETENTION_DAYS,
  FetcherStateContractError,
  FetcherStateOwnershipError,
  PostgresFetcherStateStore,
  createPostgresFetcherPool,
  type PostgresFetcherPoolOptions,
  type PostgresFetcherStateStoreOptions
} from "./postgres-state-store.js";
export {
  LocalBrokerTransport,
  LocalDnsPolicy,
  LocalFetcherWorkHandler,
  LocalHttpClient,
  ManualFetcherClock,
  createLocalFetcherDependencies,
  createMinimalFetchDelivery,
  createMinimalFetchEnvelope,
  createMinimalFetchPayload
} from "./test-doubles.js";

export interface FetcherApplication {
  readonly config: FetcherConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
  url(path?: string): string;
}

export interface FetcherApplicationAdapters {
  readonly clock?: RuntimeClock;
  readonly stateStore?: FetcherDurableStateStore;
  readonly brokerTransport?: FetcherBrokerTransport;
  readonly httpClient?: FetcherHttpClient;
  readonly dnsPolicy?: FetcherDnsPolicy;
}

export function createFetcherApplication(
  config = loadFetcherConfig(),
  adapters: FetcherApplicationAdapters = {}
): FetcherApplication {
  const clock = adapters.clock ?? SYSTEM_RUNTIME_CLOCK;
  const databaseUrl = adapters.stateStore === undefined && config.dependencyMode === "production"
    ? requiredEnv("NUTSNEWS_FETCHER_DATABASE_URL")
    : undefined;
  const stateStore = adapters.stateStore ?? createFetcherStateStore(config, clock, {
    ...(databaseUrl === undefined ? {} : {
      databaseUrl
    })
  });
  const httpClient = adapters.httpClient ?? new NodeFetcherHttpClient();
  const dnsPolicy = adapters.dnsPolicy ?? new DefaultFetcherDnsPolicy();
  const brokerAdapterMode = adapters.brokerTransport?.adapterMode
    ?? (config.dependencyMode === "production" ? "production" : "test");
  const dependencyAdapters = fetcherDependencyAdapterIdentity({
    stateStore,
    httpClient,
    dnsPolicy,
    brokerTransport: {
      adapterMode: brokerAdapterMode
    }
  });
  const identity: FetcherMetricIdentity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.deploymentMode,
    adapter: dependencyAdapters.aggregate
  };
  const logSink = bestEffortTelemetryFlusher(config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined);
  const metrics = bestEffortFetcherMetricsSink(config.metricsEnabled
    ? createFetcherPrometheusMetricsSink({
        identity,
        config,
        stateStore
      })
    : undefined);
  const telemetry = combineBestEffortTelemetrySinks(logSink, metrics);
  const reconciliationToken = reconciliationTokenFromEnv();
  const productionBrokerTransport = adapters.brokerTransport ?? (config.dependencyMode === "production"
    ? new PayloadRabbitMqTransport({
        url: requiredEnv("NUTSNEWS_FETCHER_RABBITMQ_URL"),
        prefetch: config.prefetch,
        clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : undefined);
  const baseDependencies = createLocalFetcherDependencies({
    clock,
    httpClient,
    dnsPolicy,
    stateStore,
    ...(productionBrokerTransport === undefined ? {} : {
      brokerTransport: productionBrokerTransport
    })
  });
  const dependencies = {
    ...baseDependencies,
    workHandler: createFeedFetchWorkHandler({
      config,
      dependencies: baseDependencies,
      ...(telemetry === undefined ? {} : {
        telemetry
      })
    })
  };
  const service = createFetcherService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createFetcherHttpServer({
    config,
    service,
    reconciler: createFetcherOutboxReconciler({
      clock,
      stateStore,
      publish: (command) => service.broker.publish(command)
    }),
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        let serviceError: Error | undefined;
        let httpError: Error | undefined;

        // Stop broker intake immediately. HTTP close can wait for an active
        // readiness/metrics dependency probe, so it must not delay consumer
        // cancellation or consume the service's drain budget first.
        const serviceOperation = service.stop().catch((error: unknown) => {
          serviceError = applicationError(error, "Fetcher service shutdown failed.");
        });
        const httpOperation = httpServer.close().catch((error: unknown) => {
          httpError = applicationError(error, "Fetcher HTTP shutdown failed.");
        });

        await Promise.all([
          serviceOperation,
          httpOperation
        ]);

        let stateStoreError: Error | undefined;

        try {
          await stateStore.close?.();
        } catch (error: unknown) {
          stateStoreError = applicationError(error, "Fetcher state-store shutdown failed.");
        }

        const shutdownError = serviceError ?? httpError ?? stateStoreError;

        if (shutdownError !== undefined) {
          throw shutdownError;
        }
      }
    ],
    signalSource: process,
    // Service drain and active diagnostic probes run in parallel. Reserve a
    // small cleanup window for closing the durable state pool afterwards.
    timeoutMs: Math.max(config.shutdownTimeoutMs, config.startupTimeoutMs) + 5_000,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await httpServer.listen();
      shutdown.start();

      try {
        await service.start();
      } catch (error: unknown) {
        shutdown.stop();
        await service.stop().catch(() => undefined);
        await stateStore.close?.().catch(() => undefined);
        await httpServer.close().catch(() => undefined);
        throw error;
      }
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    },
    url: (path = "/") => httpServer.url(path)
  };
}

function reconciliationTokenFromEnv(): string | undefined {
  const serviceToken = process.env.NUTSNEWS_FETCHER_RECONCILIATION_TOKEN?.trim();
  const globalToken = process.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN?.trim();
  const token = serviceToken !== undefined && serviceToken.length > 0 ? serviceToken : globalToken;

  return token === undefined || token.length === 0 ? undefined : token;
}

export const SUPPORTED_CONTRACTS_PACKAGE_VERSION = "1.0.0";
export const SUPPORTED_RUNTIME_PACKAGE_VERSION = "1.0.0";

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== SUPPORTED_CONTRACTS_PACKAGE_VERSION) {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== SUPPORTED_RUNTIME_PACKAGE_VERSION) {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

function requiredEnv(key: string): string {
  const value = process.env[key]?.trim();

  if (value === undefined || value.length === 0) {
    throw new Error(`${key} is required for production fetcher dependencies.`);
  }

  return value;
}

function applicationError(error: unknown, fallbackMessage: string): Error {
  return error instanceof Error ? error : new Error(fallbackMessage);
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createFetcherApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start fetcher");
    process.exitCode = 1;
  });
}
