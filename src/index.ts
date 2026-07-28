import { pathToFileURL } from "node:url";

import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  loadFetcherConfig,
  type FetcherConfig
} from "./config.js";
import { createFetcherHttpServer } from "./http.js";
import { createFeedFetchWorkHandler } from "./ingestion.js";
import {
  DefaultFetcherDnsPolicy,
  NodeFetcherHttpClient
} from "./network.js";
import { PayloadRabbitMqTransport } from "./rabbitmq-transport.js";
import {
  createFetcherFailClosedReconciler
} from "./reconciliation.js";
import { createFetcherService } from "./service.js";
import {
  InMemoryFetcherStateStore,
  createLocalFetcherDependencies
} from "./test-doubles.js";

export {
  FETCHER_CONFIG_SCHEMA,
  FETCHER_SERVICE_NAME,
  FETCHER_SERVICE_VERSION,
  loadFetcherConfig,
  type FetcherConfig
} from "./config.js";
export type {
  FetcherCandidateClaim,
  FetcherCandidateClaimResult,
  FetcherCandidatePublication,
  FetcherCandidateReference,
  FetcherDependencies,
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
  FetcherWorkTools,
  FetcherWorkHandler
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
  type FetcherReconciliationReport,
  type FetcherReconciliationRequest,
  type FetcherReconciler
} from "./reconciliation.js";
export {
  InMemoryFetcherStateStore,
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
}

export function createFetcherApplication(config = loadFetcherConfig()): FetcherApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const reconciliationToken = reconciliationTokenFromEnv();
  const productionBrokerTransport = config.dependencyMode === "production"
    ? new PayloadRabbitMqTransport({
        url: requiredEnv("NUTSNEWS_FETCHER_RABBITMQ_URL"),
        prefetch: config.prefetch,
        clock: SYSTEM_RUNTIME_CLOCK,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      })
    : undefined;
  const baseDependencies = createLocalFetcherDependencies({
    clock: SYSTEM_RUNTIME_CLOCK,
    httpClient: new NodeFetcherHttpClient(),
    dnsPolicy: new DefaultFetcherDnsPolicy(),
    stateStore: new InMemoryFetcherStateStore(SYSTEM_RUNTIME_CLOCK),
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
      }),
      ...(metrics === undefined ? {} : {
        metrics
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
    reconciler: createFetcherFailClosedReconciler(SYSTEM_RUNTIME_CLOCK),
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
        await httpServer.close();
      },
      async () => {
        await service.stop();
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
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
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

function reconciliationTokenFromEnv(): string | undefined {
  const serviceToken = process.env.NUTSNEWS_FETCHER_RECONCILIATION_TOKEN?.trim();
  const globalToken = process.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN?.trim();
  const token = serviceToken !== undefined && serviceToken.length > 0 ? serviceToken : globalToken;

  return token === undefined || token.length === 0 ? undefined : token;
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.3.1") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
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

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createFetcherApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start fetcher");
    process.exitCode = 1;
  });
}
