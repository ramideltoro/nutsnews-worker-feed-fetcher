import {
  getWorkerRoute
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createBrokerConsumerReadinessCheck,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  createRuntimeMessageProcessor,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerConsumerHandle,
  type BrokerLifecycle,
  type RuntimeHealthCheck,
  type RuntimeHealthProbe,
  type RuntimeHealthProbeSet,
  type RuntimeHealthReport,
  type RuntimeHealthStatus,
  type RuntimeMessageProcessingResult,
  type RuntimeMessageDelivery,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { FetcherConfig } from "./config.js";
import type {
  FetcherBrokerTransport,
  FetcherDependencyAdapterIdentity,
  FetcherDependencies,
  FetcherDependencyProbe
} from "./dependencies.js";
import { fetcherDependencyAdapterIdentity } from "./dependencies.js";
import type {
  FetcherBaseMetricsSink,
  FetcherHealthOutcome,
  FetcherHealthProbe,
  FetcherPrometheusMetricsSink
} from "./metrics.js";
import { expectedFetcherStateStoreMode } from "./state-store.js";
import {
  bestEffortFetcherMetricsSink,
  bestEffortTelemetrySink
} from "./telemetry-safety.js";

export interface FetcherServiceOptions {
  readonly config: FetcherConfig;
  readonly dependencies: FetcherDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink;
}

export interface FetcherService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly consumer: BrokerConsumerHandle | undefined;
  start(): Promise<void>;
  stop(): Promise<void>;
  processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult>;
}

export function createFetcherService(options: FetcherServiceOptions): FetcherService {
  const fetchRoute = getWorkerRoute("fetch");
  const canonicalizationRoute = getWorkerRoute("canonicalization");
  const telemetry = bestEffortTelemetrySink(options.telemetry);
  const metrics = bestEffortFetcherMetricsSink(options.metrics);
  const shutdownBudget = fetcherShutdownBudget(options.config.shutdownTimeoutMs);
  const boundedBroker = createBoundedBrokerTransport(
    options.dependencies.brokerTransport,
    shutdownBudget.consumerCancelMs,
    shutdownBudget.forceCloseMs
  );
  const broker = createBrokerLifecycle({
    transport: boundedBroker.transport,
    routes: [
      fetchRoute,
      canonicalizationRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  const runtimeProcessor = createRuntimeMessageProcessor({
    stage: "fetch",
    clock: options.dependencies.clock,
    idempotencyStore: options.dependencies.stateStore,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    handler: async (context) => {
      return options.dependencies.workHandler.handle(context, {
        publish: (command) => broker.publish(command)
      });
    }
  });
  const concurrencyGate = createConcurrencyGate(options.config.concurrency);
  const processor = async (delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> => {
    try {
      const processing = drain.track(() => concurrencyGate.run(() => runtimeProcessor(delivery)));

      metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
      return await processing;
    } finally {
      metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
    }
  };
  let started = false;
  let consumer: BrokerConsumerHandle | undefined;
  let lifecycleGeneration = 0;
  let stopping = false;
  let stopOperation: Promise<void> | undefined;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return observeHealthProbeSet(createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          notStoppingReadinessCheck(() => stopping),
          brokerReadinessCheck(broker),
          createBrokerConsumerReadinessCheck(broker, "fetch"),
          dependencyReadinessCheck("http-client", options.dependencies.httpClient),
          dependencyReadinessCheck("dns-policy", options.dependencies.dnsPolicy),
          productionAdaptersReadinessCheck(options),
          stateStoreReadinessCheck(options, metrics)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      }), metrics);
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return stopping || drain.isDraining;
    },
    get consumer(): BrokerConsumerHandle | undefined {
      return consumer;
    },
    async start(): Promise<void> {
      if (started && broker.state === "ready" && broker.consumerStatus("fetch").activeConsumers > 0) {
        return;
      }

      const startGeneration = lifecycleGeneration + 1;

      lifecycleGeneration = startGeneration;
      const stateStore = await evaluateStateStore(options);
      const productionAdapters = evaluateProductionAdapters(options);

      assertCurrentStart(startGeneration, lifecycleGeneration);

      setStateStoreReady(metrics, stateStore.status === "ok");

      if (stateStore.status !== "ok" || productionAdapters.status !== "ok") {
        started = true;
        setHealthProbe(metrics, "startup", "ok");
        setHealthProbe(metrics, "readiness", "unhealthy");
        metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
        await emitStateStoreModeTelemetry(options, stateStore, productionAdapters, telemetry);
        return;
      }

      await broker.start();

      if (startGeneration !== lifecycleGeneration) {
        await broker.stop("startup-cancelled").catch(() => undefined);
        throw fetcherStartCancelledError();
      }

      const brokerConsumer = await broker.consume("fetch", processor);

      if (startGeneration !== lifecycleGeneration) {
        await brokerConsumer.cancel().catch(() => undefined);
        await broker.stop("startup-cancelled").catch(() => undefined);
        throw fetcherStartCancelledError();
      }

      const serviceConsumer: BrokerConsumerHandle = {
        stage: brokerConsumer.stage,
        cancel: async () => {
          try {
            await brokerConsumer.cancel();
            const cancellationError = boundedBroker.takeCancellationError();

            if (cancellationError !== undefined) {
              throw cancellationError;
            }
          } finally {
            if (consumer === serviceConsumer) {
              consumer = undefined;
            }
            setHealthProbe(metrics, "readiness", "unhealthy");
          }
        }
      };
      consumer = serviceConsumer;
      started = true;
      setHealthProbe(metrics, "startup", "ok");
      metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
      await emitStateStoreModeTelemetry(options, stateStore, productionAdapters, telemetry);
      await service.health.readiness();
    },
    stop(): Promise<void> {
      if (stopOperation !== undefined) {
        return stopOperation;
      }

      const operation = (async (): Promise<void> => {
        lifecycleGeneration += 1;

        if (!started && broker.state === "closed") {
          return;
        }

        stopping = true;
        setHealthProbe(metrics, "readiness", "unhealthy");
        metrics?.setShutdownDraining(true);
        let shutdownError: Error | undefined;

        try {
          // Stop broker delivery before closing the processor gate. Otherwise
          // a healthy message delivered during shutdown can be misclassified
          // as a processor failure and transferred to retry/DLQ solely because
          // the drain has started.
          const activeConsumer = consumer;

          await activeConsumer?.cancel();
          consumer = undefined;
          drain.stopAcceptingWork();
          await drain.waitForDrain(shutdownBudget.drainMs);
        } catch (error: unknown) {
          shutdownError = fetcherError(error, "Fetcher shutdown failed.");
          consumer = undefined;
          drain.stopAcceptingWork();
          concurrencyGate.close(shutdownError);

          // A graceful drain timeout must not leave the AMQP connection and
          // lifecycle open. Closing the concrete transport first abandons its
          // in-flight wait and lets the managed lifecycle reach `closed`.
          try {
            await withTimeout(
              options.dependencies.brokerTransport.close(),
              shutdownBudget.forceCloseMs,
              "broker-force-close",
              "FetcherShutdownTimeoutError"
            );
          } catch (closeError: unknown) {
            shutdownError ??= fetcherError(closeError, "Fetcher broker force-close failed.");
          }
        }

        try {
          await broker.stop("shutdown");
          shutdownError ??= boundedBroker.takeCancellationError();
        } catch (error: unknown) {
          shutdownError ??= fetcherError(error, "Fetcher broker shutdown failed.");
        } finally {
          metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
          started = false;
          setHealthProbe(metrics, "startup", "unhealthy");
          setHealthProbe(metrics, "readiness", "unhealthy");
          metrics?.setShutdownDraining(false);
          stopping = false;
        }

        if (shutdownError !== undefined) {
          throw shutdownError;
        }
      })();

      stopOperation = operation;
      void operation.then(
        () => {
          if (stopOperation === operation) {
            stopOperation = undefined;
          }
        },
        () => {
          if (stopOperation === operation) {
            stopOperation = undefined;
          }
        }
      );
      return operation;
    },
    processDelivery(delivery: RuntimeMessageDelivery): Promise<RuntimeMessageProcessingResult> {
      return processor(delivery);
    }
  } satisfies FetcherService;

  return service;
}

interface FetcherShutdownBudget {
  readonly consumerCancelMs: number;
  readonly drainMs: number;
  readonly forceCloseMs: number;
}

interface BoundedBrokerTransport {
  readonly transport: FetcherBrokerTransport;
  takeCancellationError(): Error | undefined;
}

function fetcherShutdownBudget(totalMs: number): FetcherShutdownBudget {
  const consumerCancelMs = Math.min(5_000, Math.max(100, Math.floor(totalMs / 5)));
  const forceCloseMs = Math.min(1_000, Math.max(100, Math.floor(totalMs / 10)));

  return {
    consumerCancelMs,
    drainMs: Math.max(100, totalMs - consumerCancelMs - forceCloseMs),
    forceCloseMs
  };
}

function createBoundedBrokerTransport(
  source: FetcherBrokerTransport,
  consumerCancelTimeoutMs: number,
  forceCloseTimeoutMs: number
): BoundedBrokerTransport {
  let cancellationError: Error | undefined;
  const sourceDrain = source.drain?.bind(source);
  const sourceConsumerStatus = source.consumerStatus?.bind(source);
  const recordError = (error: unknown, fallback: string): void => {
    cancellationError ??= fetcherError(error, fallback);
  };
  const forceClose = async (): Promise<void> => {
    try {
      await withTimeout(
        source.close(),
        forceCloseTimeoutMs,
        "broker-force-close",
        "FetcherShutdownTimeoutError"
      );
    } catch (error: unknown) {
      recordError(error, "Fetcher broker force-close failed.");
    }
  };
  const transport: FetcherBrokerTransport = {
    name: source.name,
    adapterMode: source.adapterMode,
    connect: () => source.connect(),
    assertTopology: (routes) => source.assertTopology(routes),
    publish: (command) => source.publish(command),
    consume: async (stage, handler) => {
      const handle = await source.consume(stage, handler);

      return {
        stage: handle.stage,
        cancel: async () => {
          try {
            await withTimeout(
              handle.cancel(),
              consumerCancelTimeoutMs,
              "broker-consumer-cancel",
              "FetcherShutdownTimeoutError"
            );
          } catch (error: unknown) {
            recordError(error, "Fetcher broker consumer cancellation failed.");
            await forceClose();
          }
        }
      };
    },
    close: async () => {
      await forceClose();
    },
    ...(sourceDrain === undefined ? {} : {
      drain: async (timeoutMs?: number) => {
        try {
          await withTimeout(
            sourceDrain(timeoutMs),
            timeoutMs ?? forceCloseTimeoutMs,
            "broker-transport-drain",
            "FetcherShutdownTimeoutError"
          );
        } catch (error: unknown) {
          recordError(error, "Fetcher broker transport drain failed.");
          await forceClose();
        }
      }
    }),
    ...(sourceConsumerStatus === undefined ? {} : {
      consumerStatus: (stage) => sourceConsumerStatus(stage)
    })
  };

  return {
    transport,
    takeCancellationError(): Error | undefined {
      const error = cancellationError;

      cancellationError = undefined;
      return error;
    }
  };
}

interface ConcurrencyGate {
  run<T>(operation: () => T | Promise<T>): Promise<T>;
  close(error: Error): void;
}

function createConcurrencyGate(limit: number): ConcurrencyGate {
  const waiters: {
    readonly resolve: () => void;
    readonly reject: (error: Error) => void;
  }[] = [];
  let active = 0;
  let closedError: Error | undefined;

  const acquire = async (): Promise<void> => {
    if (closedError !== undefined) {
      throw closedError;
    }

    if (active < limit && waiters.length === 0) {
      active += 1;
      return;
    }

    await new Promise<void>((resolve, reject) => {
      waiters.push({
        resolve,
        reject
      });
    });
  };
  const release = (): void => {
    const next = waiters.shift();

    if (next === undefined) {
      active = Math.max(0, active - 1);
      return;
    }

    // The released permit is transferred directly to the oldest waiter.
    next.resolve();
  };

  return {
    async run<T>(operation: () => T | Promise<T>): Promise<T> {
      await acquire();

      try {
        return await operation();
      } finally {
        release();
      }
    },
    close(error: Error): void {
      closedError ??= error;

      for (const waiter of waiters.splice(0, waiters.length)) {
        waiter.reject(closedError);
      }
    }
  };
}

function assertCurrentStart(startGeneration: number, lifecycleGeneration: number): void {
  if (startGeneration !== lifecycleGeneration) {
    throw fetcherStartCancelledError();
  }
}

function fetcherStartCancelledError(): Error {
  const error = new Error("Fetcher startup was cancelled by a newer lifecycle transition.");

  error.name = "FetcherStartCancelledError";
  return error;
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function notStoppingReadinessCheck(isStopping: () => boolean): RuntimeHealthCheck {
  return {
    name: "not-stopping",
    critical: true,
    check: () => isStopping()
      ? {
          status: "unhealthy",
          details: {
            reason: "shutdown-in-progress"
          }
        }
      : "ok"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function dependencyReadinessCheck(
  name: string,
  dependency: {
    readonly name: string;
    probe(): FetcherDependencyProbe | Promise<FetcherDependencyProbe>;
  }
): RuntimeHealthCheck {
  return {
    name,
    critical: true,
    check: async () => {
      const probe = await dependency.probe();

      return {
        status: probe.status,
        details: {
          dependency: dependency.name,
          summary: probe.summary
        }
      };
    }
  };
}

interface ProductionAdapterEvaluation {
  readonly status: "ok" | "unhealthy";
  readonly reason: "test-adapters-allowed" | "production-adapters-ready" | "production-adapter-mismatch";
  readonly identity: FetcherDependencyAdapterIdentity;
}

function evaluateProductionAdapters(options: FetcherServiceOptions): ProductionAdapterEvaluation {
  const identity = fetcherDependencyAdapterIdentity(options.dependencies);

  if (options.config.dependencyMode !== "production") {
    return {
      status: "ok",
      reason: "test-adapters-allowed",
      identity
    };
  }

  return identity.aggregate === "production"
    ? {
        status: "ok",
        reason: "production-adapters-ready",
        identity
      }
    : {
        status: "unhealthy",
        reason: "production-adapter-mismatch",
        identity
      };
}

function productionAdaptersReadinessCheck(options: FetcherServiceOptions): RuntimeHealthCheck {
  return {
    name: "production-adapters",
    critical: true,
    check: () => {
      const evaluation = evaluateProductionAdapters(options);

      return {
        status: evaluation.status,
        details: {
          dependencyMode: options.config.dependencyMode,
          adapterMode: evaluation.identity.aggregate,
          stateStoreAdapterMode: evaluation.identity.stateStore,
          httpClientAdapterMode: evaluation.identity.httpClient,
          dnsPolicyAdapterMode: evaluation.identity.dnsPolicy,
          brokerAdapterMode: evaluation.identity.broker,
          reason: evaluation.reason
        }
      };
    }
  };
}

interface StateStoreEvaluation {
  readonly status: "ok" | "unhealthy";
  readonly summary: string;
  readonly expectedMode: string;
  readonly actualMode: string;
  readonly adapter: string;
  readonly durable: boolean;
  readonly probeStatus: FetcherDependencyProbe["status"];
  readonly reason: string;
}

function stateStoreReadinessCheck(
  options: FetcherServiceOptions,
  metrics: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined
): RuntimeHealthCheck {
  return {
    name: "durable-state",
    critical: true,
    check: async () => {
      const stateStore = await evaluateStateStore(options);

      setStateStoreReady(metrics, stateStore.status === "ok");

      return {
        status: stateStore.status,
        details: {
          dependency: options.dependencies.stateStore.name,
          dependencyMode: options.config.dependencyMode,
          deploymentMode: options.config.deploymentMode,
          expectedActive: options.config.expectedActive,
          expectedMode: stateStore.expectedMode,
          actualMode: stateStore.actualMode,
          adapter: stateStore.adapter,
          durable: stateStore.durable,
          probeStatus: stateStore.probeStatus,
          reason: stateStore.reason,
          summary: stateStore.summary,
          serviceVersion: options.config.serviceVersion,
          buildRevision: options.config.buildRevision
        }
      };
    }
  };
}

async function evaluateStateStore(options: FetcherServiceOptions): Promise<StateStoreEvaluation> {
  const expectedMode = expectedFetcherStateStoreMode(options.config);
  const stateStore = options.dependencies.stateStore;
  let probe: FetcherDependencyProbe;

  try {
    probe = await withTimeout(
      Promise.resolve().then(() => stateStore.probe()),
      options.config.startupTimeoutMs,
      "state-store-probe"
    );
  } catch (error: unknown) {
    probe = {
      status: "unhealthy",
      summary: error instanceof Error && error.name.length > 0 ? error.name : "state-store-probe-error"
    };
  }

  const modeMatches = options.config.dependencyMode === "production"
    ? stateStore.mode === expectedMode && stateStore.durable
    : stateStore.mode === expectedMode;
  const probeReady = probe.status === "ok";
  const reason = !modeMatches
    ? "state-store-mode-mismatch"
    : !probeReady
      ? "state-store-probe-unhealthy"
      : "state-store-ready";

  return {
    status: modeMatches && probeReady ? "ok" : "unhealthy",
    summary: probe.summary,
    expectedMode,
    actualMode: stateStore.mode,
    adapter: stateStore.adapter,
    durable: stateStore.durable,
    probeStatus: probe.status,
    reason
  };
}

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  operationName: string,
  errorName = "FetcherStartupTimeoutError"
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    return await Promise.race([
      operation,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => {
          const error = new Error(`${operationName} exceeded ${String(timeoutMs)}ms.`);

          error.name = errorName;
          reject(error);
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

function fetcherError(error: unknown, fallbackMessage: string): Error {
  if (error instanceof Error) {
    return error;
  }

  return new Error(fallbackMessage);
}

async function emitStateStoreModeTelemetry(
  options: FetcherServiceOptions,
  stateStore: StateStoreEvaluation,
  productionAdapters: ProductionAdapterEvaluation,
  telemetry: RuntimeTelemetrySink | undefined
): Promise<void> {
  await emitRuntimeTelemetry(telemetry, {
    name: "runtime.health.evaluated",
    level: stateStore.status === "ok" ? "info" : "error",
    at: runtimeNow(options.dependencies.clock),
    stage: "fetch",
    queue: getWorkerRoute("fetch").mainQueue.name,
    outcome: stateStore.status,
    attributes: {
      probe: "state-store-startup",
      dependencyMode: options.config.dependencyMode,
      deploymentMode: options.config.deploymentMode,
      expectedActive: options.config.expectedActive,
      expectedMode: stateStore.expectedMode,
      actualMode: stateStore.actualMode,
      adapterMode: stateStore.adapter,
      dependencyAdapterMode: productionAdapters.identity.aggregate,
      stateStoreAdapterMode: productionAdapters.identity.stateStore,
      httpClientAdapterMode: productionAdapters.identity.httpClient,
      dnsPolicyAdapterMode: productionAdapters.identity.dnsPolicy,
      brokerAdapterMode: productionAdapters.identity.broker,
      dependencyAdapterReason: productionAdapters.reason,
      durable: stateStore.durable,
      probeStatus: stateStore.probeStatus,
      reason: stateStore.reason,
      serviceVersion: options.config.serviceVersion,
      buildRevision: options.config.buildRevision,
      shadowMode: options.config.shadowMode,
      prefetch: options.config.prefetch,
      concurrency: options.config.concurrency
    }
  });
}

function observeHealthProbeSet(
  probes: RuntimeHealthProbeSet,
  metrics: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined
): RuntimeHealthProbeSet {
  const observe = async (
    probe: Extract<RuntimeHealthProbe, FetcherHealthProbe>,
    evaluate: () => Promise<RuntimeHealthReport>
  ): Promise<RuntimeHealthReport> => {
    try {
      const report = await evaluate();

      setHealthProbe(metrics, probe, report.status);
      return report;
    } catch (error: unknown) {
      setHealthProbe(metrics, probe, "unhealthy");
      throw error;
    }
  };

  return {
    liveness: () => observe("liveness", () => probes.liveness()),
    startup: () => observe("startup", () => probes.startup()),
    readiness: () => observe("readiness", () => probes.readiness())
  };
}

function setHealthProbe(
  metrics: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined,
  probe: FetcherHealthProbe,
  outcome: Extract<RuntimeHealthStatus, FetcherHealthOutcome>
): void {
  if (isFetcherMetrics(metrics)) {
    metrics.setHealthProbe(probe, outcome);
  }
}

function setStateStoreReady(
  metrics: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined,
  ready: boolean
): void {
  if (isFetcherMetrics(metrics)) {
    metrics.setStateStoreReady(ready);
  }
}

function isFetcherMetrics(
  metrics: FetcherBaseMetricsSink | FetcherPrometheusMetricsSink | undefined
): metrics is FetcherPrometheusMetricsSink {
  return metrics !== undefined
    && "setStateStoreReady" in metrics
    && typeof metrics.setStateStoreReady === "function"
    && "setHealthProbe" in metrics
    && typeof metrics.setHealthProbe === "function";
}
