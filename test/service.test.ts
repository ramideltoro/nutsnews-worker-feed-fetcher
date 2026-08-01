import {
  createBufferedRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import type {
  FetcherDependencyProbe,
  FetcherDurableStateStore
} from "../src/dependencies.js";
import { fetcherDependencyAdapterIdentity } from "../src/dependencies.js";
import { createFetcherPrometheusMetricsSink } from "../src/metrics.js";
import { createFetcherService } from "../src/service.js";
import { createFetcherStateStore } from "../src/state-store.js";
import {
  InMemoryFetcherStateStore,
  LocalBrokerTransport,
  LocalDnsPolicy,
  LocalFetcherWorkHandler,
  LocalHttpClient,
  ManualFetcherClock,
  createLocalFetcherDependencies,
  createMinimalFetchDelivery
} from "../src/test-doubles.js";

describe("createFetcherService", () => {
  it("exports explicit probe states before startup and transitions them with lifecycle", async () => {
    const context = createServiceContext();
    const initial = context.metrics.collect();

    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="liveness",outcome="ok"} 1');
    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="startup",outcome="unhealthy"} 1');
    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="readiness",outcome="unhealthy"} 1');

    await context.service.start();
    expect(context.metrics.collect()).toContain('probe="startup",outcome="ok"} 1');
    expect(context.metrics.collect()).toContain('probe="readiness",outcome="ok"} 1');
    expect(context.metrics.collect().split("\n").filter((line) => line.startsWith("# HELP nutsnews_worker_health_probe "))).toHaveLength(1);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.health.evaluated" && event.attributes?.probe === "readiness"
    )).toBe(true);

    await context.service.consumer?.cancel();
    expect(context.metrics.collect()).toContain('probe="readiness",outcome="unhealthy"} 1');

    await context.service.stop();
    expect(context.metrics.collect()).toContain('probe="startup",outcome="unhealthy"} 1');
  });

  it("starts, becomes ready, registers fetch and canonicalization routes, and drains cleanly", async () => {
    const context = createServiceContext();

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer?.stage).toBe("fetch");
    expect(context.broker.assertedRoutes.map((route) => route.stage)).toEqual([
      "fetch",
      "canonicalization"
    ]);
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");
    expect((await context.service.health.readiness()).status).toBe("ok");
    expect(context.metrics.collect()).toContain("nutsnews_worker_build_info");
    expect(context.metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await context.service.stop();

    expect(context.service.isStarted).toBe(false);
    expect(context.service.broker.state).toBe("closed");
    expect(context.telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("delegates a valid feed fetch delivery and acks duplicate replays without business logic", async () => {
    const context = createServiceContext();
    const delivery = createMinimalFetchDelivery();

    context.workHandler.onHandleStart = () => {
      context.clock.advance(125);
    };

    await context.service.start();

    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    context.clock.advance(25);
    await expect(context.broker.deliverFetch(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.workHandler.handled[0]?.payload).toMatchObject({
      feedId: "feed-world",
      fetchReason: "scheduled"
    });
    const metrics = context.metrics.collect();

    expect(metrics).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="success"} 1');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="duplicate"} 1');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="0.1"} 1');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="0.25"} 2');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="+Inf"} 2');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_sum{environment="local",service="fetch"} 0.125');
    expect(metrics).toContain('nutsnews_worker_uplift_stage_latency_seconds_count{environment="local",service="fetch"} 2');
    expect(metrics).toMatch(new RegExp(`nutsnews_worker_last_success_timestamp_seconds\\{[^\\n]+\\} ${String(Date.parse("2026-07-23T00:00:00.150Z") / 1_000)}`, "u"));

    await context.service.stop();
  });

  it("waits for an in-flight delivery during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.workHandler.handleGate = gate.promise;
    context.workHandler.onHandleStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const delivery = context.broker.deliverFetch();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.workHandler.handled).toHaveLength(0);
    await vi.waitFor(() => {
      expect(context.service.consumer).toBeUndefined();
    });
    await expect(context.broker.deliverFetch()).rejects.toThrow("No local consumer is registered for fetch.");

    gate.resolve(undefined);
    await expect(delivery).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await stop;

    expect(context.workHandler.handled).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("shares the configured concurrency bound across broker and programmatic deliveries", async () => {
    const context = createServiceContext({
      config: loadFetcherConfig({
        NUTSNEWS_FETCHER_CONCURRENCY: "2",
        NUTSNEWS_FETCHER_HTTP_PORT: "0",
        NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
      })
    });
    const releaseHandlers = deferred<undefined>();
    let activeHandlers = 0;
    let maxActiveHandlers = 0;
    let startedHandlers = 0;

    vi.spyOn(context.workHandler, "handle").mockImplementation(async () => {
      activeHandlers += 1;
      startedHandlers += 1;
      maxActiveHandlers = Math.max(maxActiveHandlers, activeHandlers);

      try {
        await releaseHandlers.promise;
        return {
          status: "ok"
        };
      } finally {
        activeHandlers -= 1;
      }
    });

    await context.service.start();

    const deliveries = Array.from({
      length: 6
    }, (_, index) => uniqueFetchDelivery(index));
    const processing = deliveries.map((delivery, index) => index % 2 === 0
      ? context.broker.deliverFetch(delivery)
      : context.service.processDelivery(delivery));

    await vi.waitFor(() => {
      expect(startedHandlers).toBe(2);
    });
    expect(activeHandlers).toBe(2);
    expect(maxActiveHandlers).toBe(2);

    const stopping = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    releaseHandlers.resolve(undefined);

    const results = await Promise.all(processing);

    expect(results.every((result) => result.action === "ack" && result.reason === "handled")).toBe(true);
    await stopping;

    expect(startedHandlers).toBe(deliveries.length);
    expect(maxActiveHandlers).toBe(2);
    expect(activeHandlers).toBe(0);
  });

  it("cancels queued gate waiters and closes the broker when graceful drain times out", async () => {
    vi.useFakeTimers();

    try {
      const context = createServiceContext({
        config: loadFetcherConfig({
          NUTSNEWS_FETCHER_CONCURRENCY: "1",
          NUTSNEWS_FETCHER_HTTP_PORT: "0",
          NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS: "1000",
          NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
        })
      });
      const releaseHandler = deferred<undefined>();
      const handlerStarted = deferred<undefined>();
      let startedHandlers = 0;

      context.workHandler.handleGate = releaseHandler.promise;
      context.workHandler.onHandleStart = () => {
        startedHandlers += 1;
        handlerStarted.resolve(undefined);
      };

      await context.service.start();
      const activeDelivery = context.service.processDelivery(uniqueFetchDelivery(100));
      const queuedDelivery = context.service.processDelivery(uniqueFetchDelivery(101));
      const queuedOutcome = queuedDelivery.then(
        (value) => ({
          status: "fulfilled" as const,
          value
        }),
        (reason: unknown) => ({
          status: "rejected" as const,
          reason
        })
      );

      await handlerStarted.promise;
      expect(startedHandlers).toBe(1);

      const stopping = context.service.stop();
      const stopOutcome = stopping.then(
        () => undefined,
        (reason: unknown) => reason
      );

      await vi.advanceTimersByTimeAsync(1_000);
      const stopError = await stopOutcome;

      expect(stopError).toBeInstanceOf(Error);
      expect((stopError as Error).message).toContain("Shutdown exceeded 700ms");
      expect(context.service.broker.state).toBe("closed");
      expect(context.service.consumer).toBeUndefined();
      expect(context.service.isStarted).toBe(false);
      expect(context.metrics.collect()).toContain('probe="startup",outcome="unhealthy"} 1');
      expect(context.metrics.collect()).toContain('probe="readiness",outcome="unhealthy"} 1');

      const queued = await queuedOutcome;

      expect(queued.status).toBe("rejected");
      expect(queued.status === "rejected" ? queued.reason : undefined).toBeInstanceOf(Error);
      expect(startedHandlers).toBe(1);

      releaseHandler.resolve(undefined);
      await expect(activeDelivery).resolves.toMatchObject({
        action: "ack",
        reason: "handled"
      });
      expect(startedHandlers).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds a hung consumer cancellation and makes readiness unhealthy immediately", async () => {
    vi.useFakeTimers();

    try {
      const broker = new LocalBrokerTransport();
      const context = createServiceContext({
        brokerTransport: broker,
        config: loadFetcherConfig({
          NUTSNEWS_FETCHER_HTTP_PORT: "0",
          NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS: "1000",
          NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
        })
      });

      await context.service.start();
      broker.cancelGate = new Promise<void>(() => undefined);
      const stopping = context.service.stop();
      const stopOutcome = stopping.then(
        () => undefined,
        (reason: unknown) => reason
      );

      expect(context.metrics.collect()).toContain('probe="readiness",outcome="unhealthy"} 1');
      expect((await context.service.health.readiness()).checks.find((check) => check.name === "not-stopping")?.status).toBe("unhealthy");

      await vi.advanceTimersByTimeAsync(1_000);
      const stopError = await stopOutcome;

      expect(stopError).toBeInstanceOf(Error);
      expect((stopError as Error).message).toContain("broker-consumer-cancel exceeded 200ms");
      expect(context.service.broker.state).toBe("closed");
      expect(context.service.consumer).toBeUndefined();
      expect(context.service.isStarted).toBe(false);
      expect(context.metrics.collect()).toContain('probe="startup",outcome="unhealthy"} 1');
      expect(context.metrics.collect()).toContain('probe="readiness",outcome="unhealthy"} 1');
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports readiness unhealthy when durable state is unhealthy", async () => {
    const context = createServiceContext();

    context.stateStore.status = "unhealthy";
    await context.service.start();

    expect((await context.service.health.readiness()).status).toBe("unhealthy");
    expect(context.service.consumer).toBeUndefined();
    expect(context.service.broker.state).toBe("idle");

    context.stateStore.status = "ok";
    await context.service.start();

    expect(context.service.consumer?.stage).toBe("fetch");
    expect(context.service.broker.state).toBe("ready");
    expect((await context.service.health.readiness()).status).toBe("ok");

    await context.service.stop();
  });

  it("bounds a hung state-store startup probe and remains fail-closed", async () => {
    vi.useFakeTimers();

    try {
      const config = loadFetcherConfig({
        NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS: "100",
        NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
      });
      const delegate = new InMemoryFetcherStateStore(new ManualFetcherClock());
      const stateStore: FetcherDurableStateStore = {
        name: "hung-local-state-store",
        mode: "local-memory",
        adapter: "runtime-in-memory",
        durable: false,
        probe: () => new Promise<FetcherDependencyProbe>(() => undefined),
        claim: delegate.claim.bind(delegate),
        markCompleted: delegate.markCompleted.bind(delegate),
        markFailed: delegate.markFailed.bind(delegate),
        getFeedMetadata: delegate.getFeedMetadata.bind(delegate),
        recordFetchOutcome: delegate.recordFetchOutcome.bind(delegate),
        claimCandidate: delegate.claimCandidate.bind(delegate),
        markCandidatePublished: delegate.markCandidatePublished.bind(delegate),
        markCandidatePublishFailed: delegate.markCandidatePublishFailed.bind(delegate),
        listPendingCandidatePublications: delegate.listPendingCandidatePublications.bind(delegate)
      };
      const dependencies = createLocalFetcherDependencies({
        stateStore
      });
      const metrics = createFetcherPrometheusMetricsSink({
        identity: {
          service: config.serviceName,
          version: config.serviceVersion,
          environment: config.environment,
          host: config.host
        },
        config,
        stateStore
      });
      const service = createFetcherService({
        config,
        dependencies,
        metrics
      });
      const startup = service.start();

      await vi.advanceTimersByTimeAsync(100);
      await expect(startup).resolves.toBeUndefined();
      expect(service.isStarted).toBe(true);
      expect(service.consumer).toBeUndefined();
      expect(service.broker.state).toBe("idle");
      expect(metrics.collect()).toMatch(/nutsnews_worker_state_store_ready\{[^\n]+\} 0/u);

      await service.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports readiness unhealthy when the main queue consumer is cancelled", async () => {
    const context = createServiceContext();

    await context.service.start();
    await context.service.consumer?.cancel();

    const readiness = await context.service.health.readiness();
    expect(readiness.status).toBe("unhealthy");
    const consumerCheck = readiness.checks.find((check) => check.name === "rabbitmq-consumer");
    expect(consumerCheck?.status).toBe("unhealthy");
    expect(consumerCheck?.details).toMatchObject({
      queue: "nutsnews.worker.fetch.v1",
      activeConsumers: 0
    });
    expect(context.metrics.collect()).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-feed-fetcher"} 0');

    await context.service.stop();
  });

  it("fails closed when production has no supported durable state adapter", async () => {
    const config = productionConfig();
    const stateStore = createFetcherStateStore(config, new ManualFetcherClock());
    const context = createServiceContext({
      config,
      stateStore
    });

    await context.service.start();

    expect(context.service.isStarted).toBe(true);
    expect(context.service.consumer).toBeUndefined();
    expect(context.broker.assertedRoutes).toHaveLength(0);
    expect(context.service.broker.state).toBe("idle");
    expect((await context.service.health.liveness()).status).toBe("ok");
    expect((await context.service.health.startup()).status).toBe("ok");

    const readiness = await context.service.health.readiness();
    const stateCheck = readiness.checks.find((check) => check.name === "durable-state");

    expect(readiness.status).toBe("unhealthy");
    expect(stateCheck?.details).toMatchObject({
      dependencyMode: "production",
      deploymentMode: "shadow",
      expectedActive: false,
      expectedMode: "postgresql",
      actualMode: "unsupported",
      adapter: "none",
      durable: false,
      reason: "state-store-mode-mismatch",
      serviceVersion: "0.1.0",
      buildRevision: "abc123def456"
    });
    expect(JSON.stringify(readiness)).not.toContain("state-secret");
    expect(JSON.stringify(readiness)).not.toContain("broker-secret");
    expect(context.metrics.collect()).toMatch(/nutsnews_worker_state_store_ready\{[^\n]+outcome="unsupported"[^\n]+\} 0/u);
    expect(context.telemetry.events.some((event) =>
      event.name === "runtime.health.evaluated" &&
      event.attributes?.probe === "state-store-startup" &&
      event.attributes.actualMode === "unsupported" &&
      event.outcome === "unhealthy"
    )).toBe(true);

    await context.service.stop();
  });

  it("requires a healthy durable probe and production adapter identities before registering a consumer", async () => {
    const config = productionConfig();
    const degradedContext = createServiceContext({
      config,
      stateStore: testPostgresqlStateStore("degraded")
    });

    await degradedContext.service.start();

    expect(degradedContext.service.consumer).toBeUndefined();
    expect(degradedContext.service.broker.state).toBe("idle");
    const degradedReadiness = await degradedContext.service.health.readiness();
    const degradedState = degradedReadiness.checks.find((check) => check.name === "durable-state");

    expect(degradedReadiness.status).toBe("unhealthy");
    expect(degradedState?.status).toBe("unhealthy");
    expect(degradedState?.details).toMatchObject({
      expectedMode: "postgresql",
      actualMode: "postgresql",
      adapter: "test-postgresql",
      durable: true,
      probeStatus: "degraded",
      reason: "state-store-probe-unhealthy"
    });

    await degradedContext.service.stop();

    const mixedContext = createServiceContext({
      config,
      stateStore: testPostgresqlStateStore("ok")
    });

    await mixedContext.service.start();

    expect(mixedContext.service.consumer).toBeUndefined();
    expect(mixedContext.service.broker.state).toBe("idle");
    const mixedReadiness = await mixedContext.service.health.readiness();
    const mixedAdapters = mixedReadiness.checks.find((check) => check.name === "production-adapters");

    expect(mixedReadiness.status).toBe("unhealthy");
    expect(mixedAdapters?.details).toMatchObject({
      adapterMode: "mixed",
      stateStoreAdapterMode: "production",
      httpClientAdapterMode: "test",
      dnsPolicyAdapterMode: "test",
      brokerAdapterMode: "test",
      reason: "production-adapter-mismatch"
    });
    expect(mixedContext.metrics.collect()).toContain('nutsnews_worker_deployment_info{adapter="mixed",deployment="shadow",environment="local",service="nutsnews-worker-feed-fetcher"} 1');

    await mixedContext.service.stop();

    const healthyContext = createServiceContext({
      config,
      stateStore: testPostgresqlStateStore("ok"),
      productionAdapters: true
    });

    await healthyContext.service.start();

    expect(healthyContext.service.consumer?.stage).toBe("fetch");
    expect(healthyContext.service.broker.state).toBe("ready");
    const healthyReadiness = await healthyContext.service.health.readiness();

    expect(healthyReadiness.status).toBe("ok");
    expect(healthyReadiness.checks.find((check) => check.name === "production-adapters")?.details).toMatchObject({
      adapterMode: "production",
      reason: "production-adapters-ready"
    });
    expect(healthyReadiness.checks.some((check) => check.name === "production-ownership")).toBe(false);
    expect(healthyContext.metrics.collect()).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-feed-fetcher"} 0');

    await healthyContext.service.stop();
  });

  it("preserves ack and idempotency semantics when telemetry and every metric operation reject", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalFetcherDependencies();
    const rawMetrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: dependencies.stateStore
    });
    const rejectingMetrics = {
      ...rawMetrics,
      emit: () => Promise.reject(new Error("metric emit unavailable")),
      collect: () => {
        throw new Error("metric collect unavailable");
      },
      setInFlight: () => {
        throw new Error("metric gauge unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metric drain unavailable");
      },
      setStateStoreReady: () => {
        throw new Error("metric state unavailable");
      },
      setHealthProbe: () => {
        throw new Error("metric health unavailable");
      }
    };
    const service = createFetcherService({
      config,
      dependencies,
      telemetry: {
        emit: () => Promise.reject(new Error("log unavailable"))
      },
      metrics: rejectingMetrics
    });
    const delivery = createMinimalFetchDelivery();

    await expect(service.start()).resolves.toBeUndefined();
    await expect(service.processDelivery(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(service.processDelivery(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "duplicate"
    });
    expect((dependencies.workHandler as LocalFetcherWorkHandler).handled).toHaveLength(1);
    await expect(service.stop()).resolves.toBeUndefined();
  });
});

function createServiceContext(overrides: {
  readonly brokerTransport?: LocalBrokerTransport;
  readonly config?: ReturnType<typeof loadFetcherConfig>;
  readonly stateStore?: InMemoryFetcherStateStore | ReturnType<typeof createFetcherStateStore>;
  readonly productionAdapters?: boolean;
} = {}) {
  const config = overrides.config ?? loadFetcherConfig({
    NUTSNEWS_FETCHER_HTTP_PORT: "0",
    NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
  });
  const clock = new ManualFetcherClock();
  const dependencies = createLocalFetcherDependencies({
    clock,
    ...(overrides.brokerTransport !== undefined ? {
      brokerTransport: overrides.brokerTransport
    } : overrides.productionAdapters === true ? {
      httpClient: new LocalHttpClient("production"),
      dnsPolicy: new LocalDnsPolicy("production"),
      brokerTransport: new LocalBrokerTransport("production")
    } : {}),
    ...(overrides.stateStore === undefined ? {} : {
      stateStore: overrides.stateStore
    })
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const metrics = createFetcherPrometheusMetricsSink({
    identity: {
      service: config.serviceName,
      version: config.serviceVersion,
      environment: config.environment,
      host: config.host,
      adapter: fetcherDependencyAdapterIdentity(dependencies).aggregate
    },
    config,
    stateStore: dependencies.stateStore
  });
  const telemetryFanout = {
    emit: async (event: Parameters<typeof telemetry.emit>[0]): Promise<void> => {
      await telemetry.emit(event);
      await metrics.emit(event);
    }
  };
  const service = createFetcherService({
    config,
    dependencies,
    telemetry: telemetryFanout,
    metrics
  });

  return {
    broker: dependencies.brokerTransport as LocalBrokerTransport,
    clock,
    metrics,
    service,
    stateStore: dependencies.stateStore as InMemoryFetcherStateStore,
    telemetry,
    workHandler: dependencies.workHandler as LocalFetcherWorkHandler
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

function uniqueFetchDelivery(index: number) {
  const suffix = index.toString(16).padStart(12, "0");
  const feedId = `feed-concurrency-${String(index)}`;
  const idempotencyKey = `scheduler:feed:${feedId}:20260723t000000000z`;

  return createMinimalFetchDelivery({
    envelope: {
      messageId: `018f1598-2dd5-7c4f-9f92-${suffix}`,
      idempotencyKey,
      aggregate: {
        type: "feed",
        id: feedId,
        version: 1
      }
    },
    payload: {
      feedId,
      idempotencyKey
    }
  });
}

function productionConfig(): ReturnType<typeof loadFetcherConfig> {
  return loadFetcherConfig({
    HOSTNAME: "fetcher-production-host",
    NUTSNEWS_FETCHER_BUILD_REVISION: "abc123def456",
    NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
    NUTSNEWS_FETCHER_DATABASE_URL: "postgres://state-secret@example.invalid/fetcher",
    NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://broker-secret@example.invalid",
    NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
  });
}

function testPostgresqlStateStore(status: FetcherDependencyProbe["status"]): FetcherDurableStateStore {
  const delegate = new InMemoryFetcherStateStore(new ManualFetcherClock());

  return {
    name: "test-postgresql-state-store",
    mode: "postgresql",
    adapter: "test-postgresql",
    durable: true,
    probe: () => ({
      status,
      summary: `test PostgreSQL state ${status}`
    }),
    claim: delegate.claim.bind(delegate),
    markCompleted: delegate.markCompleted.bind(delegate),
    markFailed: delegate.markFailed.bind(delegate),
    getFeedMetadata: delegate.getFeedMetadata.bind(delegate),
    recordFetchOutcome: delegate.recordFetchOutcome.bind(delegate),
    claimCandidate: delegate.claimCandidate.bind(delegate),
    markCandidatePublished: delegate.markCandidatePublished.bind(delegate),
    markCandidatePublishFailed: delegate.markCandidatePublishFailed.bind(delegate),
    listPendingCandidatePublications: delegate.listPendingCandidatePublications.bind(delegate)
  };
}
