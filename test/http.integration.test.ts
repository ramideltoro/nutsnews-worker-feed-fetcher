import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import {
  createFetcherHttpServer,
  type FetcherHttpServer
} from "../src/http.js";
import { createFetcherPrometheusMetricsSink } from "../src/metrics.js";
import {
  FETCHER_RECONCILIATION_CONFIRMATION,
  createFetcherFailClosedReconciler,
  createFetcherOutboxReconciler
} from "../src/reconciliation.js";
import { createFetcherService } from "../src/service.js";
import {
  InMemoryFetcherStateStore,
  createFetcherStateStore
} from "../src/state-store.js";
import {
  ManualFetcherClock,
  createLocalFetcherDependencies,
  createMinimalCanonicalizationCommand,
  createMinimalFetchDelivery
} from "../src/test-doubles.js";

let activeServer: FetcherHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("fetcher HTTP endpoints", () => {
  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalFetcherDependencies();
    const metrics = createFetcherPrometheusMetricsSink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      },
      config,
      stateStore: dependencies.stateStore
    });
    const service = createFetcherService({
      config,
      dependencies,
      telemetry: metrics,
      metrics
    });
    activeServer = createFetcherHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await service.processDelivery(createMinimalFetchDelivery());
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    const metricsText = await metricsResponse.text();

    expect(metricsText).toContain("nutsnews_worker_build_info");
    expect(metricsText).toContain("nutsnews_worker_deployment_info");
    expect(metricsText).toContain("nutsnews_worker_expected_active");
    expect(metricsText).toContain("nutsnews_worker_last_success_timestamp_seconds");
    expect(metricsText).toContain('nutsnews_worker_uplift_stage_events_total{environment="local",service="fetch",outcome="success"} 1');
    expect(metricsText).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="30"} 1');
    expect(metricsText).toContain('nutsnews_worker_uplift_stage_latency_seconds_bucket{environment="local",service="fetch",le="+Inf"} 1');

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_FETCHER_RABBITMQ_URL" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");

    await service.stop();
  });

  it("refreshes readiness during a metrics scrape after the state store becomes unhealthy", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const stateStore = new InMemoryFetcherStateStore();
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
      telemetry: metrics,
      metrics
    });
    activeServer = createFetcherHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();
    expect(metrics.collect()).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="readiness",outcome="ok"} 1');

    stateStore.status = "unhealthy";
    const metricsResponse = await fetch(activeServer.url("/metrics"));
    const metricsText = await metricsResponse.text();

    expect(metricsResponse.status).toBe(200);
    expect(metricsText).toContain('nutsnews_worker_health_probe{environment="local",service="fetch",probe="readiness",outcome="unhealthy"} 1');
    expect(metricsText).toMatch(/nutsnews_worker_state_store_ready\{[^\n]+\} 0/u);

    await service.stop();
  });

  it("reports an explicit disabled state instead of an empty metrics response", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_METRICS_ENABLED: "false",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const service = createFetcherService({
      config,
      dependencies: createLocalFetcherDependencies()
    });
    activeServer = createFetcherHttpServer({
      config,
      service
    });

    await service.start();
    await activeServer.listen();
    const response = await fetch(activeServer.url("/metrics"));
    const output = await response.text();

    expect(response.status).toBe(200);
    expect(output).toContain('nutsnews_worker_metrics_enabled{environment="local",service="nutsnews-worker-feed-fetcher"} 0');
    expect(output).toContain('nutsnews_worker_telemetry_collection_ready{environment="local",service="nutsnews-worker-feed-fetcher"} 0');

    await service.stop();
  });

  it("reports collector failure instead of returning an empty successful scrape", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const service = createFetcherService({
      config,
      dependencies: createLocalFetcherDependencies()
    });
    activeServer = createFetcherHttpServer({
      config,
      service,
      metrics: {
        collect: () => {
          throw new Error("collector unavailable");
        }
      }
    });

    await service.start();
    await activeServer.listen();
    const response = await fetch(activeServer.url("/metrics"));
    const output = await response.text();

    expect(response.status).toBe(200);
    expect(output).not.toBe("");
    expect(output).toContain('nutsnews_worker_metrics_enabled{environment="local",service="nutsnews-worker-feed-fetcher"} 1');
    expect(output).toContain('nutsnews_worker_telemetry_collection_ready{environment="local",service="nutsnews-worker-feed-fetcher"} 0');

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const service = createFetcherService({
      config,
      dependencies: createLocalFetcherDependencies()
    });
    activeServer = createFetcherHttpServer({
      config,
      service,
      reconciler: createFetcherFailClosedReconciler(new ManualFetcherClock()),
      reconciliationToken: "test-token"
    });

    await service.start();
    await activeServer.listen();

    const unauthorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });

  it("returns HTTP 200 when protected outbox reconciliation applies successfully", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const clock = new ManualFetcherClock();
    const stateStore = new InMemoryFetcherStateStore(clock);
    const command = createMinimalCanonicalizationCommand();

    await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      claimOwnerKey: command.envelope.messageId,
      command
    });
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimOwnerKey: command.envelope.messageId,
      reason: "BrokerPublishError"
    });
    const service = createFetcherService({
      config,
      dependencies: createLocalFetcherDependencies({
        clock,
        stateStore
      })
    });
    activeServer = createFetcherHttpServer({
      config,
      service,
      reconciliationToken: "test-token",
      reconciler: createFetcherOutboxReconciler({
        clock,
        stateStore,
        publish: (candidate) => Promise.resolve({
          messageId: candidate.envelope.messageId,
          stage: candidate.envelope.route,
          exchange: "nutsnews.worker",
          routingKey: "worker.canonicalization.v1",
          confirmed: true,
          confirmedAt: clock.now().toISOString()
        })
      })
    });

    await service.start();
    await activeServer.listen();
    const response = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token",
        "content-type": "application/json"
      },
      body: JSON.stringify({
        mode: "apply",
        minAgeSeconds: 0,
        protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
      })
    });

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      status: "applied",
      selectedCount: 1,
      replayedCount: 1,
      writesPerformed: true
    });

    await service.stop();
  });

  it("keeps diagnostics available while production readiness fails closed without durable state", async () => {
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-production-host",
      NUTSNEWS_FETCHER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_FETCHER_HTTP_PORT: "0",
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_DATABASE_URL: "postgres://secret@example.invalid/fetcher",
      NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://secret@example.invalid",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });
    const clock = new ManualFetcherClock();
    const stateStore = createFetcherStateStore(config, clock);
    const dependencies = createLocalFetcherDependencies({
      clock,
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
    activeServer = createFetcherHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");

    const readinessResponse = await fetch(activeServer.url("/ready"));
    const readiness = await readinessResponse.json() as {
      readonly status: string;
      readonly checks: readonly {
        readonly name: string;
        readonly details?: Readonly<Record<string, unknown>>;
      }[];
    };

    expect(readinessResponse.status).toBe(503);
    expect(readiness.status).toBe("unhealthy");
    expect(readiness.checks.find((check) => check.name === "durable-state")?.details).toMatchObject({
      expectedMode: "postgresql",
      actualMode: "unsupported",
      adapter: "none",
      durable: false
    });
    expect(JSON.stringify(readiness)).not.toContain("secret");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    const metricsText = await metricsResponse.text();

    expect(metricsResponse.status).toBe(200);
    expect(metricsText).toMatch(/nutsnews_worker_state_store_ready\{[^\n]+outcome="unsupported"[^\n]+\} 0/u);
    expect(metricsText).toContain('nutsnews_worker_expected_active{environment="local",service="nutsnews-worker-feed-fetcher"} 0');

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
