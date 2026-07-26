import { createPrometheusRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";
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
import {
  createFetcherFailClosedReconciler
} from "../src/reconciliation.js";
import { createFetcherService } from "../src/service.js";
import {
  ManualFetcherClock,
  createLocalFetcherDependencies
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
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createFetcherService({
      config,
      dependencies: createLocalFetcherDependencies(),
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
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    expect(await metricsResponse.text()).toContain("nutsnews_worker_dependency_duration_ms");

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };

    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_FETCHER_RABBITMQ_URL" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("amqp://");

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
    expect(authorized.status).toBe(409);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "failed_closed",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}
