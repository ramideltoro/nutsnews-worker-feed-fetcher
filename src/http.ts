import http from "node:http";
import { timingSafeEqual } from "node:crypto";
import type { AddressInfo } from "node:net";

import {
  runtimeHealthEndpointResponse
} from "@ramideltoro/nutsnews-worker-runtime";

import {
  FETCHER_CONFIG_SCHEMA,
  type FetcherConfig
} from "./config.js";
import { collectFetcherTelemetryStatusMetrics } from "./metrics.js";
import {
  FETCHER_RECONCILIATION_PATH,
  type FetcherReconciliationRequest,
  type FetcherReconciler
} from "./reconciliation.js";
import type { FetcherService } from "./service.js";

export interface FetcherHttpServerOptions {
  readonly config: FetcherConfig;
  readonly service: FetcherService;
  readonly metrics?: {
    collect(): string;
  };
  readonly reconciler?: FetcherReconciler;
  readonly reconciliationToken?: string;
}

export interface FetcherHttpServer {
  readonly server: http.Server;
  listen(): Promise<http.Server>;
  close(): Promise<void>;
  url(path?: string): string;
}

export function createFetcherHttpServer(options: FetcherHttpServerOptions): FetcherHttpServer {
  const server = http.createServer((request, response) => {
    void routeRequest(options, request, response);
  });

  return {
    server,
    listen: () => new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(options.config.http.port, options.config.http.host, () => {
        server.off("error", reject);
        resolve(server);
      });
    }),
    close: () => new Promise((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }

        resolve();
      });
    }),
    url: (path = "/") => {
      const address = server.address();

      if (!isAddressInfo(address)) {
        throw new Error("Fetcher HTTP server is not listening on a TCP address.");
      }

      return `http://127.0.0.1:${String(address.port)}${path}`;
    }
  };
}

async function routeRequest(
  options: FetcherHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  const url = new URL(request.url ?? "/", "http://localhost");

  if (request.method === "POST" && url.pathname === FETCHER_RECONCILIATION_PATH) {
    await handleReconciliationRequest(options, request, response);
    return;
  }

  if (request.method !== "GET") {
    writeJson(response, 405, {
      status: "method-not-allowed"
    });
    return;
  }

  switch (url.pathname) {
    case "/live":
    case "/healthz":
      writeHealth(response, await options.service.health.liveness());
      return;
    case "/startup":
    case "/startupz":
      writeHealth(response, await options.service.health.startup());
      return;
    case "/ready":
    case "/readyz":
      writeHealth(response, await options.service.health.readiness());
      return;
    case "/metrics":
      await refreshReadinessForMetrics(options.service);
      writeText(response, 200, collectMetrics(options.metrics, options.config), "text/plain; version=0.0.4; charset=utf-8");
      return;
    case "/config-schema":
      writeJson(response, 200, {
        service: options.config.serviceName,
        version: options.config.serviceVersion,
        variables: FETCHER_CONFIG_SCHEMA
      });
      return;
    default:
      writeJson(response, 404, {
        status: "not-found"
      });
  }
}

async function refreshReadinessForMetrics(service: FetcherService): Promise<void> {
  // Startup diagnostics must remain non-blocking while dependency probes are
  // still settling. Once startup completes, every scrape refreshes readiness.
  if (!service.isStarted) {
    return;
  }

  try {
    await service.health.readiness();
  } catch {
    // The service-owned health observer records an unhealthy readiness value.
    // Diagnostics must remain scrapeable even if evaluation itself fails.
  }
}

function collectMetrics(
  metrics: FetcherHttpServerOptions["metrics"],
  config: FetcherConfig
): string {
  try {
    return metrics?.collect() ?? `${collectFetcherTelemetryStatusMetrics({
      environment: config.environment,
      service: config.serviceName
    }, config.metricsEnabled, false)}\n`;
  } catch {
    return `${collectFetcherTelemetryStatusMetrics({
      environment: config.environment,
      service: config.serviceName
    }, config.metricsEnabled, false)}\n`;
  }
}

async function handleReconciliationRequest(
  options: FetcherHttpServerOptions,
  request: http.IncomingMessage,
  response: http.ServerResponse
): Promise<void> {
  if (options.reconciler === undefined || options.reconciliationToken === undefined) {
    writeJson(response, 503, {
      service: "fetcher",
      status: "not_configured",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "fetcher reconciliation endpoint is not configured"
      ]
    });
    return;
  }

  if (!authorized(request.headers.authorization, options.reconciliationToken)) {
    writeJson(response, 401, {
      service: "fetcher",
      status: "unauthorized",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        "valid bearer token required"
      ]
    });
    return;
  }

  let body: FetcherReconciliationRequest;

  try {
    body = await readJsonBody(request);
  } catch (error: unknown) {
    writeJson(response, 400, {
      service: "fetcher",
      status: "failed_closed",
      writesPerformed: false,
      dryRun: true,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false,
      errors: [
        error instanceof Error ? error.message : "invalid reconciliation request body"
      ]
    });
    return;
  }

  const report = await options.reconciler.reconcile(body);
  const statusCode = report.status === "dry_run" || report.status === "applied"
    ? 200
    : report.status === "kill_switch_active"
      ? 423
      : 409;

  writeJson(response, statusCode, report);
}

function authorized(header: string | undefined, expectedToken: string): boolean {
  if (!header?.startsWith("Bearer ")) {
    return false;
  }

  const provided = Buffer.from(header.slice("Bearer ".length));
  const expected = Buffer.from(expectedToken);

  return provided.length === expected.length && timingSafeEqual(provided, expected);
}

function readJsonBody(request: http.IncomingMessage): Promise<FetcherReconciliationRequest> {
  const chunks: Buffer[] = [];
  let totalBytes = 0;
  const maxBytes = 16_384;

  return new Promise((resolve, reject) => {
    request.on("data", (chunk: Buffer) => {
      totalBytes += chunk.byteLength;

      if (totalBytes > maxBytes) {
        reject(new Error("reconciliation request body is too large"));
        request.destroy();
        return;
      }

      chunks.push(chunk);
    });
    request.on("error", reject);
    request.on("end", () => {
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;

        if (!isRecord(parsed)) {
          reject(new Error("reconciliation request body must be a JSON object"));
          return;
        }

        resolve(parsed as unknown as FetcherReconciliationRequest);
      } catch {
        reject(new Error("reconciliation request body must be valid JSON"));
      }
    });
  });
}

function writeHealth(
  response: http.ServerResponse,
  report: Awaited<ReturnType<FetcherService["health"]["liveness"]>>
): void {
  const endpointResponse = runtimeHealthEndpointResponse(report);
  writeJson(response, endpointResponse.statusCode, endpointResponse.body, endpointResponse.headers);
}

function writeJson(
  response: http.ServerResponse,
  statusCode: number,
  body: unknown,
  headers: Readonly<Record<string, string>> = {}
): void {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers
  });
  response.end(`${JSON.stringify(body)}\n`);
}

function writeText(
  response: http.ServerResponse,
  statusCode: number,
  body: string,
  contentType: string
): void {
  response.writeHead(statusCode, {
    "content-type": contentType,
    "cache-control": "no-store"
  });
  response.end(body);
}

function isAddressInfo(address: string | AddressInfo | null): address is AddressInfo {
  return typeof address === "object" && address !== null;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
