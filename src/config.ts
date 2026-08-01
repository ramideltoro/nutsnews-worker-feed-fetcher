import os from "node:os";

import { WORKER_DELIVERY_BEHAVIOR } from "@ramideltoro/nutsnews-worker-contracts";

import { FETCHER_MAX_CLAIM_LEASE_MS } from "./dependencies.js";

export const FETCHER_SERVICE_NAME = "nutsnews-worker-feed-fetcher" as const;
export const FETCHER_SERVICE_VERSION = "0.1.0" as const;
export const FETCHER_CLAIM_SETTLEMENT_SAFETY_MS = 5_000 as const;

export type FetcherDependencyMode = "test" | "production";
export type FetcherDeploymentMode = "shadow" | "production";
export type FetcherTelemetryLogMode = "stdout" | "silent";

export interface FetcherConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const FETCHER_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_FETCHER_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_FETCHER_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_FETCHER_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_FETCHER_DATABASE_URL", "Backend shadow database connection string for durable fetch state.", true, true),
  variable("NUTSNEWS_FETCHER_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_FETCHER_DATABASE_POOL_MAX", "Maximum PostgreSQL connections owned by this fetcher instance.", false, false, "10"),
  variable("NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS", "PostgreSQL connect, query, and statement timeout in milliseconds.", false, false, "5000"),
  variable("NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS", "Crash-recovery lease for inbox and pending candidate publication ownership (maximum five minutes).", false, false, "300000"),
  variable("NUTSNEWS_FETCHER_CONCURRENCY", "Maximum concurrent feed-fetch message handlers.", false, false, "8"),
  variable("NUTSNEWS_FETCHER_PREFETCH", "Broker prefetch bound for feed-fetch deliveries.", false, false, "8"),
  variable("NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS", "Maximum duration for a dependency startup probe before the worker fails closed.", false, false, "30000"),
  variable("NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_FETCHER_SHADOW_MODE", "Keep fetcher output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_FETCHER_BUILD_REVISION", "Immutable source revision embedded in the service image.", false, false, "unknown"),
  variable("NUTSNEWS_FETCHER_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_FETCHER_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true"),
  variable("NUTSNEWS_FETCHER_USER_AGENT", "HTTP user agent for feed requests.", false, false, "NutsNewsWorkerFetcher/0.1"),
  variable("NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS", "HTTP connection timeout in milliseconds.", false, false, "5000"),
  variable("NUTSNEWS_FETCHER_READ_TIMEOUT_MS", "HTTP read timeout in milliseconds.", false, false, "10000"),
  variable("NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS", "Total feed fetch timeout in milliseconds.", false, false, "15000"),
  variable("NUTSNEWS_FETCHER_MAX_REDIRECTS", "Maximum safe redirects per feed fetch.", false, false, "3"),
  variable("NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES", "Maximum feed response body size in bytes.", false, false, "1048576"),
  variable("NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS", "Maximum Retry-After delay the fetcher will honor for transient feed failures.", false, false, "1800000"),
  variable("NUTSNEWS_FETCHER_ACCEPTED_CONTENT_TYPES", "Comma-separated accepted feed response content types.", false, false, "application/rss+xml,application/atom+xml,application/xml,text/xml,text/rss+xml")
] as const satisfies readonly FetcherConfigVariable[];

export interface FetcherConfig {
  readonly serviceName: typeof FETCHER_SERVICE_NAME;
  readonly serviceVersion: typeof FETCHER_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: FetcherDependencyMode;
  readonly deploymentMode: FetcherDeploymentMode;
  readonly expectedActive: boolean;
  readonly buildRevision: string;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
  };
  readonly database: {
    readonly poolMax: number;
    readonly timeoutMs: number;
    readonly idempotencyLeaseMs: number;
  };
  readonly concurrency: number;
  readonly prefetch: number;
  readonly startupTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: FetcherTelemetryLogMode;
  readonly metricsEnabled: boolean;
  readonly fetchPolicy: {
    readonly userAgent: string;
    readonly connectTimeoutMs: number;
    readonly readTimeoutMs: number;
    readonly totalTimeoutMs: number;
    readonly maxRedirects: number;
    readonly maxResponseBytes: number;
    readonly maxRetryAfterMs: number;
    readonly acceptedContentTypes: readonly string[];
  };
}

export class FetcherConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid fetcher configuration: ${issues.join("; ")}`);
    this.name = "FetcherConfigError";
    this.issues = issues;
  }
}

export function loadFetcherConfig(env: NodeJS.ProcessEnv = process.env): FetcherConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_FETCHER_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_FETCHER_DATABASE_URL),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_FETCHER_RABBITMQ_URL)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_FETCHER_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_FETCHER_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
  }

  const concurrency = parseInteger(env.NUTSNEWS_FETCHER_CONCURRENCY, "NUTSNEWS_FETCHER_CONCURRENCY", 8, 1, 128, issues);
  const prefetch = parseInteger(env.NUTSNEWS_FETCHER_PREFETCH, "NUTSNEWS_FETCHER_PREFETCH", 8, 1, 512, issues);
  const connectTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS, "NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS", 5_000, 250, 60_000, issues);
  const readTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_READ_TIMEOUT_MS, "NUTSNEWS_FETCHER_READ_TIMEOUT_MS", 10_000, 250, 120_000, issues);
  const totalTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS, "NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS", 15_000, 250, 180_000, issues);
  const shadowMode = parseBoolean(env.NUTSNEWS_FETCHER_SHADOW_MODE, "NUTSNEWS_FETCHER_SHADOW_MODE", true, issues);
  const environment = nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local");
  const config: FetcherConfig = {
    serviceName: FETCHER_SERVICE_NAME,
    serviceVersion: FETCHER_SERVICE_VERSION,
    environment,
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_FETCHER_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_FETCHER_HTTP_PORT, "NUTSNEWS_FETCHER_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    deploymentMode: shadowMode ? "shadow" : "production",
    expectedActive: !shadowMode,
    buildRevision: parseBuildRevision(env.NUTSNEWS_FETCHER_BUILD_REVISION, issues),
    dependencies,
    database: {
      poolMax: parseInteger(env.NUTSNEWS_FETCHER_DATABASE_POOL_MAX, "NUTSNEWS_FETCHER_DATABASE_POOL_MAX", 10, 1, 64, issues),
      timeoutMs: parseInteger(env.NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS, "NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS", 5_000, 100, 60_000, issues),
      idempotencyLeaseMs: parseInteger(
        env.NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS,
        "NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS",
        FETCHER_MAX_CLAIM_LEASE_MS,
        60_000,
        FETCHER_MAX_CLAIM_LEASE_MS,
        issues
      )
    },
    concurrency,
    prefetch,
    startupTimeoutMs: parseInteger(env.NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS, "NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS", 30_000, 100, 600_000, issues),
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode,
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_FETCHER_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_FETCHER_METRICS_ENABLED, "NUTSNEWS_FETCHER_METRICS_ENABLED", true, issues),
    fetchPolicy: {
      userAgent: nonEmpty(env.NUTSNEWS_FETCHER_USER_AGENT, "NutsNewsWorkerFetcher/0.1"),
      connectTimeoutMs,
      readTimeoutMs,
      totalTimeoutMs,
      maxRedirects: parseInteger(env.NUTSNEWS_FETCHER_MAX_REDIRECTS, "NUTSNEWS_FETCHER_MAX_REDIRECTS", 3, 0, 10, issues),
      maxResponseBytes: parseInteger(env.NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES, "NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES", 1_048_576, 1_024, 16_777_216, issues),
      maxRetryAfterMs: parseInteger(env.NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS, "NUTSNEWS_FETCHER_RETRY_AFTER_MAX_MS", 1_800_000, 1_000, 3_600_000, issues),
      acceptedContentTypes: parseContentTypes(env.NUTSNEWS_FETCHER_ACCEPTED_CONTENT_TYPES, issues)
    }
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_FETCHER_PREFETCH must be greater than or equal to NUTSNEWS_FETCHER_CONCURRENCY.");
  }

  if ((config.environment.toLowerCase() === "production" || config.environment.toLowerCase() === "prod")
    && config.dependencyMode !== "production") {
    issues.push("NUTSNEWS_FETCHER_DEPENDENCY_MODE must be production when NUTSNEWS_ENVIRONMENT is production or prod.");
  }

  if (config.fetchPolicy.connectTimeoutMs > config.fetchPolicy.totalTimeoutMs) {
    issues.push("NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS must be less than or equal to NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS.");
  }

  if (config.fetchPolicy.readTimeoutMs > config.fetchPolicy.totalTimeoutMs) {
    issues.push("NUTSNEWS_FETCHER_READ_TIMEOUT_MS must be less than or equal to NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS.");
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_FETCHER_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (config.dependencyMode === "production" && config.buildRevision.toLowerCase() === "unknown") {
    issues.push("NUTSNEWS_FETCHER_BUILD_REVISION must be an immutable non-unknown revision in production dependency mode.");
  }

  if (config.dependencyMode === "production" && config.database.timeoutMs > config.startupTimeoutMs) {
    issues.push("NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS must be less than or equal to NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS.");
  }

  const candidateSettlementDeadlineMs = config.database.timeoutMs
    + WORKER_DELIVERY_BEHAVIOR.confirmTimeoutMs
    + FETCHER_CLAIM_SETTLEMENT_SAFETY_MS;
  const sourceCheckpointDeadlineMs = config.fetchPolicy.totalTimeoutMs
    + config.database.timeoutMs
    + FETCHER_CLAIM_SETTLEMENT_SAFETY_MS;

  if (config.database.idempotencyLeaseMs < candidateSettlementDeadlineMs) {
    issues.push(
      "NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS must cover the PostgreSQL timeout, RabbitMQ confirm timeout, and 5000ms settlement safety margin."
    );
  }

  if (config.database.idempotencyLeaseMs < sourceCheckpointDeadlineMs) {
    issues.push(
      "NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS must cover the total feed timeout, PostgreSQL checkpoint timeout, and 5000ms settlement safety margin."
    );
  }

  if (issues.length > 0) {
    throw new FetcherConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): FetcherConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseDependencyMode(value: string | undefined, issues: string[]): FetcherDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_FETCHER_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): FetcherTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_FETCHER_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBuildRevision(value: string | undefined, issues: string[]): string {
  const revision = nonEmpty(value, "unknown");

  if (/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/u.test(revision)) {
    return revision;
  }

  issues.push("NUTSNEWS_FETCHER_BUILD_REVISION must be 1-128 characters using letters, numbers, dot, underscore, or hyphen.");
  return "unknown";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function parseContentTypes(value: string | undefined, issues: string[]): readonly string[] {
  const contentTypes = nonEmpty(value, "application/rss+xml,application/atom+xml,application/xml,text/xml,text/rss+xml")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter((entry) => entry.length > 0);

  if (contentTypes.length === 0) {
    issues.push("NUTSNEWS_FETCHER_ACCEPTED_CONTENT_TYPES must contain at least one content type.");
    return [
      "application/rss+xml"
    ];
  }

  return contentTypes;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_FETCHER_DEPENDENCY_MODE=production.`);
  }
}
