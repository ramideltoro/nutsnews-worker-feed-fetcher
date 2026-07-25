import os from "node:os";

export const FETCHER_SERVICE_NAME = "nutsnews-worker-feed-fetcher" as const;
export const FETCHER_SERVICE_VERSION = "0.1.0" as const;

export type FetcherDependencyMode = "test" | "production";
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
  variable("NUTSNEWS_FETCHER_CONCURRENCY", "Maximum concurrent feed-fetch message handlers.", false, false, "8"),
  variable("NUTSNEWS_FETCHER_PREFETCH", "Broker prefetch bound for feed-fetch deliveries.", false, false, "16"),
  variable("NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_FETCHER_SHADOW_MODE", "Keep fetcher output isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_FETCHER_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_FETCHER_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true"),
  variable("NUTSNEWS_FETCHER_USER_AGENT", "HTTP user agent for feed requests.", false, false, "NutsNewsWorkerFetcher/0.1"),
  variable("NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS", "HTTP connection timeout in milliseconds.", false, false, "5000"),
  variable("NUTSNEWS_FETCHER_READ_TIMEOUT_MS", "HTTP read timeout in milliseconds.", false, false, "10000"),
  variable("NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS", "Total feed fetch timeout in milliseconds.", false, false, "15000"),
  variable("NUTSNEWS_FETCHER_MAX_REDIRECTS", "Maximum safe redirects per feed fetch.", false, false, "3"),
  variable("NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES", "Maximum feed response body size in bytes.", false, false, "1048576")
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
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
  };
  readonly concurrency: number;
  readonly prefetch: number;
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
  const prefetch = parseInteger(env.NUTSNEWS_FETCHER_PREFETCH, "NUTSNEWS_FETCHER_PREFETCH", 16, 1, 512, issues);
  const connectTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS, "NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS", 5_000, 250, 60_000, issues);
  const readTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_READ_TIMEOUT_MS, "NUTSNEWS_FETCHER_READ_TIMEOUT_MS", 10_000, 250, 120_000, issues);
  const totalTimeoutMs = parseInteger(env.NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS, "NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS", 15_000, 250, 180_000, issues);
  const config: FetcherConfig = {
    serviceName: FETCHER_SERVICE_NAME,
    serviceVersion: FETCHER_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    http: {
      host: nonEmpty(env.NUTSNEWS_FETCHER_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_FETCHER_HTTP_PORT, "NUTSNEWS_FETCHER_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    concurrency,
    prefetch,
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_FETCHER_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_FETCHER_SHADOW_MODE, "NUTSNEWS_FETCHER_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_FETCHER_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_FETCHER_METRICS_ENABLED, "NUTSNEWS_FETCHER_METRICS_ENABLED", true, issues),
    fetchPolicy: {
      userAgent: nonEmpty(env.NUTSNEWS_FETCHER_USER_AGENT, "NutsNewsWorkerFetcher/0.1"),
      connectTimeoutMs,
      readTimeoutMs,
      totalTimeoutMs,
      maxRedirects: parseInteger(env.NUTSNEWS_FETCHER_MAX_REDIRECTS, "NUTSNEWS_FETCHER_MAX_REDIRECTS", 3, 0, 10, issues),
      maxResponseBytes: parseInteger(env.NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES, "NUTSNEWS_FETCHER_MAX_RESPONSE_BYTES", 1_048_576, 1_024, 16_777_216, issues)
    }
  };

  if (config.prefetch < config.concurrency) {
    issues.push("NUTSNEWS_FETCHER_PREFETCH must be greater than or equal to NUTSNEWS_FETCHER_CONCURRENCY.");
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

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_FETCHER_DEPENDENCY_MODE=production.`);
  }
}
