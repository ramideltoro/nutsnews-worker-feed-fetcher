import {
  describe,
  expect,
  it
} from "vitest";

import {
  FetcherConfigError,
  loadFetcherConfig
} from "../src/config.js";

describe("loadFetcherConfig", () => {
  it("loads local test defaults without secret values", () => {
    const config = loadFetcherConfig({
      HOSTNAME: "fetcher-host"
    });

    expect(config).toMatchObject({
      serviceName: "nutsnews-worker-feed-fetcher",
      dependencyMode: "test",
      deploymentMode: "shadow",
      expectedActive: false,
      buildRevision: "unknown",
      host: "fetcher-host",
      concurrency: 8,
      prefetch: 8,
      startupTimeoutMs: 30_000,
      shadowMode: true,
      fetchPolicy: {
        maxRetryAfterMs: 1_800_000,
        acceptedContentTypes: [
          "application/rss+xml",
          "application/atom+xml",
          "application/xml",
          "text/xml",
          "text/rss+xml"
        ]
      },
      dependencies: {
        databaseConfigured: false,
        rabbitmqConfigured: false
      },
      database: {
        idempotencyLeaseMs: 300_000
      }
    });
  });

  it("fails production config by missing secret names only", () => {
    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
    })).toThrow(FetcherConfigError);

    try {
      loadFetcherConfig({
        NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
        NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
      });
    } catch (error: unknown) {
      expect(error).toBeInstanceOf(FetcherConfigError);
      const configError = error as FetcherConfigError;

      expect(configError.issues).toEqual([
        "NUTSNEWS_FETCHER_DATABASE_URL is required when NUTSNEWS_FETCHER_DEPENDENCY_MODE=production.",
        "NUTSNEWS_FETCHER_RABBITMQ_URL is required when NUTSNEWS_FETCHER_DEPENDENCY_MODE=production."
      ]);
      expect(configError.message).not.toContain("postgres://");
      expect(configError.message).not.toContain("amqp://");
    }
  });

  it("does not permit a production environment to select test dependencies", () => {
    expect(() => loadFetcherConfig({
      NUTSNEWS_ENVIRONMENT: "production"
    })).toThrowError(/NUTSNEWS_FETCHER_DEPENDENCY_MODE must be production/u);

    const config = loadFetcherConfig({
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
    });

    expect(config).toMatchObject({
      environment: "production",
      dependencyMode: "production"
    });
  });

  it("rejects unsafe bounds and shadow cutover in this repo", () => {
    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_CONCURRENCY: "12",
      NUTSNEWS_FETCHER_PREFETCH: "4",
      NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS: "20000",
      NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS: "10000",
      NUTSNEWS_FETCHER_STARTUP_TIMEOUT_MS: "99",
      NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS: "300001",
      NUTSNEWS_FETCHER_SHADOW_MODE: "false"
    })).toThrow(FetcherConfigError);
  });

  it("requires a claimed candidate to settle before its PostgreSQL lease can expire", () => {
    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS: "51000",
      NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS: "60000",
      NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_READ_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS: "250"
    })).toThrowError(
      /must cover the PostgreSQL timeout, RabbitMQ confirm timeout, and 5000ms settlement safety margin/u
    );

    expect(loadFetcherConfig({
      NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS: "50000",
      NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS: "60000",
      NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_READ_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS: "250"
    }).database.idempotencyLeaseMs).toBe(60_000);

    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_DATABASE_TIMEOUT_MS: "5000",
      NUTSNEWS_FETCHER_IDEMPOTENCY_LEASE_MS: "60000",
      NUTSNEWS_FETCHER_CONNECT_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_READ_TIMEOUT_MS: "250",
      NUTSNEWS_FETCHER_TOTAL_TIMEOUT_MS: "50001"
    })).toThrowError(
      /must cover the total feed timeout, PostgreSQL checkpoint timeout, and 5000ms settlement safety margin/u
    );
  });

  it("accepts explicit production dependency presence without retaining values", () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_DATABASE_URL: "postgres://example.invalid/worker",
      NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://example.invalid",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90",
      NUTSNEWS_FETCHER_TELEMETRY_LOGS: "silent"
    });

    expect(config.dependencies).toEqual({
      databaseConfigured: true,
      rabbitmqConfigured: true
    });
    expect(config.buildRevision).toBe("501ededcad48924b632b0547679f4dcb54ed4a90");
    expect(config.deploymentMode).toBe("shadow");
    expect(config.expectedActive).toBe(false);
    expect(JSON.stringify(config)).not.toContain("postgres://example.invalid");
    expect(JSON.stringify(config)).not.toContain("amqp://example.invalid");
  });

  it("rejects unbounded build revision labels", () => {
    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_BUILD_REVISION: `revision-${"x".repeat(200)}`
    })).toThrow(FetcherConfigError);

    expect(() => loadFetcherConfig({
      NUTSNEWS_FETCHER_BUILD_REVISION: "revision with spaces"
    })).toThrow(FetcherConfigError);
  });
});
