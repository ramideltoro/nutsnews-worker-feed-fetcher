import {
  describe,
  expect,
  it
} from "vitest";

import { loadFetcherConfig } from "../src/config.js";
import {
  FetcherStateStoreUnavailableError,
  InMemoryFetcherStateStore,
  UnsupportedProductionFetcherStateStore,
  createFetcherStateStore,
  expectedFetcherStateStoreMode
} from "../src/state-store.js";
import { PostgresFetcherStateStore } from "../src/postgres-state-store.js";
import {
  ManualFetcherClock,
  createMinimalCanonicalizationCommand
} from "../src/test-doubles.js";

describe("fetcher state store selection", () => {
  it("uses an explicitly ephemeral adapter only in local dependency mode", async () => {
    const config = loadFetcherConfig();
    const stateStore = createFetcherStateStore(config, new ManualFetcherClock());

    expect(stateStore).toBeInstanceOf(InMemoryFetcherStateStore);
    expect(expectedFetcherStateStoreMode(config)).toBe("local-memory");
    expect(stateStore).toMatchObject({
      name: "local-memory-state-store",
      mode: "local-memory",
      adapter: "runtime-in-memory",
      durable: false
    });
    expect(await stateStore.probe()).toMatchObject({
      status: "ok"
    });
  });

  it("selects an unavailable fail-closed adapter in production instead of volatile memory", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_DATABASE_URL: "postgres://secret@example.invalid/fetcher",
      NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://secret@example.invalid",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
    });
    const stateStore = createFetcherStateStore(config, new ManualFetcherClock());

    expect(stateStore).toBeInstanceOf(UnsupportedProductionFetcherStateStore);
    expect(stateStore).not.toBeInstanceOf(InMemoryFetcherStateStore);
    expect(expectedFetcherStateStoreMode(config)).toBe("postgresql");
    expect(stateStore).toMatchObject({
      name: "unsupported-production-state-store",
      mode: "unsupported",
      adapter: "none",
      durable: false
    });
    expect(await stateStore.probe()).toMatchObject({
      status: "unhealthy"
    });
    await expect(stateStore.getFeedMetadata("feed-world")).rejects.toBeInstanceOf(FetcherStateStoreUnavailableError);
    await expect(stateStore.claimCandidate("candidate", {
      feedId: "feed-world",
      sourceItemId: "item-one",
      contentFingerprint: "fingerprint",
      firstSeenAt: "2026-07-23T00:00:00.000Z",
      claimOwnerKey: "claim-owner-one",
      command: createMinimalCanonicalizationCommand()
    })).rejects.toMatchObject({
      name: "FetcherStateStoreUnavailableError",
      operation: "claimCandidate"
    });
    expect(JSON.stringify(stateStore)).not.toContain("postgres://");
    expect(JSON.stringify(stateStore)).not.toContain("amqp://");
  });

  it("selects the durable PostgreSQL adapter when the application supplies the protected URL", async () => {
    const config = loadFetcherConfig({
      NUTSNEWS_FETCHER_DEPENDENCY_MODE: "production",
      NUTSNEWS_FETCHER_DATABASE_URL: "postgres://secret@example.invalid/fetcher",
      NUTSNEWS_FETCHER_RABBITMQ_URL: "amqp://secret@example.invalid",
      NUTSNEWS_FETCHER_BUILD_REVISION: "501ededcad48924b632b0547679f4dcb54ed4a90"
    });
    const stateStore = createFetcherStateStore(config, new ManualFetcherClock(), {
      databaseUrl: "postgres://secret@example.invalid/fetcher"
    });

    expect(stateStore).toBeInstanceOf(PostgresFetcherStateStore);
    expect(stateStore).toMatchObject({
      name: "postgresql-fetcher-state-store",
      mode: "postgresql",
      adapter: "backend-postgresql",
      durable: true
    });
    expect(JSON.stringify(stateStore)).not.toContain("postgres://");

    await stateStore.close?.();
  });
});
