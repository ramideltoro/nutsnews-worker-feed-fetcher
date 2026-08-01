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
  it("fences same-message candidate redelivery and atomically claims reconciliation replay", async () => {
    const clock = new ManualFetcherClock();
    const stateStore = new InMemoryFetcherStateStore(clock, 60_000);
    const command = createMinimalCanonicalizationCommand();
    const candidate = {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      command
    };
    const first = await stateStore.claimCandidate("candidate-world-one", candidate);

    if (first.status !== "claimed") {
      throw new Error("Expected first candidate claim.");
    }

    const expiryClock = new ManualFetcherClock();
    const expiryStore = new InMemoryFetcherStateStore(expiryClock, 60_000);
    const expiring = await expiryStore.claimCandidate("candidate-expiring", candidate);

    if (expiring.status !== "claimed") {
      throw new Error("Expected expiring candidate claim.");
    }
    expiryClock.advance(60_000);
    await expect(expiryStore.markCandidatePublished("candidate-expiring", {
      publishedAt: expiryClock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: expiring.claimToken
    })).rejects.toBeInstanceOf(Error);
    const reclaimedExpiring = await expiryStore.claimCandidate("candidate-expiring", candidate);

    if (reclaimedExpiring.status !== "claimed") {
      throw new Error("Expected expired candidate reclaim.");
    }
    expect(reclaimedExpiring.claimToken).not.toBe(expiring.claimToken);

    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: first.claimToken,
      reason: "BrokerPublishError"
    });
    const second = await stateStore.claimCandidate("candidate-world-one", candidate);

    if (second.status !== "claimed") {
      throw new Error("Expected same-message candidate reclaim.");
    }
    expect(second.claimToken).not.toBe(first.claimToken);
    await expect(stateStore.markCandidatePublished("candidate-world-one", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: first.claimToken
    })).rejects.toBeInstanceOf(Error);
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: second.claimToken,
      reason: "BrokerPublishError"
    });

    const [left, right] = await Promise.all([
      stateStore.claimPendingCandidatePublications({ maxItems: 1, minAgeSeconds: 0 }),
      stateStore.claimPendingCandidatePublications({ maxItems: 1, minAgeSeconds: 0 })
    ]);

    expect([...left, ...right]).toHaveLength(1);
    expect(left).toHaveLength(1);
    expect(right).toHaveLength(0);
  });

  it("claims exactly one replay row even when a caller requests a batch", async () => {
    const clock = new ManualFetcherClock();
    const stateStore = new InMemoryFetcherStateStore(clock, 60_000);
    const candidates = [
      {
        candidateId: "candidate-batch-one",
        command: createMinimalCanonicalizationCommand()
      },
      {
        candidateId: "candidate-batch-two",
        command: createMinimalCanonicalizationCommand({
          candidateId: "candidate-batch-two",
          messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3632",
          idempotencyKey: "fetcher:canonicalization:candidate-batch-two:fingerprint-v1"
        })
      }
    ];

    for (const [index, candidate] of candidates.entries()) {
      const claim = await stateStore.claimCandidate(candidate.candidateId, {
        feedId: "feed-world",
        sourceItemId: `guid-batch-${String(index + 1)}`,
        contentFingerprint: `fingerprint-batch-${String(index + 1)}`,
        firstSeenAt: clock.now().toISOString(),
        command: candidate.command
      });

      if (claim.status !== "claimed") {
        throw new Error("Expected candidate claim.");
      }
      await stateStore.markCandidatePublishFailed(candidate.candidateId, {
        failedAt: clock.now().toISOString(),
        idempotencyKey: candidate.command.envelope.idempotencyKey,
        claimToken: claim.claimToken,
        reason: "FetcherDefinitePublishError"
      });
    }

    await expect(stateStore.claimPendingCandidatePublications({
      maxItems: 100,
      minAgeSeconds: 0
    })).resolves.toHaveLength(1);
    await expect(stateStore.claimPendingCandidatePublications({
      maxItems: 100,
      minAgeSeconds: 0
    })).resolves.toHaveLength(1);
    await expect(stateStore.claimPendingCandidatePublications({
      maxItems: 100,
      minAgeSeconds: 0
    })).resolves.toEqual([]);
  });

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
