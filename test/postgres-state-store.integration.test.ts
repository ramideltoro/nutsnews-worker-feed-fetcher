import { readFile } from "node:fs/promises";

import { Pool } from "pg";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it
} from "vitest";

import {
  FetcherStateOwnershipError,
  PostgresFetcherStateStore
} from "../src/postgres-state-store.js";
import {
  ManualFetcherClock,
  createMinimalCanonicalizationCommand,
  createMinimalFetchEnvelope
} from "../src/test-doubles.js";

const postgresUrl = process.env.FETCHER_INTEGRATION_POSTGRES_URL;
const describeWithPostgres = postgresUrl === undefined ? describe.skip : describe;

describeWithPostgres("PostgreSQL fetcher state store integration", () => {
  const clock = new ManualFetcherClock();
  let pool: Pool;
  let stateStore: PostgresFetcherStateStore;

  beforeAll(async () => {
    pool = new Pool({
      connectionString: postgresUrl,
      max: 4
    });
    const migration = await readFile(
      new URL("../migrations/001_worker_uplift_fetcher_state.sql", import.meta.url),
      "utf8"
    );

    await pool.query(migration);
    stateStore = new PostgresFetcherStateStore({
      pool,
      clock,
      leaseMs: 60_000
    });
  });

  beforeEach(async () => {
    await pool.query(`
      TRUNCATE TABLE
        worker_uplift_fetcher.fetch_outcomes,
        worker_uplift_fetcher.feed_health_projections,
        worker_uplift_fetcher.fetch_versions,
        worker_uplift_fetcher.outbox,
        worker_uplift_fetcher.inbox
      RESTART IDENTITY
    `);
  });

  afterAll(async () => {
    await pool.end();
  });

  it("probes the backend-owned versioned state contract", async () => {
    await expect(stateStore.probe()).resolves.toMatchObject({
      status: "ok",
      summary: "backend PostgreSQL fetcher state contract v1 ready"
    });
  });

  it("fences inbox completion and reclaims an expired processing lease", async () => {
    const firstEnvelope = createMinimalFetchEnvelope();
    const idempotencyKey = firstEnvelope.idempotencyKey;
    const firstContext = {
      envelope: firstEnvelope,
      stage: "fetch" as const,
      receivedAt: clock.now().toISOString()
    };

    await expect(stateStore.claim(idempotencyKey, firstContext)).resolves.toMatchObject({
      status: "claimed",
      replay: false
    });
    await expect(stateStore.claim(idempotencyKey, firstContext)).resolves.toMatchObject({
      status: "in-progress"
    });

    clock.advance(60_001);
    const secondEnvelope = createMinimalFetchEnvelope({
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3699"
    });
    const secondContext = {
      envelope: secondEnvelope,
      stage: "fetch" as const,
      receivedAt: clock.now().toISOString()
    };

    await expect(stateStore.claim(idempotencyKey, secondContext)).resolves.toMatchObject({
      status: "claimed",
      replay: true
    });
    await expect(stateStore.markCompleted(idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      stage: "fetch"
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.markCompleted(idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: secondEnvelope.messageId,
      stage: "fetch"
    })).resolves.toBeUndefined();
    await expect(stateStore.claim(idempotencyKey, secondContext)).resolves.toMatchObject({
      status: "already-completed"
    });
  });

  it("persists feed versions, outcome history, and the latest durable metadata", async () => {
    await stateStore.recordFetchOutcome({
      feedId: "feed-world",
      feedUrl: "https://feeds.example.test/world.xml",
      fetchedAt: clock.now().toISOString(),
      fetchStatus: "success",
      httpStatus: 200,
      etag: "\"rss-v1\"",
      lastModified: "Thu, 23 Jul 2026 04:05:06 GMT",
      contentFingerprint: "fingerprint-v1",
      bodyBytes: 1024,
      itemCount: 2,
      durationMs: 12
    });

    await expect(stateStore.getFeedMetadata("feed-world")).resolves.toMatchObject({
      feedId: "feed-world",
      etag: "\"rss-v1\"",
      lastModified: "Thu, 23 Jul 2026 04:05:06 GMT",
      contentFingerprint: "fingerprint-v1"
    });

    await stateStore.recordFetchOutcome({
      feedId: "feed-world",
      feedUrl: "https://feeds.example.test/world.xml",
      fetchedAt: new Date(clock.now().getTime() + 1_000).toISOString(),
      fetchStatus: "transient_failure",
      bodyBytes: 0,
      itemCount: 0,
      durationMs: 25,
      failure: {
        failureClass: "timeout",
        code: "read-timeout",
        retryable: true,
        action: "retry",
        safeFeedUrl: "https://feeds.example.test/world.xml",
        diagnosticSample: "bounded timeout"
      }
    });

    const counts = await pool.query<{
      readonly outcomes: string;
      readonly versions: string;
      readonly projections: string;
      readonly retention_days: string;
    }>(`
      SELECT
        (SELECT count(*) FROM worker_uplift_fetcher.fetch_outcomes)::text AS outcomes,
        (SELECT count(*) FROM worker_uplift_fetcher.fetch_versions)::text AS versions,
        (SELECT count(*) FROM worker_uplift_fetcher.feed_health_projections)::text AS projections,
        (SELECT min(extract(epoch FROM (redact_after - created_at)) / 86400)::integer::text
           FROM worker_uplift_fetcher.fetch_outcomes) AS retention_days
    `);

    expect(counts.rows[0]).toEqual({
      outcomes: "2",
      versions: "1",
      projections: "2",
      retention_days: "30"
    });
  });

  it("stores the canonicalization command before publish and fences confirmation ownership", async () => {
    const command = createMinimalCanonicalizationCommand();
    const claim = {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      claimOwnerKey: command.envelope.messageId,
      command
    };

    await expect(stateStore.claimCandidate("candidate-world-one", claim)).resolves.toMatchObject({
      status: "claimed",
      command
    });
    await expect(stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimOwnerKey: command.envelope.messageId,
      reason: "BrokerPublishError"
    })).resolves.toBeUndefined();
    await expect(stateStore.listPendingCandidatePublications({
      maxItems: 10,
      minAgeSeconds: 0
    })).resolves.toMatchObject([{
      candidateId: "candidate-world-one",
      claimOwnerKey: command.envelope.messageId,
      command
    }]);
    const retryCommand = createMinimalCanonicalizationCommand({
      messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3639",
      idempotencyKey: command.envelope.idempotencyKey
    });
    const retryClaim = {
      ...claim,
      claimOwnerKey: retryCommand.envelope.messageId,
      command: retryCommand
    };

    await expect(stateStore.claimCandidate("candidate-world-one", retryClaim)).resolves.toMatchObject({
      status: "claimed",
      command
    });
    await expect(stateStore.markCandidatePublished("candidate-world-one", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimOwnerKey: "wrong-owner"
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.markCandidatePublished("candidate-world-one", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimOwnerKey: retryCommand.envelope.messageId
    })).resolves.toBeUndefined();
    await expect(stateStore.claimCandidate("candidate-world-one", retryClaim)).resolves.toMatchObject({
      status: "already-published",
      messageId: command.envelope.messageId
    });
    await expect(stateStore.listPendingCandidatePublications({
      maxItems: 10,
      minAgeSeconds: 0
    })).resolves.toEqual([]);
  });
});
