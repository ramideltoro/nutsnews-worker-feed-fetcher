import { readFile } from "node:fs/promises";

import { Pool } from "pg";
import { createRuntimeMessageProcessor } from "@ramideltoro/nutsnews-worker-runtime";
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
  createMinimalFetchDelivery,
  createMinimalFetchEnvelope
} from "../src/test-doubles.js";

const postgresUrl = process.env.FETCHER_INTEGRATION_POSTGRES_URL;
const describeWithPostgres = postgresUrl === undefined ? describe.skip : describe;

describeWithPostgres("PostgreSQL fetcher state store integration", () => {
  const clock = new ManualFetcherClock();
  let pool: Pool;
  let stateStore: PostgresFetcherStateStore;
  let fencingMigration = "";

  beforeAll(async () => {
    pool = new Pool({
      connectionString: postgresUrl,
      max: 4
    });
    const baseMigration = await readFile(
      new URL("../migrations/001_worker_uplift_fetcher_state.sql", import.meta.url),
      "utf8"
    );
    fencingMigration = await readFile(
      new URL("../migrations/002_fetcher_claim_fencing.sql", import.meta.url),
      "utf8"
    );
    await pool.query(baseMigration);
    await pool.query(fencingMigration);
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
      summary: "backend PostgreSQL fetcher state contract v2 ready"
    });
  });

  it("enforces the bounded token and positive five-minute lease truth table", async () => {
    const envelope = createMinimalFetchEnvelope();
    const inboxClaim = await stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "fetch",
      receivedAt: clock.now().toISOString()
    });
    const command = createMinimalCanonicalizationCommand();
    const outboxClaim = await stateStore.claimCandidate("candidate-lease-contract", {
      feedId: "feed-world",
      sourceItemId: "guid-lease-contract",
      contentFingerprint: "fingerprint-lease-contract",
      firstSeenAt: clock.now().toISOString(),
      command
    });

    expect(inboxClaim.status).toBe("claimed");
    expect(outboxClaim.status).toBe("claimed");

    const targets = [
      {
        table: "inbox",
        keyColumn: "idempotency_key",
        key: envelope.idempotencyKey
      },
      {
        table: "outbox",
        keyColumn: "entity_id",
        key: "candidate-lease-contract"
      }
    ] as const;

    for (const target of targets) {
      const setLease = (token: string | null, duration: string, acquired = true) => pool.query(
        `UPDATE worker_uplift_fetcher.${target.table}
         SET claim_token = $1,
             claim_acquired_at = ${acquired ? "statement_timestamp()" : "NULL"},
             claim_expires_at = statement_timestamp() + $2::interval
         WHERE ${target.keyColumn} = $3`,
        [token, duration, target.key]
      );

      for (const invalidToken of [
        "abcdefg",
        `a${"b".repeat(160)}`,
        "-abcdefg",
        "abc/defg",
        "abc defgh",
        "éabcdefg"
      ]) {
        await expect(setLease(invalidToken, "1 second")).rejects.toMatchObject({
          code: "23514"
        });
      }

      await expect(setLease("abcdefgh", "1 microsecond")).resolves.toBeDefined();
      await expect(setLease(`a${"b".repeat(159)}`, "5 minutes")).resolves.toBeDefined();
      await expect(setLease("abcdefgh", "0 seconds")).rejects.toMatchObject({
        code: "23514"
      });
      await expect(setLease("abcdefgh", "-1 microsecond")).rejects.toMatchObject({
        code: "23514"
      });
      await expect(setLease("abcdefgh", "5 minutes 1 microsecond")).rejects.toMatchObject({
        code: "23514"
      });
      await expect(setLease("abcdefgh", "1 second", false)).rejects.toMatchObject({
        code: "23514"
      });
      await expect(pool.query(
        `UPDATE worker_uplift_fetcher.${target.table}
         SET claim_token = NULL,
             claim_acquired_at = NULL,
             claim_expires_at = NULL
         WHERE ${target.keyColumn} = $1`,
        [target.key]
      )).resolves.toBeDefined();
    }
  });

  it("keeps migration replay row- and contract-idempotent", async () => {
    const envelope = createMinimalFetchEnvelope();
    const inboxClaim = await stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "fetch",
      receivedAt: clock.now().toISOString()
    });

    if (inboxClaim.status !== "claimed") {
      throw new Error("Expected inbox claim.");
    }
    await stateStore.markCompleted(envelope.idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: envelope.messageId,
      claimToken: inboxClaim.claimToken,
      stage: "fetch"
    });

    const command = createMinimalCanonicalizationCommand();
    const outboxClaim = await stateStore.claimCandidate("candidate-migration-idempotence", {
      feedId: "feed-world",
      sourceItemId: "guid-migration-idempotence",
      contentFingerprint: "fingerprint-migration-idempotence",
      firstSeenAt: clock.now().toISOString(),
      command
    });

    if (outboxClaim.status !== "claimed") {
      throw new Error("Expected outbox claim.");
    }
    await stateStore.markCandidatePublished("candidate-migration-idempotence", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: outboxClaim.claimToken
    });

    const before = await migrationReplayEvidence(pool, envelope.idempotencyKey, "candidate-migration-idempotence");

    await pool.query(fencingMigration);

    const after = await migrationReplayEvidence(pool, envelope.idempotencyKey, "candidate-migration-idempotence");

    expect(after).toEqual(before);
  });

  it("fails readiness for a same-named but malformed lease constraint", async () => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");
      await client.query(`
        ALTER TABLE worker_uplift_fetcher.inbox
          DROP CONSTRAINT worker_uplift_fetcher_inbox_claim_lease_check;
        ALTER TABLE worker_uplift_fetcher.inbox
          ADD CONSTRAINT worker_uplift_fetcher_inbox_claim_lease_check CHECK (true)
      `);
      const driftedStore = new PostgresFetcherStateStore({
        pool: client as unknown as Pool,
        clock,
        leaseMs: 60_000
      });

      await expect(driftedStore.probe()).resolves.toMatchObject({
        status: "unhealthy",
        summary: "backend PostgreSQL fetcher state contract is missing or incomplete"
      });
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("fails readiness for a same-named but malformed claim-token index", async () => {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");
      await client.query(`
        DROP INDEX worker_uplift_fetcher.worker_uplift_fetcher_inbox_claim_token_idx;
        CREATE INDEX worker_uplift_fetcher_inbox_claim_token_idx
          ON worker_uplift_fetcher.inbox (claim_expires_at)
      `);
      const driftedStore = new PostgresFetcherStateStore({
        pool: client as unknown as Pool,
        clock,
        leaseMs: 60_000
      });

      await expect(driftedStore.probe()).resolves.toMatchObject({
        status: "unhealthy",
        summary: "backend PostgreSQL fetcher state contract is missing or incomplete"
      });
    } finally {
      await client.query("ROLLBACK");
      client.release();
    }
  });

  it("fences inbox completion and reclaims an expired processing lease", async () => {
    const firstEnvelope = createMinimalFetchEnvelope();
    const idempotencyKey = firstEnvelope.idempotencyKey;
    const firstContext = {
      envelope: firstEnvelope,
      stage: "fetch" as const,
      receivedAt: clock.now().toISOString()
    };

    const firstClaim = await stateStore.claim(idempotencyKey, firstContext);

    expect(firstClaim).toMatchObject({
      status: "claimed",
      replay: false
    });
    if (firstClaim.status !== "claimed") {
      throw new Error("Expected first inbox claim.");
    }
    await expect(stateStore.claim(idempotencyKey, firstContext)).resolves.toMatchObject({
      status: "in-progress"
    });

    await pool.query(
      `UPDATE worker_uplift_fetcher.inbox
       SET claim_acquired_at = statement_timestamp() - interval '2 seconds',
           claim_expires_at = statement_timestamp() - interval '1 second'
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );
    await expect(stateStore.markCompleted(idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      claimToken: firstClaim.claimToken,
      stage: "fetch"
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.markFailed(idempotencyKey, {
      failedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      claimToken: firstClaim.claimToken,
      stage: "fetch",
      reason: "expired-delivery",
      retryable: true
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.releaseClaim(idempotencyKey, {
      failedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      claimToken: firstClaim.claimToken,
      stage: "fetch",
      reason: "expired-delivery",
      retryable: true
    })).resolves.toEqual({
      status: "not-owned"
    });
    const secondEnvelope = firstEnvelope;
    const secondContext = {
      envelope: secondEnvelope,
      stage: "fetch" as const,
      receivedAt: clock.now().toISOString()
    };

    const secondClaim = await stateStore.claim(idempotencyKey, secondContext);

    expect(secondClaim).toMatchObject({
      status: "claimed",
      replay: true
    });
    if (secondClaim.status !== "claimed") {
      throw new Error("Expected reclaimed inbox claim.");
    }
    expect(secondClaim.claimToken).not.toBe(firstClaim.claimToken);
    await expect(stateStore.markCompleted(idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      claimToken: firstClaim.claimToken,
      stage: "fetch"
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.releaseClaim(idempotencyKey, {
      failedAt: clock.now().toISOString(),
      messageId: firstEnvelope.messageId,
      claimToken: firstClaim.claimToken,
      stage: "fetch",
      reason: "stale-delivery",
      retryable: true
    })).resolves.toEqual({
      status: "not-owned"
    });
    await expect(stateStore.markCompleted(idempotencyKey, {
      completedAt: clock.now().toISOString(),
      messageId: secondEnvelope.messageId,
      claimToken: secondClaim.claimToken,
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
      command
    };

    const firstClaim = await stateStore.claimCandidate("candidate-world-one", claim);

    expect(firstClaim).toMatchObject({
      status: "claimed",
      command
    });
    if (firstClaim.status !== "claimed") {
      throw new Error("Expected first candidate claim.");
    }
    await expect(stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: firstClaim.claimToken,
      reason: "BrokerPublishError"
    })).resolves.toBeUndefined();
    await expect(stateStore.listPendingCandidatePublications({
      maxItems: 10,
      minAgeSeconds: 0
    })).resolves.toMatchObject([{
      candidateId: "candidate-world-one",
      command
    }]);
    const secondClaim = await stateStore.claimCandidate("candidate-world-one", claim);

    expect(secondClaim).toMatchObject({
      status: "claimed",
      command
    });
    if (secondClaim.status !== "claimed") {
      throw new Error("Expected reclaimed candidate.");
    }
    expect(secondClaim.claimToken).not.toBe(firstClaim.claimToken);
    await expect(stateStore.markCandidatePublished("candidate-world-one", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: firstClaim.claimToken
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.markCandidatePublished("candidate-world-one", {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: secondClaim.claimToken
    })).resolves.toBeUndefined();
    await expect(stateStore.claimCandidate("candidate-world-one", claim)).resolves.toMatchObject({
      status: "already-published",
      messageId: command.envelope.messageId
    });
    await expect(stateStore.listPendingCandidatePublications({
      maxItems: 10,
      minAgeSeconds: 0
    })).resolves.toEqual([]);
  });

  it("rejects candidate settlement at or after the PostgreSQL lease deadline", async () => {
    const command = createMinimalCanonicalizationCommand();
    const candidateId = "candidate-expired-settlement";
    const candidate = {
      feedId: "feed-world",
      sourceItemId: "guid-expired",
      contentFingerprint: "fingerprint-expired",
      firstSeenAt: clock.now().toISOString(),
      command
    };
    const claim = await stateStore.claimCandidate(candidateId, candidate);

    if (claim.status !== "claimed") {
      throw new Error("Expected candidate claim.");
    }
    await pool.query(
      `UPDATE worker_uplift_fetcher.outbox
       SET claim_acquired_at = statement_timestamp() - interval '2 seconds',
           claim_expires_at = statement_timestamp() - interval '1 second'
       WHERE entity_id = $1`,
      [candidateId]
    );
    await expect(stateStore.markCandidatePublished(candidateId, {
      publishedAt: clock.now().toISOString(),
      messageId: command.envelope.messageId,
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: claim.claimToken
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);
    await expect(stateStore.markCandidatePublishFailed(candidateId, {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: claim.claimToken,
      reason: "expired-settlement"
    })).rejects.toBeInstanceOf(FetcherStateOwnershipError);

    const reclaimed = await stateStore.claimCandidate(candidateId, candidate);

    expect(reclaimed.status).toBe("claimed");
    if (reclaimed.status === "claimed") {
      expect(reclaimed.claimToken).not.toBe(claim.claimToken);
    }
  });

  it("atomically gives one reconciler a fresh replay claim", async () => {
    const command = createMinimalCanonicalizationCommand();
    const initial = await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      command
    });

    if (initial.status !== "claimed") {
      throw new Error("Expected initial candidate claim.");
    }
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: initial.claimToken,
      reason: "BrokerPublishError"
    });

    const [left, right] = await Promise.all([
      stateStore.claimPendingCandidatePublications({ maxItems: 1, minAgeSeconds: 0 }),
      stateStore.claimPendingCandidatePublications({ maxItems: 1, minAgeSeconds: 0 })
    ]);
    const claimed = [...left, ...right];

    expect(claimed).toHaveLength(1);
    expect(claimed[0]).toMatchObject({
      candidateId: "candidate-world-one",
      command
    });
    expect(claimed[0]?.claimToken).not.toBe(initial.claimToken);
    await expect(stateStore.claimPendingCandidatePublications({
      maxItems: 1,
      minAgeSeconds: 0
    })).resolves.toEqual([]);
  });

  it("claims exactly one PostgreSQL replay row even when a caller requests a batch", async () => {
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

  it("preserves a committed completion when the response is ambiguous", async () => {
    const envelope = createMinimalFetchEnvelope();
    const delivery = createMinimalFetchDelivery({ envelope });
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      clock,
      handler: () => ({ status: "ok" }),
      idempotencyStore: {
        claim: stateStore.claim.bind(stateStore),
        markCompleted: async (idempotencyKey, completion) => {
          await stateStore.markCompleted(idempotencyKey, completion);
          throw new Error("simulated response loss after commit");
        },
        markFailed: stateStore.markFailed.bind(stateStore),
        releaseClaim: stateStore.releaseClaim.bind(stateStore)
      }
    });

    await expect(processor(delivery)).resolves.toMatchObject({
      action: "ack",
      reason: "handled"
    });
    await expect(stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "fetch",
      receivedAt: clock.now().toISOString()
    })).resolves.toMatchObject({
      status: "already-completed"
    });
  });

  it("does not release a claim whose successful response was lost", async () => {
    const envelope = createMinimalFetchEnvelope();
    const delivery = createMinimalFetchDelivery({ envelope });
    const processor = createRuntimeMessageProcessor({
      stage: "fetch",
      clock,
      handler: () => ({ status: "ok" }),
      idempotencyStore: {
        claim: async (idempotencyKey, context) => {
          await stateStore.claim(idempotencyKey, context);
          throw new Error("simulated claim response loss");
        },
        markCompleted: stateStore.markCompleted.bind(stateStore),
        markFailed: stateStore.markFailed.bind(stateStore),
        releaseClaim: stateStore.releaseClaim.bind(stateStore)
      }
    });

    await expect(processor(delivery)).resolves.toMatchObject({
      action: "retry",
      reason: "idempotency-claim-error"
    });
    await expect(stateStore.claim(envelope.idempotencyKey, {
      envelope,
      stage: "fetch",
      receivedAt: clock.now().toISOString()
    })).resolves.toMatchObject({
      status: "in-progress"
    });
  });
});

async function migrationReplayEvidence(
  pool: Pool,
  inboxIdempotencyKey: string,
  outboxCandidateId: string
): Promise<Readonly<Record<string, string>>> {
  const result = await pool.query<Readonly<Record<string, string>>>(
    `SELECT
       (SELECT updated_at::text
          FROM worker_uplift_fetcher.inbox
         WHERE idempotency_key = $1) AS inbox_updated_at,
       (SELECT updated_at::text
          FROM worker_uplift_fetcher.outbox
         WHERE entity_id = $2) AS outbox_updated_at,
       (SELECT migrated_at::text
          FROM worker_uplift_fetcher.state_contract
         WHERE component = 'fetcher_state_store') AS contract_migrated_at,
       (SELECT oid::text
          FROM pg_constraint
         WHERE conrelid = 'worker_uplift_fetcher.inbox'::regclass
           AND conname = 'worker_uplift_fetcher_inbox_claim_lease_check') AS inbox_constraint_oid,
       (SELECT oid::text
          FROM pg_constraint
         WHERE conrelid = 'worker_uplift_fetcher.outbox'::regclass
           AND conname = 'worker_uplift_fetcher_outbox_claim_lease_check') AS outbox_constraint_oid`,
    [inboxIdempotencyKey, outboxCandidateId]
  );

  return result.rows[0] ?? {};
}
