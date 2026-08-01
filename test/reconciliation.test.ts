import {
  describe,
  expect,
  it
} from "vitest";

import {
  FETCHER_RECONCILIATION_CONFIRMATION,
  createFetcherFailClosedReconciler,
  createFetcherOutboxReconciler
} from "../src/reconciliation.js";
import { InMemoryFetcherStateStore } from "../src/state-store.js";
import {
  ManualFetcherClock,
  createMinimalCanonicalizationCommand
} from "../src/test-doubles.js";

describe("fetcher reconciliation", () => {
  it("reports a bounded no-op dry-run when no service-owned replay candidates exist", async () => {
    const reconciler = createFetcherFailClosedReconciler(new ManualFetcherClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "fetcher",
      status: "dry_run",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors).toEqual([]);
  });

  it("dry-runs and replays only durable pending outbox commands after protected confirmation", async () => {
    const clock = new ManualFetcherClock();
    const stateStore = new InMemoryFetcherStateStore(clock);
    const command = createMinimalCanonicalizationCommand();
    const published: typeof command[] = [];

    const candidateClaim = await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      command
    });
    if (candidateClaim.status !== "claimed") {
      throw new Error("Expected the test candidate to be claimed.");
    }
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: candidateClaim.claimToken,
      reason: "BrokerPublishError"
    });
    const reconciler = createFetcherOutboxReconciler({
      clock,
      stateStore,
      publish: (candidate) => {
        published.push(candidate);
        return Promise.resolve({
          messageId: candidate.envelope.messageId,
          stage: candidate.envelope.route,
          exchange: "nutsnews.worker",
          routingKey: "worker.canonicalization.v1",
          confirmed: true,
          confirmedAt: clock.now().toISOString()
        });
      }
    });

    const dryRun = await reconciler.reconcile({
      mode: "dry-run",
      maxItems: 1,
      minAgeSeconds: 0
    });

    expect(dryRun).toMatchObject({
      status: "dry_run",
      selectedCount: 1,
      replayedCount: 0,
      writesPerformed: false
    });
    expect(published).toHaveLength(0);

    const denied = await reconciler.reconcile({
      mode: "apply",
      minAgeSeconds: 0
    });

    expect(denied).toMatchObject({
      status: "failed_closed",
      selectedCount: 0,
      writesPerformed: false
    });
    expect(published).toHaveLength(0);

    const applied = await reconciler.reconcile({
      mode: "apply",
      minAgeSeconds: 0,
      protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
    });

    expect(applied).toMatchObject({
      status: "applied",
      selectedCount: 1,
      replayedCount: 1,
      failedClosedCount: 0,
      writesPerformed: true
    });
    expect(published).toEqual([command]);

    const repeated = await reconciler.reconcile({
      mode: "apply",
      minAgeSeconds: 0,
      protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
    });

    expect(repeated).toMatchObject({
      status: "applied",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false
    });
    expect(published).toHaveLength(1);
  });

  it("fails closed without reopening a replay whose committed finalization response is lost", async () => {
    const clock = new ManualFetcherClock();
    const stateStore = new AmbiguousReconciliationFinalizationStateStore(clock, true);
    const command = createMinimalCanonicalizationCommand();
    const initial = await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      command
    });

    if (initial.status !== "claimed") {
      throw new Error("Expected candidate claim.");
    }
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: initial.claimToken,
      reason: "FetcherDefinitePublishError"
    });
    stateStore.publishFailureRecords = 0;
    const published: typeof command[] = [];
    const reconciler = createFetcherOutboxReconciler({
      clock,
      stateStore,
      publish: (candidate) => {
        published.push(candidate);
        return Promise.resolve({
          messageId: candidate.envelope.messageId,
          stage: candidate.envelope.route,
          exchange: "nutsnews.worker",
          routingKey: "worker.canonicalization.v1",
          confirmed: true,
          confirmedAt: clock.now().toISOString()
        });
      }
    });

    await expect(reconciler.reconcile({
      mode: "apply",
      minAgeSeconds: 0,
      protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
    })).resolves.toMatchObject({
      status: "failed_closed",
      selectedCount: 1,
      replayedCount: 0,
      failedClosedCount: 1
    });
    expect(stateStore.publishFailureRecords).toBe(0);

    await expect(reconciler.reconcile({
      mode: "apply",
      minAgeSeconds: 0,
      protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
    })).resolves.toMatchObject({
      status: "applied",
      selectedCount: 0,
      replayedCount: 0
    });
    expect(published).toEqual([command]);
  });

  it("retains an uncommitted finalization lease until expiry and replays the persisted message id", async () => {
    const clock = new ManualFetcherClock();
    const stateStore = new AmbiguousReconciliationFinalizationStateStore(clock, false);
    const command = createMinimalCanonicalizationCommand();
    const initial = await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      command
    });

    if (initial.status !== "claimed") {
      throw new Error("Expected candidate claim.");
    }
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimToken: initial.claimToken,
      reason: "FetcherDefinitePublishError"
    });
    stateStore.publishFailureRecords = 0;
    const publishedMessageIds: string[] = [];
    const reconciler = createFetcherOutboxReconciler({
      clock,
      stateStore,
      publish: (candidate) => {
        publishedMessageIds.push(candidate.envelope.messageId);
        return Promise.resolve({
          messageId: candidate.envelope.messageId,
          stage: candidate.envelope.route,
          exchange: "nutsnews.worker",
          routingKey: "worker.canonicalization.v1",
          confirmed: true,
          confirmedAt: clock.now().toISOString()
        });
      }
    });
    const apply = () => reconciler.reconcile({
      mode: "apply" as const,
      minAgeSeconds: 0,
      protectedConfirmation: FETCHER_RECONCILIATION_CONFIRMATION
    });

    await expect(apply()).resolves.toMatchObject({
      status: "failed_closed",
      selectedCount: 1,
      replayedCount: 0
    });
    await expect(apply()).resolves.toMatchObject({
      status: "applied",
      selectedCount: 0
    });
    expect(stateStore.publishFailureRecords).toBe(0);

    clock.advance(60_000);
    await expect(apply()).resolves.toMatchObject({
      status: "applied",
      selectedCount: 1,
      replayedCount: 1
    });
    expect(publishedMessageIds).toEqual([
      command.envelope.messageId,
      command.envelope.messageId
    ]);
    expect(stateStore.publishFailureRecords).toBe(0);
  });
});

class AmbiguousReconciliationFinalizationStateStore extends InMemoryFetcherStateStore {
  publishFailureRecords = 0;
  private failFinalization = true;

  constructor(clock: ManualFetcherClock, private readonly commitBeforeThrow: boolean) {
    super(clock, 60_000);
  }

  override async markCandidatePublished(
    ...args: Parameters<InMemoryFetcherStateStore["markCandidatePublished"]>
  ): Promise<void> {
    if (!this.failFinalization) {
      return super.markCandidatePublished(...args);
    }

    this.failFinalization = false;

    if (this.commitBeforeThrow) {
      await super.markCandidatePublished(...args);
    }

    throw new Error("simulated ambiguous publication finalization");
  }

  override markCandidatePublishFailed(
    ...args: Parameters<InMemoryFetcherStateStore["markCandidatePublishFailed"]>
  ): ReturnType<InMemoryFetcherStateStore["markCandidatePublishFailed"]> {
    this.publishFailureRecords += 1;
    return super.markCandidatePublishFailed(...args);
  }
}
