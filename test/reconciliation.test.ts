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

    await stateStore.claimCandidate("candidate-world-one", {
      feedId: "feed-world",
      sourceItemId: "guid-001",
      contentFingerprint: "fingerprint-v1",
      firstSeenAt: clock.now().toISOString(),
      claimOwnerKey: command.envelope.messageId,
      command
    });
    await stateStore.markCandidatePublishFailed("candidate-world-one", {
      failedAt: clock.now().toISOString(),
      idempotencyKey: command.envelope.idempotencyKey,
      claimOwnerKey: command.envelope.messageId,
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
});
